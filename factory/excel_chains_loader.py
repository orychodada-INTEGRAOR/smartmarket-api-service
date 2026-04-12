"""
excel_chains_loader.py — load retail chains from Excel → queue + discovery report
==================================================================================
  python factory/excel_chains_loader.py --file "chains.xlsx"
  python factory/excel_chains_loader.py --file "chains.xlsx" --dry-run

Requires: pandas, openpyxl
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

import httpx
import pandas as pd
from urllib.parse import urlparse

_INTEGRATOR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _INTEGRATOR not in sys.path:
    sys.path.insert(0, _INTEGRATOR)

from factory.discovery_engine import (  # noqa: E402
    USER_AGENT,
    detect_site_type,
    discover_working_base,
    load_queue,
)

QUEUE_PATH = os.path.join(_INTEGRATOR, "NEW_SITES_QUEUE.json")
DISCOVERED_PATH = os.path.join(_INTEGRATOR, "chains_discovered.json")


def urlparse_domain(url: str) -> str:
    p = urlparse(url)
    return p.netloc or url

NAME_KEYS = ("chain_name", "name", "רשת", "שם רשת", "chain", "שם")
TYPE_KEYS = ("chain_type", "type", "category", "סוג", "תחום", "קטגוריה")
URL_KEYS = ("website_url", "url", "website", "אתר", "site", "דומיין")


def _ensure_utf8_stdout() -> None:
    if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass


def _norm_col(c: Any) -> str:
    return str(c).strip().lower().replace("\ufeff", "")


def _pick_column(df: pd.DataFrame, keys: tuple[str, ...]) -> str | None:
    cmap = {_norm_col(c): c for c in df.columns}
    for k in keys:
        lk = k.lower()
        if lk in cmap:
            return cmap[lk]
        for nc, orig in cmap.items():
            if lk in nc or nc in lk:
                return orig
    return None


def _cell_str(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    return "" if s.lower() == "nan" else s


def find_domain(chain_name: str, hint_url: str | None = None) -> tuple[str | None, str]:
    """
    Resolve a working storefront base URL and coarse engine label.
    Uses discovery_engine.discover_working_base + detect_site_type (no separate find_domain in module).
    """
    chain_name = (chain_name or "").strip()
    if not chain_name:
        return None, ""
    hint = (hint_url or "").strip() or None
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/json,*/*"}
    with httpx.Client(timeout=httpx.Timeout(45.0), headers=headers, follow_redirects=True) as client:
        base = discover_working_base(chain_name, hint, client)
        if not base:
            return None, ""
        det = detect_site_type(base, client)
        return base, det.kind


def _approved_entry(
    chain_name: str,
    chain_type: str,
    url: str,
) -> dict[str, Any]:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return {
        "url": url if url.startswith("http") else f"https://{url}",
        "category": chain_type or "unknown",
        "chain_name": chain_name,
        "discovered": today,
        "status": "approved",
        "approved_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "source": "excel_chains_loader",
    }


def _merge_approved(queue: dict[str, Any], entry: dict[str, Any]) -> bool:
    approved = queue.setdefault("approved", [])
    key = (entry.get("chain_name") or "", entry.get("url") or "")
    for a in approved:
        if (a.get("chain_name") or "", a.get("url") or "") == key:
            return False
    approved.append(entry)
    return True


def _save_queue(queue: dict[str, Any], dry_run: bool) -> None:
    if dry_run:
        return
    with open(QUEUE_PATH, "w", encoding="utf-8") as f:
        json.dump(queue, f, ensure_ascii=False, indent=2)


def _save_discovered(payload: dict[str, Any], dry_run: bool) -> None:
    if dry_run:
        return
    with open(DISCOVERED_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def run(path: str, dry_run: bool) -> int:
    _ensure_utf8_stdout()
    if not os.path.isfile(path):
        print(f"קובץ לא נמצא: {path}")
        return 2

    df = pd.read_excel(path, engine="openpyxl")
    if df.empty:
        print("גיליון ריק.")
        return 2

    name_col = _pick_column(df, NAME_KEYS)
    type_col = _pick_column(df, TYPE_KEYS)
    url_col = _pick_column(df, URL_KEYS)

    if not name_col:
        print("לא נמצאה עמודת שם רשת (צפוי: chain_name / שם רשת / name וכו').")
        return 2

    queue = load_queue()

    found: list[dict[str, Any]] = []
    manual: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for idx, row in df.iterrows():
        chain_name = _cell_str(row.get(name_col))
        chain_type = _cell_str(row[type_col]) if type_col else ""
        website = _cell_str(row[url_col]) if url_col else ""

        if not chain_name:
            skipped.append({"row": int(idx) + 2, "name": "", "reason": "שורה ללא שם"})
            continue

        if website:
            url = website if website.startswith(("http://", "https://")) else f"https://{website}"
            found.append(
                {"name": chain_name, "domain": urlparse_domain(url), "engine": "from_sheet"}
            )
            entry = _approved_entry(chain_name, chain_type or "unknown", url)
            if not _merge_approved(queue, entry):
                skipped.append({"name": chain_name, "reason": "כבר קיים בתור מאושר"})
            continue

        base, engine = find_domain(chain_name, None)
        if base:
            found.append({"name": chain_name, "domain": urlparse_domain(base), "engine": engine})
            entry = _approved_entry(chain_name, chain_type or "unknown", base)
            if not _merge_approved(queue, entry):
                skipped.append({"name": chain_name, "reason": "כבר קיים בתור מאושר"})
        else:
            manual.append({"name": chain_name, "reason": "לא נמצא אתר"})

    out: dict[str, Any] = {
        "found": found,
        "manual": manual,
        "skipped": skipped,
    }
    _save_queue(queue, dry_run)
    _save_discovered(out, dry_run)

    print("")
    print("═" * 44)
    print("  סיכום — excel_chains_loader")
    print("═" * 44)
    print(f"  נמצאו (אתר + מנוע):     {len(found)}")
    print(f"  ידני / לא נמצא:         {len(manual)}")
    print(f"  דולגו:                  {len(skipped)}")
    if dry_run:
        print("  (dry-run — לא נשמרו קבצים)")
    else:
        print(f"  NEW_SITES_QUEUE:        {QUEUE_PATH}")
        print(f"  chains_discovered.json: {DISCOVERED_PATH}")
    print("═" * 44)
    if manual:
        print("  דורשים טיפול ידני:")
        for m in manual[:25]:
            print(f"    • {m.get('name')}: {m.get('reason')}")
        if len(manual) > 25:
            print(f"    ... ועוד {len(manual) - 25}")
    print("")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Load chain Excel → NEW_SITES_QUEUE + chains_discovered.json")
    parser.add_argument("--file", "-f", type=str, default=None, help="Path to .xlsx")
    parser.add_argument("--dry-run", action="store_true", help="Do not write JSON files")
    args = parser.parse_args()

    path = args.file
    if not path:
        _ensure_utf8_stdout()
        path = input("נתיב לקובץ Excel (.xlsx): ").strip().strip('"').strip("'")
    path = os.path.abspath(os.path.expanduser(path))

    try:
        return run(path, args.dry_run)
    except KeyboardInterrupt:
        print("\nהופסק")
        return 130
    except Exception as e:
        print(f"שגיאה: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
