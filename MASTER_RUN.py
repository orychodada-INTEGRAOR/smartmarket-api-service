"""
MASTER_RUN.py — SmartMarket full scrape orchestrator
====================================================
Runs scrapers in order, streams output, logs to file, prints DB stats.

  python MASTER_RUN.py           # full run
  python MASTER_RUN.py --status  # DB stats only
  python MASTER_RUN.py --quick   # fast scrapers only

Windows: SETUP_SCHEDULER.bat registers SmartMarket_Daily (02:00) and SmartMarket_6H (hourly /mo 6).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None  # type: ignore
    RealDictCursor = None  # type: ignore

ROOT = Path(__file__).resolve().parent
STATE_PATH = ROOT / ".master_run_state.json"
LOG_DIR = ROOT / "logs"
PYTHON = sys.executable

# (label, relpath, extra_args, quick_mode_include)
SCRAPERS_FULL: list[tuple[str, str, list[str], bool]] = [
    ("fetch_all_stores", "fetch_all_stores.py", [], True),
    ("publishedprices (13 chains)", "playwright_publishedprices.py", [], False),
    ("shufersal", "playwright_shufersal.py", [], False),
    ("bina (10 chains)", "playwright_bina.py", [], False),
    ("laib (victory +)", "playwright_laib.py", [], False),
    ("html (hazi-hinam etc)", "html_scraper.py", [], True),
    ("baby/kids", str(Path("factory") / "playwright_baby.py"), [], True),
    ("openformat gov", str(Path("factory") / "openformat_scraper.py"), [], True),
]


def _load_state() -> dict[str, Any]:
    if STATE_PATH.is_file():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"run_count": 0}


def _save_state(data: dict[str, Any]) -> None:
    try:
        STATE_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception:
        pass


def _connect():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is required (pip install psycopg2-binary)")
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(url, sslmode="require")


def fetch_db_stats() -> dict[str, Any]:
    """Aggregate stats for dashboard and final report."""
    out: dict[str, Any] = {
        "products_total": 0,
        "stores_total": 0,
        "chains_distinct": 0,
        "by_bucket": {"food": 0, "baby": 0, "electronics": 0},
        "domains_raw": [],
        "last_product_update": None,
    }
    try:
        conn = _connect()
    except Exception as e:
        out["error"] = str(e)
        return out

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS c FROM products")
            out["products_total"] = int(cur.fetchone()["c"] or 0)

            cur.execute("SELECT COUNT(*) AS c FROM stores")
            out["stores_total"] = int(cur.fetchone()["c"] or 0)

            cur.execute("SELECT COUNT(DISTINCT chain_name) AS c FROM products WHERE chain_name IS NOT NULL")
            out["chains_distinct"] = int(cur.fetchone()["c"] or 0)

            cur.execute(
                """
                SELECT domain, COUNT(*) AS n
                FROM products
                GROUP BY domain
                ORDER BY n DESC
                """
            )
            rows = cur.fetchall()
            out["domains_raw"] = [(r["domain"], int(r["n"])) for r in rows]

            food = baby = elec = 0
            for domain, n in out["domains_raw"]:
                d = (domain or "").strip().lower()
                if d == "baby":
                    baby += n
                elif d in ("electronics", "electronic"):
                    elec += n
                else:
                    food += n
            out["by_bucket"] = {"food": food, "baby": baby, "electronics": elec}

            cur.execute("SELECT MAX(updated_at) AS m FROM products")
            row = cur.fetchone()
            out["last_product_update"] = row["m"] if row else None
    finally:
        conn.close()

    return out


def _fmt_int(n: int) -> str:
    return f"{n:,}"


def _pct(part: int, total: int) -> str:
    if total <= 0:
        return "0%"
    return f"{100.0 * part / total:.0f}%"


def print_db_stats_banner(title: str = "DB snapshot") -> None:
    s = fetch_db_stats()
    ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print(f"\n{'─' * 45}", flush=True)
    print(f"  {title}  ({ts})", flush=True)
    if s.get("error"):
        print(f"  ⚠️ DB: {s['error']}", flush=True)
        print(f"{'─' * 45}\n", flush=True)
        return
    total = s["products_total"]
    b = s["by_bucket"]
    print(f"  📦 מוצרים: {_fmt_int(total)}", flush=True)
    print(f"  🏪 חנויות:  {_fmt_int(s['stores_total'])}", flush=True)
    print(f"  🔗 רשתות (מוצרים): {_fmt_int(s['chains_distinct'])}", flush=True)
    if total:
        print(
            f"     מזון≈ {_fmt_int(b['food'])} ({_pct(b['food'], total)}) | "
            f"תינוקות {_fmt_int(b['baby'])} ({_pct(b['baby'], total)}) | "
            f"אלקטרוניקה {_fmt_int(b['electronics'])} ({_pct(b['electronics'], total)})",
            flush=True,
        )
    print(f"{'─' * 45}\n", flush=True)


def _run_one_scraper(
    label: str,
    script_rel: str,
    extra: list[str],
    log_fp,
) -> tuple[bool, str | None]:
    script_path = ROOT / script_rel
    if not script_path.is_file():
        msg = f"missing script: {script_path}"
        print(f"  ⚠️ {msg}", flush=True)
        log_fp.write(f"[SKIP] {msg}\n")
        return False, msg

    cmd = [PYTHON, str(script_path), *extra]
    log_fp.write(f"\n{'═' * 60}\n>>> {' '.join(cmd)}\n{'═' * 60}\n")
    print(f"\n▶▶ {label} — {' '.join(cmd)}", flush=True)

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip("\n")
            print(f"  │ {line}", flush=True)
            log_fp.write(line + "\n")
        code = proc.wait()
        if code != 0:
            msg = f"exit code {code}"
            print(f"  ❌ {label}: {msg}", flush=True)
            log_fp.write(f"[FAIL] {msg}\n")
            return False, msg
        print(f"  ✅ {label} הושלם", flush=True)
        return True, None
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        log_fp.write(traceback.format_exc() + "\n")
        print(f"  ❌ {label}: {msg}", flush=True)
        return False, msg


def run_master(quick: bool) -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"master_run_{stamp}.log"

    state = _load_state()
    state["run_count"] = int(state.get("run_count", 0)) + 1
    _save_state(state)
    run_no = state["run_count"]

    if quick:
        scrapers = []
        for lab, path, extra, inc in SCRAPERS_FULL:
            if not inc:
                continue
            ex = list(extra)
            if "openformat_scraper.py" in path.replace("\\", "/"):
                ex.extend(["--limit", "10000"])
            scrapers.append((lab, path, ex))
    else:
        scrapers = [(s[0], s[1], list(s[2])) for s in SCRAPERS_FULL]

    started = datetime.now()
    ok = 0
    failures: list[str] = []

    with open(log_path, "w", encoding="utf-8") as log_fp:
        log_fp.write(f"SmartMarket MASTER_RUN start {stamp} quick={quick} run=#{run_no}\n")
        print(f"\n{'═' * 45}", flush=True)
        print(f"  SmartMarket — MASTER_RUN  (#{run_no})", flush=True)
        print(f"  מצב: {'quick' if quick else 'מלא'} | לוג: {log_path}", flush=True)
        print(f"{'═' * 45}\n", flush=True)

        print_db_stats_banner("לפני ריצה")

        for label, rel, extra in scrapers:
            try:
                success, err = _run_one_scraper(label, rel, extra, log_fp)
                if success:
                    ok += 1
                elif err:
                    failures.append(f"{label}: {err}")
            except Exception:
                log_fp.write(traceback.format_exc() + "\n")
                failures.append(f"{label}: unexpected error")
                print(f"  ❌ {label}: unexpected error (see log)", flush=True)

            try:
                print_db_stats_banner(f"אחרי — {label}")
            except Exception as e:
                print(f"  ⚠️ stats after {label}: {e}", flush=True)
                log_fp.write(f"stats error: {e}\n")

        try:
            from promotions_engine import run_promotions

            print("\n▶▶ promotions_engine (PromoFull.gz)", flush=True)
            log_fp.write("\n=== promotions_engine ===\n")
            run_promotions(log_fp=log_fp)
            try:
                print_db_stats_banner("אחרי — promotions_engine")
            except Exception as ex:
                print(f"  ⚠️ stats after promotions_engine: {ex}", flush=True)
                log_fp.write(f"stats error: {ex}\n")
        except Exception as e:
            log_fp.write(f"promotions_engine: {e}\n")
            print(f"  ⚠️ promotions_engine: {e}", flush=True)

        elapsed = datetime.now() - started
        hours = elapsed.total_seconds() / 3600.0

        s = fetch_db_stats()
        total_p = s.get("products_total", 0)
        stores_n = s.get("stores_total", 0)
        chains_n = s.get("chains_distinct", 0)
        b = s.get("by_bucket") or {"food": 0, "baby": 0, "electronics": 0}
        date_s = datetime.now().strftime("%d/%m/%Y")

        n_scrapers = len(scrapers)
        report_lines = [
            "",
            "═" * 39,
            f"  SmartMarket — Final Report",
            f"  {date_s} | Run #{run_no}",
            "═" * 39,
            f"  📦 מוצרים סה\"כ:      {_fmt_int(total_p)}",
            f"  🏪 חנויות:               {_fmt_int(stores_n)}",
            f"  🔗 רשתות:                 {_fmt_int(chains_n)}",
            "",
            f"  מזון:         {_fmt_int(b['food'])} ({_pct(b['food'], total_p)})",
            f"  תינוקות:            {_fmt_int(b['baby'])} ({_pct(b['baby'], total_p)})",
            f"  אלקטרוניקה:       {_fmt_int(b['electronics'])} ({_pct(b['electronics'], total_p)})",
            "",
            f"  זמן ריצה: {hours:.1f} שעות",
            f"  הצליח: {ok}/{n_scrapers} סורקים",
            "═" * 39,
            "",
        ]
        block = "\n".join(report_lines)
        print(block, flush=True)
        log_fp.write(block + "\n")
        if failures:
            log_fp.write("Failures:\n" + "\n".join(failures) + "\n")

    return 0 if ok == n_scrapers else 1


def main() -> int:
    if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
    parser = argparse.ArgumentParser(description="SmartMarket master scrape runner")
    parser.add_argument("--status", action="store_true", help="Show DB stats only")
    parser.add_argument("--quick", action="store_true", help="Run only fast scrapers")
    args = parser.parse_args()

    try:
        if args.status:
            print_db_stats_banner("SmartMarket — DB status")
            return 0
        return run_master(quick=args.quick)
    except KeyboardInterrupt:
        print("\n⚠️ stopped by user", flush=True)
        return 130
    except Exception as e:
        print(f"⚠️ MASTER_RUN outer error: {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
