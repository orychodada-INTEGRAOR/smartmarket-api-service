"""
STATUS.py — SmartMarket quick health + DB snapshot (~seconds)

  python STATUS.py
"""

from __future__ import annotations

import os
import sys
import traceback
from datetime import datetime
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

from dotenv import load_dotenv

load_dotenv()

try:
    import psycopg2
except ImportError:
    psycopg2 = None  # type: ignore


DEFAULT_API = os.getenv("COMPARE_API_URL", "http://127.0.0.1:8001").rstrip("/")


def _connect():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is required")
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(url, sslmode="require")


def _fmt_int(n: int) -> str:
    return f"{n:,}"


def _api_running(base: str, timeout: float = 2.0) -> bool:
    try:
        with urlopen(f"{base}/health", timeout=timeout) as resp:
            return 200 <= getattr(resp, "status", 200) < 300
    except (URLError, OSError):
        return False


def _last_scrape_en(last: datetime | None) -> str:
    if last is None:
        return "unknown"
    now = datetime.now()
    ref = last.replace(tzinfo=None) if getattr(last, "tzinfo", None) else last
    sec = max(0, int((now - ref).total_seconds()))
    if sec < 60:
        return "just now"
    if sec < 3600:
        m = sec // 60
        return f"{m} minutes ago"
    if sec < 86400:
        h = sec // 3600
        return f"{h} hours ago"
    d = sec // 86400
    return f"{d} days ago"


def _next_scrape_en() -> str:
    """Daily scrape assumed 02:00 (matches SETUP_SCHEDULER.bat)."""
    now = datetime.now()
    target = now.replace(hour=2, minute=0, second=0, microsecond=0)
    if now < target:
        return "Tonight 02:00"
    return "Tomorrow 02:00"


def fetch_counts() -> dict[str, Any]:
    out: dict[str, Any] = {
        "products": 0,
        "stores": 0,
        "last_scrape": None,
        "db_error": None,
    }
    try:
        conn = _connect()
    except Exception as e:
        out["db_error"] = str(e)
        return out
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM products")
        out["products"] = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT COUNT(*) FROM stores")
        out["stores"] = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT MAX(updated_at) FROM products")
        row = cur.fetchone()
        out["last_scrape"] = row[0] if row else None
        cur.close()
    finally:
        conn.close()
    return out


def main() -> int:
    try:
        if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
            try:
                sys.stdout.reconfigure(encoding="utf-8")
            except Exception:
                pass
        ts = datetime.now().strftime("%d/%m/%Y %H:%M")
        print("")
        print("═" * 39)
        print("  SmartMarket — System Status")
        print(f"  {ts}")
        print("═" * 39)

        db_url = os.getenv("DATABASE_URL", "")
        is_supabase = "supabase.co" in db_url.lower()

        counts = fetch_counts()
        if counts.get("db_error"):
            print(f"  DB: ❌ {counts['db_error']}")
            print("  API: ⏭️ (skipped)")
            print("")
            print("═" * 39)
            print("")
            return 1

        label = "Connected (Supabase)" if is_supabase else "Connected"
        print(f"  DB: ✅ {label}")

        api_ok = _api_running(DEFAULT_API)
        if api_ok:
            print("  API: ✅ Running (:8001)")
        else:
            print("  API: ❌ Not reachable (:8001)")

        print("")
        print(f"  📦 Products:    {_fmt_int(counts['products'])}")
        print(f"  🏪 Stores:          {_fmt_int(counts['stores'])}")
        print("")
        print(f"  Last scrape: {_last_scrape_en(counts.get('last_scrape'))}")
        print(f"  Next scrape: {_next_scrape_en()}")
        print("")
        print("═" * 39)
        print("")
        return 0
    except Exception:
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
