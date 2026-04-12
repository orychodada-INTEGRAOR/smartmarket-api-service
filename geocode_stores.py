"""
geocode_stores.py — fill store GPS via Nominatim (2 strategies) + optional Google Geocoding
========================================================================================
Strategy order per store:
  1. Nominatim: full address + city + ישראל
  2. Nominatim: city only + ישראל
  3. Google Geocoding API (GOOGLE_MAPS_KEY in .env)
  4. UPDATE geocode_status='manual'

Nominatim: max ~1 req/s (sleep between requests).

  python geocode_stores.py --limit 100
  python geocode_stores.py --all
  python geocode_stores.py --status
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any
from urllib.parse import quote_plus, urlencode
from urllib.request import Request, urlopen

import psycopg2
from dotenv import load_dotenv

load_dotenv()

NOMINATIM = "https://nominatim.openstreetmap.org/search"
GOOGLE_GEOCODE = "https://maps.googleapis.com/maps/api/geocode/json"
UA = "SmartMarket/1.1 (Israel retail; geocode_stores.py)"


def _conn():
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise SystemExit("DATABASE_URL is not set")
    return psycopg2.connect(url, sslmode="require")


def _ensure_utf8_stdout() -> None:
    if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass


def ensure_geocode_status_column(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            ALTER TABLE stores ADD COLUMN IF NOT EXISTS geocode_status TEXT
            """
        )
    conn.commit()


def nominatim_search(q: str) -> tuple[float, float] | None:
    """Single Nominatim request. Caller sleeps ~1s between Nominatim calls (usage policy)."""
    params = {
        "q": q,
        "format": "json",
        "limit": 1,
        "countrycodes": "il",
        "addressdetails": "0",
    }
    url = f"{NOMINATIM}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urlopen(req, timeout=35) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    data = json.loads(raw)
    if not data or not isinstance(data, list):
        return None
    first = data[0]
    try:
        lat = float(first["lat"])
        lon = float(first["lon"])
    except (KeyError, TypeError, ValueError):
        return None
    return lat, lon


def google_geocode_full_address(address_line: str, api_key: str) -> tuple[float, float] | None:
    if not api_key.strip():
        return None
    params = urlencode(
        {
            "address": address_line,
            "key": api_key.strip(),
            "language": "he",
        },
        quote_via=quote_plus,
    )
    url = f"{GOOGLE_GEOCODE}?{params}"
    req = Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urlopen(req, timeout=25) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    payload = json.loads(raw)
    if payload.get("status") not in ("OK", "ZERO_RESULTS"):
        return None
    results = payload.get("results") or []
    if not results:
        return None
    loc = results[0].get("geometry", {}).get("location") or {}
    try:
        lat = float(loc["lat"])
        lng = float(loc["lng"])
    except (KeyError, TypeError, ValueError):
        return None
    return lat, lng


def build_full_line(address: str, city: str | None, chain_name: str, name: str | None) -> str:
    addr = (address or "").strip()
    c = (city or "").strip()
    if addr and c:
        return f"{addr}, {c}, ישראל"
    if addr:
        return f"{addr}, ישראל"
    if c:
        return f"{c}, ישראל"
    n = (name or "").strip()
    if n and c:
        return f"{chain_name} {n}, {c}, ישראל"
    if n:
        return f"{chain_name} {n}, ישראל"
    return f"{chain_name}, ישראל"


def build_city_only(city: str | None, chain_name: str) -> str:
    c = (city or "").strip()
    if c:
        return f"{c}, ישראל"
    return f"{chain_name}, ישראל"


def geocode_one_store(
    store_id: str,
    chain_name: str,
    name: str | None,
    address: str | None,
    city: str | None,
    google_key: str,
) -> tuple[tuple[float, float] | None, str, int, list[str]]:
    """
    Returns (coords or None, provider_used, attempts, attempt_log_lines).
    provider_used: nominatim_full | nominatim_city | google | manual
    """
    attempts: list[str] = []
    n = 0

    line_full = build_full_line(
        address or "",
        city,
        chain_name,
        name,
    )
    q1 = line_full
    n += 1
    attempts.append(f"nominatim_full:{q1[:120]}")
    try:
        c1 = nominatim_search(q1)
        if c1:
            return (c1[0], c1[1]), "nominatim_full", n, attempts
    except Exception as e:
        attempts.append(f"nominatim_full_error:{e}")

    q2 = build_city_only(city, chain_name)
    if q2 != q1:
        time.sleep(1.0)
        n += 1
        attempts.append(f"nominatim_city:{q2[:120]}")
        try:
            c2 = nominatim_search(q2)
            if c2:
                return (c2[0], c2[1]), "nominatim_city", n, attempts
        except Exception as e:
            attempts.append(f"nominatim_city_error:{e}")

    if google_key.strip():
        time.sleep(1.0)
        n += 1
        attempts.append(f"google:{line_full[:120]}")
        try:
            g = google_geocode_full_address(line_full, google_key)
            if g:
                return (g[0], g[1]), "google", n, attempts
        except Exception as e:
            attempts.append(f"google_error:{e}")

    return None, "manual", n, attempts


def fetch_status(cur) -> dict[str, Any]:
    cur.execute("SELECT COUNT(*) FROM stores")
    total = int(cur.fetchone()[0] or 0)
    cur.execute(
        """
        SELECT COUNT(*) FROM stores
        WHERE lat IS NOT NULL AND lng IS NOT NULL
        """
    )
    with_gps = int(cur.fetchone()[0] or 0)
    cur.execute(
        """
        SELECT COUNT(*) FROM stores
        WHERE (lat IS NULL OR lng IS NULL)
          AND COALESCE(geocode_status, '') <> 'manual'
        """
    )
    pending = int(cur.fetchone()[0] or 0)
    cur.execute(
        """
        SELECT COUNT(*) FROM stores WHERE geocode_status = 'manual'
        """
    )
    manual = int(cur.fetchone()[0] or 0)
    return {
        "total": total,
        "with_gps": with_gps,
        "pending_geocode": pending,
        "manual": manual,
    }


def run_status() -> None:
    _ensure_utf8_stdout()
    conn = _conn()
    ensure_geocode_status_column(conn)
    try:
        cur = conn.cursor()
        s = fetch_status(cur)
        cur.close()
        print(f"סה\"כ חנויות:           {s['total']}")
        print(f"עם GPS (lat+lng):       {s['with_gps']}")
        print(f"ממתינות לגיאוקוד:      {s['pending_geocode']}")
        print(f"סומנו ידני (manual):   {s['manual']}")
    finally:
        conn.close()


def run_geocode(limit: int | None) -> None:
    _ensure_utf8_stdout()
    google_key = os.getenv("GOOGLE_MAPS_KEY", "").strip()

    conn = _conn()
    ensure_geocode_status_column(conn)
    conn.autocommit = False
    cur = None
    try:
        cur = conn.cursor()
        status = fetch_status(cur)
        cur.execute(
            """
            SELECT store_id, chain_name, name, address, city
            FROM stores
            WHERE (lat IS NULL OR lng IS NULL)
              AND COALESCE(geocode_status, '') <> 'manual'
            ORDER BY
              CASE WHEN COALESCE(TRIM(address), '') <> '' THEN 0 ELSE 1 END,
              chain_name, store_id
            """
            + ("" if limit is None else " LIMIT %s"),
            () if limit is None else (limit,),
        )
        rows = cur.fetchall()
        pending_total = len(rows)
        if pending_total == 0:
            print("אין חנויות לעדכון — כולן עם GPS.")
            return

        print(
            f"מתחיל גיאוקוד: {pending_total} חנויות (ממתינות בסך הכל: {status['pending_geocode']})"
        )
        if not google_key:
            print("⚠️ GOOGLE_MAPS_KEY ריק — שלב Google ידולג (הוסף ל-.env).")

        done = 0
        manual_marked = 0

        for i, (store_id, chain_name, name, address, city) in enumerate(rows):
            coords, provider, attempt_count, att_detail = geocode_one_store(
                str(store_id),
                chain_name or "",
                name,
                address,
                city,
                google_key,
            )

            if coords:
                lat, lng = coords
                cur.execute(
                    """
                    UPDATE stores
                    SET lat = %s, lng = %s, geocode_status = 'ok', updated_at = NOW()
                    WHERE store_id = %s AND chain_name = %s
                    """,
                    (lat, lng, store_id, chain_name),
                )
                conn.commit()
                done += 1
                st = "ok"
            else:
                cur.execute(
                    """
                    UPDATE stores
                    SET geocode_status = 'manual', updated_at = NOW()
                    WHERE store_id = %s AND chain_name = %s
                    """,
                    (store_id, chain_name),
                )
                conn.commit()
                manual_marked += 1
                st = "manual"

            log_line = (
                f"store_id={store_id} chain={chain_name!r} "
                f"provider_used={provider} status={st} attempts={attempt_count}"
            )
            print(log_line, flush=True)
            for ad in att_detail[-5:]:
                print(f"    {ad}", flush=True)

            cur.execute("SELECT COUNT(*) FROM stores WHERE lat IS NOT NULL AND lng IS NOT NULL")
            with_gps = int(cur.fetchone()[0] or 0)
            cur.execute("SELECT COUNT(*) FROM stores")
            total = int(cur.fetchone()[0] or 0)
            print(
                f"{with_gps}/{total} חנויות עם GPS  ({i + 1}/{pending_total} בטיפול)\n",
                flush=True,
            )

            if i < pending_total - 1:
                time.sleep(1.0)

        print(
            f"סיום: קואורדינטות חדשות {done}, סומנו manual {manual_marked}, "
            f"בקטע {pending_total} חנויות."
        )
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Geocode SmartMarket stores")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--status", action="store_true", help="Show DB counts only")
    g.add_argument("--all", action="store_true", help="Geocode all pending stores")
    g.add_argument("--limit", type=int, metavar="N", help="Geocode at most N pending stores")
    args = parser.parse_args()

    try:
        if args.status:
            run_status()
        elif args.all:
            run_geocode(limit=None)
        else:
            if args.limit is not None and args.limit < 1:
                print("--limit must be >= 1")
                return 2
            run_geocode(limit=args.limit)
        return 0
    except KeyboardInterrupt:
        print("\nהופסק על ידי המשתמש")
        return 130
    except Exception as e:
        print(f"שגיאה: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
