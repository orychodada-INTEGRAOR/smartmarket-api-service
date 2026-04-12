"""
scout_engine.py — Google scout for Israeli retail sites (WooCommerce / Shopify candidates)
=========================================================================================
Searches Google with fixed queries, dedupes against chains_all.json + NEW_SITES_QUEUE + DB,
classifies with discovery_engine.detect_site_type, queues WC/Shopify into pending_approval.

Uses requests + BeautifulSoup for Google; httpx for site classification (detect_site_type).

Does not modify playwright_*.py, html_scraper.py, fetch_all_stores.py, compare_api_final.py,
MASTER_RUN.py, STATUS.py.

  python factory/scout_engine.py --category food
  python factory/scout_engine.py --category fashion
  python factory/scout_engine.py --all
  python factory/scout_engine.py --query "מכולת אונליין ישראל"
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, unquote, urlencode, urlparse, urlunparse

import httpx
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
LOG = logging.getLogger("scout")

_FACTORY = os.path.dirname(os.path.abspath(__file__))
_INTEGRATOR = os.path.abspath(os.path.join(_FACTORY, ".."))
if _INTEGRATOR not in sys.path:
    sys.path.insert(0, _INTEGRATOR)

CHAINS_PATH = os.path.join(_FACTORY, "chains_all.json")
QUEUE_PATH = os.path.join(_INTEGRATOR, "NEW_SITES_QUEUE.json")
REPORT_PATH = os.path.join(_INTEGRATOR, "scout_report.json")

REQUEST_GAP_SEC = 3.0
GOOGLE_SEARCH = "https://www.google.com/search"
NUM_RESULTS = 15

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/122.0.0.0",
]

QUERIES_FOOD = [
    'site:*.co.il "wp-json/wc/v3" "מזון"',
    '"woocommerce" "סופרמרקט" OR "מכולת" site:co.il',
    '"קנייה אונליין" "הוסף לסל" site:co.il',
    '"חנות מזון" "משלוחים" "woocommerce" ישראל',
]

QUERIES_FASHION = [
    'site:*.co.il "wp-json/wc/v3" "אופנה"',
    '"woocommerce" "בגדים" OR "הלבשה" site:co.il',
    '"קנייה אונליין" "woocommerce" "אופנה" site:co.il',
]

BLOCKED_HOST_SUBSTR = (
    "google.",
    "googleusercontent.",
    "gstatic.",
    "youtube.",
    "facebook.",
    "fb.com",
    "instagram.",
    "linkedin.",
    "wikipedia.",
    "twitter.",
    "x.com",
    "pinterest.",
    "tiktok.",
)


def _ensure_utf8_stdout() -> None:
    if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass


def _throttle(last_ts: list[float]) -> None:
    """Enforce ~3s between outbound requests."""
    now = time.monotonic()
    if last_ts[0] > 0:
        wait = REQUEST_GAP_SEC - (now - last_ts[0])
        if wait > 0:
            time.sleep(wait)
    last_ts[0] = time.monotonic()


def normalize_domain(url_or_host: str) -> str:
    if not url_or_host or str(url_or_host).lower() in ("null", "none"):
        return ""
    s = (url_or_host or "").strip()
    if not s.startswith(("http://", "https://")):
        s = "https://" + s
    p = urlparse(s)
    host = (p.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def to_base_https(url: str) -> str:
    p = urlparse(url if url.startswith(("http://", "https://")) else "https://" + url)
    if not p.netloc:
        return ""
    return f"https://{p.netloc}".rstrip("/")


def is_blocked_host(host: str) -> bool:
    h = host.lower()
    return any(b in h for b in BLOCKED_HOST_SUBSTR)


def is_israeli_retail_host(host: str) -> bool:
    """Loose filter: Israeli ccTLD / common retail TLDs in scope."""
    if not host:
        return False
    h = host.lower()
    if h.endswith(".co.il") or h.endswith(".org.il") or h.endswith(".net.il") or h.endswith(".gov.il"):
        return True
    if h.endswith(".il"):
        return True
    return False


def extract_real_url_from_google_href(href: str) -> str | None:
    if not href or href.startswith("#") or href.startswith("javascript:"):
        return None
    # /url?q=https://...
    if "/url" in href and "q=" in href:
        try:
            q = urlparse(href)
            qs = parse_qs(q.query)
            if "q" in qs and qs["q"]:
                return unquote(qs["q"][0])
            if "url" in qs and qs["url"]:
                return unquote(qs["url"][0])
        except Exception:
            return None
    if href.startswith("http://") or href.startswith("https://"):
        if "google.com/url" in href:
            try:
                q = urlparse(href)
                qs = parse_qs(q.query)
                if "q" in qs and qs["q"]:
                    return unquote(qs["q"][0])
            except Exception:
                return None
        return href
    return None


def parse_google_result_urls(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()
    out: list[str] = []
    for a in soup.find_all("a", href=True):
        raw = a.get("href") or ""
        real = extract_real_url_from_google_href(raw)
        if not real or not real.startswith("http"):
            continue
        p = urlparse(real)
        if not p.netloc or is_blocked_host(p.netloc):
            continue
        if not is_israeli_retail_host(p.netloc):
            continue
        base = urlunparse((p.scheme, p.netloc, "", "", "", ""))
        if base not in seen:
            seen.add(base)
            out.append(base)
    return out


def fetch_google_html(query: str, last_ts: list[float]) -> str | None:
    _throttle(last_ts)
    url = GOOGLE_SEARCH + "?" + urlencode({"q": query, "num": NUM_RESULTS, "hl": "he", "gl": "il"})
    headers = {"User-Agent": random.choice(USER_AGENTS), "Accept-Language": "he-IL,he;q=0.9,en;q=0.8"}
    try:
        r = requests.get(url, headers=headers, timeout=35)
        if r.status_code != 200:
            LOG.warning("Google HTTP %s for query snippet: %s", r.status_code, query[:40])
            return None
        return r.text
    except Exception as e:
        LOG.warning("Google request failed: %s", e)
        return None


def load_chains_domains() -> set[str]:
    out: set[str] = set()
    if not os.path.isfile(CHAINS_PATH):
        return out
    try:
        with open(CHAINS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        for c in data.get("chains") or []:
            u = c.get("url")
            if u and isinstance(u, str) and u.strip():
                d = normalize_domain(u)
                if d:
                    out.add(d)
    except Exception as e:
        LOG.warning("chains_all.json: %s", e)
    return out


def load_queue_domains() -> set[str]:
    out: set[str] = set()
    if not os.path.isfile(QUEUE_PATH):
        return out
    try:
        with open(QUEUE_PATH, "r", encoding="utf-8") as f:
            q = json.load(f)
        for bucket in ("pending_approval", "approved", "rejected"):
            for e in q.get(bucket) or []:
                if not isinstance(e, dict):
                    continue
                u = e.get("url")
                if u and isinstance(u, str):
                    d = normalize_domain(u)
                    if d:
                        out.add(d)
    except Exception as e:
        LOG.warning("NEW_SITES_QUEUE: %s", e)
    return out


def load_db_domains() -> set[str]:
    out: set[str] = set()
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        return out
    try:
        import psycopg2

        conn = psycopg2.connect(db_url, sslmode="require")
        try:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        SELECT DISTINCT regexp_replace(
                          split_part(split_part(source_base::text, '://', 2), '/', 1),
                          '^www\\.', ''
                        )
                        FROM raw_products
                        WHERE source_base IS NOT NULL AND trim(source_base::text) <> ''
                        """
                    )
                    for row in cur.fetchall():
                        if row[0]:
                            out.add(str(row[0]).lower().strip())
                except Exception:
                    pass
        finally:
            conn.close()
    except Exception as e:
        LOG.debug("DB domain load skipped: %s", e)
    return out


def build_known_domains() -> set[str]:
    k = set()
    k |= load_chains_domains()
    k |= load_queue_domains()
    k |= load_db_domains()
    return k


def load_queue_file() -> dict[str, Any]:
    if not os.path.isfile(QUEUE_PATH):
        return {"pending_approval": [], "approved": [], "rejected": []}
    with open(QUEUE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_queue_file(data: dict[str, Any]) -> None:
    with open(QUEUE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def pending_has_domain(queue: dict[str, Any], domain: str) -> bool:
    d = normalize_domain(domain)
    for e in queue.get("pending_approval") or []:
        if not isinstance(e, dict):
            continue
        u = e.get("url")
        if u and normalize_domain(u) == d:
            return True
    return False


def append_pending(
    queue: dict[str, Any],
    base_url: str,
    category: str,
    engine: str,
) -> bool:
    domain = normalize_domain(base_url)
    if pending_has_domain(queue, domain):
        return False
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    entry = {
        "chain_name": domain,
        "url": base_url.rstrip("/") + "/",
        "category": category,
        "status": "pending",
        "discovered": today,
        "source": "scout_engine",
        "engine_hint": engine,
    }
    queue.setdefault("pending_approval", []).append(entry)
    return True


def run_scout(
    queries: list[str],
    category: str,
    last_ts: list[float],
    known: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    from factory.discovery_engine import detect_site_type

    found_new: list[dict[str, Any]] = []
    already_known: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []

    seen_domains: set[str] = set()
    queue_mut = load_queue_file()
    headers_probe = {"User-Agent": random.choice(USER_AGENTS), "Accept": "text/html,application/json,*/*"}

    with httpx.Client(timeout=45.0, headers=headers_probe, verify=True, follow_redirects=True) as client:
        for q in queries:
            html = fetch_google_html(q, last_ts)
            if not html:
                failed.append({"query": q, "error": "google_fetch_empty_or_failed"})
                continue
            urls = parse_google_result_urls(html)
            if not urls:
                failed.append({"query": q, "error": "no_result_urls_parsed"})
                continue

            for base in urls:
                dom = normalize_domain(base)
                if not dom or dom in seen_domains:
                    continue
                seen_domains.add(dom)

                if dom in known:
                    already_known.append({"domain": dom, "url": base, "reason": "chains_queue_or_db"})
                    continue

                base_https = to_base_https(base)
                if not base_https:
                    failed.append({"url": base, "error": "invalid_base"})
                    continue

                _throttle(last_ts)
                try:
                    det = detect_site_type(base_https, client)
                    kind = det.kind
                except Exception as e:
                    failed.append({"url": base_https, "error": str(e)[:500]})
                    continue

                if kind in ("woocommerce", "shopify"):
                    queued = append_pending(queue_mut, base_https, category, kind)
                    if queued:
                        save_queue_file(queue_mut)
                        known.add(dom)
                        found_new.append(
                            {
                                "domain": dom,
                                "url": base_https,
                                "engine": kind,
                                "queued": True,
                                "notes": "; ".join(det.notes or []),
                            }
                        )
                        LOG.info("נמצא: %s → %s → ממתין לאישור", dom, kind)
                    else:
                        found_new.append(
                            {
                                "domain": dom,
                                "url": base_https,
                                "engine": kind,
                                "queued": False,
                                "notes": "already_pending",
                            }
                        )
                else:
                    found_new.append(
                        {
                            "domain": dom,
                            "url": base_https,
                            "engine": kind,
                            "queued": False,
                            "notes": "; ".join(det.notes or []),
                        }
                    )

    return found_new, already_known, failed


def main() -> int:
    _ensure_utf8_stdout()
    ap = argparse.ArgumentParser(description="Google scout for new Israeli retail sites")
    ap.add_argument("--category", choices=("food", "fashion"), default=None, help="Preset query set")
    ap.add_argument("--all", action="store_true", help="Run food + fashion query sets")
    ap.add_argument(
        "--query",
        type=str,
        default=None,
        help='Custom Google query (default queue label: food unless --category is set)',
    )
    args = ap.parse_args()

    if args.query:
        if args.all:
            LOG.error("Use either --query or --all, not both")
            return 2
        queries = [args.query]
        cat = args.category or "food"
        if not args.category:
            LOG.info("Queue label %r (pass --category fashion to override)", cat)
    elif args.all:
        if args.category:
            LOG.error("Use either --all or --category, not both")
            return 2
        queries = []
        cat = "mixed"
    elif args.category == "food":
        queries = list(QUERIES_FOOD)
        cat = "food"
    elif args.category == "fashion":
        queries = list(QUERIES_FASHION)
        cat = "fashion"
    else:
        ap.error("Specify one of: --category food|fashion, --all, or --query ...")

    last_ts = [0.0]
    known = build_known_domains()
    LOG.info("Known domains loaded: %d", len(known))

    if args.all:
        fn, ak, fl = run_scout(list(QUERIES_FOOD), "food", last_ts, known)
        fn2, ak2, fl2 = run_scout(list(QUERIES_FASHION), "fashion", last_ts, known)
        found_new = fn + fn2
        already_known = ak + ak2
        failed = fl + fl2
        queries = list(QUERIES_FOOD) + list(QUERIES_FASHION)
    else:
        found_new, already_known, failed = run_scout(queries, cat, last_ts, known)

    report = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "category": cat,
        "queries": queries,
        "found_new": found_new,
        "already_known": already_known,
        "failed": failed,
    }
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    LOG.info(
        "Report: %s | new=%d already=%d failed=%d",
        REPORT_PATH,
        len(found_new),
        len(already_known),
        len(failed),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
