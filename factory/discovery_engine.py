"""
discovery_engine.py — discover site type and ingest products into raw_products
============================================================================
Reads approved sites from integrator/NEW_SITES_QUEUE.json (`approved` array).
Each entry should include `url`. Optional `chain_name` helps `--chain` matching
for Hebrew-only labels (e.g. 'קסטרו').

  python discovery_engine.py --scan
  python discovery_engine.py --chain "קסטרו"
  python discovery_engine.py --status

Rate limit: ~1 request/second per target site (time.sleep between steps).
Does not modify playwright_*.py scrapers, MASTER_RUN.py, or compare_api_final.py.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import Json, execute_batch

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
LOG = logging.getLogger("discovery")

INTEGRATOR_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
QUEUE_PATH = os.path.join(INTEGRATOR_DIR, "NEW_SITES_QUEUE.json")
STATE_PATH = os.path.join(INTEGRATOR_DIR, ".discovery_engine_state.json")

USER_AGENT = "SmartMarket-Discovery/1.0 (research; respectful crawling)"
HTTP_TIMEOUT = 45.0


def _ensure_utf8_stdout() -> None:
    if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass


def get_conn():
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is missing")
    return psycopg2.connect(url, sslmode="require")


def ensure_raw_products_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_products (
                id BIGSERIAL PRIMARY KEY,
                chain_name TEXT NOT NULL,
                dedupe_key TEXT NOT NULL,
                discovery_type TEXT,
                source_base TEXT,
                product_name TEXT,
                price NUMERIC(14, 4),
                image_url TEXT,
                category TEXT,
                product_url TEXT,
                raw_payload JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS raw_products_dedupe_key_uidx
            ON raw_products (dedupe_key)
            """
        )
    conn.commit()


def _dedupe_key(chain_name: str, name: str | None, product_url: str | None) -> str:
    s = f"{chain_name}\0{name or ''}\0{product_url or ''}"
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def load_queue() -> dict[str, Any]:
    if not os.path.isfile(QUEUE_PATH):
        return {"pending_approval": [], "approved": [], "rejected": []}
    with open(QUEUE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state_entry(entry: dict[str, Any]) -> None:
    state: dict[str, Any] = {"runs": []}
    if os.path.isfile(STATE_PATH):
        try:
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                state = json.load(f)
        except Exception:
            pass
    if "runs" not in state or not isinstance(state["runs"], list):
        state["runs"] = []
    state["runs"].append(entry)
    state["runs"] = state["runs"][-200:]
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _latin_slug(name: str) -> str:
    """Extract a crude latin slug from chain label (e.g. 'Rami Levy' -> ramilevy)."""
    parts = re.findall(r"[a-zA-Z0-9]+", name)
    if not parts:
        return ""
    return "".join(parts).lower()


def candidate_urls_from_chain_name(chain_name: str) -> list[str]:
    """Common domain patterns (STEP 1)."""
    slug = _latin_slug(chain_name)
    out: list[str] = []
    if len(slug) >= 2:
        for tld in ("co.il", "com", "org.il"):
            out.append(f"https://www.{slug}.{tld}")
            out.append(f"https://{slug}.{tld}")
    return out


def variant_urls(seed: str) -> list[str]:
    """Try apex / www / scheme variants."""
    seed = seed.strip()
    if not seed.startswith(("http://", "https://")):
        seed = "https://" + seed
    p = urlparse(seed)
    if not p.netloc:
        return [seed]
    host = p.netloc
    scheme = p.scheme or "https"
    paths = [f"{scheme}://{host}"]
    if host.startswith("www."):
        paths.append(f"{scheme}://{host[4:]}")
    else:
        paths.append(f"{scheme}://www.{host}")
    # alternate TLD guess: x.com <-> x.co.il
    m = re.match(r"^(www\.)?(.+)$", host)
    if m:
        core = m.group(2)
        if core.endswith(".com"):
            alt = core[: -len(".com")] + ".co.il"
            paths.append(f"{scheme}://{alt}")
            paths.append(f"{scheme}://www.{alt}")
        elif core.endswith(".co.il"):
            alt = core[: -len(".co.il")] + ".com"
            paths.append(f"{scheme}://{alt}")
            paths.append(f"{scheme}://www.{alt}")
    seen: set[str] = set()
    uniq: list[str] = []
    for u in paths:
        if u not in seen:
            seen.add(u)
            uniq.append(u.rstrip("/"))
    return uniq


def discover_working_base(chain_name: str, hint_url: str | None, client: httpx.Client) -> str | None:
    """STEP 1: return first base URL that responds with HTTP 200."""
    candidates: list[str] = []
    if hint_url:
        candidates.extend(variant_urls(hint_url))
    candidates.extend(candidate_urls_from_chain_name(chain_name))
    seen: set[str] = set()
    ordered: list[str] = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            ordered.append(c)
    for i, base in enumerate(ordered):
        try:
            r = client.get(base + "/", follow_redirects=True)
            if r.status_code == 200:
                final = str(r.url)
                p = urlparse(final)
                return f"{p.scheme}://{p.netloc}".rstrip("/")
        except Exception as e:
            LOG.debug("probe %s: %s", base, e)
        if i < len(ordered) - 1:
            time.sleep(1.0)
    return None


@dataclass
class Detection:
    kind: str = "playwright"
    api_hint: str | None = None
    notes: list[str] = field(default_factory=list)


def detect_site_type(base: str, client: httpx.Client) -> Detection:
    """STEP 2: classify site."""
    d = Detection()
    time.sleep(1.0)
    try:
        home = client.get(base + "/", follow_redirects=True)
        text = home.text if home.status_code == 200 else ""
    except Exception as e:
        LOG.warning("homepage fetch failed: %s", e)
        text = ""

    low = text.lower()

    if "cdn.shopify.com" in low or (".myshopify.com" in low) or ("shopify.com/s/files" in low):
        d.kind = "shopify"
        d.notes.append("shopify assets")
        return d

    if re.search(r"\.gz[\"']|\.xml[\"']", text) and (
        "openformat" in low or "economy.gov.il" in low or "price" in low and "file" in low
    ):
        d.kind = "gov_xml"
        d.notes.append("gz/xml gov pattern")
        return d
    if re.search(r'href=["\'][^"\']+\.(?:gz|xml)(?:\?[^"\']*)?["\']', text, re.I):
        d.kind = "gov_xml"
        d.notes.append("xml/gz links in HTML")
        return d

    wc_url = base.rstrip("/") + "/wp-json/wc/v3/products?per_page=1"
    time.sleep(1.0)
    try:
        wc = client.get(wc_url, follow_redirects=True)
        ct = wc.headers.get("content-type", "")
        if wc.status_code in (200, 401) and "json" in ct.lower():
            d.kind = "woocommerce"
            d.notes.append("wc v3 endpoint")
            return d
        if wc.status_code == 200:
            try:
                data = wc.json()
                if isinstance(data, list):
                    d.kind = "woocommerce"
                    d.notes.append("wc json list")
                    return d
            except Exception:
                pass
    except Exception as e:
        LOG.debug("wc probe: %s", e)

    if "wp-content" in low or "wp-includes" in low:
        d.kind = "woocommerce"
        d.notes.append("wp-content in HTML")
        return d

    api_patterns = [
        r'["\'](https?://[^"\']+/api/v?\d*/[^"\']+)["\']',
        r'["\'](https?://[^"\']+/wp-json/[^"\']+)["\']',
    ]
    for pat in api_patterns:
        m = re.search(pat, text[:800_000], re.I)
        if m:
            d.kind = "api"
            d.api_hint = m.group(1)
            d.notes.append("API URL in HTML")
            return d

    d.kind = "playwright"
    d.notes.append("fallback")
    return d


def _save_products(
    conn,
    chain_name: str,
    discovery_type: str,
    source_base: str,
    rows: list[dict[str, Any]],
) -> int:
    if not rows:
        return 0
    deduped: dict[str, dict[str, Any]] = {}
    for r in rows:
        pname = (r.get("name") or "")[:2000]
        purl = (r.get("product_url") or "")[:2000]
        dk = _dedupe_key(chain_name, pname, purl)
        deduped[dk] = r
    rows = list(deduped.values())
    batch = []
    for r in rows:
        pname = (r.get("name") or "")[:2000]
        purl = (r.get("product_url") or "")[:2000]
        batch.append(
            {
                "chain_name": chain_name,
                "dedupe_key": _dedupe_key(chain_name, pname, purl),
                "discovery_type": discovery_type,
                "source_base": source_base,
                "product_name": pname,
                "price": r.get("price"),
                "image_url": r.get("image_url"),
                "category": (r.get("category") or "")[:500] if r.get("category") else None,
                "product_url": purl or None,
                "raw_payload": Json(r.get("raw") or {}),
            }
        )
    with conn.cursor() as cur:
        execute_batch(
            cur,
            """
            INSERT INTO raw_products
              (chain_name, dedupe_key, discovery_type, source_base, product_name, price, image_url, category, product_url, raw_payload)
            VALUES
              (%(chain_name)s, %(dedupe_key)s, %(discovery_type)s, %(source_base)s, %(product_name)s, %(price)s, %(image_url)s, %(category)s, %(product_url)s, %(raw_payload)s)
            ON CONFLICT (dedupe_key) DO NOTHING
            """,
            batch,
            page_size=200,
        )
    conn.commit()
    return len(batch)


def scrape_woocommerce(base: str, chain_name: str, conn, client: httpx.Client) -> int:
    total_saved = 0
    page = 1
    while True:
        url = f"{base.rstrip('/')}/wp-json/wc/v3/products?per_page=100&page={page}"
        time.sleep(1.0)
        try:
            r = client.get(url, follow_redirects=True)
            if r.status_code != 200:
                break
            data = r.json()
        except Exception as e:
            LOG.warning("woocommerce page %s: %s", page, e)
            break
        if not isinstance(data, list) or len(data) == 0:
            break
        rows: list[dict[str, Any]] = []
        for p in data:
            try:
                price = p.get("price")
                if price is not None:
                    price = float(str(price).replace(",", ""))
                else:
                    price = None
            except Exception:
                price = None
            img = None
            if p.get("images") and isinstance(p["images"], list) and p["images"]:
                img = p["images"][0].get("src")
            rows.append(
                {
                    "name": p.get("name"),
                    "price": price,
                    "image_url": img,
                    "category": (p.get("categories") or [{}])[0].get("name") if p.get("categories") else None,
                    "product_url": p.get("permalink"),
                    "raw": {"id": p.get("id"), "sku": p.get("sku")},
                }
            )
        total_saved += _save_products(conn, chain_name, "woocommerce", base, rows)
        if len(data) < 100:
            break
        page += 1
        if page > 500:
            break
    return total_saved


def scrape_shopify(base: str, chain_name: str, conn, client: httpx.Client) -> int:
    total_saved = 0
    page = 1
    while True:
        url = f"{base.rstrip('/')}/products.json?page={page}&limit=250"
        time.sleep(1.0)
        try:
            r = client.get(url, follow_redirects=True)
            if r.status_code != 200:
                break
            payload = r.json()
        except Exception as e:
            LOG.warning("shopify page %s: %s", page, e)
            break
        products = payload.get("products") if isinstance(payload, dict) else None
        if not products:
            break
        rows: list[dict[str, Any]] = []
        for p in products:
            try:
                cents = p.get("variants", [{}])[0].get("price")
                price = float(cents) if cents is not None else None
            except Exception:
                price = None
            img = p.get("image", {}).get("src") if isinstance(p.get("image"), dict) else p.get("image")
            rows.append(
                {
                    "name": p.get("title"),
                    "price": price,
                    "image_url": img,
                    "category": (p.get("product_type") or "")[:500] or None,
                    "product_url": f"{base.rstrip('/')}/products/{p.get('handle')}" if p.get("handle") else None,
                    "raw": {"id": p.get("id"), "handle": p.get("handle")},
                }
            )
        total_saved += _save_products(conn, chain_name, "shopify", base, rows)
        if len(products) < 250:
            break
        page += 1
        if page > 500:
            break
    return total_saved


def scrape_api_hint(base: str, hint: str, chain_name: str, conn, client: httpx.Client) -> int:
    time.sleep(1.0)
    try:
        r = client.get(hint, follow_redirects=True)
        if r.status_code != 200:
            return 0
        data = r.json()
    except Exception:
        return 0
    rows: list[dict[str, Any]] = []
    items = data if isinstance(data, list) else data.get("data") or data.get("items") or data.get("products") or []
    if isinstance(items, dict):
        items = [items]
    if not isinstance(items, list):
        return 0
    for p in items[:500]:
        if not isinstance(p, dict):
            continue
        name = p.get("name") or p.get("title") or p.get("description")
        price = p.get("price") or p.get("sale_price")
        try:
            if price is not None:
                price = float(str(price).replace(",", ""))
        except Exception:
            price = None
        rows.append(
            {
                "name": name,
                "price": price,
                "image_url": p.get("image") or p.get("image_url"),
                "category": str(p.get("category") or "")[:500] or None,
                "product_url": p.get("url") or p.get("link"),
                "raw": p,
            }
        )
    return _save_products(conn, chain_name, "api", base, rows)


def scrape_playwright_internal(base: str, chain_name: str, conn) -> int:
    """Minimal Playwright listing scrape (no separate playwright_*.py module)."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        LOG.warning("playwright not installed; skip fallback scrape")
        return 0
    rows: list[dict[str, Any]] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.goto(base + "/", wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(2000)
            anchors = page.query_selector_all("a[href*='/product'], a[href*='/products/']")
            seen: set[str] = set()
            for a in anchors[:120]:
                try:
                    href = a.get_attribute("href")
                    if not href or href in seen:
                        continue
                    seen.add(href)
                    name = (a.inner_text() or "").strip()[:500]
                    if len(name) < 2:
                        continue
                    rows.append(
                        {
                            "name": name,
                            "price": None,
                            "image_url": None,
                            "category": None,
                            "product_url": urljoin(base + "/", href),
                            "raw": {"source": "playwright_dom"},
                        }
                    )
                except Exception:
                    continue
        finally:
            browser.close()
    return _save_products(conn, chain_name, "playwright", base, rows)


def chain_label(entry: dict[str, Any], fallback: str) -> str:
    return (entry.get("chain_name") or entry.get("category") or fallback).strip() or fallback


def approved_entries_for_chain(queue: dict[str, Any], chain_filter: str | None) -> list[dict[str, Any]]:
    approved = [x for x in (queue.get("approved") or []) if isinstance(x, dict)]
    if not chain_filter:
        return approved
    cf = chain_filter.strip()
    out: list[dict[str, Any]] = []
    for e in approved:
        label = chain_label(e, "")
        url = (e.get("url") or "").strip()
        if label == cf or cf in label or cf in url:
            out.append(e)
    return out


def process_entry(entry: dict[str, Any], client: httpx.Client, conn) -> tuple[str, str, str, int]:
    """Returns (chain_label, domain, kind, count)."""
    hint = (entry.get("url") or "").strip()
    chain = chain_label(entry, urlparse(hint).netloc or "unknown")
    base = discover_working_base(chain, hint or None, client)
    if not base:
        LOG.error("No working URL for %s", chain)
        return chain, "", "none", 0
    host = urlparse(base).netloc or base
    time.sleep(1.0)
    det = detect_site_type(base, client)
    kind = det.kind
    n = 0
    if kind == "gov_xml":
        LOG.info(
            "gov_xml detected for %s — use existing openformat / published-prices pipelines; skipping product fetch here.",
            base,
        )
        save_state_entry(
            {
                "chain": chain,
                "domain": host,
                "type": kind,
                "products": 0,
                "at": datetime.now(timezone.utc).isoformat(),
                "note": "delegated_to_existing_engines",
            }
        )
        return chain, host, kind, 0
    if kind == "woocommerce":
        n = scrape_woocommerce(base, chain, conn, client)
    elif kind == "shopify":
        n = scrape_shopify(base, chain, conn, client)
    elif kind == "api" and det.api_hint:
        n = scrape_api_hint(base, det.api_hint, chain, conn, client)
    else:
        n = scrape_playwright_internal(base, chain, conn)
    save_state_entry(
        {
            "chain": chain,
            "domain": host,
            "type": kind,
            "products": n,
            "at": datetime.now(timezone.utc).isoformat(),
        }
    )
    return chain, host, kind, n


def run_status(conn) -> None:
    ensure_raw_products_table(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT discovery_type, COUNT(*) AS n,
                   COUNT(DISTINCT chain_name) AS chains
            FROM raw_products
            GROUP BY discovery_type
            ORDER BY n DESC
            """
        )
        rows = cur.fetchall()
        cur.execute("SELECT COUNT(*), COUNT(DISTINCT chain_name) FROM raw_products")
        total, uchains = cur.fetchone()
    print(f"raw_products: {total or 0} rows, {uchains or 0} distinct chains")
    for r in rows:
        print(f"  {r[0] or '?'}: {r[1]} rows ({r[2]} chains)")
    if os.path.isfile(STATE_PATH):
        try:
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                st = json.load(f)
            runs = st.get("runs") or []
            print("\nLast 8 discovery runs:")
            for run in runs[-8:]:
                print(
                    f"  {run.get('chain')} → {run.get('domain')} → {run.get('type')} → {run.get('products')} products"
                )
        except Exception as e:
            print("(could not read state file)", e)


def main() -> int:
    _ensure_utf8_stdout()
    parser = argparse.ArgumentParser(description="SmartMarket discovery engine")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--scan", action="store_true", help="Process all approved queue entries")
    g.add_argument("--chain", type=str, metavar="NAME", help="Process one chain (matches queue entry)")
    g.add_argument("--status", action="store_true", help="Show raw_products + recent runs")
    args = parser.parse_args()

    try:
        conn = get_conn()
        ensure_raw_products_table(conn)
    except Exception as e:
        LOG.error("DB: %s", e)
        return 1

    if args.status:
        try:
            run_status(conn)
        finally:
            conn.close()
        return 0

    queue = load_queue()
    entries = approved_entries_for_chain(queue, args.chain if args.chain else None)
    if not entries:
        LOG.error("No matching approved entries in NEW_SITES_QUEUE.json")
        conn.close()
        return 2

    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/json,*/*"}
    with httpx.Client(timeout=HTTP_TIMEOUT, headers=headers, verify=True) as client:
        for entry in entries:
            try:
                chain, host, kind, n = process_entry(entry, client, conn)
                print(f"{chain} → {host} → {kind} → {n:,} products")
            except Exception as e:
                LOG.exception("failed entry %s: %s", entry, e)
                print(f"{entry.get('url', '?')} → error → {e}")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
