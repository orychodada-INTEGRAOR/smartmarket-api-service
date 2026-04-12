"""
category_search_engine.py — search & ingest products by retail category
====================================================================
Uses chain definitions from factory/chains_all.json and existing building blocks:
  - gov_xml  → OpenFormat government XML (openformat_scraper)
  - woocommerce / shopify / api → discovery_engine HTTP scrapers
  - playwright → discovery_engine minimal Playwright listing scrape

Writes into `products` with category_l1 / category_l2 (columns added if missing).
Does not modify playwright_*.py, html_scraper.py, compare_api_final.py, MASTER_RUN.py.

  python category_search_engine.py --category food
  python category_search_engine.py --all
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from typing import Any

import httpx
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_batch
from urllib.parse import urljoin, urlparse

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
LOG = logging.getLogger("category_search")

_FACTORY = os.path.dirname(os.path.abspath(__file__))
_INTEGRATOR = os.path.abspath(os.path.join(_FACTORY, ".."))
if _INTEGRATOR not in sys.path:
    sys.path.insert(0, _INTEGRATOR)
CHAINS_PATH = os.path.join(_FACTORY, "chains_all.json")

USER_AGENT = "SmartMarket-CategoryEngine/1.0"
HTTP_TIMEOUT = 45.0

VALID_CATEGORIES = (
    "food",
    "fashion",
    "home",
    "pharma",
    "baby",
    "electronics",
    "stock",
    "pets",
)


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


def ensure_products_category_columns(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DO $$ BEGIN
                ALTER TABLE products ADD COLUMN category_l1 TEXT;
            EXCEPTION WHEN duplicate_column THEN NULL; END $$;
            """
        )
        cur.execute(
            """
            DO $$ BEGIN
                ALTER TABLE products ADD COLUMN category_l2 TEXT;
            EXCEPTION WHEN duplicate_column THEN NULL; END $$;
            """
        )
    conn.commit()


def _norm_chain(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "", s)
    return s


def chain_matches(product_chain: str, allowed: set[str]) -> bool:
    pn = _norm_chain(product_chain)
    if not pn:
        return False
    for a in allowed:
        an = _norm_chain(a)
        if not an:
            continue
        if pn == an or an in pn or pn in an:
            return True
    return False


def load_chains() -> list[dict[str, Any]]:
    if not os.path.isfile(CHAINS_PATH):
        raise FileNotFoundError(f"Missing {CHAINS_PATH}")
    with open(CHAINS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    chains = data.get("chains") or []
    return [c for c in chains if isinstance(c, dict)]


def chains_for_category(all_chains: list[dict[str, Any]], category: str) -> list[dict[str, Any]]:
    return [c for c in all_chains if (c.get("type") or "").strip().lower() == category]


def _barcode_synthetic(chain_name: str, name: str, product_url: str | None) -> str:
    h = abs(hash(f"{chain_name}\0{name}\0{product_url or ''}"))
    return str(h)[:13]


def save_products_batch(conn, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_batch(
            cur,
            """
            INSERT INTO products
              (barcode, name, price, brand, chain_name, domain, category_l1, category_l2,
               image_url, external_id, is_active, updated_at)
            VALUES
              (%(barcode)s, %(name)s, %(price)s, %(brand)s, %(chain_name)s, %(domain)s,
               %(category_l1)s, %(category_l2)s, %(image_url)s, %(external_id)s, %(is_active)s, NOW())
            ON CONFLICT (barcode, chain_name) DO UPDATE SET
              name        = EXCLUDED.name,
              price       = EXCLUDED.price,
              brand       = EXCLUDED.brand,
              domain      = EXCLUDED.domain,
              category_l1 = EXCLUDED.category_l1,
              category_l2 = EXCLUDED.category_l2,
              image_url   = EXCLUDED.image_url,
              external_id = EXCLUDED.external_id,
              is_active   = EXCLUDED.is_active,
              updated_at  = NOW()
            """,
            rows,
            page_size=400,
        )
    conn.commit()
    return len(rows)


def ingest_openformat_for_chains(
    conn,
    category_key: str,
    chain_labels: set[str],
    limit: int | None,
) -> int:
    """Download OpenFormat XML feeds and keep rows whose chain matches `chain_labels`."""
    import factory.openformat_scraper as of

    urls = of.discover_file_urls()
    if not urls:
        LOG.warning("OpenFormat: no XML links discovered")
        return 0

    total_saved = 0
    matched = 0
    for url in urls:
        if limit is not None and matched >= limit:
            break
        LOG.info("OpenFormat file: %s", url)
        content = of.download_content(url)
        if not content:
            continue
        remaining = None if limit is None else max(limit - matched, 0)
        parsed = of.parse_xml(content, source_url=url, limit=remaining)
        batch: list[dict[str, Any]] = []
        for p in parsed:
            cn = (p.get("chain_name") or "").strip()
            if not chain_matches(cn, chain_labels):
                continue
            batch.append(
                {
                    "barcode": p["barcode"],
                    "name": p["name"],
                    "price": p["price"],
                    "brand": (p.get("brand") or "")[:100] or None,
                    "chain_name": cn[:200],
                    "domain": category_key,
                    "category_l1": category_key,
                    "category_l2": (p.get("brand") or cn)[:200] or None,
                    "image_url": p.get("image_url"),
                    "external_id": p.get("external_id"),
                    "is_active": True,
                }
            )
        if batch:
            total_saved += save_products_batch(conn, batch)
            matched += len(batch)
    return total_saved


def rows_from_woocommerce(
    base: str,
    chain_name: str,
    category_key: str,
    client: httpx.Client,
) -> list[dict[str, Any]]:
    # Same HTTP shape as discovery_engine.scrape_woocommerce (writes to products here).
    rows: list[dict[str, Any]] = []
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
            name = (p.get("name") or "").strip()
            purl = p.get("permalink")
            cat0 = (p.get("categories") or [{}])[0].get("name") if p.get("categories") else None
            bc = str(p.get("sku") or "").strip() or _barcode_synthetic(chain_name, name, purl)
            rows.append(
                {
                    "barcode": bc[:50],
                    "name": name[:500],
                    "price": price,
                    "brand": None,
                    "chain_name": chain_name[:200],
                    "domain": category_key,
                    "category_l1": category_key,
                    "category_l2": (cat0 or "")[:200] or None,
                    "image_url": img,
                    "external_id": str(p.get("id")) if p.get("id") is not None else None,
                    "is_active": True,
                }
            )
        if len(data) < 100:
            break
        page += 1
        if page > 200:
            break
    return rows


def run_discovery_style_chain(
    entry: dict[str, Any],
    category_key: str,
    client: httpx.Client,
    conn,
) -> int:
    """Classify site and scrape with the same engines as discovery_engine (without raw_products)."""
    from factory.discovery_engine import Detection, detect_site_type, discover_working_base

    hint = (entry.get("url") or "").strip()
    chain = (entry.get("name") or "").strip() or "unknown"
    if not hint or hint.lower() == "null":
        LOG.warning("No URL for %s", chain)
        return 0

    base = discover_working_base(chain, hint, client)
    if not base:
        LOG.error("No working base URL for %s", chain)
        return 0

    time.sleep(1.0)
    det: Detection = detect_site_type(base, client)
    kind = det.kind
    LOG.info("%s → %s → engine=%s", chain, urlparse(base).netloc, kind)

    if kind == "gov_xml":
        LOG.info(
            "gov_xml site %s — chain-specific XML is handled via OpenFormat batch for this category.",
            base,
        )
        return 0

    n = 0
    if kind == "woocommerce":
        rows = rows_from_woocommerce(base, chain, category_key, client)
        n = save_products_batch(conn, rows)
    elif kind == "shopify":
        # scrape_shopify uses _save_products internally — duplicate one pass
        page = 1
        all_rows: list[dict[str, Any]] = []
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
            for p in products:
                try:
                    cents = p.get("variants", [{}])[0].get("price")
                    price = float(cents) if cents is not None else None
                except Exception:
                    price = None
                img = p.get("image", {}).get("src") if isinstance(p.get("image"), dict) else p.get("image")
                handle = p.get("handle")
                purl = f"{base.rstrip('/')}/products/{handle}" if handle else None
                title = (p.get("title") or "").strip()
                all_rows.append(
                    {
                        "barcode": _barcode_synthetic(chain, title, purl)[:50],
                        "name": title[:500],
                        "price": price,
                        "brand": None,
                        "chain_name": chain[:200],
                        "domain": category_key,
                        "category_l1": category_key,
                        "category_l2": ((p.get("product_type") or "")[:200]) or None,
                        "image_url": img,
                        "external_id": str(p.get("id")) if p.get("id") is not None else None,
                        "is_active": True,
                    }
                )
            if len(products) < 250:
                break
            page += 1
            if page > 200:
                break
        n = save_products_batch(conn, all_rows)
    elif kind == "api" and det.api_hint:
        # scrape_api_hint writes raw_products — inline minimal save
        time.sleep(1.0)
        try:
            r = client.get(det.api_hint, follow_redirects=True)
            data = r.json() if r.status_code == 200 else None
        except Exception:
            data = None
        rows_out: list[dict[str, Any]] = []
        if data:
            items = data if isinstance(data, list) else data.get("data") or data.get("items") or data.get("products") or []
            if isinstance(items, dict):
                items = [items]
            if isinstance(items, list):
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
                    purl = p.get("url") or p.get("link")
                    nm = (str(name or ""))[:500]
                    rows_out.append(
                        {
                            "barcode": _barcode_synthetic(chain, nm, str(purl))[:50],
                            "name": nm,
                            "price": price,
                            "brand": None,
                            "chain_name": chain[:200],
                            "domain": category_key,
                            "category_l1": category_key,
                            "category_l2": (str(p.get("category") or "")[:200]) or None,
                            "image_url": p.get("image") or p.get("image_url"),
                            "external_id": None,
                            "is_active": True,
                        }
                    )
        n = save_products_batch(conn, rows_out)
    else:
        # Playwright listing — discovery_engine saves raw_products; use internal scrape then map
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            LOG.warning("playwright not installed; skip %s", chain)
            return 0
        raw_rows: list[dict[str, Any]] = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.goto(base + "/", wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(2000)
                anchors = page.query_selector_all("a[href*='/product'], a[href*='/products/']")

                seen: set[str] = set()
                for a in anchors[:150]:
                    try:
                        href = a.get_attribute("href")
                        if not href or href in seen:
                            continue
                        seen.add(href)
                        name = (a.inner_text() or "").strip()[:500]
                        if len(name) < 2:
                            continue
                        purl = urljoin(base + "/", href)
                        raw_rows.append(
                            {
                                "barcode": _barcode_synthetic(chain, name, purl)[:50],
                                "name": name,
                                "price": None,
                                "brand": None,
                                "chain_name": chain[:200],
                                "domain": category_key,
                                "category_l1": category_key,
                                "category_l2": None,
                                "image_url": None,
                                "external_id": None,
                                "is_active": True,
                            }
                        )
                    except Exception:
                        continue
            finally:
                browser.close()
        n = save_products_batch(conn, raw_rows)

    return n


def run_category(category: str, conn, limit: int | None) -> None:
    category = category.strip().lower()
    if category not in VALID_CATEGORIES:
        raise SystemExit(f"Unknown category {category!r}. Expected one of {VALID_CATEGORIES}")

    all_chains = load_chains()
    subset = chains_for_category(all_chains, category)
    if not subset:
        LOG.warning("No chains with type=%r in chains_all.json", category)
        return

    gov_labels = {c["name"] for c in subset if (c.get("engine") or "") == "gov_xml"}
    if gov_labels:
        LOG.info("OpenFormat pass for %d gov_xml chain name(s) in category %s", len(gov_labels), category)
        n = ingest_openformat_for_chains(conn, category, gov_labels, limit)
        LOG.info("OpenFormat → saved ~%s product rows (matched chains)", n)

    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/json,*/*"}
    with httpx.Client(timeout=HTTP_TIMEOUT, headers=headers, verify=True) as client:
        for entry in subset:
            eng = (entry.get("engine") or "").lower()
            if eng == "gov_xml":
                continue
            if not entry.get("url"):
                continue
            try:
                saved = run_discovery_style_chain(entry, category, client, conn)
                LOG.info("%s: saved %s products (non-gov)", entry.get("name"), saved)
            except Exception as e:
                LOG.exception("chain failed %s: %s", entry.get("name"), e)


def main() -> int:
    _ensure_utf8_stdout()
    ap = argparse.ArgumentParser(description="Category-based product search & DB ingest")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--category", type=str, help=f"One of: {', '.join(VALID_CATEGORIES)}")
    g.add_argument("--all", action="store_true", help="Run every category that has chains")
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max products from OpenFormat parse (per run), for testing",
    )
    args = ap.parse_args()

    try:
        conn = get_conn()
        ensure_products_category_columns(conn)
    except Exception as e:
        LOG.error("DB: %s", e)
        return 1

    try:
        if args.all:
            seen_types: set[str] = set()
            for c in load_chains():
                t = (c.get("type") or "").strip().lower()
                if t in VALID_CATEGORIES:
                    seen_types.add(t)
            for cat in sorted(seen_types):
                LOG.info("========== category: %s ==========", cat)
                run_category(cat, conn, args.limit)
        else:
            run_category(args.category, conn, args.limit)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
