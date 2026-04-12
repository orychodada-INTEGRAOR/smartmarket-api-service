"""
excel_store_search_engine.py — ingest from food-chain Excel URLs
================================================================
Reads `אתרי רשתת המזון.xlsx` (or the single .xlsx in this folder), then per chain:
  - discover store XML / WooCommerce / listing patterns
  - upsert stores (fill missing addresses where possible)
  - optional WooCommerce products + coupon-based promotions

Does not modify playwright_*.py, html_scraper.py, fetch_all_stores.py, compare_api_final.py,
MASTER_RUN.py, STATUS.py.

  python excel_store_search_engine.py --chain "שופרסל"
  python excel_store_search_engine.py --all
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
LOG = logging.getLogger("excel_store_engine")

_FACTORY = os.path.dirname(os.path.abspath(__file__))
_INTEGRATOR = os.path.abspath(os.path.join(_FACTORY, ".."))
if _INTEGRATOR not in sys.path:
    sys.path.insert(0, _INTEGRATOR)

USER_AGENT = "SmartMarket-ExcelStoreEngine/1.0"
HTTP_TIMEOUT = 45.0

DEFAULT_XLSX_NAME = "אתרי רשתת המזון.xlsx"

# excel_chains_loader column hints
NAME_KEYS = ("chain_name", "name", "רשת", "שם רשת", "chain", "שם")
URL_KEYS = ("website_url", "url", "website", "אתר", "site", "דומיין", "כתובת")


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


def resolve_excel_path(explicit: str | None) -> str:
    if explicit:
        p = os.path.abspath(os.path.expanduser(explicit))
        if os.path.isfile(p):
            return p
        raise FileNotFoundError(explicit)
    default = os.path.join(_FACTORY, DEFAULT_XLSX_NAME)
    if os.path.isfile(default):
        return default
    cands = sorted(glob.glob(os.path.join(_FACTORY, "*.xlsx")))
    if len(cands) == 1:
        return cands[0]
    if not cands:
        raise FileNotFoundError(f"No .xlsx in {_FACTORY}")
    raise FileNotFoundError(f"Multiple .xlsx files in {_FACTORY}; pass --excel PATH")


def load_sheet_rows(path: str) -> list[dict[str, str]]:
    df = pd.read_excel(path, engine="openpyxl")
    if df.empty:
        return []
    name_col = _pick_column(df, NAME_KEYS)
    url_col = _pick_column(df, URL_KEYS)
    if not name_col or not url_col:
        raise RuntimeError("Excel must have chain name + URL columns (see excel_chains_loader.py hints).")
    rows: list[dict[str, str]] = []
    for _, r in df.iterrows():
        nm = _cell_str(r.get(name_col))
        u = _cell_str(r.get(url_col))
        if nm and u:
            if not u.startswith(("http://", "https://")):
                u = "https://" + u
            rows.append({"name": nm, "url": u})
    return rows


def _barcode_synthetic(chain_name: str, name: str, product_url: str | None) -> str:
    h = abs(hash(f"{chain_name}\0{name}\0{product_url or ''}"))
    return str(h)[:13]


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


def save_products_food(conn, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_batch(
            cur,
            """
            INSERT INTO products
              (barcode, name, price, brand, chain_name, domain, category_l1, category_l2,
               image_url, is_active, updated_at)
            VALUES
              (%(barcode)s, %(name)s, %(price)s, %(brand)s, %(chain_name)s, %(domain)s,
               %(category_l1)s, %(category_l2)s, %(image_url)s, %(is_active)s, NOW())
            ON CONFLICT (barcode, chain_name) DO UPDATE SET
              name        = EXCLUDED.name,
              price       = EXCLUDED.price,
              domain      = EXCLUDED.domain,
              category_l1 = EXCLUDED.category_l1,
              category_l2 = EXCLUDED.category_l2,
              image_url   = EXCLUDED.image_url,
              is_active   = EXCLUDED.is_active,
              updated_at  = NOW()
            """,
            rows,
            page_size=400,
        )
    conn.commit()
    return len(rows)


def fetch_wc_products(base: str, chain_name: str, client: httpx.Client) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    page = 1
    while True:
        url = f"{base.rstrip('/')}/wp-json/wc/v3/products?per_page=100&page={page}"
        time.sleep(0.8)
        try:
            r = client.get(url, follow_redirects=True)
            if r.status_code != 200:
                break
            data = r.json()
        except Exception as e:
            LOG.debug("WC products %s", e)
            break
        if not isinstance(data, list) or not data:
            break
        for p in data:
            try:
                price = p.get("price")
                price = float(str(price).replace(",", "")) if price is not None else None
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
                    "domain": "food",
                    "category_l1": "food",
                    "category_l2": (cat0 or "")[:200] or None,
                    "image_url": img,
                    "is_active": True,
                }
            )
        if len(data) < 100:
            break
        page += 1
        if page > 150:
            break
    return rows


def fetch_wc_coupon_promotions(
    base: str, chain_name: str, client: httpx.Client
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    page = 1
    loaded_at = datetime.now(timezone.utc)
    while True:
        url = f"{base.rstrip('/')}/wp-json/wc/v3/coupons?per_page=100&page={page}"
        time.sleep(0.8)
        try:
            r = client.get(url, follow_redirects=True)
            if r.status_code != 200:
                break
            data = r.json()
        except Exception:
            break
        if not isinstance(data, list) or not data:
            break
        for c in data:
            if not isinstance(c, dict):
                continue
            cid = c.get("id")
            code = (c.get("code") or "")[:200]
            desc = (c.get("description") or c.get("amount") or "")[:2000]
            rows.append(
                {
                    "chain_name": chain_name[:200],
                    "item_code": f"wc_coupon_{cid}",
                    "item_name": code or f"coupon_{cid}",
                    "promo_description": desc or code,
                    "start_date": date.today(),
                    "end_date": date.today(),
                    "min_qty": None,
                    "discount_rate": None,
                    "source_file": f"woocommerce:{base}",
                    "loaded_at": loaded_at,
                }
            )
        if len(data) < 100:
            break
        page += 1
        if page > 20:
            break
    return rows


def find_xml_links(html: str, base: str) -> list[str]:
    links = set()
    for m in re.finditer(
        r'href=["\']([^"\']+\.(?:xml|gz)(?:\?[^"\']*)?)["\']',
        html,
        re.IGNORECASE,
    ):
        links.add(urljoin(base, m.group(1)))
    for m in re.finditer(r'https?://[^\s"\'<>]+\.(?:xml|gz)\b', html, re.IGNORECASE):
        links.add(m.group(0))
    return list(links)


def bina_download_gz_urls(main_html: str, base: str) -> list[str]:
    out: list[str] = []
    for m in re.finditer(r"Download\(['\"]([^'\"]*Stores?[^'\"]*\.gz)['\"]", main_html, re.IGNORECASE):
        fname = m.group(1)
        api_url = f"{base.rstrip('/')}/Download.aspx?FileNm={fname}"
        out.append(api_url)
    return out


def sync_playwright_main_html(url: str) -> str | None:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(2000)
            return page.content()
        except Exception as e:
            LOG.debug("playwright %s: %s", url, e)
            return None
        finally:
            browser.close()


def resolve_bina_real_url(client: httpx.Client, api_url: str) -> str | None:
    try:
        r = client.get(api_url, follow_redirects=True, timeout=60)
        if r.status_code != 200:
            return None
        try:
            data = r.json()
        except Exception:
            return None
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0].get("SPath")
        if isinstance(data, dict):
            return data.get("SPath")
    except Exception as e:
        LOG.debug("bina json %s: %s", api_url, e)
    return None


def upsert_stores_merge(conn, stores: list[dict[str, Any]]) -> int:
    """Insert new stores; on conflict keep existing non-empty address/city/geo when scrape is empty."""
    if not stores:
        return 0
    with conn.cursor() as cur:
        execute_batch(
            cur,
            """
            INSERT INTO stores
              (store_id, chain_name, name, address, city, zip_code, latitude, longitude, phone, updated_at)
            VALUES
              (%(store_id)s, %(chain_name)s, %(name)s, %(address)s, %(city)s, %(zip_code)s,
               %(latitude)s, %(longitude)s, %(phone)s, NOW())
            ON CONFLICT (store_id, chain_name) DO UPDATE SET
              name = COALESCE(EXCLUDED.name, stores.name),
              address = CASE
                WHEN stores.address IS NOT NULL AND trim(stores.address) <> '' THEN stores.address
                ELSE COALESCE(EXCLUDED.address, stores.address) END,
              city = CASE
                WHEN stores.city IS NOT NULL AND trim(stores.city) <> '' THEN stores.city
                ELSE COALESCE(EXCLUDED.city, stores.city) END,
              zip_code = CASE
                WHEN stores.zip_code IS NOT NULL AND trim(stores.zip_code::text) <> '' THEN stores.zip_code
                ELSE COALESCE(EXCLUDED.zip_code, stores.zip_code) END,
              latitude = COALESCE(stores.latitude, EXCLUDED.latitude),
              longitude = COALESCE(stores.longitude, EXCLUDED.longitude),
              phone = CASE
                WHEN stores.phone IS NOT NULL AND trim(stores.phone) <> '' THEN stores.phone
                ELSE COALESCE(EXCLUDED.phone, stores.phone) END,
              updated_at = NOW()
            """,
            stores,
            page_size=200,
        )
    conn.commit()
    return len(stores)


def ingest_stores_for_chain(
    conn,
    chain_name: str,
    start_url: str,
    client: httpx.Client,
) -> int:
    from fetch_stores import parse_stores_xml

    base = f"{urlparse(start_url).scheme}://{urlparse(start_url).netloc}"
    all_stores: list[dict[str, Any]] = []

    try:
        r = client.get(start_url, follow_redirects=True, timeout=60)
        html = r.text if r.status_code == 200 else ""
    except Exception as e:
        LOG.warning("GET %s failed: %s", start_url, e)
        html = ""

    if html:
        for link in find_xml_links(html, base)[:8]:
            try:
                sr = client.get(link, follow_redirects=True, timeout=120)
                if sr.status_code == 200:
                    parsed = parse_stores_xml(sr.content, chain_name)
                    all_stores.extend(parsed)
            except Exception as e:
                LOG.debug("store xml %s: %s", link, e)

    if "binaprojects.com" in (start_url or ""):
        html2 = html
        if not html2:
            html2 = sync_playwright_main_html(start_url) or ""
        for api_u in bina_download_gz_urls(html2, base)[:3]:
            real = resolve_bina_real_url(client, api_u)
            if not real:
                continue
            try:
                sr = client.get(real, follow_redirects=True, timeout=120)
                if sr.status_code == 200:
                    all_stores.extend(parse_stores_xml(sr.content, chain_name))
            except Exception as e:
                LOG.debug("bina store dl %s: %s", real, e)

    if not all_stores:
        html3 = html or sync_playwright_main_html(start_url) or ""
        for link in find_xml_links(html3, base)[:8]:
            try:
                sr = client.get(link, follow_redirects=True, timeout=120)
                if sr.status_code == 200:
                    all_stores.extend(parse_stores_xml(sr.content, chain_name))
            except Exception:
                continue

    return upsert_stores_merge(conn, all_stores) if all_stores else 0


def save_promotions(conn, batch: list[dict[str, Any]]) -> int:
    if not batch:
        return 0
    from promotions_engine import ensure_promotions_table, upsert_promotions

    ensure_promotions_table(conn)
    return upsert_promotions(conn, batch)


def chain_filter_matches(row_name: str, needle: str) -> bool:
    a = (needle or "").strip()
    b = (row_name or "").strip()
    if not a:
        return False
    return a == b or a in b or b in a


def run_one_chain(
    conn,
    chain_name: str,
    start_url: str,
    client: httpx.Client,
    skip_stores: bool,
    skip_products: bool,
    skip_promos: bool,
) -> None:
    from fetch_stores import ensure_stores_table

    ensure_stores_table()
    ensure_products_category_columns(conn)

    base = f"{urlparse(start_url).scheme}://{urlparse(start_url).netloc}"
    LOG.info("Chain: %s | %s", chain_name, start_url)

    if not skip_stores:
        n = ingest_stores_for_chain(conn, chain_name, start_url, client)
        LOG.info("  stores upserted / parsed: %s", n)

    if not skip_products:
        prows = fetch_wc_products(base, chain_name, client)
        if prows:
            np = save_products_food(conn, prows)
            LOG.info("  WC products saved: %s", np)
        else:
            LOG.info("  (no WooCommerce products API response)")

    if not skip_promos:
        promos = fetch_wc_coupon_promotions(base, chain_name, client)
        if promos:
            nk = save_promotions(conn, promos)
            LOG.info("  promotions rows: %s", nk)
        else:
            LOG.info("  (no WC coupons or endpoint blocked)")


def main() -> int:
    _ensure_utf8_stdout()
    ap = argparse.ArgumentParser(description="Excel-driven chain store / product ingest")
    ap.add_argument("--excel", type=str, default=None, help="Path to .xlsx (default: factory Hebrew filename)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--chain", type=str, help="Hebrew chain name (substring match against Excel)")
    g.add_argument("--all", action="store_true", help="Process every row in the sheet")
    ap.add_argument("--skip-stores", action="store_true")
    ap.add_argument("--skip-products", action="store_true")
    ap.add_argument("--skip-promos", action="store_true")
    args = ap.parse_args()

    try:
        path = resolve_excel_path(args.excel)
    except FileNotFoundError as e:
        LOG.error("%s", e)
        return 2

    rows = load_sheet_rows(path)
    if not rows:
        LOG.error("No data rows in %s", path)
        return 2

    if args.all:
        selected = rows
    else:
        needle = (args.chain or "").strip()
        selected = [r for r in rows if chain_filter_matches(r["name"], needle)]
        if not selected:
            LOG.error("No Excel row matched --chain %r", needle)
            return 2

    try:
        conn = get_conn()
    except Exception as e:
        LOG.error("DB: %s", e)
        return 1

    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/json,*/*"}
    with httpx.Client(timeout=HTTP_TIMEOUT, headers=headers, verify=True) as client:
        for r in selected:
            try:
                run_one_chain(
                    conn,
                    r["name"],
                    r["url"],
                    client,
                    skip_stores=args.skip_stores,
                    skip_products=args.skip_products,
                    skip_promos=args.skip_promos,
                )
            except Exception as e:
                LOG.exception("failed %s: %s", r.get("name"), e)

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
