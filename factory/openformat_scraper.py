"""
openformat_scraper.py — SmartMarket OpenFormat Government Feed Scraper
======================================================================

Source:
  https://openformat.economy.gov.il/natural/

Flow:
1) Discover XML / GZ links from OpenFormat directory/feed endpoints.
2) Download XML-like files.
3) Parse products (barcode, name, price, chain_name, store_id).
4) Upsert into `products` table.

Run:
  python factory/openformat_scraper.py --limit 10000
  python factory/openformat_scraper.py --dry-run
"""

from __future__ import annotations

import argparse
import gzip
import io
import logging
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import urljoin

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
LOG = logging.getLogger("openformat")

BATCH_SIZE = 500
BASE_URL = "https://openformat.economy.gov.il/natural/"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SmartMarket/3.1)"}


def get_conn() -> psycopg2.extensions.connection:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is missing")
    return psycopg2.connect(db_url, sslmode="require")


def _clean_text(v: str | None, limit: int = 255) -> str:
    if not v:
        return ""
    return re.sub(r"\s+", " ", v).strip()[:limit]


def _clean_price(v: str | None) -> float | None:
    if not v:
        return None
    text = re.sub(r"[^\d.,]", "", str(v)).replace(",", ".")
    if text.count(".") > 1:
        parts = text.split(".")
        text = "".join(parts[:-1]) + "." + parts[-1]
    try:
        value = float(text)
    except ValueError:
        return None
    if value <= 0:
        return None
    return round(value, 2)


def _extract_links_from_text(text: str, base_url: str) -> list[str]:
    links: set[str] = set()

    # absolute urls
    for m in re.finditer(r'https?://[^\s"\'<>]+(?:\.xml|\.gz)\b', text, re.IGNORECASE):
        links.add(m.group(0))

    # href relative/absolute paths
    for m in re.finditer(r'href=["\']([^"\']+(?:\.xml|\.gz))["\']', text, re.IGNORECASE):
        href = m.group(1).strip()
        links.add(urljoin(base_url, href))

    return sorted(links)


def discover_file_urls(base_url: str = BASE_URL) -> list[str]:
    """
    Discover candidate XML feeds.
    Includes robust fallbacks since endpoint availability may vary by environment/network.
    """
    urls_to_probe = [
        base_url,
        urljoin(base_url, "sitemap.xml"),
        urljoin(base_url, "index.xml"),
        urljoin(base_url, "feed.xml"),
    ]
    found: set[str] = set()

    for url in urls_to_probe:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code != 200:
                continue
            content_type = (r.headers.get("content-type") or "").lower()
            text = r.text
            links = _extract_links_from_text(text, url)
            found.update(links)
            LOG.info("Probe %s (%s) -> %d links", url, content_type or "unknown", len(links))
        except Exception as exc:
            LOG.warning("Probe failed %s: %s", url, exc)

    # Prefer pricefull-like files if available.
    preferred = sorted(
        found,
        key=lambda u: (
            0 if "pricefull" in u.lower() else 1,
            0 if u.lower().endswith(".gz") else 1,
            u,
        ),
    )
    return preferred


def download_content(url: str) -> bytes | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=120, stream=True)
        r.raise_for_status()
        buf = io.BytesIO()
        for chunk in r.iter_content(65536):
            if chunk:
                buf.write(chunk)
        data = buf.getvalue()
        if data[:2] == b"\x1f\x8b":
            return gzip.decompress(data)
        return data
    except Exception as exc:
        LOG.warning("Download failed %s: %s", url, exc)
        return None


def parse_xml(content: bytes, source_url: str, limit: int | None = None) -> list[dict]:
    products: list[dict] = []
    # iterparse for memory efficiency
    try:
        it = ET.iterparse(io.BytesIO(content), events=("start", "end"))
    except ET.ParseError as exc:
        LOG.warning("XML parse init failed %s: %s", source_url, exc)
        return products

    current: dict[str, str] = {}
    in_item = False
    item_tags = {"item", "product", "row", "record"}

    for event, elem in it:
        tag = elem.tag.split("}")[-1].lower()
        if event == "start" and tag in item_tags:
            in_item = True
            current = {}
        elif event == "end" and in_item:
            if tag in item_tags:
                name = _clean_text(
                    current.get("itemname")
                    or current.get("itemnm")
                    or current.get("productname")
                    or current.get("name")
                )
                barcode = _clean_text(
                    current.get("itemcode")
                    or current.get("barcode")
                    or current.get("itemid"),
                    limit=64,
                )
                price = _clean_price(
                    current.get("itemprice")
                    or current.get("price")
                    or current.get("unitprice")
                )
                chain_name = _clean_text(
                    current.get("chainname")
                    or current.get("chain")
                    or current.get("storename")
                    or current.get("manufacturername")
                    or "OpenFormat",
                    limit=100,
                )
                store_id = _clean_text(
                    current.get("storeid")
                    or current.get("store")
                    or current.get("storecode"),
                    limit=64,
                )

                if name and barcode and price is not None:
                    products.append(
                        {
                            "barcode": barcode,
                            "name": name,
                            "price": price,
                            "brand": chain_name[:100],
                            "chain_name": chain_name,
                            "domain": "food",
                            "category_l1": "food",
                            "image_url": None,
                            "external_id": store_id or None,
                            "is_active": True,
                        }
                    )
                    if limit and len(products) >= limit:
                        break
                in_item = False
                current = {}
                elem.clear()
            else:
                # keep tag text for likely useful fields
                txt = (elem.text or "").strip()
                if txt:
                    current[tag] = txt
                elem.clear()

    # de-dup by barcode+chain_name keeping the lower price
    dedup: dict[tuple[str, str], dict] = {}
    for p in products:
        key = (p["barcode"], p["chain_name"])
        prev = dedup.get(key)
        if prev is None or p["price"] < prev["price"]:
            dedup[key] = p
    return list(dedup.values())


def save_to_db(products: list[dict], dry_run: bool = False) -> int:
    if not products:
        return 0
    if dry_run:
        LOG.info("[DRY-RUN] would save %d products", len(products))
        return len(products)

    conn = get_conn()
    cur = conn.cursor()
    try:
        execute_batch(
            cur,
            """
            INSERT INTO products
              (barcode, name, price, brand, chain_name, domain, category_l1, image_url, external_id, is_active, updated_at)
            VALUES
              (%(barcode)s, %(name)s, %(price)s, %(brand)s, %(chain_name)s, %(domain)s, %(category_l1)s, %(image_url)s, %(external_id)s, %(is_active)s, NOW())
            ON CONFLICT (barcode, chain_name) DO UPDATE SET
              name        = EXCLUDED.name,
              price       = EXCLUDED.price,
              brand       = EXCLUDED.brand,
              domain      = EXCLUDED.domain,
              category_l1 = EXCLUDED.category_l1,
              image_url   = EXCLUDED.image_url,
              external_id = EXCLUDED.external_id,
              is_active   = EXCLUDED.is_active,
              updated_at  = NOW()
            """,
            products,
            page_size=BATCH_SIZE,
        )
        conn.commit()
        LOG.info("💾 saved %d products", len(products))
        return len(products)
    except Exception as exc:
        conn.rollback()
        LOG.error("DB error: %s", exc)
        return 0
    finally:
        cur.close()
        conn.close()


def run(limit: int | None = None, dry_run: bool = False) -> int:
    LOG.info("%s", "=" * 60)
    LOG.info("OpenFormat scraper start | %s", datetime.now().strftime("%d/%m/%Y %H:%M"))
    LOG.info("Mode: %s", "DRY-RUN" if dry_run else "LIVE")
    LOG.info("%s", "=" * 60)

    urls = discover_file_urls(BASE_URL)
    if not urls:
        LOG.warning("No XML links discovered from OpenFormat")
        return 0

    total_saved = 0
    total_seen = 0
    for url in urls:
        if limit and total_seen >= limit:
            break
        LOG.info("Processing: %s", url)
        content = download_content(url)
        if not content:
            continue
        remaining = None if limit is None else max(limit - total_seen, 0)
        rows = parse_xml(content, source_url=url, limit=remaining)
        total_seen += len(rows)
        if not rows:
            continue
        saved = save_to_db(rows, dry_run=dry_run)
        total_saved += saved

    LOG.info("%s", "=" * 60)
    LOG.info("Done. seen=%d saved=%d", total_seen, total_saved)
    LOG.info("%s", "=" * 60)
    return total_saved


def main() -> int:
    parser = argparse.ArgumentParser(description="SmartMarket OpenFormat XML scraper")
    parser.add_argument("--limit", type=int, default=None, help="Max number of products to process")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    args = parser.parse_args()
    return run(limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())

