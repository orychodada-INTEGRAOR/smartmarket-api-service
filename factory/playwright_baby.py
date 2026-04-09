"""
playwright_baby.py — SmartMarket Baby/Kids Scraper
==================================================

Targets:
1) https://www.shilav.co.il   (שילב)
2) https://www.terminalx.com/baby (TerminalX Baby)
3) https://www.fox.co.il/baby (Fox Baby)

Extracted fields:
- name
- price
- image_url
- category_l1/category ("baby")
- chain_name
- domain ("baby")

Run:
  python playwright_baby.py
  python playwright_baby.py --dry-run
  python playwright_baby.py --show
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import os
import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

import psycopg2
from dotenv import load_dotenv
from playwright.async_api import BrowserContext, Page, async_playwright
from psycopg2.extras import execute_batch

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
LOG = logging.getLogger("baby")

BATCH_SIZE = 500

TARGETS: list[dict[str, str]] = [
    {
        "id": "shilav",
        "name": "שילב",
        "base_url": "https://www.shilav.co.il",
        "start_url": "https://www.shilav.co.il",
    },
    {
        "id": "terminalx_baby",
        "name": "TerminalX Baby",
        "base_url": "https://www.terminalx.com",
        "start_url": "https://www.terminalx.com/baby",
    },
    {
        "id": "fox_baby",
        "name": "Fox Baby",
        "base_url": "https://www.fox.co.il",
        "start_url": "https://www.fox.co.il/baby",
    },
]

# broad selectors to support multiple storefront engines
PRODUCT_CARD_SELECTORS = [
    ".product-grid-item",  # Shilav verified
    "[data-product-id]",
    ".product-item",
    ".product",
    ".product-card",
    ".grid-product",
    ".item.product",
    "li.product",
]

NAME_SELECTORS = [
    ".grid-product__title",  # Shilav verified
    ".product-grid-item .grid-product__title",
    "[data-testid='product-title']",
    ".product-name",
    ".product-title",
    ".item-title",
    "h2",
    "h3",
    "a[title]",
]

PRICE_SELECTORS = [
    ".grid-product__price--current",  # Shilav verified
    ".grid-product__price",
    "[data-testid='price']",
    ".price",
    ".product-price",
    ".price-final",
    ".special-price",
    ".woocommerce-Price-amount",
]

IMAGE_SELECTORS = [
    ".product-grid-item img",  # Shilav verified
    "img[data-src]",
    "img[srcset]",
    "img[src]",
]


def get_conn() -> psycopg2.extensions.connection:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is missing")
    return psycopg2.connect(db_url, sslmode="require")


def _clean_text(s: str | None, limit: int = 255) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()[:limit]


def _clean_price(price_raw: str | None) -> float | None:
    if not price_raw:
        return None
    x = re.sub(r"[^\d.,]", "", price_raw).replace(",", ".")
    if x.count(".") > 1:
        parts = x.split(".")
        x = "".join(parts[:-1]) + "." + parts[-1]
    try:
        value = float(x)
    except ValueError:
        return None
    if value <= 0:
        return None
    return round(value, 2)


def _synthetic_barcode(chain_id: str, name: str, image_url: str | None) -> str:
    base = f"{chain_id}|{name}|{image_url or ''}"
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:24]
    return f"baby_{chain_id}_{digest}"


def save_to_db(products: list[dict[str, Any]], dry_run: bool = False) -> int:
    if not products:
        return 0
    if dry_run:
        LOG.info("  [DRY-RUN] would save %d products", len(products))
        return len(products)

    conn = get_conn()
    cur = conn.cursor()
    try:
        execute_batch(
            cur,
            """
            INSERT INTO products
              (barcode, name, price, brand, chain_name, domain, category_l1, image_url, is_active, updated_at)
            VALUES
              (%(barcode)s, %(name)s, %(price)s, %(brand)s, %(chain_name)s, %(domain)s, %(category_l1)s, %(image_url)s, %(is_active)s, NOW())
            ON CONFLICT (barcode, chain_name) DO UPDATE SET
              name        = EXCLUDED.name,
              price       = EXCLUDED.price,
              domain      = EXCLUDED.domain,
              category_l1 = EXCLUDED.category_l1,
              image_url   = EXCLUDED.image_url,
              is_active   = EXCLUDED.is_active,
              updated_at  = NOW()
            """,
            products,
            page_size=BATCH_SIZE,
        )
        conn.commit()
        LOG.info("  💾 saved %d products", len(products))
        return len(products)
    except Exception as exc:
        conn.rollback()
        LOG.error("  DB error: %s", exc)
        return 0
    finally:
        cur.close()
        conn.close()


async def _auto_scroll(page: Page, rounds: int = 5) -> None:
    for _ in range(rounds):
        await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        await page.wait_for_timeout(900)


async def _extract_from_cards(page: Page, target: dict[str, str], max_cards: int = 1200) -> list[dict[str, Any]]:
    products: list[dict[str, Any]] = []
    seen: set[str] = set()

    # single JS extractor for speed and resilience
    data = await page.evaluate(
        """(args) => {
            const {cardSelectors, nameSelectors, priceSelectors, imageSelectors, maxCards} = args;
            const cards = [];
            for (const s of cardSelectors) {
              document.querySelectorAll(s).forEach(el => cards.push(el));
            }
            const uniq = Array.from(new Set(cards)).slice(0, maxCards);

            const pickText = (root, sels) => {
              for (const s of sels) {
                const el = root.querySelector(s);
                if (!el) continue;
                const txt = (el.innerText || el.textContent || "").trim();
                if (txt) return txt;
                const title = (el.getAttribute("title") || "").trim();
                if (title) return title;
              }
              return "";
            };
            const pickImg = (root, sels) => {
              for (const s of sels) {
                const el = root.querySelector(s);
                if (!el) continue;
                const src = (el.getAttribute("src") || el.getAttribute("data-src") || "").trim();
                if (src) return src;
              }
              return "";
            };
            return uniq.map(card => ({
              name: pickText(card, nameSelectors),
              price: pickText(card, priceSelectors),
              image: pickImg(card, imageSelectors),
            }));
        }""",
        {
            "cardSelectors": PRODUCT_CARD_SELECTORS,
            "nameSelectors": NAME_SELECTORS,
            "priceSelectors": PRICE_SELECTORS,
            "imageSelectors": IMAGE_SELECTORS,
            "maxCards": max_cards,
        },
    )

    for item in data:
        name = _clean_text(item.get("name"))
        price = _clean_price(item.get("price"))
        if not name or price is None:
            continue
        image_url = item.get("image") or None
        if image_url:
            image_url = urljoin(target["base_url"], image_url)

        key = f"{name}|{price}|{image_url or ''}"
        if key in seen:
            continue
        seen.add(key)

        products.append(
            {
                "barcode": _synthetic_barcode(target["id"], name, image_url),
                "name": name,
                "price": price,
                "brand": target["name"],
                "chain_name": target["name"],
                "domain": "baby",
                "category_l1": "baby",
                "image_url": image_url,
                "is_active": True,
            }
        )

    return products


async def scrape_target(context: BrowserContext, target: dict[str, str], dry_run: bool = False) -> int:
    page = await context.new_page()
    try:
        LOG.info("\n%s", "=" * 56)
        LOG.info("[Baby] %s — %s", target["name"], target["start_url"])
        await page.goto(target["start_url"], wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(2000)
        await _auto_scroll(page, rounds=6)
        await page.wait_for_timeout(1200)

        products = await _extract_from_cards(page, target)
        LOG.info("  extracted %d candidates", len(products))
        if not products:
            LOG.warning("  no products extracted")
            return 0

        saved = save_to_db(products, dry_run=dry_run)
        LOG.info("  ✅ %s: %d", target["name"], saved)
        return saved
    except Exception as exc:
        LOG.error("  ❌ %s failed: %s", target["name"], exc)
        return 0
    finally:
        await page.close()


async def run(dry_run: bool = False, headless: bool = True, only: str | None = None) -> int:
    total = 0
    LOG.info("%s", "=" * 56)
    LOG.info("SmartMarket Baby Scraper | %s", datetime.now().strftime("%d/%m/%Y %H:%M"))
    LOG.info("Mode: %s", "DRY-RUN" if dry_run else "LIVE")
    LOG.info("%s", "=" * 56)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=headless,
            args=[
                "--ignore-certificate-errors",
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        )
        targets = TARGETS
        if only:
            wanted = only.strip().lower()
            targets = [t for t in TARGETS if t["id"].lower() == wanted]
            if not targets:
                LOG.error("Unknown target id '%s'. options: %s", only, [t["id"] for t in TARGETS])
                await browser.close()
                return 1

        for target in targets:
            count = await scrape_target(context, target, dry_run=dry_run)
            total += count
            await asyncio.sleep(1)
        await browser.close()

    LOG.info("\n%s", "=" * 56)
    LOG.info("Total saved (baby): %d", total)
    LOG.info("%s", "=" * 56)
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description="SmartMarket Baby Playwright Scraper")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    parser.add_argument("--show", action="store_true", help="Run browser in headed mode")
    parser.add_argument("--only", default=None, help="Run only one target id (e.g. shilav)")
    args = parser.parse_args()
    return asyncio.run(run(dry_run=args.dry_run, headless=not args.show, only=args.only))


if __name__ == "__main__":
    raise SystemExit(main())

