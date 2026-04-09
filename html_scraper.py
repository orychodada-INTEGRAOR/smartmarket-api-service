"""
html_scraper.py — SmartMarket HTML Scraper
==========================================
שואב מוצרים מ-4 רשתות מזון שלא משתמשות ב-XML:
  - חצי חינם   (shop.hazi-hinam.co.il)
  - נתיב החסד  (www.netiv-hachesed.co.il)
  - סיטי מרקט  (www.citymarket.co.il)
  - קיי.טי      (www.kt.co.il)

הרץ:
  python html_scraper.py --test              # חצי חינם בלבד
  python html_scraper.py                     # כל 4
  python html_scraper.py --chain hazi_hinam
  python html_scraper.py --dry-run
"""

import os, re, asyncio, argparse, logging, json
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("html")

CHAINS = [
    {
        "id":   "hazi_hinam",
        "name": "חצי חינם",
        "url":  "https://www.hazi-hinam.co.il/category/",
        "type": "hazi_hinam",
    },
    {
        "id":   "netiv_hachesed",
        "name": "נתיב החסד",
        "url":  "https://www.netiv-hachesed.co.il/online-store/",
        "type": "netiv",
    },
    {
        "id":   "citymarket",
        "name": "סיטי מרקט",
        "url":  "https://www.citymarket.co.il/store/",
        "type": "citymarket",
    },
    {
        "id":   "kt_pages",
        "name": "קיי.טי",
        "url":  "https://www.kt.co.il/",
        "type": "kt",
    },
]

# ── DB ──────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')

def save_to_db(products: list, dry_run=False) -> int:
    if not products:
        log.info("  אין מוצרים")
        return 0
    if dry_run:
        log.info(f"  [DRY] {len(products)} מוצרים")
        for p in products[:3]:
            log.info(f"    {p['chain_name']} | {p['name'][:40]} | {p['price']}₪")
        return len(products)
    conn = get_conn()
    cur  = conn.cursor()
    execute_batch(cur, """
        INSERT INTO products
          (barcode, name, price, brand, chain_name, domain, is_active, updated_at)
        VALUES
          (%(barcode)s, %(name)s, %(price)s, %(brand)s,
           %(chain_name)s, %(domain)s, %(is_active)s, NOW())
        ON CONFLICT (barcode, chain_name) DO UPDATE SET
          name=EXCLUDED.name, price=EXCLUDED.price,
          is_active=EXCLUDED.is_active, updated_at=NOW()
    """, products, page_size=500)
    conn.commit()
    cur.close(); conn.close()
    log.info(f"  💾 {len(products)} נשמרו")
    return len(products)

def make_product(name, price_str, chain_name, barcode=None, brand=""):
    """בנה dict מוצר מנתונים גולמיים"""
    try:
        price = float(re.sub(r'[^\d.]', '', str(price_str)) or '0')
    except:
        price = 0.0
    # בנה ברקוד מ-hash אם אין
    if not barcode:
        barcode = str(abs(hash(f"{chain_name}_{name}")))[:13]
    return {
        'barcode':    str(barcode)[:50],
        'name':       str(name).strip()[:200],
        'price':      price,
        'brand':      str(brand).strip()[:100],
        'chain_name': chain_name,
        'domain':     'GROCERY',
        'is_active':  True,
    }

# ── Scrapers לפי רשת ────────────────────────────────────────

async def scrape_hazi_hinam(page, chain) -> list:
    """חצי חינם — מוצרים לפי קטגוריה"""
    log.info(f"  [{chain['name']}] {chain['url']}")
    products = []

    # רשימת קטגוריות
    categories = [
        "https://www.hazi-hinam.co.il/category/dairy/",
        "https://www.hazi-hinam.co.il/category/bread/",
        "https://www.hazi-hinam.co.il/category/vegetables/",
        "https://www.hazi-hinam.co.il/category/meat/",
        "https://www.hazi-hinam.co.il/category/cleaning/",
    ]

    for cat_url in categories[:3]:
        try:
            await page.goto(cat_url, wait_until='domcontentloaded', timeout=20000)
            await page.wait_for_timeout(1500)

            items = await page.evaluate("""() => {
                const products = [];
                // נסה כמה סלקטורים
                const selectors = [
                    '.product-item', '.product', '.item',
                    '[class*="product"]', 'article'
                ];
                for (const sel of selectors) {
                    const els = document.querySelectorAll(sel);
                    if (els.length > 2) {
                        els.forEach(el => {
                            const name = el.querySelector(
                                'h2,h3,.title,.name,.product-name,[class*="name"],[class*="title"]'
                            )?.innerText?.trim();
                            const price = el.querySelector(
                                '.price,[class*="price"],span[class*="price"]'
                            )?.innerText?.trim();
                            if (name && name.length > 2) {
                                products.push({name, price: price || '0'});
                            }
                        });
                        if (products.length > 0) break;
                    }
                }
                return products.slice(0, 100);
            }""")

            for item in items:
                p = make_product(item['name'], item.get('price','0'), chain['name'])
                products.append(p)

            log.info(f"    {cat_url.split('/')[-2]}: {len(items)} מוצרים")
            await asyncio.sleep(0.5)

        except Exception as e:
            log.debug(f"    {cat_url}: {e}")

    # fallback — דף ראשי
    if not products:
        try:
            await page.goto(chain['url'], wait_until='domcontentloaded', timeout=20000)
            await page.wait_for_timeout(2000)
            html = await page.content()

            # חפש JSON של מוצרים בעמוד
            json_matches = re.findall(
                r'"name"\s*:\s*"([^"]{3,80})"[^}]*"price"\s*:\s*"?(\d+\.?\d*)"?',
                html
            )
            for name, price in json_matches[:200]:
                products.append(make_product(name, price, chain['name']))
        except Exception as e:
            log.warning(f"    fallback error: {e}")

    return products


async def scrape_netiv(page, chain) -> list:
    """נתיב החסד"""
    log.info(f"  [{chain['name']}] {chain['url']}")
    products = []

    urls = [
        "https://www.netiv-hachesed.co.il/online-store/",
        "https://www.netiv-hachesed.co.il/product-category/food/",
    ]

    for url in urls[:2]:
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=20000)
            await page.wait_for_timeout(2000)

            items = await page.evaluate("""() => {
                const res = [];
                document.querySelectorAll('.product,.woocommerce-loop-product,.product-item').forEach(el => {
                    const name  = el.querySelector('.woocommerce-loop-product__title, h2, .name')?.innerText?.trim();
                    const price = el.querySelector('.price, .woocommerce-Price-amount')?.innerText?.trim();
                    if (name) res.push({name, price: price || '0'});
                });
                return res.slice(0,200);
            }""")

            for item in items:
                products.append(make_product(item['name'], item.get('price','0'), chain['name']))

            if items:
                log.info(f"    {len(items)} מוצרים מ-{url}")
                break

        except Exception as e:
            log.debug(f"    {e}")

    return products


async def scrape_citymarket(page, chain) -> list:
    """סיטי מרקט"""
    log.info(f"  [{chain['name']}] {chain['url']}")
    products = []

    try:
        await page.goto(chain['url'], wait_until='domcontentloaded', timeout=20000)
        await page.wait_for_timeout(2000)

        items = await page.evaluate("""() => {
            const res = [];
            const selectors = ['.product','.item','[class*="product"]','li[class*="item"]'];
            for (const sel of selectors) {
                const els = document.querySelectorAll(sel);
                if (els.length > 3) {
                    els.forEach(el => {
                        const name  = el.querySelector('h2,h3,.name,[class*="name"]')?.innerText?.trim();
                        const price = el.querySelector('.price,[class*="price"]')?.innerText?.trim();
                        if (name && name.length > 2) res.push({name, price: price||'0'});
                    });
                    if (res.length > 0) break;
                }
            }
            return res.slice(0,300);
        }""")

        for item in items:
            products.append(make_product(item['name'], item.get('price','0'), chain['name']))

        log.info(f"    {len(items)} מוצרים")

    except Exception as e:
        log.warning(f"    {e}")

    return products


async def scrape_kt(page, chain) -> list:
    """קיי.טי"""
    log.info(f"  [{chain['name']}] {chain['url']}")
    products = []

    urls = [
        "https://www.kt.co.il/",
        "https://www.kt.co.il/category/food/",
        "https://www.kt.co.il/products/",
    ]

    for url in urls:
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=20000)
            await page.wait_for_timeout(2000)

            items = await page.evaluate("""() => {
                const res = [];
                document.querySelectorAll('.product,.item,article,[class*="product"]').forEach(el => {
                    const name  = el.querySelector('h2,h3,.title,.name')?.innerText?.trim();
                    const price = el.querySelector('.price,[class*="price"]')?.innerText?.trim();
                    if (name && name.length > 2) res.push({name, price: price||'0'});
                });
                return res.slice(0,300);
            }""")

            if items:
                for item in items:
                    products.append(make_product(item['name'], item.get('price','0'), chain['name']))
                log.info(f"    {len(items)} מוצרים מ-{url}")
                break

        except Exception as e:
            log.debug(f"    {url}: {e}")

    return products


# ── מפה: id → פונקציה ───────────────────────────────────────
SCRAPERS = {
    "hazi_hinam":     scrape_hazi_hinam,
    "netiv":          scrape_netiv,
    "citymarket":     scrape_citymarket,
    "kt":             scrape_kt,
}

# ── Main ─────────────────────────────────────────────────────
async def run(chain_id=None, dry_run=False, test=False):
    from playwright.async_api import async_playwright

    chains = CHAINS
    if chain_id:
        chains = [c for c in chains if c['id'] == chain_id]
    if test:
        chains = chains[:1]

    print(f"\n{'='*55}")
    print(f"  SmartMarket — HTML Scraper")
    print(f"  {len(chains)} רשתות | {'DRY' if dry_run else 'LIVE'}")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{'='*55}\n")

    grand_total = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            ignore_https_errors=True,
        )
        page = await ctx.new_page()
        await page.route("**/*.{png,jpg,jpeg,gif,svg,ico,woff,woff2,css}", lambda r: r.abort())

        for chain in chains:
            log.info(f"\n{'='*50}")
            log.info(f"[HTML] {chain['name']}")

            scraper_fn = SCRAPERS.get(chain['type'])
            if not scraper_fn:
                log.warning(f"  אין scraper ל-{chain['type']}")
                continue

            try:
                products = await scraper_fn(page, chain)
                log.info(f"  סה\"כ: {len(products)} מוצרים")
                total = save_to_db(products, dry_run)
                grand_total += total
            except Exception as e:
                log.error(f"  ❌ {e}")

        await browser.close()

    print(f"\n{'='*55}")
    print(f"  ✅ HTML סה\"כ: {grand_total} מוצרים")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument('--test',    action='store_true')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--chain',   default=None)
    a = ap.parse_args()
    asyncio.run(run(chain_id=a.chain, dry_run=a.dry_run, test=a.test))