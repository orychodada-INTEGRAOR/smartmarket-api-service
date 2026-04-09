"""
playwright_laib.py — SmartMarket LaibCatalog Scraper
=====================================================
שואב מ-3 רשתות LaibCatalog: ויקטורי, ח. כהן, מחסני השוק

הרץ:
  python playwright_laib.py --test
  python playwright_laib.py --dry-run
  python playwright_laib.py
"""
import os, re, gzip, argparse, logging
import xml.etree.ElementTree as ET
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("laib")

CHAINS = [
    {"id": "victory",        "name": "ויקטורי",              "url": "https://laibcatalog.co.il/victory/index.html"},
    {"id": "h_cohen",        "name": "ח. כהן מזון ומשקאות", "url": "https://laibcatalog.co.il/"},
    {"id": "machanei_hashuk","name": "מחסני השוק",            "url": "https://laibcatalog.co.il/"},
]

def get_conn():
    return psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')

def parse_xml(content: bytes, chain_name: str) -> list:
    try:
        if content[:2] == b'\x1f\x8b':
            content = gzip.decompress(content)
        for enc in ['utf-8', 'windows-1255']:
            try: text = content.decode(enc); break
            except: continue
        root = ET.fromstring(text)
        products = []
        for item in root.iter():
            if item.tag.lower() in ('item', 'product', 'row'):
                barcode = (item.findtext('ItemCode') or item.findtext('Barcode') or '')
                name    = (item.findtext('ItemName') or item.findtext('Name') or '')
                price   = (item.findtext('ItemPrice') or item.findtext('Price') or '0')
                brand   = (item.findtext('ManufacturerName') or '')
                if not name or not barcode: continue
                try: price_f = float(re.sub(r'[^\d.]', '', price) or '0')
                except: price_f = 0.0
                products.append({'barcode': barcode.strip(), 'name': name.strip(),
                                  'price': price_f, 'brand': brand.strip(),
                                  'chain_name': chain_name, 'domain': 'GROCERY', 'is_active': True})
        return products
    except Exception as e:
        log.warning(f"  parse error: {e}"); return []

def save_to_db(products, dry_run=False):
    if dry_run or not products:
        log.info(f"  {'[DRY] ' if dry_run else ''}{len(products)} מוצרים"); return len(products)
    conn = get_conn(); cur = conn.cursor()
    sql = """INSERT INTO products (barcode,name,price,brand,chain_name,domain,is_active,updated_at)
             VALUES (%(barcode)s,%(name)s,%(price)s,%(brand)s,%(chain_name)s,%(domain)s,%(is_active)s,NOW())
             ON CONFLICT (barcode,chain_name) DO UPDATE SET
             name=EXCLUDED.name,price=EXCLUDED.price,brand=EXCLUDED.brand,
             is_active=EXCLUDED.is_active,updated_at=NOW()"""
    execute_batch(cur, sql, products, page_size=500)
    conn.commit(); cur.close(); conn.close()
    log.info(f"  💾 {len(products)} נשמרו"); return len(products)

async def scrape_chain(chain, dry_run=False):
    from playwright.async_api import async_playwright
    import aiohttp
    
    log.info(f"\n{'='*50}")
    log.info(f"[Laib] {chain['name']} — {chain['url']}")
    products = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page    = await browser.new_page()
        await page.route("**/*.{png,jpg,jpeg,gif,svg,ico}", lambda r: r.abort())
        
        try:
            await page.goto(chain['url'], wait_until='domcontentloaded', timeout=30000)
            await page.wait_for_timeout(3000)
            
            # LaibCatalog: חפש לינקים לקבצי XML/GZ
            links = await page.eval_on_selector_all('a[href]', '''
                els => els.map(e => e.href).filter(h => 
                    h.includes('.gz') || h.includes('.xml') || h.includes('PriceFull'))
            ''')
            
            # אם לא נמצא — חפש ב-page source
            if not links:
                content_page = await page.content()
                links = re.findall(r'https?://[^\s"\'<>]+(?:\.gz|\.xml|PriceFull[^\s"\'<>]*)', content_page)
            
            log.info(f"  נמצאו {len(links)} קבצים")
            
            async with aiohttp.ClientSession() as session:
                for link in [l for l in links if 'Price' in l and 'Promo' not in l][:5]:
                    try:
                        log.info(f"  מוריד: {link.split('/')[-1][:50]}")
                        async with session.get(link, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                            if resp.status == 200:
                                data    = await resp.read()
                                parsed  = parse_xml(data, chain['name'])
                                products.extend(parsed)
                                log.info(f"  {len(parsed)} מוצרים")
                    except Exception as e:
                        log.warning(f"  {e}")
        except Exception as e:
            log.error(f"  שגיאה: {e}")
        finally:
            await browser.close()
    
    total = save_to_db(products, dry_run)
    log.info(f"  סה\"כ {chain['name']}: {total}")
    return total

async def run(dry_run=False, test=False):
    import asyncio
    chains = CHAINS[:1] if test else CHAINS
    print(f"\n{'='*50}")
    print(f"  Laib Scraper | {len(chains)} רשתות | {'DRY' if dry_run else 'LIVE'}")
    print(f"{'='*50}\n")
    total = 0
    for chain in chains:
        total += await scrape_chain(chain, dry_run)
        await asyncio.sleep(1)
    print(f"\n  ✅ Laib סה\"כ: {total}\n")

if __name__ == "__main__":
    import asyncio
    p = argparse.ArgumentParser()
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--test',    action='store_true')
    a = p.parse_args()
    asyncio.run(run(dry_run=a.dry_run, test=a.test))