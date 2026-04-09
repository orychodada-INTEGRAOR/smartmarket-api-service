"""
playwright_bina.py v5 — SmartMarket BinaProjects Scraper
=========================================================
פתרון סופי — מבוסס על debug אמיתי:

1. Main.aspx → HTML עם onclick="Download('filename.gz')"
2. Download.aspx?FileNm=filename.gz → JSON: [{"SPath":"https://.../Download/filename.gz"}]
3. הURL ב-SPath → קובץ GZ אמיתי → parse XML → DB

הרץ:
  python playwright_bina.py --test --dry-run   # בדיקה
  python playwright_bina.py --test             # קינג סטור, קובץ אחד
  python playwright_bina.py                    # כל 10 רשתות
  python playwright_bina.py --chain kingstore
"""

import os, re, gzip, json, asyncio, argparse, logging
import xml.etree.ElementTree as ET
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("bina")

CHAINS = [
    {"id": "kingstore",     "name": "קינג סטור",          "subdomain": "kingstore"},
    {"id": "maayan2000",    "name": "מעיין 2000",          "subdomain": "maayan2000"},
    {"id": "goodpharm",     "name": "גוד פארם",            "subdomain": "goodpharm"},
    {"id": "zolvebegadol",  "name": "זול ובגדול",          "subdomain": "zolvebegadol"},
    {"id": "supersapir",    "name": "סופר ספיר",           "subdomain": "supersapir"},
    {"id": "superbareket",  "name": "עוף והודו ברקת",      "subdomain": "superbareket"},
    {"id": "citymarket_kg", "name": "סיטי מרקט קרית גת",  "subdomain": "citymarketkiryatgat"},
    {"id": "ktshivuk",      "name": "קיי.טי יבוא ושיווק", "subdomain": "ktshivuk"},
    {"id": "shuk_hayir",    "name": "שוק העיר",            "subdomain": "shuk-hayir"},
    {"id": "shefabirkat",   "name": "שפע ברכת השם",        "subdomain": "shefabirkathashem"},
]

# ── DB ──────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')

def save_to_db(products: list, dry_run=False) -> int:
    if not products:
        return 0
    if dry_run:
        log.info(f"  [DRY] {len(products)} מוצרים")
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
          name=EXCLUDED.name, price=EXCLUDED.price, brand=EXCLUDED.brand,
          is_active=EXCLUDED.is_active, updated_at=NOW()
    """, products, page_size=500)
    conn.commit()
    cur.close(); conn.close()
    log.info(f"  💾 {len(products)} נשמרו ב-DB")
    return len(products)

# ── XML Parser ──────────────────────────────────────────────────
def parse_xml(content: bytes, chain_name: str) -> list:
    try:
        # ZIP (PK signature) — חלץ את קובץ ה-XML מבפנים
        if content[:2] == b'PK':
            import zipfile, io
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                xml_files = [n for n in zf.namelist() if n.lower().endswith('.xml')]
                if xml_files:
                    content = zf.read(xml_files[0])
        # GZIP
        elif content[:2] == b'\x1f\x8b':
            content = gzip.decompress(content)
        text = None
        for enc in ['utf-8', 'windows-1255', 'iso-8859-8']:
            try:
                text = content.decode(enc).lstrip('\ufeff')
                break
            except:
                continue
        if not text:
            return []
        root = ET.fromstring(text)
        products = []
        for item in root.iter():
            tag = item.tag.lower().split('}')[-1]
            if tag not in ('item', 'product', 'row', 'record'):
                continue
            barcode = (item.findtext('ItemCode') or item.findtext('Barcode') or
                       item.findtext('barcodenum') or item.get('ItemCode') or '')
            name    = (item.findtext('ItemNm') or item.findtext('ItemName') or
                       item.findtext('Name') or item.findtext('itemname') or '')
            price   = (item.findtext('ItemPrice') or item.findtext('Price') or
                       item.findtext('UnitOfMeasurePrice') or '0')
            brand   = (item.findtext('ManufacturerName') or item.findtext('Brand') or '')
            if not barcode or not name:
                continue
            try:
                price_f = float(re.sub(r'[^\d.]', '', str(price)) or '0')
            except:
                price_f = 0.0
            products.append({
                'barcode':    barcode.strip()[:50],
                'name':       name.strip()[:200],
                'price':      price_f,
                'brand':      brand.strip()[:100],
                'chain_name': chain_name,
                'domain':     'GROCERY',
                'is_active':  True,
            })
        return products
    except Exception as e:
        log.warning(f"  parse error: {e}")
        return []

# ── Resolve real URL via Download.aspx ─────────────────────────
async def resolve_url(session, base: str, filename: str) -> str:
    """
    Download.aspx?FileNm=X.gz → JSON [{"SPath":"https://.../Download/X.gz"}]
    מחזיר את ה-URL האמיתי לקובץ ה-GZ
    """
    api_url = f"{base}/Download.aspx?FileNm={filename}"
    try:
        async with session.get(api_url, headers={'User-Agent': 'Mozilla/5.0'}) as resp:
            if resp.status == 200:
                data = await resp.json(content_type=None)
                if data and isinstance(data, list) and 'SPath' in data[0]:
                    return data[0]['SPath']
    except Exception as e:
        log.debug(f"  resolve error: {e}")
    return None

# ── Scrape One Chain ─────────────────────────────────────────────
async def scrape_chain(chain: dict, dry_run=False, headless=True, max_files=3) -> int:
    import aiohttp
    from playwright.async_api import async_playwright

    sub  = chain['subdomain']
    name = chain['name']
    base = f"https://{sub}.binaprojects.com"
    url  = f"{base}/Main.aspx"

    log.info(f"\n{'='*55}")
    log.info(f"[Bina] {name} — {url}")

    # ── שלב 1: קבל רשימת שמות קבצים מה-HTML ──────────────────
    filenames = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        ctx  = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            ignore_https_errors=True,
        )
        page = await ctx.new_page()
        await page.route(
            "**/*.{png,jpg,jpeg,gif,svg,ico,woff,woff2,css}",
            lambda r: r.abort()
        )
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=25000)
            await page.wait_for_load_state('networkidle', timeout=15000)
            await page.wait_for_timeout(1000)

            html = await page.content()

            # ✅ חלץ שמות קבצים מ: onclick="Download('filename.gz')"
            filenames = re.findall(r"Download\(['\"]([^'\"]+\.gz)['\"]", html)
            filenames += re.findall(r"Download\(['\"]([^'\"]+\.xml)['\"]", html)
            filenames = list(dict.fromkeys(filenames))  # ייחודיים
            log.info(f"  נמצאו {len(filenames)} קבצים")

        except Exception as e:
            log.warning(f"  שגיאה בטעינת דף: {e}")
        finally:
            await browser.close()

    if not filenames:
        log.warning(f"  ❌ לא נמצאו קבצים ל-{name}")
        return 0

    # ── שלב 2: העדף PriceFull ──────────────────────────────────
    pricefull = [f for f in filenames if 'pricefull' in f.lower()]
    others    = [f for f in filenames if 'pricefull' not in f.lower()]
    to_process = (pricefull + others)[:max_files]
    log.info(f"  מעבד {len(to_process)} קבצים (PriceFull: {len(pricefull)})")

    # ── שלב 3: resolve URL + הורד ─────────────────────────────
    products = []
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False),
        timeout=aiohttp.ClientTimeout(total=90),
    ) as session:
        for fname in to_process:
            # ✅ קבל URL אמיתי מ-Download.aspx → JSON → SPath
            real_url = await resolve_url(session, base, fname)
            if not real_url:
                log.warning(f"  ⚠️ לא נמצא URL ל-{fname}")
                continue

            log.info(f"  מוריד: {fname[:60]}")
            try:
                async with session.get(
                    real_url,
                    headers={'User-Agent': 'Mozilla/5.0'}
                ) as resp:
                    if resp.status == 200:
                        content = await resp.read()
                        if len(content) < 50:
                            log.info(f"  ⚠️ קובץ ריק")
                            continue
                        parsed = parse_xml(content, name)
                        log.info(f"  ✅ {len(parsed)} מוצרים")
                        products.extend(parsed)
                    else:
                        log.warning(f"  HTTP {resp.status}")
            except Exception as e:
                log.warning(f"  ❌ {e}")

    total = save_to_db(products, dry_run)
    log.info(f"  סה\"כ {name}: {total}")
    return total

# ── Main ────────────────────────────────────────────────────────
async def run(chain_id=None, dry_run=False, test=False, headless=True):
    chains = CHAINS
    if chain_id:
        chains = [c for c in chains if c['id'] == chain_id]
    if test:
        chains = chains[:1]

    print(f"\n{'='*55}")
    print(f"  SmartMarket — Bina Scraper v5")
    print(f"  {len(chains)} רשתות | {'DRY' if dry_run else 'LIVE'} | headless={headless}")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{'='*55}\n")

    total = 0
    for chain in chains:
        count = await scrape_chain(
            chain,
            dry_run=dry_run,
            headless=headless,
            max_files=1 if test else 3,
        )
        total += count
        await asyncio.sleep(1)

    print(f"\n{'='*55}")
    print(f"  ✅ BinaProjects סה\"כ: {total} מוצרים")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument('--test',    action='store_true', help='קינג סטור בלבד, קובץ אחד')
    ap.add_argument('--dry-run', action='store_true', help='בלי DB')
    ap.add_argument('--chain',   default=None,        help='ID רשת')
    ap.add_argument('--show',    action='store_true', help='headless=False')
    a = ap.parse_args()
    asyncio.run(run(
        chain_id=a.chain,
        dry_run=a.dry_run,
        test=a.test,
        headless=not a.show,
    ))