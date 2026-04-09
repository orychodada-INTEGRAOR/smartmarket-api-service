"""
fetch_stores.py — SmartMarket Stores Fetcher
=============================================
שואב נתוני חנויות (כתובת, עיר, קואורדינטות) מכל הרשתות.
מקורות:
1. שופרסל — http://prices.shufersal.co.il/ (קבצי Stores.xml)
2. BinaProjects — Stores XML מאותו Download.aspx
3. PublishedPrices — Stores קבצים

הרץ:
  python fetch_stores.py --test     # שופרסל בלבד
  python fetch_stores.py            # כל הרשתות
  python fetch_stores.py --dry-run  # בלי DB
"""

import os, re, gzip, json, zipfile, io, asyncio, argparse, logging
import xml.etree.ElementTree as ET
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("stores")

# ── DB ────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')

def ensure_stores_table():
    """צור טבלת stores אם לא קיימת"""
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stores (
            id          SERIAL PRIMARY KEY,
            store_id    TEXT,
            chain_name  TEXT,
            name        TEXT,
            address     TEXT,
            city        TEXT,
            zip_code    TEXT,
            latitude    NUMERIC,
            longitude   NUMERIC,
            phone       TEXT,
            hours       TEXT,
            updated_at  TIMESTAMP DEFAULT NOW(),
            UNIQUE(store_id, chain_name)
        )
    """)
    conn.commit()
    cur.close(); conn.close()
    log.info("✅ טבלת stores מוכנה")

def save_stores(stores: list, dry_run=False) -> int:
    if not stores:
        return 0
    if dry_run:
        log.info(f"  [DRY] {len(stores)} חנויות")
        for s in stores[:3]:
            log.info(f"    {s.get('chain_name')} | {s.get('name')} | {s.get('city')}")
        return len(stores)
    conn = get_conn()
    cur  = conn.cursor()
    execute_batch(cur, """
        INSERT INTO stores (store_id,chain_name,name,address,city,zip_code,latitude,longitude,phone,updated_at)
        VALUES (%(store_id)s,%(chain_name)s,%(name)s,%(address)s,%(city)s,%(zip_code)s,%(latitude)s,%(longitude)s,%(phone)s,NOW())
        ON CONFLICT (store_id,chain_name) DO UPDATE SET
          name=EXCLUDED.name, address=EXCLUDED.address, city=EXCLUDED.city,
          latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude, updated_at=NOW()
    """, stores, page_size=200)
    conn.commit()
    cur.close(); conn.close()
    log.info(f"  💾 {len(stores)} חנויות נשמרו")
    return len(stores)

# ── XML Parsers ───────────────────────────────────────────
def parse_stores_xml(content: bytes, chain_name: str) -> list:
    """מפענח Stores.xml מפורמט ממשלתי סטנדרטי"""
    try:
        # ZIP
        if content[:2] == b'PK':
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                xml_files = [n for n in zf.namelist() if n.lower().endswith('.xml')]
                if xml_files:
                    content = zf.read(xml_files[0])
        # GZIP
        elif content[:2] == b'\x1f\x8b':
            content = gzip.decompress(content)

        text = None
        for enc in ['utf-8', 'windows-1255']:
            try:
                text = content.decode(enc).lstrip('\ufeff')
                break
            except:
                continue
        if not text:
            return []

        root = ET.fromstring(text)
        stores = []

        # חפש Store/Branch elements
        for store in root.iter():
            tag = store.tag.lower().split('}')[-1]
            if tag not in ('store', 'branch', 'branche'):
                continue

            def get(*tags):
                for t in tags:
                    v = store.findtext(t) or store.findtext(t.lower()) or ''
                    if v.strip(): return v.strip()
                return ''

            store_id = get('StoreId','StoreID','BranchId','BranchID','store_id')
            name     = get('StoreName','BranchName','Name','ChainName')
            address  = get('Address','Street','StreetAddress')
            city     = get('City','CityHe')
            zipcode  = get('ZipCode','Zip','PostalCode')
            phone    = get('Phone','Telephone')

            # קואורדינטות
            lat = get('Latitude','Lat')
            lng = get('Longitude','Lng','Long')
            try:
                lat = float(lat) if lat else None
                lng = float(lng) if lng else None
            except:
                lat = lng = None

            if not store_id and not name:
                continue

            stores.append({
                'store_id':   (store_id or name)[:50],
                'chain_name': chain_name,
                'name':       name[:200],
                'address':    address[:300],
                'city':       city[:100],
                'zip_code':   zipcode[:20],
                'latitude':   lat,
                'longitude':  lng,
                'phone':      phone[:50],
            })

        return stores
    except Exception as e:
        log.warning(f"  parse error: {e}")
        return []

# ── Scrapers ──────────────────────────────────────────────
async def fetch_shufersal_stores(session, dry_run=False) -> int:
    """שופרסל — Stores.xml ישיר"""
    log.info("\n[Stores] שופרסל")
    urls = [
        "http://prices.shufersal.co.il/FileObject/UpdateCategory?storeId=001&catID=0&FileType=2",
        "http://prices.shufersal.co.il/FileObject/UpdateCategory?catID=0&FileType=2",
    ]
    stores = []
    for url in urls:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    # חפש לינקי Stores
                    store_urls = re.findall(r'(https?://[^\s"\'<>]*Stores?[^\s"\'<>]*\.(?:gz|xml))', text, re.IGNORECASE)
                    for su in store_urls[:3]:
                        async with session.get(su) as sr:
                            if sr.status == 200:
                                content = await sr.read()
                                parsed = parse_stores_xml(content, "שופרסל")
                                stores.extend(parsed)
                    if stores:
                        break
        except Exception as e:
            log.debug(f"  {e}")

    # fallback — נסה URL ישיר
    if not stores:
        direct_urls = [
            "http://prices.shufersal.co.il/FileObject/UpdateCategory?storeId=001&catID=0&FileType=2",
        ]
        for url in direct_urls:
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        content = await resp.read()
                        parsed = parse_stores_xml(content, "שופרסל")
                        stores.extend(parsed)
            except:
                pass

    log.info(f"  נמצאו {len(stores)} חנויות שופרסל")
    return save_stores(stores, dry_run)

async def fetch_bina_stores(session, dry_run=False) -> int:
    """BinaProjects — Stores XML מ-Download.aspx"""
    from playwright.async_api import async_playwright

    BINA_CHAINS = [
        {"name": "קינג סטור",    "subdomain": "kingstore"},
        {"name": "מעיין 2000",   "subdomain": "maayan2000"},
        {"name": "גוד פארם",     "subdomain": "goodpharm"},
        {"name": "זול ובגדול",   "subdomain": "zolvebegadol"},
        {"name": "סופר ספיר",    "subdomain": "supersapir"},
        {"name": "עוף והודו ברקת","subdomain": "superbareket"},
    ]

    total = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        for chain in BINA_CHAINS:
            sub  = chain['subdomain']
            name = chain['name']
            base = f"https://{sub}.binaprojects.com"

            try:
                ctx  = await browser.new_context(ignore_https_errors=True)
                page = await ctx.new_page()
                await page.route("**/*.{png,jpg,jpeg,gif,svg,ico,woff,css}", lambda r: r.abort())
                await page.goto(f"{base}/Main.aspx", wait_until='domcontentloaded', timeout=20000)
                await page.wait_for_load_state('networkidle', timeout=10000)

                html = await page.content()
                # חפש קבצי Stores
                store_files = re.findall(r"Download\(['\"]([^'\"]*Stores?[^'\"]*\.gz)['\"]", html, re.IGNORECASE)

                for fname in store_files[:1]:
                    api_url = f"{base}/Download.aspx?FileNm={fname}"
                    async with session.get(api_url) as r:
                        data = await r.json(content_type=None)
                        if data and 'SPath' in data[0]:
                            real_url = data[0]['SPath']
                            async with session.get(real_url) as sr:
                                if sr.status == 200:
                                    content = await sr.read()
                                    stores = parse_stores_xml(content, name)
                                    log.info(f"  {name}: {len(stores)} חנויות")
                                    total += save_stores(stores, dry_run)

                await ctx.close()
            except Exception as e:
                log.warning(f"  {name}: {e}")

        await browser.close()

    return total

async def fetch_publishedprices_stores(session, dry_run=False) -> int:
    """PublishedPrices — Stores קבצים (אחרי login)"""
    from playwright.async_api import async_playwright

    CHAINS = [
        {"name": "רמי לוי",  "user": "RamiLevi",  "url": "https://url.publishedprices.co.il/login"},
        {"name": "יוחננוף",  "user": "yohananof", "url": "https://url.publishedprices.co.il/login"},
        {"name": "שופרסל",   "user": "",           "url": "http://prices.shufersal.co.il/"},
    ]

    total = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        for chain in CHAINS[:2]:  # רק 2 כדי לחסוך זמן
            try:
                ctx  = await browser.new_context(ignore_https_errors=True)
                page = await ctx.new_page()
                await page.goto(chain['url'], wait_until='domcontentloaded', timeout=20000)

                if chain['user']:
                    await page.fill('input[name="username"], #username, input[type="text"]', chain['user'])
                    await page.click('button[type="submit"], input[type="submit"]')
                    await page.wait_for_load_state('networkidle', timeout=10000)

                html = await page.content()
                # חפש קבצי Stores
                store_links = re.findall(r'href=["\']([^"\']*Stores?[^"\']*\.(?:gz|xml))["\']', html, re.IGNORECASE)

                for link in store_links[:2]:
                    if not link.startswith('http'):
                        link = 'https://url.publishedprices.co.il' + link
                    async with session.get(link) as sr:
                        if sr.status == 200:
                            content = await sr.read()
                            stores = parse_stores_xml(content, chain['name'])
                            log.info(f"  {chain['name']}: {len(stores)} חנויות")
                            total += save_stores(stores, dry_run)

                await ctx.close()
            except Exception as e:
                log.warning(f"  {chain['name']}: {e}")

        await browser.close()

    return total

# ── Main ──────────────────────────────────────────────────
async def run(dry_run=False, test=False):
    import aiohttp

    print(f"\n{'='*55}")
    print(f"  SmartMarket — Stores Fetcher")
    print(f"  {'DRY-RUN' if dry_run else 'LIVE'}")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{'='*55}\n")

    if not dry_run:
        ensure_stores_table()

    total = 0
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False),
        timeout=aiohttp.ClientTimeout(total=60)
    ) as session:
        total += await fetch_shufersal_stores(session, dry_run)
        if not test:
            total += await fetch_bina_stores(session, dry_run)
            total += await fetch_publishedprices_stores(session, dry_run)

    print(f"\n{'='*55}")
    print(f"  ✅ סה\"כ חנויות: {total}")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--test',    action='store_true', help='שופרסל בלבד')
    a = ap.parse_args()
    asyncio.run(run(dry_run=a.dry_run, test=a.test))