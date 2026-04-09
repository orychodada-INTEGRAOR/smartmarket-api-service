"""
playwright_publishedprices.py — SmartMarket
תיקון: ON CONFLICT (barcode, chain_name) במקום (barcode)
"""
import asyncio, gzip, io, os, re
from datetime import datetime, timezone
import requests, urllib3, psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from playwright.async_api import async_playwright
import xml.etree.ElementTree as ET

urllib3.disable_warnings()
load_dotenv()

BATCH_SIZE = 500

PUBLISHED_SOURCES = [
    {"name": "רמי לוי",     "username": "RamiLevi",      "password": ""},
    {"name": "יוחננוף",     "username": "yohananof",     "password": ""},
    {"name": "אושר עד",     "username": "osherad",       "password": ""},
    {"name": "טיב טעם",     "username": "TivTaam",       "password": ""},
    {"name": "פרשמרקט",     "username": "freshmarket",   "password": ""},
    {"name": "קשת טעמים",   "username": "Keshet",        "password": ""},
    {"name": "פוליצר חדרה", "username": "politzer",      "password": ""},
    {"name": "סאלח דבאח",   "username": "SalachD",       "password": "12345"},
    {"name": "סופר קופיקס", "username": "SuperCofixApp", "password": ""},
    {"name": "סטופ מרקט",   "username": "Stop_Market",   "password": "",
     "login_url": "https://url.retail.publishedprices.co.il/login"},
    {"name": "דור אלון",    "username": "doralon",       "password": ""},
    {"name": "פז יילו",     "username": "Paz_bo",        "password": "paz468"},
    {"name": "סופר יודה",   "username": "yuda_ho",       "password": "Yud@147",
     "login_url": "https://publishedprices.co.il/login"},
]

BASE_LOGIN = "https://url.publishedprices.co.il/login"

_pool = None

def _get_pool():
    global _pool
    if _pool is None:
        _pool = pool.SimpleConnectionPool(1, 5, os.getenv("DATABASE_URL"), sslmode="require")
        print("🔌 DB מחובר")
    return _pool

# ── תיקון: chain_name + ON CONFLICT (barcode, chain_name) ────────
def _upsert(rows, chain_name=""):
    if not rows:
        return 0
    now = datetime.now(timezone.utc)

    # dedup לפי ברקוד
    seen = {}
    for r in rows:
        seen[r[0]] = r
    unique = list(seen.values())

    full = [(r[0], r[1], r[2], r[3], chain_name, now, True) for r in unique]
    sql  = """
        INSERT INTO products
            (barcode, name, brand, price, chain_name, updated_at, is_active)
        VALUES %s
        ON CONFLICT (barcode, chain_name) DO UPDATE
        SET price      = EXCLUDED.price,
            name       = EXCLUDED.name,
            brand      = EXCLUDED.brand,
            updated_at = EXCLUDED.updated_at,
            is_active  = TRUE;
    """
    db   = _get_pool()
    conn = db.getconn()
    try:
        for i in range(0, len(full), BATCH_SIZE):
            with conn.cursor() as cur:
                execute_values(cur, sql, full[i:i+BATCH_SIZE])
            conn.commit()
        return len(unique)
    except Exception as e:
        conn.rollback()
        print(f"  DB error: {e}")
        return 0
    finally:
        db.putconn(conn)

def _parse_xml(xml_bytes, brand):
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        print(f"  XML error: {e}")
        return []
    NAME_F    = ["ItemName","ITEM_NAME","item_name"]
    PRICE_F   = ["ItemPrice","ITEM_PRICE","item_price"]
    BARCODE_F = ["ItemCode","ITEM_CODE","Barcode","BARCODE"]
    MFR_F     = ["ManufactureName","ManufacturerName","MANUFACTURER_NAME"]
    def f(el, fields):
        for fn in fields:
            v = el.findtext(fn)
            if v and v.strip():
                return v.strip()
        return ""
    items = (root.findall(".//Item") or root.findall(".//ITEM") or
             root.findall(".//item") or root.findall(".//ItemData"))
    rows = []
    for item in items:
        name    = f(item, NAME_F)
        price_s = f(item, PRICE_F)
        barcode = f(item, BARCODE_F)
        mfr     = f(item, MFR_F) or brand
        if not name or not price_s or not barcode:
            continue
        barcode = re.sub(r"[^\d]", "", barcode)
        if not barcode:
            continue
        try:
            price = float(price_s)
        except:
            continue
        if price <= 0:
            continue
        rows.append((barcode, name[:255], mfr[:100], price))
    return rows

def _download_gz(url, cookies_dict):
    sess = requests.Session()
    sess.verify = False
    sess.headers.update({"User-Agent": "Mozilla/5.0 Chrome/120.0.0.0"})
    for k, v in cookies_dict.items():
        sess.cookies.set(k, v)
    r = sess.get(url, stream=True, timeout=120)
    r.raise_for_status()
    buf = io.BytesIO()
    for chunk in r.iter_content(65536):
        if chunk:
            buf.write(chunk)
    content = buf.getvalue()
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
            return gz.read()
    except:
        return content

async def login_and_get_files(page, login_url, username, password):
    gz_links   = []
    network_gz = []

    def on_response(resp):
        if re.search(r"PriceFull.*\.gz", resp.url, re.IGNORECASE):
            network_gz.append(resp.url)

    page.on("response", on_response)

    print(f"  נכנס לדף: {login_url}")
    await page.goto(login_url, timeout=30000, wait_until="domcontentloaded")
    await asyncio.sleep(2)

    await page.evaluate(f"""
        () => {{
            const inputs = document.querySelectorAll('input');
            for (const inp of inputs) {{
                const t = (inp.type || '').toLowerCase();
                const n = (inp.name || inp.id || inp.placeholder || '').toLowerCase();
                if (t === 'text' || n.includes('user') || n.includes('name') || n === '') {{
                    inp.value = '{username}';
                    inp.dispatchEvent(new Event('input', {{bubbles: true}}));
                    inp.dispatchEvent(new Event('change', {{bubbles: true}}));
                    break;
                }}
            }}
            const pwds = document.querySelectorAll('input[type="password"]');
            for (const pwd of pwds) {{
                pwd.value = '{password}';
                pwd.dispatchEvent(new Event('input', {{bubbles: true}}));
                pwd.dispatchEvent(new Event('change', {{bubbles: true}}));
            }}
        }}
    """)

    await asyncio.sleep(1)

    clicked   = False
    selectors = [
        'form button[type="submit"]',
        'button[ng-click*="login"], button[ng-click*="submit"]',
        '.login-form button',
        'form button:not([data-toggle])',
        'button.btn-primary',
        'input[type="submit"]',
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0 and await btn.is_visible():
                await btn.click(timeout=5000)
                clicked = True
                print(f"  לחץ על: {sel}")
                break
        except:
            continue

    if not clicked:
        print(f"  fallback: Enter")
        await page.keyboard.press("Enter")

    await page.wait_for_load_state("networkidle", timeout=20000)
    await asyncio.sleep(3)

    base      = re.sub(r"/(login|file).*$", "", login_url)
    files_url = base + "/file"
    print(f"  דף קבצים: {files_url}")
    await page.goto(files_url, timeout=30000, wait_until="networkidle")
    await asyncio.sleep(4)

    content = await page.content()
    found   = re.findall(
        r'https?://[^\s"\'<>]+PriceFull[^\s"\'<>]*\.gz',
        content, re.IGNORECASE
    )
    gz_links = list(set(found + network_gz))

    if not gz_links:
        all_text = await page.evaluate("() => document.body.innerText")
        found2   = re.findall(r'https?://\S+PriceFull\S+\.gz', all_text, re.IGNORECASE)
        gz_links = list(set(found2))

    if not gz_links:
        hrefs = await page.evaluate("""
            () => Array.from(document.querySelectorAll('a[href]'))
                       .map(a => a.href)
                       .filter(h => h.includes('PriceFull') && h.includes('.gz'))
        """)
        gz_links = list(set(hrefs))

    gz_links.sort(
        key=lambda u: (re.search(r"(\d{8,12})", u.split("/")[-1]) or
                       type('', (), {'group': lambda s,x: '0'})()).group(1),
        reverse=True
    )
    return gz_links

async def run_all():
    total = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--ignore-certificate-errors",
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ]
        )
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"
        )

        for source in PUBLISHED_SOURCES:
            name      = source["name"]
            username  = source["username"]
            password  = source.get("password", "")
            login_url = source.get("login_url", BASE_LOGIN)

            print(f"\n{'='*40}")
            print(f"רשת: {name} (user: {username})")

            page = await context.new_page()
            try:
                gz_links = await login_and_get_files(page, login_url, username, password)
                print(f"  נמצאו {len(gz_links)} קבצים")

                if not gz_links:
                    print(f"  ריק")
                    await page.close()
                    continue

                cookies      = await context.cookies()
                cookies_dict = {c["name"]: c["value"] for c in cookies}

                count = 0
                for url in gz_links[:3]:
                    print(f"  מוריד: {url.split('/')[-1]}")
                    try:
                        xml_bytes = _download_gz(url, cookies_dict)
                        rows      = _parse_xml(xml_bytes, name)
                        # ← תיקון: מעביר chain_name
                        saved     = _upsert(rows, chain_name=name)
                        count    += saved
                        print(f"  {saved:,} מוצרים")
                    except Exception as e:
                        print(f"  שגיאת הורדה: {e}")

                total += count
                print(f"  סה\"כ {name}: {count:,}")

            except Exception as e:
                print(f"  שגיאה: {e}")
            finally:
                await page.close()

        await browser.close()

    print(f"\n{'='*50}")
    print(f"סה\"כ מוצרים נשמרו: {total:,}")
    print(f"{'='*50}")
    if _pool:
        _pool.closeall()

if __name__ == "__main__":
    asyncio.run(run_all())