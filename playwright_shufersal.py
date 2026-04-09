"""
playwright_shufersal.py — SmartMarket
שואב נתונים משופרסל — Azure Blob Storage עם SAS token.
"""

import asyncio, gzip, io, re, os, logging, sys, requests
from datetime import datetime, timezone
from pathlib import Path
from playwright.async_api import async_playwright
from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
import xml.etree.ElementTree as ET

load_dotenv()

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            LOG_DIR / f"shufersal_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
            encoding="utf-8"
        ),
    ]
)
log = logging.getLogger("shufersal")

CHAIN_NAME = "שופרסל"
BASE_URL   = "http://prices.shufersal.co.il"
MAX_FILES  = 80
BATCH_SIZE = 500
HEADERS    = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"}

_pool = None

def get_pool():
    global _pool
    if _pool is None:
        _pool = pool.SimpleConnectionPool(1, 5, os.getenv("DATABASE_URL"))
        log.info("DB מחובר")
    return _pool

def dedup(rows):
    seen = {}
    for r in rows:
        seen[r[0]] = r
    return list(seen.values())

def upsert(rows, chain_name):
    if not rows:
        return 0
    unique = dedup(rows)
    now = datetime.now(timezone.utc)
    db = get_pool()

    full = [(r[0], r[1], r[2], r[3], chain_name, now, True) for r in unique]
    sql = """
        INSERT INTO products (barcode, name, brand, price, chain_name, updated_at, is_active)
        VALUES %s
        ON CONFLICT (barcode, chain_name) DO UPDATE
        SET price=EXCLUDED.price, name=EXCLUDED.name,
            brand=EXCLUDED.brand, updated_at=EXCLUDED.updated_at, is_active=TRUE
    """
    sql_fallback = """
        INSERT INTO products (barcode, name, brand, price, updated_at, is_active)
        VALUES %s
        ON CONFLICT (barcode) DO UPDATE
        SET price=EXCLUDED.price, name=EXCLUDED.name,
            updated_at=EXCLUDED.updated_at, is_active=TRUE
    """

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        conn = None
        try:
            conn = db.getconn()
            if getattr(conn, "closed", 0):
                db.putconn(conn, close=True)
                conn = db.getconn()

            for i in range(0, len(full), BATCH_SIZE):
                with conn.cursor() as cur:
                    execute_values(cur, sql, full[i:i+BATCH_SIZE])
                conn.commit()
            return len(unique)
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            if conn is not None:
                try:
                    db.putconn(conn, close=True)
                except Exception:
                    pass
                conn = None
            log.warning(f"DB connection dropped (attempt {attempt}/{max_attempts}): {e}")
            if attempt == max_attempts:
                break
        except Exception as e:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            # Keep legacy fallback path without chain_name conflict key.
            try:
                full2 = [(r[0], r[1], r[2], r[3], now, True) for r in unique]
                for i in range(0, len(full2), BATCH_SIZE):
                    with conn.cursor() as cur:
                        execute_values(cur, sql_fallback, full2[i:i+BATCH_SIZE])
                    conn.commit()
                return len(unique)
            except Exception as e2:
                if conn is not None:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                log.error(f"DB error: {e}; fallback failed: {e2}")
                return 0
        finally:
            if conn is not None:
                try:
                    db.putconn(conn)
                except Exception:
                    pass

    log.error("DB error: connection closed after retries")
    return 0

def parse_xml(data: bytes) -> list:
    try:
        with gzip.open(io.BytesIO(data), "rb") as f:
            raw = f.read()
    except Exception:
        raw = data
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        log.warning(f"XML error: {e}")
        return []
    rows = []
    for item in root.iter("Item"):
        def f(tags):
            for t in tags:
                v = item.findtext(t)
                if v and v.strip():
                    return v.strip()
            return ""
        barcode = re.sub(r"[^\d]", "", f(["ItemCode","Barcode","ItemId"]))
        name    = f(["ItemName","Name"])[:255]
        price_s = f(["ItemPrice","Price"])
        brand   = f(["ManufacturerName","ManufactureName","BrandName"])[:100]
        if not barcode or not name:
            continue
        try:
            price = float(price_s)
        except (ValueError, TypeError):
            continue
        if price <= 0:
            continue
        rows.append((barcode, name, brand, price))
    return rows

async def get_download_links(page, url):
    try:
        await page.goto(url, timeout=30000, wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)
        links = await page.evaluate("""
            () => {
                const links = [];
                document.querySelectorAll('a[href]').forEach(a => {
                    const href = a.href;
                    if (href && (href.includes('.gz') || href.includes('download') ||
                        href.includes('Download') || href.includes('FileObject'))) {
                        links.push(href);
                    }
                });
                document.querySelectorAll('[onclick]').forEach(el => {
                    const onclick = el.getAttribute('onclick');
                    if (onclick && onclick.includes('.gz')) {
                        links.push(onclick);
                    }
                });
                return links;
            }
        """)
        content = await page.content()
        gz_links = set()
        for m in re.finditer(r'https?://[a-z0-9]+\.blob\.core\.windows\.net[^\s"\'<>]+\.gz', content, re.IGNORECASE):
            gz_links.add(m.group(0))
        for m in re.finditer(r'https?://[^\s"\'<>]+PriceFull[^\s"\'<>]*\.gz', content, re.IGNORECASE):
            gz_links.add(m.group(0))
        for m in re.finditer(r'href=["\']([^"\']*\.gz)["\']', content, re.IGNORECASE):
            href = m.group(1)
            if href.startswith("http"):
                gz_links.add(href)
            else:
                gz_links.add(BASE_URL + "/" + href.lstrip("/"))
        for link in links:
            if ".gz" in link.lower():
                gz_links.add(link)
        return list(gz_links)
    except Exception as e:
        log.error(f"  שגיאה: {e}")
        return []

def get_shufersal_links_direct() -> list:
    links = []
    session = requests.Session()
    session.headers.update(HEADERS)
    for store_id in range(1, 20):
        url = f"{BASE_URL}/FileObject/UpdateCategory?storeId={store_id:03d}&catID=0&FileType=0"
        try:
            r = session.get(url, timeout=15)
            if r.status_code == 200:
                content = r.text
                for m in re.finditer(r'https?://[^\s"\'<>]+\.gz', content, re.IGNORECASE):
                    link = m.group(0)
                    if "PriceFull" in link or "pricefull" in link.lower():
                        links.append(link)
                for m in re.finditer(r'(PriceFull[^\s"\'<>]*\.gz)', content, re.IGNORECASE):
                    fname = m.group(1)
                    links.append(f"https://pricesprodpublic.blob.core.windows.net/pricefull/{fname}")
        except Exception:
            continue
    return list(set(links))

def try_download(url: str) -> bytes | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=60)
        r.raise_for_status()
        return r.content
    except Exception:
        if "blob.core.windows.net" in url:
            fname = url.split("/")[-1].split("?")[0]
            alt_url = f"{BASE_URL}/FileObject/UpdateCategory/{fname}"
            try:
                r = requests.get(alt_url, headers=HEADERS, timeout=60)
                r.raise_for_status()
                return r.content
            except Exception:
                pass
        return None

async def scrape_shufersal():
    total = 0
    log.info("="*55)
    log.info(f"רשת: {CHAIN_NAME}")

    all_links = set()

    # שיטה 1: Playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=HEADERS["User-Agent"])
        page    = await context.new_page()

        urls = [
            f"{BASE_URL}/",
            f"{BASE_URL}/FileObject/UpdateCategory?storeId=001",
            f"{BASE_URL}/FileObject/UpdateCategory?storeId=002",
            f"{BASE_URL}/FileObject/UpdateCategory?storeId=001&catID=0&FileType=0",
        ]
        for url in urls:
            log.info(f"  סורק: {url}")
            found = await get_download_links(page, url)
            all_links.update(found)

        await browser.close()

    # שיטה 2: requests ישיר
    log.info("  מנסה שיטה ישירה...")
    all_links.update(get_shufersal_links_direct())

    # שיטה 3: API ידוע
    log.info("  מנסה API ידוע...")
    for store_id in range(1, 4):
        api_url = f"{BASE_URL}/FileObject/UpdateCategory?storeId={store_id:03d}&catID=0&FileType=0"
        try:
            r = requests.get(api_url, headers=HEADERS, timeout=15)
            for m in re.finditer(r'(PriceFull\S+\.gz)', r.text):
                fname = m.group(1).strip('"\'')
                all_links.add(f"https://pricesprodpublic.blob.core.windows.net/pricefull/{fname}")
        except Exception:
            continue

    log.info(f"  סה\"כ קישורים שנמצאו: {len(all_links)}")

    if not all_links:
        log.warning("  לא נמצאו קישורים.")
        return 0

    # ← תיקון IndentationError: השורה הזו בתוך הפונקציה
    # השאר רק קישורים עם SAS token (עובדים בוודאות)
    all_links = {u for u in all_links if "sig=" in u or "sv=" in u
                 or "blob.core.windows.net" not in u}

    # מיין לפי תאריך — הכי חדש ראשון
    def date_key(u):
        m = re.search(r"(\d{8,12})", u.split("/")[-1])
        return m.group(1) if m else "0"

    sorted_links = sorted(all_links, key=date_key, reverse=True)
    log.info(f"  קישורים תקינים: {len(sorted_links)}")

    # הורד קבצים
    for gz_url in sorted_links[:MAX_FILES]:
        fname = gz_url.split("/")[-1].split("?")[0]
        log.info(f"  מוריד: {fname}")
        data = try_download(gz_url)
        if data:
            rows  = parse_xml(data)
            saved = upsert(rows, CHAIN_NAME)
            total += saved
            log.info(f"  {saved:,} מוצרים")
        else:
            log.warning(f"  נכשל: {gz_url[:60]}")

    log.info(f"  סה\"כ {CHAIN_NAME}: {total:,}")
    log.info("="*55)
    return total

if __name__ == "__main__":
    saved = asyncio.run(scrape_shufersal())
    print(f"\nסיום — {saved:,} מוצרים נשמרו מ{CHAIN_NAME}")