"""
promotions_engine.py — PromoFull*.gz מ-PublishedPrices + Bina → טבלת promotions
===============================================================================
לא משנה את playwright_publishedprices.py / playwright_bina.py — משכפל דפוסי URL/התחברות.

  python promotions_engine.py        # הרצה ידנית
"""

from __future__ import annotations

import asyncio
import gzip
import io
import logging
import os
import re
import sys
from datetime import date, datetime, timezone
from typing import Any, BinaryIO, TextIO
from xml.etree import ElementTree as ET

from dotenv import load_dotenv
from playwright.async_api import async_playwright
import psycopg2
from psycopg2 import errors as pg_errors
from psycopg2.extras import execute_batch

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
LOG = logging.getLogger("promotions_engine")

ROOT = os.path.dirname(os.path.abspath(__file__))

# ── אותם מקורות כמו playwright_publishedprices (קריאה בלבד; הרשימה משוכפלת) ──
PUBLISHED_SOURCES = [
    {"name": "רמי לוי", "username": "RamiLevi", "password": ""},
    {"name": "יוחננוף", "username": "yohananof", "password": ""},
    {"name": "אושר עד", "username": "osherad", "password": ""},
    {"name": "טיב טעם", "username": "TivTaam", "password": ""},
    {"name": "פרשמרקט", "username": "freshmarket", "password": ""},
    {"name": "קשת טעמים", "username": "Keshet", "password": ""},
    {"name": "פוליצר חדרה", "username": "politzer", "password": ""},
    {"name": "סאלח דבאח", "username": "SalachD", "password": "12345"},
    {"name": "סופר קופיקס", "username": "SuperCofixApp", "password": ""},
    {"name": "סטופ מרקט", "username": "Stop_Market", "password": "",
     "login_url": "https://url.retail.publishedprices.co.il/login"},
    {"name": "דור אלון", "username": "doralon", "password": ""},
    {"name": "פז יילו", "username": "Paz_bo", "password": "paz468"},
    {"name": "סופר יודה", "username": "yuda_ho", "password": "Yud@147",
     "login_url": "https://publishedprices.co.il/login"},
]

BASE_LOGIN = "https://url.publishedprices.co.il/login"

# ── Bina: אותם subdomains כמו playwright_bina ──
BINA_CHAINS = [
    {"name": "קינג סטור", "subdomain": "kingstore"},
    {"name": "מעיין 2000", "subdomain": "maayan2000"},
    {"name": "גוד פארם", "subdomain": "goodpharm"},
    {"name": "זול ובגדול", "subdomain": "zolvebegadol"},
    {"name": "סופר ספיר", "subdomain": "supersapir"},
    {"name": "עוף והודו ברקת", "subdomain": "superbareket"},
    {"name": "סיטי מרקט קרית גת", "subdomain": "citymarketkiryatgat"},
    {"name": "קיי.טי יבוא ושיווק", "subdomain": "ktshivuk"},
    {"name": "שוק העיר", "subdomain": "shuk-hayir"},
    {"name": "שפע ברכת השם", "subdomain": "shefabirkathashem"},
]


def _log_line(fp: TextIO | None, msg: str) -> None:
    LOG.info(msg)
    if fp:
        fp.write(msg + "\n")


def _local_tag(tag: str) -> str:
    return tag.split("}")[-1].lower()


def _txt(el: ET.Element | None, *names: str) -> str:
    if el is None:
        return ""
    for n in names:
        for child in el:
            if _local_tag(child.tag) == n.lower():
                t = (child.text or "").strip()
                if t:
                    return t
        v = el.findtext(n)
        if v and str(v).strip():
            return str(v).strip()
    return ""


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    s = str(s).strip()[:32]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y"):
        try:
            return datetime.strptime(s[:19].replace("T", " ")[:10], fmt[: len(fmt)]).date()
        except ValueError:
            continue
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    return None


def _parse_float(s: str | None) -> float | None:
    if not s:
        return None
    try:
        return float(re.sub(r"[^\d.\-]", "", str(s).replace(",", ".")))
    except ValueError:
        return None


def decompress_xml(content: bytes) -> str | None:
    if not content:
        return None
    raw = content
    if raw[:2] == b"\x1f\x8b":
        try:
            raw = gzip.decompress(raw)
        except OSError:
            return None
    for enc in ("utf-8", "windows-1255", "iso-8859-8"):
        try:
            return raw.decode(enc).lstrip("\ufeff")
        except UnicodeDecodeError:
            continue
    return None


def _deep_first_text(root_el: ET.Element, *names: str) -> str:
    """First non-empty text among descendants whose local tag matches one of `names`."""
    want = {n.lower() for n in names}
    for el in root_el.iter():
        if _local_tag(el.tag) not in want:
            continue
        t = (el.text or "").strip()
        if t:
            return t
    return ""


def _gather_promo_candidates(root: ET.Element) -> list[ET.Element]:
    """PromoFull XML varies by chain; collect likely record elements."""
    promo_tags = frozenset(
        {
            "promotion",
            "promo",
            "sale",
            "special",
            "item",
            "row",
            "record",
            "product",
            "line",
            "saleline",
            "promoline",
            "promotionitem",
            "promotionline",
            "benefit",
        }
    )
    code_child_tags = frozenset(
        {
            "itemcode",
            "item_code",
            "barcode",
            "code",
            "productcode",
            "itemid",
        }
    )
    candidates: list[ET.Element] = []
    seen: set[int] = set()
    for el in root.iter():
        lt = _local_tag(el.tag)
        if lt in promo_tags:
            if id(el) not in seen:
                seen.add(id(el))
                candidates.append(el)
            continue
        for ch in el:
            if _local_tag(ch.tag) in code_child_tags and (ch.text or "").strip():
                if id(el) not in seen:
                    seen.add(id(el))
                    candidates.append(el)
                break
    return candidates


def parse_promo_xml(text: str, chain_name: str, source_file: str, loaded_at: datetime) -> list[dict[str, Any]]:
    """מחלץ רשומות מבצע מ-PromoFull / מבנים דומים."""
    rows: list[dict[str, Any]] = []
    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        LOG.warning("XML parse error %s: %s", source_file, e)
        return rows

    candidates = _gather_promo_candidates(root)
    if not candidates:
        for el in root.iter():
            if _local_tag(el.tag) in ("item", "row", "record"):
                if _txt(el, "PromotionDescription", "PromotionId", "DiscountRate"):
                    candidates.append(el)

    seen: set[tuple[str, str, str, str]] = set()
    for el in candidates:
        code = (
            _txt(el, "ItemCode", "ITEM_CODE", "itemcode", "Barcode", "barcode", "ItemId")
            or _deep_first_text(
                el,
                "ItemCode",
                "ITEM_CODE",
                "itemcode",
                "Barcode",
                "barcode",
                "ItemId",
                "ProductCode",
            )
            or el.get("ItemCode")
            or el.get("itemcode")
            or ""
        )
        name = (
            _txt(el, "ItemName", "ItemNm", "ITEM_NAME", "Name", "itemname")
            or _deep_first_text(el, "ItemName", "ItemNm", "ITEM_NAME", "Name", "ProductName")
            or ""
        )
        desc = _txt(
            el,
            "PromotionDescription",
            "Description",
            "Memo",
            "PromotionDetails",
            "Remark",
        ) or _deep_first_text(
            el,
            "PromotionDescription",
            "Description",
            "Memo",
            "PromotionDetails",
            "Remark",
        )
        sd = _parse_date(
            _txt(el, "StartDate", "PromotionStartDate", "FromDate", "startdate")
            or _deep_first_text(el, "StartDate", "PromotionStartDate", "FromDate")
        )
        ed = _parse_date(
            _txt(el, "EndDate", "PromotionEndDate", "ToDate", "enddate")
            or _deep_first_text(el, "EndDate", "PromotionEndDate", "ToDate")
        )
        min_q = _parse_float(
            _txt(el, "MinQty", "MinimumQuantity", "MinPurchaseQty", "MinQTY")
            or _deep_first_text(el, "MinQty", "MinimumQuantity", "MinPurchaseQty", "MinQTY")
        )
        min_qty_int: int | None = None
        if min_q is not None:
            try:
                min_qty_int = max(1, int(round(float(min_q))))
            except (TypeError, ValueError):
                min_qty_int = None
        disc = _parse_float(
            _txt(el, "DiscountRate", "DiscountPercent", "PromotionDiscount", "Discount")
            or _deep_first_text(
                el,
                "DiscountRate",
                "DiscountPercent",
                "PromotionDiscount",
                "Discount",
            )
        )
        if not code and not name:
            continue
        sd_d = sd or date(1970, 1, 1)
        ed_d = ed or date(1970, 1, 1)
        key = (chain_name, str(code)[:200], str(sd_d), str(ed_d))
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "chain_name": chain_name[:500],
                "item_code": str(code)[:200] if code else "unknown",
                "item_name": (name or "")[:2000],
                "promo_description": (desc or "")[:4000],
                "start_date": sd_d,
                "end_date": ed_d,
                "min_qty": min_qty_int,
                "discount_rate": disc,
                "source_file": source_file[:500],
                "loaded_at": loaded_at,
            }
        )
    return rows


def _table_exists(cur, name: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (name,),
    )
    return cur.fetchone() is not None


def _column_exists(cur, table: str, col: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
        """,
        (table, col),
    )
    return cur.fetchone() is not None


def _relax_legacy_not_null(cur) -> None:
    """Parser-era `promotions` may have NOT NULL columns we do not fill (e.g. promotion_id)."""
    if not _table_exists(cur, "promotions"):
        return
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'promotions'
          AND is_nullable = 'NO'
          AND column_name IN (
              'promotion_id',
              'discount_type',
              'description'
          )
        """
    )
    for (col,) in cur.fetchall() or []:
        try:
            cur.execute(
                f"ALTER TABLE promotions ALTER COLUMN {col} DROP NOT NULL"
            )
        except psycopg2.Error as e:
            LOG.warning("promotions DROP NOT NULL %s: %s", col, e)


def _migrate_legacy_promotions(cur) -> None:
    """Old DBs may have a promotions table from another module without chain_name — add columns."""
    if not _table_exists(cur, "promotions"):
        return
    alters = [
        ("chain_name", "TEXT"),
        ("item_code", "TEXT"),
        ("item_name", "TEXT"),
        ("promo_description", "TEXT"),
        ("start_date", "DATE"),
        ("end_date", "DATE"),
        ("min_qty", "INTEGER DEFAULT 1"),
        ("discount_rate", "NUMERIC"),
        ("source_file", "TEXT"),
        ("loaded_at", "TIMESTAMP DEFAULT NOW()"),
    ]
    for col, ddl in alters:
        if _column_exists(cur, "promotions", col):
            continue
        try:
            cur.execute(f"ALTER TABLE promotions ADD COLUMN {col} {ddl}")
        except pg_errors.DuplicateColumn:
            pass
        except psycopg2.Error as e:
            LOG.warning("promotions ADD COLUMN %s: %s", col, e)


def ensure_promotions_table(conn) -> None:
    """
    Schema aligned with Supabase / manual DDL:
      UNIQUE (chain_name, item_code, start_date, end_date)
    If an older promotions table exists (e.g. without chain_name), columns are added.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS promotions (
                id SERIAL PRIMARY KEY,
                chain_name TEXT NOT NULL,
                item_code TEXT,
                item_name TEXT,
                promo_description TEXT,
                start_date DATE,
                end_date DATE,
                min_qty INTEGER DEFAULT 1,
                discount_rate NUMERIC,
                source_file TEXT,
                loaded_at TIMESTAMP DEFAULT NOW(),
                UNIQUE (chain_name, item_code, start_date, end_date)
            )
            """
        )
        _migrate_legacy_promotions(cur)
        try:
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS promotions_chain_item_dates_uidx
                ON promotions (chain_name, item_code, start_date, end_date)
                """
            )
        except Exception as e:
            LOG.warning("promotions unique index: %s", e)
        _relax_legacy_not_null(cur)
    conn.commit()


def upsert_promotions(conn, batch: list[dict[str, Any]]) -> int:
    if not batch:
        return 0
    for r in batch:
        if r.get("min_qty") is None:
            r["min_qty"] = 1
    with conn.cursor() as cur:
        execute_batch(
            cur,
            """
            INSERT INTO promotions
              (chain_name, item_code, item_name, promo_description,
               start_date, end_date, min_qty, discount_rate, source_file, loaded_at)
            VALUES
              (%(chain_name)s, %(item_code)s, %(item_name)s, %(promo_description)s,
               %(start_date)s, %(end_date)s, %(min_qty)s, %(discount_rate)s, %(source_file)s, %(loaded_at)s)
            ON CONFLICT (chain_name, item_code, start_date, end_date) DO UPDATE SET
              item_name = EXCLUDED.item_name,
              promo_description = EXCLUDED.promo_description,
              min_qty = EXCLUDED.min_qty,
              discount_rate = EXCLUDED.discount_rate,
              source_file = EXCLUDED.source_file,
              loaded_at = EXCLUDED.loaded_at
            """,
            batch,
            page_size=300,
        )
    conn.commit()
    return len(batch)


def _download_gz_requests(url: str, cookies_dict: dict[str, str]) -> bytes:
    import requests
    import urllib3

    urllib3.disable_warnings()
    sess = requests.Session()
    sess.verify = False
    sess.headers.update({"User-Agent": "Mozilla/5.0 Chrome/120.0.0.0 SmartMarket-Promotions"})
    for k, v in cookies_dict.items():
        sess.cookies.set(k, v)
    r = sess.get(url, stream=True, timeout=180)
    r.raise_for_status()
    buf = io.BytesIO()
    for chunk in r.iter_content(65536):
        if chunk:
            buf.write(chunk)
    content = buf.getvalue()
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
            return gz.read()
    except OSError:
        return content


async def login_and_get_promo_gz_links(page, login_url: str, username: str, password: str) -> list[str]:
    """כמו playwright_publishedprices.login_and_get_files — אך PromoFull במקום PriceFull."""
    network_gz: list[str] = []

    def on_response(resp):
        if re.search(r"PromoFull.*\.gz", resp.url, re.IGNORECASE):
            network_gz.append(resp.url)

    page.on("response", on_response)

    await page.goto(login_url, timeout=30000, wait_until="domcontentloaded")
    await asyncio.sleep(2)

    await page.evaluate(
        f"""
        () => {{
            const inputs = document.querySelectorAll('input');
            for (const inp of inputs) {{
                const t = (inp.type || '').toLowerCase();
                const n = (inp.name || inp.id || inp.placeholder || '').toLowerCase();
                if (t === 'text' || n.includes('user') || n.includes('name') || n === '') {{
                    inp.value = {username!r};
                    inp.dispatchEvent(new Event('input', {{bubbles: true}}));
                    inp.dispatchEvent(new Event('change', {{bubbles: true}}));
                    break;
                }}
            }}
            const pwds = document.querySelectorAll('input[type="password"]');
            for (const pwd of pwds) {{
                pwd.value = {password!r};
                pwd.dispatchEvent(new Event('input', {{bubbles: true}}));
                pwd.dispatchEvent(new Event('change', {{bubbles: true}}));
            }}
        }}
    """
    )

    await asyncio.sleep(1)
    for sel in (
        'form button[type="submit"]',
        'button[ng-click*="login"]',
        ".login-form button",
        "form button:not([data-toggle])",
        "button.btn-primary",
        'input[type="submit"]',
    ):
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0 and await btn.is_visible():
                await btn.click(timeout=5000)
                break
        except Exception:
            continue
    else:
        await page.keyboard.press("Enter")

    await page.wait_for_load_state("networkidle", timeout=20000)
    await asyncio.sleep(3)

    base = re.sub(r"/(login|file).*$", "", login_url)
    files_url = base + "/file"
    await page.goto(files_url, timeout=30000, wait_until="networkidle")
    await asyncio.sleep(4)

    content = await page.content()
    found = re.findall(r'https?://[^\s"\'<>]+PromoFull[^\s"\'<>]*\.gz', content, re.IGNORECASE)
    gz_links = list(set(found + network_gz))

    if not gz_links:
        all_text = await page.evaluate("() => document.body.innerText")
        found2 = re.findall(r"https?://\S+PromoFull\S+\.gz", all_text, re.IGNORECASE)
        gz_links = list(set(found2))

    if not gz_links:
        hrefs = await page.evaluate(
            """
            () => Array.from(document.querySelectorAll('a[href]'))
                .map(a => a.href)
                .filter(h => h.includes('PromoFull') && h.includes('.gz'))
        """
        )
        gz_links = list(set(hrefs))

    def _sort_key(u: str) -> str:
        m = re.search(r"(\d{8,12})", u.split("/")[-1])
        return m.group(1) if m else "0"

    gz_links.sort(key=_sort_key, reverse=True)
    return gz_links


async def run_published_promos(conn, loaded_at: datetime, log_fp: TextIO | None, max_files_per_chain: int = 2) -> int:
    total_rows = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--ignore-certificate-errors", "--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
        )

        for source in PUBLISHED_SOURCES:
            name = source["name"]
            username = source["username"]
            password = source.get("password", "")
            login_url = source.get("login_url", BASE_LOGIN)
            page = await context.new_page()
            try:
                links = await login_and_get_promo_gz_links(page, login_url, username, password)
                _log_line(log_fp, f"  [PP] {name}: נמצאו {len(links)} קישורי PromoFull")
                if not links:
                    continue
                cookies = await context.cookies()
                cookies_dict = {c["name"]: c["value"] for c in cookies}
                for url in links[:max_files_per_chain]:
                    fn = url.split("/")[-1][:200]
                    try:
                        xml_bytes = _download_gz_requests(url, cookies_dict)
                        text = decompress_xml(xml_bytes)
                        if not text:
                            continue
                        batch = parse_promo_xml(text, name, fn, loaded_at)
                        if batch:
                            total_rows += upsert_promotions(conn, batch)
                            _log_line(log_fp, f"    נשמרו {len(batch)} מבצעים מ-{fn}")
                    except Exception as e:
                        _log_line(log_fp, f"    שגיאה {fn}: {e}")
                    await asyncio.sleep(1.0)
            except Exception as e:
                _log_line(log_fp, f"  [PP] שגיאה {name}: {e}")
            finally:
                await page.close()

        await browser.close()
    return total_rows


async def resolve_bina_download_url(session, base: str, filename: str) -> str | None:
    import aiohttp

    api_url = f"{base}/Download.aspx?FileNm={filename}"
    try:
        async with session.get(api_url, headers={"User-Agent": "Mozilla/5.0"}) as resp:
            if resp.status == 200:
                data = await resp.json(content_type=None)
                if data and isinstance(data, list) and data and "SPath" in data[0]:
                    return data[0]["SPath"]
    except Exception as e:
        LOG.debug("bina resolve %s", e)
    return None


async def run_bina_promos(conn, loaded_at: datetime, log_fp: TextIO | None, max_files: int = 2) -> int:
    import aiohttp

    total_rows = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        for chain in BINA_CHAINS:
            name = chain["name"]
            base = f"https://{chain['subdomain']}.binaprojects.com"
            url = f"{base}/Main.aspx"
            filenames: list[str] = []
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                ignore_https_errors=True,
            )
            page = await ctx.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=25000)
                await page.wait_for_load_state("networkidle", timeout=15000)
                await page.wait_for_timeout(1000)
                html = await page.content()
                filenames = re.findall(r"Download\(['\"]([^'\"]+\.gz)['\"]", html)
                filenames += re.findall(r"Download\(['\"]([^'\"]+\.xml)['\"]", html)
                filenames = list(dict.fromkeys(filenames))
            except Exception as e:
                _log_line(log_fp, f"  [Bina] {name}: דף Main נכשל: {e}")
            finally:
                await page.close()
                await ctx.close()

            promo_files = [f for f in filenames if "promofull" in f.lower()]
            if not promo_files:
                promo_files = [f for f in filenames if "promo" in f.lower()]
            to_fetch = promo_files[:max_files] if promo_files else []

            if not to_fetch:
                _log_line(log_fp, f"  [Bina] {name}: אין PromoFull ברשימה ({len(filenames)} קבצים)")
                continue

            async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=False),
                timeout=aiohttp.ClientTimeout(total=120),
            ) as session:
                for fname in to_fetch:
                    spath = await resolve_bina_download_url(session, base, fname)
                    if not spath:
                        continue
                    try:
                        async with session.get(spath) as resp:
                            body = await resp.read()
                        text = decompress_xml(body)
                        if not text:
                            continue
                        batch = parse_promo_xml(text, name, fname, loaded_at)
                        if batch:
                            total_rows += upsert_promotions(conn, batch)
                            _log_line(log_fp, f"    [Bina] {name} {fname}: {len(batch)} מבצעים")
                    except Exception as e:
                        _log_line(log_fp, f"    שגיאה {fname}: {e}")
                    await asyncio.sleep(1.0)

        await browser.close()
    return total_rows


def run_promotions(log_fp: TextIO | None = None) -> int:
    """
    מוריד PromoFull מ-PublishedPrices + Bina, מפרס ל-promotions.
    מחזיר מספר רשומות שנוספו/עודכנו (הערכה כוללת upsert).
    """
    if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        _log_line(log_fp, "DATABASE_URL חסר — מדלג על promotions_engine")
        return 0

    loaded_at = datetime.now(timezone.utc)
    conn = psycopg2.connect(db_url, sslmode="require")
    ensure_promotions_table(conn)

    async def _run():
        n1 = await run_published_promos(conn, loaded_at, log_fp)
        n2 = await run_bina_promos(conn, loaded_at, log_fp)
        return n1 + n2

    try:
        total = asyncio.run(_run())
        _log_line(log_fp, f"✅ promotions_engine הושלם — כ-{total} רשומות בעיבוד (upsert)")
        return total
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    n = run_promotions()
    print(f"Done, rows touched ~ {n}")
