"""
human_browser.py — human-like product search (httpx + BeautifulSoup, optional Playwright)
========================================================================================
Additional tool for sites not covered by main scrapers — not a replacement.

  from factory.human_browser import search_products
  products = await search_products("ramilevi.co.il", "חלב")

JS-heavy pages: tries ``factory.playwright_engine.search_products_playwright`` if present,
otherwise runs a built-in Playwright fallback.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlparse, urlunparse

import httpx
from bs4 import BeautifulSoup

LOG = logging.getLogger("human_browser")

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 SmartMarket-HumanBrowser/1.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "he,en-US;q=0.9,en;q=0.8",
}

PRICE_RE = re.compile(
    r"(?:₪|NIS|ILS)?\s*(\d{1,5}(?:[.,]\d{1,3})?)",
    re.IGNORECASE,
)


@dataclass
class HumanProduct:
    name: str
    price: float | None
    image_url: str | None
    product_url: str | None


def _normalize_base(store_url: str) -> str:
    s = (store_url or "").strip()
    if not s.lower().startswith(("http://", "https://")):
        s = "https://" + s
    p = urlparse(s)
    if not p.netloc:
        raise ValueError(f"Invalid store URL: {store_url!r}")
    return urlunparse((p.scheme or "https", p.netloc, "", "", "", "")).rstrip("/")


def _is_likely_js_spa(html: str) -> bool:
    low = html.lower()
    if "__next_data__" in low or 'id="root"' in low and len(html) > 5000:
        if html.count("<script") > 8 and len(BeautifulSoup(html, "html.parser").get_text(strip=True)) < 400:
            return True
    if "data-reactroot" in low or 'id="__next"' in low:
        t = BeautifulSoup(html, "html.parser").get_text(strip=True)
        if len(t) < 350:
            return True
    return False


def _search_url_from_homepage(soup: BeautifulSoup, base: str, term: str) -> str | None:
    """Derive search URL from a homepage form + common search inputs."""
    inputs = soup.select(
        'input[type="search"], input[name="q"], input[name="query"], input[name="search"], '
        'input[name="s"], input#search, #search input, .search-input, [class*="search"] input[type="text"]'
    )
    seen: set[str] = set()
    for inp in inputs:
        form = inp.find_parent("form")
        if not form:
            continue
        action = form.get("action") or "/"
        method = (form.get("method") or "get").lower().strip()
        abs_action = urljoin(base + "/", action)
        name = inp.get("name") or "q"
        if method != "get":
            continue
        p = urlparse(abs_action)
        q = dict(parse_qsl(p.query, keep_blank_values=True))
        q[name] = term
        new_query = urlencode(q, doseq=True)
        built = urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))
        if built not in seen:
            seen.add(built)
            return built
    return None


def _guess_search_urls(base: str, term: str) -> list[str]:
    q = quote(term, safe="")
    paths = [
        f"{base}/search?q={q}",
        f"{base}/search/?q={q}",
        f"{base}/?s={q}",
        f"{base}/catalogsearch/result/?q={q}",
        f"{base}/catalogsearch/result?q={q}",
        f"{base}/products/search?q={q}",
        f"{base}/shop?s={q}",
        f"{base}/חיפוש?q={q}",
    ]
    out: list[str] = []
    seen: set[str] = set()
    for u in paths:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _extract_price(text: str) -> float | None:
    if not text:
        return None
    m = PRICE_RE.search(text.replace("\xa0", " "))
    if not m:
        return None
    raw = m.group(1).replace(",", ".")
    try:
        v = float(raw)
        return v if 0 < v < 100_000 else None
    except ValueError:
        return None


def _parse_product_cards(soup: BeautifulSoup, base: str, max_results: int) -> list[HumanProduct]:
    """Heuristic extraction of product cards from a search / listing page."""
    candidates = soup.select(
        '[class*="product"], .product-item, .product-card, li.product, article.product, '
        "[data-product], .grid-item, .catalog-item, article"
    )
    products: list[HumanProduct] = []
    seen: set[str] = set()

    for node in candidates:
        if len(products) >= max_results:
            break
        try:
            title_el = node.select_one("h2 a, h3 a, h4 a, a.product-name, .product-title a, a[href*='/product']")
            if not title_el:
                title_el = node.select_one("h2, h3, .title, .product-title")
            name = (title_el.get_text(strip=True) if title_el else "") or ""
            if len(name) < 2:
                continue

            link_el = title_el if title_el and title_el.name == "a" else node.select_one("a[href]")
            href = link_el.get("href") if link_el else None
            product_url = urljoin(base + "/", href) if href else None
            key = product_url or name
            if key in seen:
                continue
            seen.add(key)

            price_el = node.select_one(
                "[class*='price'], .price, .product-price, [data-price], .amount, ins .amount"
            )
            price_txt = price_el.get_text(" ", strip=True) if price_el else node.get_text(" ", strip=True)
            price = _extract_price(price_txt)

            img_el = node.select_one("img[src], img[data-src]")
            image_url = None
            if img_el:
                image_url = img_el.get("data-src") or img_el.get("src")
                if image_url:
                    image_url = urljoin(base + "/", image_url)

            products.append(
                HumanProduct(
                    name=name[:500],
                    price=price,
                    image_url=image_url,
                    product_url=product_url,
                )
            )
        except Exception:
            continue

    return products


async def _fetch_text(client: httpx.AsyncClient, url: str) -> tuple[int, str]:
    r = await client.get(url, follow_redirects=True)
    return r.status_code, r.text


async def _static_search(base: str, term: str, max_results: int) -> list[HumanProduct]:
    async with httpx.AsyncClient(
        headers=DEFAULT_HEADERS,
        timeout=httpx.Timeout(35.0),
        follow_redirects=True,
    ) as client:
        code, home = await _fetch_text(client, base + "/")
        if code != 200:
            LOG.warning("Homepage %s returned %s", base, code)
        soup = BeautifulSoup(home, "html.parser")

        urls: list[str] = []
        form_url = _search_url_from_homepage(soup, base, term)
        if form_url:
            urls.append(form_url)
        urls.extend(_guess_search_urls(base, term))

        seen: set[str] = set()
        best: list[HumanProduct] = []
        for u in urls:
            if u in seen:
                continue
            seen.add(u)
            try:
                sc, html = await _fetch_text(client, u)
                if sc != 200:
                    continue
                psoup = BeautifulSoup(html, "html.parser")
                found = _parse_product_cards(psoup, base, max_results)
                if len(found) > len(best):
                    best = found
                if len(found) >= max(3, min(8, max_results // 4)):
                    return found[:max_results]
                if found and not _is_likely_js_spa(html):
                    return found[:max_results]
            except Exception as e:
                LOG.debug("search URL failed %s: %s", u, e)
                continue

        return best[:max_results]


def _products_from_mapping(rows: Any) -> list[HumanProduct]:
    if not rows:
        return []
    out: list[HumanProduct] = []
    for r in rows:
        if isinstance(r, HumanProduct):
            out.append(r)
            continue
        if not isinstance(r, dict):
            continue
        pr = r.get("price")
        price_f: float | None = None
        if pr is not None:
            try:
                price_f = float(pr)
            except (TypeError, ValueError):
                price_f = None
        out.append(
            HumanProduct(
                name=str(r.get("name") or "")[:500],
                price=price_f,
                image_url=r.get("image_url"),
                product_url=r.get("product_url"),
            )
        )
    return out


async def _playwright_inline(base: str, term: str, max_results: int) -> list[HumanProduct]:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        LOG.warning("playwright not installed; cannot run JS fallback")
        return []

    out: list[HumanProduct] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            page = await browser.new_page()
            await page.goto(base + "/", wait_until="domcontentloaded", timeout=60_000)
            await asyncio.sleep(0.5)
            filled = False
            for sel in (
                'input[type="search"]',
                'input[name="q"]',
                'input[name="s"]',
                "#search",
                ".search-input",
            ):
                loc = page.locator(sel).first
                try:
                    if await loc.count() > 0:
                        await loc.fill(term)
                        await page.keyboard.press("Enter")
                        filled = True
                        break
                except Exception:
                    continue
            if not filled:
                q = quote(term, safe="")
                await page.goto(f"{base}/?s={q}", wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(2500)
            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")
            out = _parse_product_cards(soup, base, max_results)
        finally:
            await browser.close()

    return out


async def _delegate_playwright(base: str, term: str, max_results: int) -> list[HumanProduct]:
    try:
        from factory.playwright_engine import search_products_playwright  # type: ignore
    except ImportError:
        return await _playwright_inline(base, term, max_results)
    try:
        raw = await search_products_playwright(base, term, max_results=max_results)
        conv = _products_from_mapping(raw)
        if conv:
            return conv
    except Exception as e:
        LOG.warning("playwright_engine failed (%s), using inline fallback", e)
    return await _playwright_inline(base, term, max_results)


async def search_products(
    store_url: str,
    term: str,
    *,
    max_results: int = 40,
    force_playwright: bool = False,
) -> list[HumanProduct]:
    """
    Load store homepage, resolve search (form + common URL patterns), parse HTML.
    If results are empty, delegate to Playwright (optional ``playwright_engine`` or inline).
    """
    base = _normalize_base(store_url)
    if force_playwright:
        return await _delegate_playwright(base, term, max_results)

    try:
        static = await _static_search(base, term, max_results)
    except httpx.RequestError as e:
        LOG.warning("static search request error: %s", e)
        static = []
    if static:
        return static

    return await _delegate_playwright(base, term, max_results)
