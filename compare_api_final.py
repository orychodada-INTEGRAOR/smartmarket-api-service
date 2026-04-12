"""
compare_api_final.py — SmartMarket Production API
===================================================
Port 8001 — כל ה-endpoints במקום אחד:

  GET  /health
  GET  /api/search?q=חלב&limit=20
  GET  /api/compare?list_id=1&lat=32.08&lng=34.78&mode=balanced
  POST /api/enrich?limit=200
  GET  /api/promotions?domain=food&limit=50
  GET  /api/stats

הרץ: uvicorn compare_api_final:app --port 8001 --reload
"""

import os, re, math, json, time, logging, asyncio, subprocess, sys, threading
from datetime import datetime
from typing import Optional, Literal, List, Any, Dict
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

try:
    from dotenv import load_dotenv; load_dotenv()
except: pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("api")

DB_URL = os.getenv("DATABASE_URL", "")
SUPABASE_URL = (
    os.getenv("SUPABASE_URL", "")
    or os.getenv("NEXT_PUBLIC_SUPABASE_URL", "")
).rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

_INTEGRATOR_DIR = os.path.dirname(os.path.abspath(__file__))
NEW_SITES_QUEUE_PATH = os.path.join(_INTEGRATOR_DIR, "NEW_SITES_QUEUE.json")
_sites_queue_lock = threading.Lock()


def _default_sites_queue() -> Dict[str, Any]:
    return {"pending_approval": [], "approved": [], "rejected": []}


def _read_sites_queue() -> Dict[str, Any]:
    data = _default_sites_queue()
    if not os.path.isfile(NEW_SITES_QUEUE_PATH):
        return data
    try:
        with open(NEW_SITES_QUEUE_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if isinstance(raw, dict):
            for k in data:
                v = raw.get(k)
                data[k] = v if isinstance(v, list) else []
        return data
    except Exception as e:
        log.warning("NEW_SITES_QUEUE read failed: %s", e)
        return _default_sites_queue()


def _write_sites_queue(data: Dict[str, Any]) -> None:
    with open(NEW_SITES_QUEUE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _python_for_integrator() -> str:
    win_py = os.path.join(_INTEGRATOR_DIR, ".venv", "Scripts", "python.exe")
    if os.path.isfile(win_py):
        return win_py
    unix_py = os.path.join(_INTEGRATOR_DIR, ".venv", "bin", "python")
    if os.path.isfile(unix_py):
        return unix_py
    return sys.executable


def _start_master_run_quick() -> None:
    script = os.path.join(_INTEGRATOR_DIR, "MASTER_RUN.py")
    if not os.path.isfile(script):
        log.warning("MASTER_RUN.py not found; skip quick scrape trigger")
        return
    cmd = [_python_for_integrator(), script, "--quick"]
    kw: dict = dict(
        cwd=_INTEGRATOR_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if sys.platform == "win32":
        kw["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    subprocess.Popen(cmd, **kw)

# ══ Pool ═════════════════════════════════════════
_pool: Optional[asyncpg.Pool] = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DB_URL, ssl="require", min_size=2, max_size=15, command_timeout=30)
    return _pool

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Lazy DB mode for Render: do not fail app boot if DB is unavailable.
    try:
        if DB_URL:
            log.info("🚀 API startup complete (DB pool will initialize on first request)")
        else:
            log.warning("⚠️ DATABASE_URL is missing at startup (lazy DB init)")
    except Exception as e:
        log.warning("⚠️ startup DB check skipped: %s", e)
    yield
    if _pool: await _pool.close()

app = FastAPI(title="SmartMarket API", version="2.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ══ Models ═══════════════════════════════════════
class ChainResult(BaseModel):
    chain_name:       str
    total_price:      float
    distance_km:      float
    availability_pct: float
    promo_count:      int
    missing_count:    int
    price_trend:      str
    is_winner:        bool = False

class CompareResponse(BaseModel):
    list_id:   int
    mode:      str
    winner:    Optional[ChainResult]
    chains:    List[ChainResult]
    generated: str


class OnDemandRequest(BaseModel):
    query: str


class AdminApproveSiteBody(BaseModel):
    url: str
    approved: bool

# ══ Helpers ═══════════════════════════════════════
def clean_price(s) -> float:
    if not s: return 0.0
    c = re.sub(r'[^\d.,]','',str(s)).replace(',','.')
    parts = c.split('.')
    if len(parts) > 2: c = ''.join(parts[:-1])+'.'+parts[-1]
    try: v=float(c); return v if 5<v<50000 else 0.0
    except: return 0.0

def haversine(lat1, lng1, lat2, lng2) -> float:
    if not all([lat1, lng1, lat2, lng2]): return 999.0
    R = 6371
    p1, p2 = math.radians(float(lat1)), math.radians(float(lat2))
    dp = math.radians(float(lat2) - float(lat1))
    dl = math.radians(float(lng2) - float(lng1))
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return round(2 * R * math.asin(math.sqrt(a)), 2)

def price_trend(avg30, current) -> str:
    if not avg30 or not current or avg30 == 0: return "stable"
    r = current / avg30
    if r < 0.97: return "down"
    if r > 1.03: return "up"
    return "stable"

def score(c, mode) -> float:
    p = c["total_price"] or 999
    d = c["distance_km"]
    a = c["avail_pct"] / 100
    if mode == "cheap": return p - a * 3
    if mode == "near":  return d * 8 + p * 0.05
    return p * 0.5 + d * 2.5 - a * 6

# ══ /api/search ══════════════════════════════════
@app.get("/api/search")
async def smart_search(
    q:      str = Query(..., min_length=2),
    domain: str = Query(""),
    limit:  int = Query(20, le=50)
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                p.barcode,
                p.name,
                p.chain_name,
                p.price,
                p.promo_price
            FROM products p
            WHERE (
                p.name ILIKE $1 || '%'
                OR p.name ILIKE '%' || $1 || '%'
            )
            AND ($2 = '' OR p.domain = $2)
            ORDER BY
                CASE WHEN p.name ILIKE $1 || '%' THEN 0 ELSE 1 END,
                p.price ASC NULLS LAST
            LIMIT $3
            """,
            q,
            domain,
            limit,
        )

    return {
        "query":   q,
        "count":   len(rows),
        "results": [dict(r) for r in rows]
    }


@app.post("/api/search/on-demand")
async def smart_search_on_demand(payload: OnDemandRequest):
    q = (payload.query or "").strip()
    if len(q) < 2:
        return {"results": [], "scraped_now": False}

    script = os.path.join(os.path.dirname(__file__), "factory", "on_demand_scraper.py")
    cmd = [sys.executable, script, "--query", q]
    proc = await asyncio.to_thread(
        subprocess.run,
        cmd,
        capture_output=True,
        text=True,
        cwd=os.path.dirname(__file__),
    )

    if proc.returncode != 0:
        raise HTTPException(status_code=500, detail=f"on-demand scraper failed: {proc.stderr.strip()}")

    try:
        data = json.loads((proc.stdout or "").strip() or "{}")
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="on-demand scraper returned invalid JSON")

    return {
        "results": data.get("results", []),
        "scraped_now": bool(data.get("scraped_now", False)),
    }

# ══ /api/compare ═════════════════════════════════
@app.get("/api/compare", response_model=CompareResponse)
async def compare_basket(
    list_id: int,
    lat:  float = Query(32.08),
    lng:  float = Query(34.78),
    mode: Literal["cheap","near","balanced"] = "balanced"
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        items = await conn.fetch("SELECT barcode FROM shopping_list_items WHERE list_id = $1", list_id)
        if not items:
            raise HTTPException(404, f"רשימה {list_id} ריקה")
        barcodes = [r["barcode"] for r in items]

        rows = await conn.fetch("""
            SELECT p.chain_name, p.barcode, p.name,
                   COALESCE(p.promo_price, p.price) AS eff_price,
                   p.promo_price IS NOT NULL         AS is_promo,
                   COALESCE(p.is_available, true)    AS in_stock,
                   s.lat AS store_lat, s.lng AS store_lng
            FROM products p
            LEFT JOIN stores s ON s.chain_name = p.chain_name AND s.lat IS NOT NULL
            WHERE p.barcode = ANY($1::text[]) AND p.price IS NOT NULL
        """, barcodes)

        hist = await conn.fetch("""
            SELECT barcode, AVG(price) AS avg30
            FROM price_history
            WHERE barcode = ANY($1::text[]) AND recorded_at >= NOW() - INTERVAL '30 days'
            GROUP BY barcode
        """, barcodes)

    trends = {r["barcode"]: float(r["avg30"]) for r in hist}
    chains: dict = {}
    for r in rows:
        cn = r["chain_name"]
        if cn not in chains:
            chains[cn] = {"total": 0.0, "found": set(), "promos": 0, "in_stock": 0,
                          "lat": r["store_lat"], "lng": r["store_lng"]}
        bc = r["barcode"]
        if bc not in chains[cn]["found"]:
            chains[cn]["found"].add(bc)
            chains[cn]["total"]    += float(r["eff_price"] or 0)
            if r["is_promo"]: chains[cn]["promos"]   += 1
            if r["in_stock"]: chains[cn]["in_stock"] += 1

    results: List[ChainResult] = []
    for cn, c in chains.items():
        found  = len(c["found"])
        avail  = round(c["in_stock"] / found * 100 if found else 0, 1)
        dist   = haversine(lat, lng, c["lat"], c["lng"])
        avg30  = sum(trends.get(bc, 0) for bc in c["found"]) / found if found else 0
        trend  = price_trend(avg30, c["total"])
        results.append(ChainResult(
            chain_name=cn, total_price=round(c["total"],2),
            distance_km=dist, availability_pct=avail,
            promo_count=c["promos"], missing_count=len(barcodes)-found,
            price_trend=trend
        ))

    results.sort(key=lambda x: score(
        {"total_price":x.total_price,"distance_km":x.distance_km,"avail_pct":x.availability_pct}, mode
    ))
    if results: results[0].is_winner = True

    return CompareResponse(list_id=list_id, mode=mode,
        winner=results[0] if results else None,
        chains=results, generated=datetime.now().isoformat())

# ══ /api/promotions ════════════════════════════
@app.get("/api/promotions")
async def get_promotions(
    domain: str = Query("food"),
    limit:  int = Query(50, le=200)
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT p.barcode, p.name, p.chain_name,
                   p.price, p.promo_price,
                   mc.image_url,
                   ROUND((p.price - p.promo_price) / p.price * 100, 1) AS discount_pct
            FROM products p
            LEFT JOIN master_catalogue mc ON mc.barcode = p.barcode
            WHERE p.domain = $1
              AND p.promo_price IS NOT NULL
              AND p.promo_price < p.price
              AND p.promo_price > 0
            ORDER BY discount_pct DESC
            LIMIT $2
        """, domain, limit)
    return {"domain": domain, "count": len(rows), "promotions": [dict(r) for r in rows]}

# ══ /api/enrich ════════════════════════════════
@app.post("/api/enrich")
async def enrich_images(background_tasks: BackgroundTasks, limit: int = Query(200)):
    background_tasks.add_task(_enrich_task, limit)
    return {"status": "started", "limit": limit}

async def _enrich_task(limit: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT barcode FROM master_catalogue
            WHERE image_url IS NULL
              AND barcode ~ '^[0-9]{8,13}$'
            ORDER BY CASE WHEN barcode LIKE '729%' THEN 0 ELSE 1 END
            LIMIT $1
        """, limit)

    updated = 0
    async with httpx.AsyncClient(
        headers={"User-Agent": "SmartMarket/1.0"},
        timeout=10
    ) as client:
        for i, row in enumerate(rows):
            bc = row["barcode"]
            try:
                r = await client.get(f"https://world.openfoodfacts.org/api/v0/product/{bc}.json")
                if r.status_code == 429:
                    await asyncio.sleep(3)
                    continue
                if r.status_code != 200: continue
                d = r.json()
                if d.get("status") != 1: continue
                prod = d["product"]
                img  = prod.get("image_front_url") or prod.get("image_url")
                name = prod.get("product_name_he") or prod.get("product_name")
                if not img: continue
                pool2 = await get_pool()
                async with pool2.acquire() as conn2:
                    if name:
                        await conn2.execute("""
                            UPDATE master_catalogue SET image_url=$1,
                                product_name=CASE WHEN LENGTH($2)>LENGTH(product_name) THEN $2 ELSE product_name END,
                                last_updated=NOW() WHERE barcode=$3
                        """, img, name, bc)
                    else:
                        await conn2.execute(
                            "UPDATE master_catalogue SET image_url=$1,last_updated=NOW() WHERE barcode=$2", img, bc
                        )
                updated += 1
            except: pass
            if (i+1) % 10 == 0:
                await asyncio.sleep(2)
            else:
                await asyncio.sleep(0.15)

    log.info(f"✅ Enrich: {updated}/{len(rows)} תמונות")


@app.get("/stores/{chain_name}")
async def get_stores_by_chain(chain_name: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                chain_name,
                store_id,
                store_name,
                address,
                city,
                lat,
                lng
            FROM stores
            WHERE chain_name ILIKE $1
            ORDER BY city NULLS LAST, address NULLS LAST, store_name NULLS LAST
            """,
            chain_name,
        )
    return {"chain_name": chain_name, "count": len(rows), "stores": [dict(r) for r in rows]}


@app.get("/products/{barcode}/prices")
async def get_product_prices(barcode: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                p.barcode,
                p.name,
                p.chain_name,
                p.price,
                p.promo_price,
                COALESCE(p.promo_price, p.price) AS effective_price,
                s.store_id,
                s.store_name,
                s.address,
                s.city,
                s.lat,
                s.lng
            FROM products p
            LEFT JOIN stores s ON s.chain_name = p.chain_name
            WHERE p.barcode = $1
              AND p.price IS NOT NULL
            ORDER BY effective_price ASC NULLS LAST, p.chain_name, s.store_id
            """,
            barcode,
        )
    return {"barcode": barcode, "count": len(rows), "prices": [dict(r) for r in rows]}


@app.get("/promotions")
async def get_active_promotions(limit: int = Query(100, ge=1, le=500)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                p.barcode,
                p.name,
                p.chain_name,
                p.price,
                p.promo_price,
                ROUND(((p.price - p.promo_price) / NULLIF(p.price, 0)) * 100, 1) AS discount_pct,
                s.store_id,
                s.store_name,
                s.address,
                s.city
            FROM products p
            LEFT JOIN stores s ON s.chain_name = p.chain_name
            WHERE p.promo_price IS NOT NULL
              AND p.promo_price > 0
              AND p.price IS NOT NULL
              AND p.promo_price < p.price
            ORDER BY discount_pct DESC NULLS LAST
            LIMIT $1
            """,
            limit,
        )
    return {"count": len(rows), "promotions": [dict(r) for r in rows]}

# ══ /api/stats ════════════════════════════════
@app.get("/api/stats")
async def get_stats():
    pool = await get_pool()
    async with pool.acquire() as conn:
        domains = await conn.fetch("""
            SELECT domain, COUNT(*) as products,
                   COUNT(DISTINCT chain_name) as chains,
                   COUNT(promo_price) as promos
            FROM products GROUP BY domain ORDER BY products DESC
        """)
        catalogue = await conn.fetchrow("""
            SELECT COUNT(*) as total, COUNT(image_url) as with_image
            FROM master_catalogue
        """)
        stores = await conn.fetchrow("""
            SELECT COUNT(*) as total, COUNT(lat) as with_gps FROM stores
        """)
    return {
        "domains":   [dict(r) for r in domains],
        "catalogue": dict(catalogue),
        "stores":    dict(stores),
        "generated": datetime.now().isoformat()
    }


@app.get("/api/admin/chains-status")
async def get_admin_chains_status():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                chain_name,
                COUNT(*) AS products,
                MAX(updated_at) AS last_run
            FROM products
            GROUP BY chain_name
            ORDER BY last_run DESC NULLS LAST
            """
        )

    chains = [
        {
            "chain_name": r["chain_name"],
            "products": int(r["products"] or 0),
            "last_run": r["last_run"].isoformat() if r["last_run"] else None,
            "status": "ok",
            "last_run_at": r["last_run"].isoformat() if r["last_run"] else None,
        }
        for r in rows
    ]
    return {"count": len(chains), "chains": chains}


@app.post("/api/admin/scrape")
async def post_admin_scrape():
    script = os.path.join(os.path.dirname(__file__), "run_all_data.py")
    subprocess.Popen(["python", script], cwd=os.path.dirname(__file__))
    return {"status": "started", "message": "Scrape started"}


@app.get("/api/admin/recent-users")
async def get_admin_recent_users():
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise HTTPException(status_code=500, detail="Supabase admin credentials are not configured")

    url = f"{SUPABASE_URL}/auth/v1/admin/users?page=1&per_page=20"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    }
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(url, headers=headers)

    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=f"Supabase admin error: {resp.text}")

    payload = resp.json()
    users = payload.get("users", []) if isinstance(payload, dict) else []
    users_sorted = sorted(
        users,
        key=lambda u: u.get("created_at") or "",
        reverse=True,
    )[:20]
    normalized = [
        {
            "id": u.get("id"),
            "email": u.get("email"),
            "created_at": u.get("created_at"),
        }
        for u in users_sorted
    ]
    return {"count": len(normalized), "users": normalized}


@app.get("/api/admin/pending-sites")
async def get_admin_pending_sites():
    with _sites_queue_lock:
        data = _read_sites_queue()
    pending = data.get("pending_approval") or []
    return {"count": len(pending), "pending_approval": pending}


@app.post("/api/admin/approve-site")
async def post_admin_approve_site(body: AdminApproveSiteBody):
    url_key = (body.url or "").strip()
    if not url_key:
        raise HTTPException(status_code=400, detail="url is required")

    moved: Optional[dict] = None
    with _sites_queue_lock:
        data = _read_sites_queue()
        pending = data.get("pending_approval") or []
        idx = next(
            (
                i
                for i, x in enumerate(pending)
                if isinstance(x, dict) and (x.get("url") or "").strip() == url_key
            ),
            None,
        )
        if idx is None:
            raise HTTPException(status_code=404, detail="URL not found in pending_approval")

        item = dict(pending.pop(idx))
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if body.approved:
            item["status"] = "approved"
            item["approved_at"] = now
            data.setdefault("approved", []).append(item)
        else:
            item["status"] = "rejected"
            item["rejected_at"] = now
            data.setdefault("rejected", []).append(item)

        data["pending_approval"] = pending
        _write_sites_queue(data)
        moved = item

    scrape_started = False
    if body.approved:
        try:
            _start_master_run_quick()
            scrape_started = True
        except Exception as e:
            log.warning("quick scrape trigger failed: %s", e)

    return {
        "ok": True,
        "url": url_key,
        "approved": body.approved,
        "item": moved,
        "scrape_started": scrape_started,
    }


# ══ Health ════════════════════════════════════
@app.get("/health")
async def health():
    pool = await get_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM master_catalogue")
    return {"status": "ok", "catalogue": count, "time": datetime.now().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("compare_api_final:app", host="0.0.0.0", port=8001, reload=False)