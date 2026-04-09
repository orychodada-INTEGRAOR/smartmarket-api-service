"""
run_all_data.py — SmartMarket Master Data Runner v2
=====================================================
פקודה אחת — כל הנתונים נשאבים לDB:
  מוצרים + מחירים + חנויות + מבצעים

הרץ:
  python run_all_data.py              # הכל
  python run_all_data.py --schedule   # כל 3 שעות
  python run_all_data.py --only bina  # סורק ספציפי
  python run_all_data.py --dry-run    # בדיקה
"""

import os, sys, time, asyncio, argparse, logging, subprocess
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("run_all_data.log", encoding="utf-8"),
    ]
)
log = logging.getLogger("runner")

BASE = Path(__file__).parent

# ── כל הסורקים לפי סדר ────────────────────────────────────
SCRAPERS = [{
        "id":     "stores_v2",
        "label":  "חנויות + GPS (כל הרשתות)",
        "script": "fetch_all_stores.py",
        "args":   [],
    },
    {
        "id":     "publishedprices",
        "label":  "PublishedPrices (13 רשתות)",
        "script": "playwright_publishedprices.py",
        "args":   [],
    },
    {
        "id":     "shufersal",
        "label":  "שופרסל",
        "script": "playwright_shufersal.py",
        "args":   [],
    },
    {
        "id":     "bina",
        "label":  "BinaProjects (10 רשתות)",
        "script": "playwright_bina.py",
        "args":   [],
    },
    {
        "id":     "laib",
        "label":  "LaibCatalog (ויקטורי + עוד)",
        "script": "playwright_laib.py",
        "args":   [],
    },
    {
        "id":     "baby",
        "label":  "Baby (שילב בלבד)",
        "script": "factory/playwright_baby.py",
        "args":   ["--only", "shilav"],
    },
    {
        "id":     "openformat",
        "label":  "OpenFormat API (ממשלתי)",
        "script": "factory/openformat_scraper.py",
        "args":   [],
    },
    {
        "id":     "carrefour",
        "label":  "קרפור + קוויק",
        "script": "carrefour_scraper.py",
        "args":   [],
    },
    {
        "id":     "wolt",
        "label":  "וולט",
        "script": "wolt_scraper.py",
        "args":   [],
    },
    {
        "id":     "html",
        "label":  "HTML Scrapers (חצי חינם + נתיב החסד + סיטי מרקט + קיי.טי)",
        "script": "html_scraper.py",
        "args":   [],
    },
    # ── חנויות ─────────────────────────────────────────────
    {
        "id":     "stores",
        "label":  "חנויות (כתובות + GPS)",
        "script": "fetch_stores.py",
        "args":   [],
    },
]

# ── הרץ סורק אחד ──────────────────────────────────────────
def run_scraper(scraper: dict, dry_run=False) -> dict:
    script = BASE / scraper["script"]
    if not script.exists():
        log.warning(f"  ⚠️  {scraper['script']} לא קיים — דלג")
        return {"id": scraper["id"], "status": "skip", "duration": 0}

    cmd = [sys.executable, str(script)] + scraper["args"]
    if dry_run:
        cmd.append("--dry-run")

    log.info(f"\n{'━'*55}")
    log.info(f"  ▶ {scraper['label']}")
    log.info(f"{'━'*55}")

    start = time.time()
    try:
        result = subprocess.run(cmd, cwd=str(BASE), timeout=1800)
        duration = round(time.time() - start)
        status = "ok" if result.returncode == 0 else "error"
        log.info(f"  {'✅' if status=='ok' else '❌'} {scraper['label']} — {duration}s")
        return {"id": scraper["id"], "status": status, "duration": duration}
    except subprocess.TimeoutExpired:
        log.error(f"  ⏱️ timeout — {scraper['label']}")
        return {"id": scraper["id"], "status": "timeout", "duration": 1800}
    except Exception as e:
        log.error(f"  ❌ {e}")
        return {"id": scraper["id"], "status": "error", "duration": 0}

# ── הרצה מלאה ─────────────────────────────────────────────
def run_all(only=None, dry_run=False):
    start_time = time.time()
    now = datetime.now().strftime("%d/%m/%Y %H:%M")

    print(f"\n{'═'*55}")
    print(f"  SmartMarket — Master Runner v2")
    print(f"  {now} | {'DRY-RUN' if dry_run else 'LIVE'}")
    print(f"{'═'*55}\n")

    scrapers = SCRAPERS
    if only:
        scrapers = [s for s in SCRAPERS if s["id"] == only]
        if not scrapers:
            log.error(f"סורק '{only}' לא נמצא.")
            log.info(f"אפשרויות: {[s['id'] for s in SCRAPERS]}")
            return

    results = []
    for scraper in scrapers:
        r = run_scraper(scraper, dry_run=dry_run)
        results.append(r)

    # ── סיכום ─────────────────────────────────────────────
    total_time = round(time.time() - start_time)
    ok      = [r for r in results if r["status"] == "ok"]
    skipped = [r for r in results if r["status"] == "skip"]
    errors  = [r for r in results if r["status"] in ("error","timeout")]

    print(f"\n{'═'*55}")
    print(f"  סיכום — {total_time}s")
    print(f"  ✅ הצליח:  {len(ok)}")
    print(f"  ⏭️  דולג:   {len(skipped)}")
    print(f"  ❌ נכשל:   {len(errors)}")
    if errors:
        for e in errors:
            print(f"     — {e['id']}: {e['status']}")
    print(f"{'═'*55}\n")
    return results

# ── שעון אוטומטי ──────────────────────────────────────────
def schedule_loop(interval_hours=3, dry_run=False):
    log.info(f"🕐 שעון — כל {interval_hours} שעות")
    while True:
        log.info(f"\n🔄 {datetime.now().strftime('%d/%m/%Y %H:%M')} — מתחיל הרצה")
        run_all(dry_run=dry_run)
        next_run = datetime.fromtimestamp(time.time() + interval_hours * 3600)
        log.info(f"⏰ הרצה הבאה: {next_run.strftime('%d/%m/%Y %H:%M')}")
        time.sleep(interval_hours * 3600)

# ── Main ──────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="SmartMarket — Master Runner")
    ap.add_argument("--schedule", action="store_true", help="הרץ כל 3 שעות")
    ap.add_argument("--hours",    type=int, default=3,  help="מרווח שעות")
    ap.add_argument("--dry-run",  action="store_true",  help="בלי DB")
    ap.add_argument("--only",     default=None,
                    help="stores_v2/publishedprices/shufersal/bina/laib/baby/openformat/carrefour/wolt/html/stores")
    a = ap.parse_args()

    if a.schedule:
        schedule_loop(interval_hours=a.hours, dry_run=a.dry_run)
    else:
        run_all(only=a.only, dry_run=a.dry_run)