import os
import requests
import gzip
import shutil
import urllib3
import re
from pathlib import Path
import logging
from urllib.parse import urljoin

# הגדרות לוגינג
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# נתיבים
BASE_URL = "https://prices.carrefour.co.il/"
CURRENT_DIR = Path(__file__).parent
DUMPS_DIR = CURRENT_DIR.parent / "dumps" / "Carrefour"

def setup_dir():
    DUMPS_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"📁 יעד הורדה: {DUMPS_DIR}")

def get_latest_files():
    logger.info(f"🔍 סורק את האתר (דף גדול: 183KB)...")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        resp = requests.get(BASE_URL, headers=headers, verify=False, timeout=30)
        resp.raise_for_status()
        html_content = resp.text
        logger.info(f"📡 התקבלו {len(html_content)} תווים. מחפש תבניות קבצים...")

        # חיפוש אגרסיבי של כל מה שנגמר ב-.gz בתוך ה-HTML
        # מחפש PriceFull, PromoFull או Store ואחריהם סיומת gz
        found_files = re.findall(r'([^\s"\'<>]+(?:PriceFull|PromoFull|Store|Prices|Price|Promo)[^\s"\'<>]*\.gz)', html_content, re.IGNORECASE)
        
        if not found_files:
            # ניסיון אחרון - כל קובץ .gz
            found_files = re.findall(r'([^\s"\'<>]*\.gz)', html_content, re.IGNORECASE)

        if not found_files:
            logger.error("❌ לא נמצאו שמות קבצים בתוך הטקסט של הדף.")
            return []

        # ניקוי כפילויות והפיכה לקישורים מלאים
        unique_links = list(set([urljoin(BASE_URL, f.lstrip('/')) for f in found_files]))
        logger.info(f"📋 נמצאו {len(unique_links)} קישורים פוטנציאליים.")

        # בחירת הכי חדש מכל סוג
        latest = {}
        for prefix in ['PriceFull', 'PromoFull', 'Store']:
            category = [l for l in unique_links if prefix.lower() in l.lower()]
            if category:
                # מיון לפי שם הקובץ (מכיל תאריך)
                latest[prefix] = sorted(category)[-1]
                logger.info(f"✅ זוהה הכי חדש ל-{prefix}: {latest[prefix].split('/')[-1]}")

        return list(latest.values())

    except Exception as e:
        logger.error(f"❌ תקלה בסריקה: {e}")
        return []

def process_file(url):
    filename = url.split('/')[-1].split('?')[0] # מנקה פרמטרים אם יש
    gz_path = DUMPS_DIR / filename
    xml_path = DUMPS_DIR / filename.replace('.gz', '.xml')
    
    logger.info(f"⬇️ מוריד: {filename}")
    try:
        with requests.get(url, stream=True, verify=False, timeout=120) as r:
            r.raise_for_status()
            with open(gz_path, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        
        logger.info(f"📦 מחלץ...")
        with gzip.open(gz_path, 'rb') as f_in:
            with open(xml_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        gz_path.unlink() # מוחק את ה-GZ
        logger.info(f"✨ מוכן: {xml_path.name}")
    except Exception as e:
        logger.error(f"❌ שגיאה בהורדת {filename}: {e}")

if __name__ == "__main__":
    setup_dir()
    targets = get_latest_files()
    if targets:
        for t in targets:
            process_file(t)
        logger.info(f"🎉 סיימנו! הקבצים בתיקייה: {DUMPS_DIR}")
    else:
        logger.error("❌ לא הצלחנו למצוא קבצים להורדה.")