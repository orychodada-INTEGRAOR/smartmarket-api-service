# דוח ביקורת מערכת — SmartMarket (Smartic)

**תאריך:** 11 באפריל 2026  
**מקור נתונים:** קוד המאגר, שאילתות PostgreSQL (Supabase), והרצת `python STATUS.py`  
**הערה:** קבצים «קדושים» לא שונו במסגרת הדוח.

---

## 1. מצב נוכחי (Current State Audit)

### 1.1 קבצי Python תחת `integrator/` (ללא `.venv`)

| קובץ | תפקיד עיקרי |
|------|-------------|
| `compare_api_final.py` | API FastAPI (פורט 8001): חיפוש, השוואת מחירים, מבצעים, סטטיסטיקות, ניהול. |
| `MASTER_RUN.py` | אורקסטרציה של סורקים + לוגים + דוח ריצה. |
| `STATUS.py` | בדיקת חיבור DB, ספירות מוצרים/חנויות, בדיקת `/health` של ה-API. |
| `fetch_all_stores.py` | משיכת חנויות + כתובות/GPS ממקורות ממשלה/רשתות. |
| `fetch_stores.py` | גרסה/זרימה נוספת לחנויות (משלים ל-`run_all_data`). |
| `playwright_publishedprices.py` | סריקת PublishedPrices (רשתות מרובות). |
| `playwright_shufersal.py` | סריקת שופרסל. |
| `playwright_bina.py` | סריקת BinaProjects (מספר רשתות). |
| `playwright_laib.py` | סריקת Laib (ויקטורי ועוד). |
| `html_scraper.py` | אתרים מבוססי HTML (חצי חינם, נתיב החסד, סיטי מרקט וכו'). |
| `carrefour_scraper.py` | קרפור / קוויק (לפי `run_all_data`). |
| `run_all_data.py` | ראנר מאסטר לסורקים (כולל תת-מודולים שקיימים בדיסק). |
| `geocode_stores.py` | השלמת קואורדינטות לחנויות דרך Nominatim. |
| `factory/openformat_scraper.py` | OpenFormat (ממשלתי) → מוצרים. |
| `factory/playwright_baby.py` | אתרי תינוקות/ילדים (שילב וכו'). |
| `factory/discovery_engine.py` | גילוי סוג אתר + כתיבה ל-`raw_products`. |
| `factory/human_browser.py` | חיפוש «אנושי» (httpx + BS4 + Playwright אופציונלי). |
| `factory/excel_chains_loader.py` | טעינת רשתות מ-Excel → תור אתרים. |
| `factory/__init__.py` | מזהה חבילת `factory`. |

קבצים שמוזכרים ב-`run_all_data.py` אך **לא** הופיעו בסריקת דיסק (ייתכן סביבה חלקית): `wolt_scraper.py` ועוד — יש לוודא קיום לפני הרצה.

### 1.2 טבלאות במסד (schema `public`) — נכון לביקורת

טבלאות שזוהו:  
`categories`, `chains`, `household_pantry`, `households`, `list_items`, `master_catalogue`, `message_templates`, `messages`, `otp_codes`, `price_history`, `products`, `profiles`, `promotion_items`, `promotions`, `raw_products`, `shopping_list_items`, `shopping_lists`, `stores`, `supplier_promotions`, `suppliers`, `user_roles`, `users`.

### 1.3 נתונים עיקריים (מדגם SQL, 11/04/2026)

| מדד | ערך |
|-----|-----|
| שורות ב-`products` | 406,259 |
| שורות ב-`stores` | 296 |
| רשתות שונות (`COUNT(DISTINCT chain_name)` במוצרים) | 37 |
| שורות ב-`master_catalogue` | 111,910 |
| שורות ב-`price_history` | 324,656 |
| שורות ב-`promotions` (טבלה) | **0** |
| שורות ב-`raw_products` | 6,769 |
| מוצרים עם `image_url` | 635 |
| מוצרים עם `promo_price` > 0 | 6 |
| רשימות קניות (`shopping_lists`) | 4 |
| פריטי רשימה (`shopping_list_items`) | 8 |

**מסקנה מהירה:** כיסוי ברקוד טוב ברמת הטבלה `products`; תמונות נדירות יחסית; מבצעים כשדה במוצר כמעט לא בשימוש; טבלת `promotions` ריקה.

### 1.4 פלט `python STATUS.py` (הרצה בזמן הביקורת)

```
═══════════════════════════════════════
  SmartMarket — System Status
  11/04/2026 13:09
═══════════════════════════════════════
  DB: ✅ Connected (Supabase)
  API: ❌ Not reachable (:8001)

  📦 Products:    406,259
  🏪 Stores:          296

  Last scrape: 5 hours ago
  Next scrape: Tomorrow 02:00

═══════════════════════════════════════
```

**פרשנות:** החיבור ל-Supabase תקין. ה-API המקומי על פורט 8001 לא היה פעיל בזמן הבדיקה — בפרודקשן מקובל שימוש ב-Render (`NEXT_PUBLIC_API_URL` בפרונטנד מצביע לעיתים ל-`smartmarket-api-service.onrender.com`).

---

## 2. מה עובד היום (What Works Today)

### 2.1 סורקים וזרימות נתונים

- **חנויות:** `fetch_all_stores.py` (וב-`run_all_data` גם `fetch_stores.py`) — בסיס לכתובות ו-GPS.
- **מקורות מחירים מרכזיים:** PublishedPrices, שופרסל, Bina, Laib, OpenFormat, HTML, תינוקות — לפי סדר ב-`MASTER_RUN.py` / `run_all_data.py`.
- **תוספות:** `carrefour_scraper.py` ברשימת `run_all_data`; `discovery_engine` / `raw_products` לניסויים ואתרים חדשים.
- **גיאוקוד:** `geocode_stores.py` להשלמת קואורדינטות.

הצלחה בפועל תלויה בהרצה אחרונה, בלוגים ובזמינות אתרי המקור — לא בוצעה הרצת E2E של כל הסורקים במסגרת דוח זה.

### 2.2 APIs

- **Compare API** (`compare_api_final.py`): חיפוש, השוואה, מבצעים מהמוצרים, `/health`, ניהול — כמתועד בקובץ.
- **Pantry API:** מתועד ב-`CLAUDE.md` כ-`integrator/factory/pantry_api.py` (פורט 8002) — **לא אותר בקוד המאגר הנוכחי**; ייתכן מיקום אחר או שלא נכלל בסנכרון.

### 2.3 פרונטנד (`smartmarket-web`)

- דף בית עם חיפוש, קטגוריות, רשימות (מצבי רשימה), עגלה — מול `NEXT_PUBLIC_API_URL`.
- דפים: `login`, `register`, `profile`, `compare`, `list`, `supplier`, `admin`.
- ברירת מחדל של API בדף הבית: שירות Render אם לא הוגדר env מקומי.

---

## 3. ניתוח פערים (Gaps Analysis)

### 3.1 נתונים חסרים או חלשים

| תחום | מצב |
|------|-----|
| **תמונות** | ~635 מוצרים עם `image_url` מתוך >400K — צורך חזק בהעשרה (`/api/enrich`, קטלוג). |
| **מבצעים** | טבלת `promotions` ריקה; כמעט אין `promo_price` בשימוש — חוויית «מבצעים חמים» תלויה בהטמעה. |
| **מחיר ברמת חנות** | המודל הנפוץ הוא מחיר לרשת; `stores` משמשים למרחק/הצלבה — לא בהכרח מחיר פר-חנות פר-מוצר בכל הרשומות. |
| **היסטוריית מחירים** | קיימת טבלת `price_history` (מאות אלפי שורות) — כדאי לוודא שימוש עקבי ב-API ובאנליטיקה. |
| **raw_products** | שכבת ביניים/ניסויים — לא מחליפה את `products` עד סנכרון מסודר. |

### 3.2 רשתות שלא מכוסות או חלקית

- רשימת הרצה רשמית כוללת מקורות מוגדרים; רשתות מחוץ ל-PublishedPrices/Bina/Laib/HTML/OpenFormat/שופרסל/קרפור דורשות **מנוע ייעודי** או שימוש ב-`discovery_engine` / `human_browser` / תור `NEW_SITES_QUEUE.json`.
- רשתות אופנה/נעלה קטנות מופיעות בנתוני `products` עם נפח נמוך — כנראה ממקורות נקודתיים ולא סריקה מלאה.

### 3.3 מנועים נדרשים

- **מנועי אתרים חדשים:** WooCommerce/Shopify/API פתוחים — כבר קיימת תשתית ב-`discovery_engine`; נדרש חיבור ל-pipeline ייצור.
- **שירותי משלוח/שוק מקוון** (למשל Wolt) — אם הקובץ `wolt_scraper.py` חסר, נדרש פיתוח או הסרה מ-`run_all_data`.
- **העשרת קטלוג** — מילוי `master_catalogue` ותמונות ברמת ברקוד.

---

## 4. שיפורים מומלצים (לפי עדיפות)

1. **איכות נתונים:** הרצת `/api/enrich` או תהליך רקע לתמונות; כללי איכות למחירים וכפילויות.
2. **מבצעים:** טעינה לטבלת `promotions` או שימוש עקבי ב-`promo_price` + תצוגה בפרונטנד.
3. **יישור סורקים:** סנכרון `MASTER_RUN.py` מול `run_all_data.py` (קרפור, Wolt, סדר הרצות) כדי למנוע סורקים «רק בחלק מהסקריפטים».
4. **תשתית:** הפעלת Compare API מקומית/CI לבדיקות; ניטור זמני סריקה ולוגים.
5. **Pantry API:** לאמת מיקום הקוד ופריסה לפורט 8002 או לעדכן תיעוד.

---

## 5. מפת דרכים — 30 יום הבאים

### שבוע 1 — שלמות נתונים
- סטטוס סורקים שבועי; השלמת GPS (`geocode_stores`) לחנויות ללא קואורדינטות.
- יעד להעשרת תמונות (דגימה + אוטומציה).
- בדיקת מילוי `promotions` / מבצעים ממקורות קיימים.

### שבוע 2 — מסכי משתמש
- בדיקות חיפוש והשוואה מקצה לקצה מול API פרודקשן.
- שיפור טעינה ושגיאות כשה-API למטה.
- התאמת קטגוריות לנתונים האמיתיים ב-`domain` / `category_l1`.

### שבוע 3 — Admin + ספקים
- פאנל `admin`: תור אתרים, סטטוס שרשראות.
- `supplier`: זרימת העלאה ובדיקות אינטגרציה.

### שבוע 4 — הכנה להשקה
- בדיקות עומס קלות על Compare API; גיבוי מדיניות DB.
- Runbook: הפעלת `MASTER_RUN`, מעקב אחרי Supabase, Rollback.

---

## נספח — הערות טכניות

- **Smartic:** בשם הפרויקט במסמך זה — SmartMarket/Smartic — מתייחסים לאותו מוצר לפי הקשר המשתמש.
- **דוח זה** נוצר אוטומטית לפי מצב המאגר וה-DB בזמן הריצה; מספרים משתנים לאחר כל סריקה.
