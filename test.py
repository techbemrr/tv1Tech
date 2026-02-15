import sys
import os
import time
import json
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import gspread
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    print(msg, flush=True)

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 0

# ✅ Speed: resolve chromedriver path ONCE (restart will reuse it)
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log("🌐 Initializing Hardened Chrome Instance...")
    opts = Options()

    # ✅ Faster: don't wait for full page assets
    opts.page_load_strategy = "eager"

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])

    # ✅ small extra speed/consistency flags (no extractor change)
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--mute-audio")

    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(
        service=Service(CHROME_DRIVER_PATH),
        options=opts
    )
    driver.set_page_load_timeout(40)

    # ---- COOKIE LOGIC ----
    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(2)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)

            for c in cookies:
                try:
                    driver.add_cookie({
                        k: v for k, v in c.items()
                        if k in ("name", "value", "path", "secure", "expiry")
                    })
                except:
                    continue

            driver.refresh()
            time.sleep(1)
            log("✅ Cookies applied successfully")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:80]}")

    return driver

# ---------------- SCRAPER LOGIC ---------------- #
# ❌ NOT CHANGED (as you asked)
def scrape_tradingview(driver, url):
    try:
        driver.get(url)
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((
                By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
            ))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values
    except (TimeoutException, NoSuchElementException):
        return []
    except WebDriverException:
        log("🛑 Browser Crash Detected")
        return "RESTART"

def scrape_with_retry(driver, url):
    """Retry once if empty. Returns list, [] or 'RESTART'."""
    values = scrape_tradingview(driver, url)
    if values == []:
        try:
            driver.refresh()
            time.sleep(0.7)
        except:
            pass
        values = scrape_tradingview(driver, url)
    return values

# ---------------- INITIAL SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet16")

    name_list = sheet_main.col_values(1)      # Names
    url_list_c = sheet_main.col_values(3)     # ✅ Column C links
    url_list_d = sheet_main.col_values(4)     # ✅ Column D links

    total_rows = max(len(name_list), len(url_list_c), len(url_list_d))
    log(f"✅ Setup complete | Shard {SHARD_INDEX}/{SHARD_STEP} | Resume index {last_i} | Total {total_rows}")
except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = create_driver()
batch_list = []

BATCH_SIZE = 300
current_date = date.today().strftime("%m/%d/%Y")
ROW_SLEEP = 0.05

def flush_batch():
    global batch_list
    if not batch_list:
        return
    for attempt in range(3):
        try:
            sheet_data.batch_update(batch_list)
            log(f"🚀 Saved {len(batch_list)} updates")
            batch_list = []
            return
        except Exception as e:
            msg = str(e)
            log(f"⚠️ API Error: {msg[:160]}")
            if "429" in msg:
                log("⏳ Quota hit, sleeping 60s...")
                time.sleep(60)
            else:
                time.sleep(3)

def safe_get(lst, idx):
    return (lst[idx] if idx < len(lst) else "").strip()

try:
    for i in range(last_i, total_rows):

        # ✅ Keep sharding
        if i % SHARD_STEP != SHARD_INDEX:
            continue

        name = safe_get(name_list, i) or f"Row {i+1}"
        url_c = safe_get(url_list_c, i)   # Column C
        url_d = safe_get(url_list_d, i)   # Column D

        target_row = i + 1
        log(f"🔍 [{target_row}/{total_rows}] Scraping: {name}")

        combined_values = []

        # ---------- SCRAPE COLUMN C LINK ----------
        values_c = []
        if url_c.startswith("http"):
            log(f"   🌐 C link visiting...")
            values_c = scrape_with_retry(driver, url_c)

            if values_c == "RESTART":
                try:
                    driver.quit()
                except:
                    pass
                driver = create_driver()
                values_c = scrape_with_retry(driver, url_c)
                if values_c == "RESTART":
                    values_c = []
        else:
            log(f"   ⏭️ C link invalid/blank")

        # ---------- SCRAPE COLUMN D LINK ----------
        values_d = []
        if url_d.startswith("http"):
            log(f"   🌐 D link visiting...")
            values_d = scrape_with_retry(driver, url_d)

            if values_d == "RESTART":
                try:
                    driver.quit()
                except:
                    pass
                driver = create_driver()
                values_d = scrape_with_retry(driver, url_d)
                if values_d == "RESTART":
                    values_d = []
        else:
            log(f"   ⏭️ D link invalid/blank")

        # ✅ COMBINE: C values + D values (single merged array)
        if isinstance(values_c, list) and values_c:
            combined_values.extend(values_c)
        if isinstance(values_d, list) and values_d:
            combined_values.extend(values_d)

        # ---------- WRITE OUTPUT ----------
        # ✅ Always write name/date. Values only if combined has data.
        batch_list.append({"range": f"A{target_row}", "values": [[name]]})
        batch_list.append({"range": f"J{target_row}", "values": [[current_date]]})

        if combined_values:
            batch_list.append({"range": f"K{target_row}", "values": [combined_values]})
            log(f"✅ Combined values: {len(combined_values)} cells (C:{len(values_c)} + D:{len(values_d)})")
        else:
            log(f"⚠️ No values found from both links (C:{len(values_c)} + D:{len(values_d)})")

        if len(batch_list) >= BATCH_SIZE:
            flush_batch()

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        if ROW_SLEEP:
            time.sleep(ROW_SLEEP)

finally:
    flush_batch()
    try:
        driver.quit()
    except:
        pass
    log("🏁 Scraping completed successfully")
