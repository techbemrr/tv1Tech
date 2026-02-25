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
last_i = int(open(checkpoint_file).read().strip()) if os.path.exists(checkpoint_file) else 0

# ✅ Speed: resolve chromedriver path ONCE
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log("🌐 Initializing Hardened Chrome Instance...")
    opts = Options()
    opts.page_load_strategy = "eager"
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--mute-audio")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
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
                    driver.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
                except:
                    continue
            driver.refresh()
            time.sleep(1)
            log("✅ Cookies applied successfully")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:80]}")

    return driver

# ---------------- SCRAPER LOGIC ---------------- #
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

# ---------------- INITIAL SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")

    company_list = sheet_main.col_values(1)  # Names (Col A) - 1-based
    url_d_list = sheet_main.col_values(4)    # URLs Col D
    url_h_list = sheet_main.col_values(8)    # URLs Col H

    log(f"✅ Setup complete | Shard {SHARD_INDEX}/{SHARD_STEP} | Resume index {last_i} | Total {len(company_list)}")
except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = None
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

def ensure_driver():
    global driver
    if driver is None:
        driver = create_driver()
    return driver

def restart_driver():
    global driver
    try:
        if driver:
            driver.quit()
    except:
        pass
    driver = create_driver()
    return driver

def get_all_values_for_row(i):
    """Scrape both D and H URLs, return COMBINED values list"""
    combined_values = []
    
    # Col D URL
    url_d = (url_d_list[i] if i < len(url_d_list) else "").strip()
    if url_d.startswith("http"):
        driver = ensure_driver()
        values_d = scrape_tradingview(driver, url_d)
        if values_d == "RESTART":
            driver = restart_driver()
            values_d = scrape_tradingview(driver, url_d) or []
        if isinstance(values_d, list):
            combined_values.extend(values_d)
        log(f"✅ Col D: {len(values_d) if isinstance(values_d, list) else 0} values")
    
    # Col H URL  
    url_h = (url_h_list[i] if i < len(url_h_list) else "").strip()
    if url_h.startswith("http"):
        driver = ensure_driver()
        values_h = scrape_tradingview(driver, url_h)
        if values_h == "RESTART":
            driver = restart_driver()
            values_h = scrape_tradingview(driver, url_h) or []
        if isinstance(values_h, list):
            combined_values.extend(values_h)
        log(f"✅ Col H: {len(values_h) if isinstance(values_h, list) else 0} values")
    
    return combined_values

try:
    for i in range(last_i, len(company_list)):
        # ✅ Keep sharding
        if i % SHARD_STEP != SHARD_INDEX:
            continue

        name = (company_list[i] if i < len(company_list) else f"Row {i+1}").strip()

        # Skip if BOTH URLs invalid/blank
        url_d = (url_d_list[i] if i < len(url_d_list) else "").strip()
        url_h = (url_h_list[i] if i < len(url_h_list) else "").strip()
        if not url_d.startswith("http") and not url_h.startswith("http"):
            log(f"⏭️ Row {i+1}: both URLs invalid/blank -> skipped")
            with open(checkpoint_file, "w") as f:
                f.write(str(i + 1))
            continue

        log(f"🔍 [{i+1}/{len(company_list)}] Scraping: {name} (D+H)")

        # Get COMBINED values - FIXED driver handling
        combined_values = get_all_values_for_row(i)

        target_row = i + 1

        if combined_values:
            batch_list.append({"range": f"A{target_row}", "values": [[name]]})
            batch_list.append({"range": f"J{target_row}", "values": [[current_date]]})
            batch_list.append({"range": f"K{target_row}", "values": [combined_values]})
            log(f"✅ COMBINED: {len(combined_values)} values | Buffered: {len(batch_list)}/{BATCH_SIZE}")
        else:
            batch_list.append({"range": f"A{target_row}", "values": [[name]]})
            batch_list.append({"range": f"J{target_row}", "values": [[current_date]]})
            log(f"⚠️ No combined values for {name} (A/J updated only)")

        if len(batch_list) >= BATCH_SIZE:
            flush_batch()

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        if ROW_SLEEP:
            time.sleep(ROW_SLEEP)

finally:
    flush_batch()
    try:
        if driver:
            driver.quit()
    except:
        pass
    log("🏁 Scraping completed successfully")
