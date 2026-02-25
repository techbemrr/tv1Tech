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

    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(3)  # 🔧 INCREASED: Better cookie context
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    driver.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
                except:
                    continue
            driver.refresh()
            time.sleep(2)  # 🔧 INCREASED: Cookie settle
            log("✅ Cookies applied successfully")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:80]}")

    return driver

# ---------------- FIXED SCRAPER - MORE ROBUST ---------------- #
def scrape_tradingview(driver, url, url_type=""):
    """🔧 IMPROVED: Better wait + retry logic for Col H pages"""
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            log(f"   📡 Loading {url_type} URL (attempt {attempt+1}/{max_retries+1})...")
            driver.get(url)
            
            # 🔧 FLEXIBLE WAIT: Multiple TradingView load indicators
            WebDriverWait(driver, 50).until(
                lambda d: any([
                    len(d.find_elements(By.CSS_SELECTOR, ".valueValue-l31H9iuA")) > 0,
                    d.execute_script("return document.readyState") == "complete",
                    len(d.find_elements(By.XPATH, "//div[contains(@class, 'valueValue')]")) > 0
                ])
            )
            
            # 🔧 EXTRA WAIT for dynamic content
            time.sleep(3)
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            values = [
                el.get_text().strip().replace('−', '-').replace('∅', 'None')
                for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
                if el.get_text().strip()
            ]
            
            if values:
                log(f"   ✅ Found {len(values)} values")
                return values
            else:
                log(f"   ⚠️ No values found (empty page?)")
                
        except (TimeoutException, NoSuchElementException) as e:
            log(f"   ⏳ Timeout/Element missing (attempt {attempt+1})")
        except WebDriverException as e:
            log("🛑 Browser Crash Detected")
            return "RESTART"
    
    log(f"   ❌ {url_type} FAILED after {max_retries+1} retries")
    return []

# ---------------- INITIAL SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")

    company_list = sheet_main.col_values(1)  # Names (Col A)
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
ROW_SLEEP = 0.1  # 🔧 SLIGHT INCREASE for stability

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
    """Scrape both D and H URLs with FULL PAGE REFRESH between"""
    combined_values = []
    
    # 🔧 FULL CLEANUP + RELOAD between D and H
    driver = ensure_driver()
    
    # Col D URL
    url_d = (url_d_list[i] if i < len(url_d_list) else "").strip()
    values_d = []
    if url_d.startswith("http"):
        values_d = scrape_tradingview(driver, url_d, "Col D")
        if values_d == "RESTART":
            driver = restart_driver()
            values_d = scrape_tradingview(driver, url_d, "Col D")
        if isinstance(values_d, list):
            combined_values.extend(values_d)
    
    # 🔧 CRITICAL FIX: FULL REFRESH + LONGER WAIT for Col H
    try:
        driver.delete_all_cookies()  # Clear session state
        driver.refresh()  # Clear page state
        time.sleep(2)
    except:
        pass
    
    # Col H URL  
    url_h = (url_h_list[i] if i < len(url_h_list) else "").strip()
    values_h = []
    if url_h.startswith("http"):
        values_h = scrape_tradingview(driver, url_h, "Col H")
        if values_h == "RESTART":
            driver = restart_driver()
            values_h = scrape_tradingview(driver, url_h, "Col H")
        if isinstance(values_h, list):
            combined_values.extend(values_h)
    
    log(f"✅ FINAL: D={len(values_d) if isinstance(values_d,list) else 0} + H={len(values_h) if isinstance(values_h,list) else 0} = {len(combined_values)}")
    return combined_values

try:
    for i in range(last_i, len(company_list)):
        if i % SHARD_STEP != SHARD_INDEX:
            continue

        name = (company_list[i] if i < len(company_list) else f"Row {i+1}").strip()
        url_d = (url_d_list[i] if i < len(url_d_list) else "").strip()
        url_h = (url_h_list[i] if i < len(url_h_list) else "").strip()
        
        if not url_d.startswith("http") and not url_h.startswith("http"):
            log(f"⏭️ Row {i+1}: both URLs invalid/blank -> skipped")
            with open(checkpoint_file, "w") as f:
                f.write(str(i + 1))
            continue

        log(f"🔍 [{i+1}/{len(company_list)}] Scraping: {name} (D+H)")

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
