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
    opts.add_argument("--mute-audio")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    driver.set_page_load_timeout(15)
    driver.set_script_timeout(15)

    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(1)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    driver.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
                except: continue
            driver.refresh()
            time.sleep(0.5)
            log("✅ Cookies applied successfully")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:120]}")
    return driver

def safe_get(driver, url):
    try:
        driver.get(url)
        return True
    except TimeoutException:
        try: driver.execute_script("window.stop();")
        except: pass
        return False
    except WebDriverException as e:
        log(f"🛑 WebDriver error: {str(e)[:120]}")
        return "RESTART"

def scrape_tradingview(driver, url):
    try:
        ok = safe_get(driver, url)
        if ok == "RESTART": return "RESTART"
        if not ok: return []

        # Wait specifically for the technical values to load
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, "valueValue-l31H9iuA"))
        )
        
        # Small sleep to ensure the numbers inside the containers are populated
        time.sleep(1)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None').strip()
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values
    except (TimeoutException, NoSuchElementException):
        return []
    except WebDriverException:
        return "RESTART"

def scrape_with_recovery(driver, url):
    values = scrape_tradingview(driver, url)
    if values == "RESTART":
        try: driver.quit()
        except: pass
        driver = create_driver()
        values = scrape_tradingview(driver, url)
        if values == "RESTART": values = []
    return driver, values

# ---------------- INITIAL SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")

    company_list = sheet_main.col_values(4)  # URLs in D
    url_h_list   = sheet_main.col_values(8)  # URLs in H
    name_list    = sheet_main.col_values(1)  # Names

    log(f"✅ Setup complete | Shard {SHARD_INDEX}/{SHARD_STEP} | Resuming at {last_i}")
except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = create_driver()
batch_list = []
BATCH_SIZE = 10
current_date = date.today().strftime("%m/%d/%Y")

def flush_batch():
    global batch_list
    if not batch_list: return
    for attempt in range(3):
        try:
            sheet_data.batch_update(batch_list)
            log(f"🚀 Saved {len(batch_list) // 3} rows to Google Sheets")
            batch_list = []
            return
        except Exception as e:
            if "429" in str(e):
                log("⏳ Quota hit, sleeping 60s...")
                time.sleep(60)
            else: 
                log(f"⚠️ API Error: {e}")
                time.sleep(2)

try:
    for i in range(last_i, len(company_list)):
        if i % SHARD_STEP != SHARD_INDEX: continue

        url_d = (company_list[i] or "").strip()
        url_h = (url_h_list[i] if i < len(url_h_list) else "").strip()
        name  = (name_list[i] if i < len(name_list) else f"Row {i+1}").strip()

        if (not url_d.startswith("http")) and (not url_h.startswith("http")):
            with open(checkpoint_file, "w") as f: f.write(str(i + 1))
            continue

        # Prevent double-scraping if URLs are identical
        if url_h and url_d and url_h == url_d:
            url_h = ""

        log(f"🔍 [{i+1}/{len(company_list)}] Processing: {name}")
        
        row_values = []
        
        # --- SCRAPE LINK D ---
        if url_d.startswith("http"):
            driver, vals_d = scrape_with_recovery(driver, url_d)
            if isinstance(vals_d, list) and vals_d:
                row_values.extend(vals_d)
                log(f"   📊 Link D: {len(vals_d)} values found")
            else:
                log(f"   ⚠️ Link D: No values found")

        # --- SCRAPE LINK H ---
        if url_h.startswith("http"):
            driver, vals_h = scrape_with_recovery(driver, url_h)
            if isinstance(vals_h, list) and vals_h:
                row_values.extend(vals_h)
                log(f"   📊 Link H: {len(vals_h)} values found (Total: {len(row_values)})")
            else:
                log(f"   ⚠️ Link H: No values found")

        target_row = i + 1
        
        # Prepare Batch Update
        batch_list.append({"range": f"A{target_row}", "values": [[name]]})
        batch_list.append({"range": f"J{target_row}", "values": [[current_date]]})
        
        if row_values:
            # Writes D values followed immediately by H values starting at Column K
            batch_list.append({"range": f"K{target_row}", "values": [row_values]})
        
        if len(batch_list) >= (BATCH_SIZE * 3): # 3 updates per row
            flush_batch()

        with open(checkpoint_file, "w") as f: f.write(str(i + 1))

finally:
    flush_batch()
    try: driver.quit()
    except: pass
    log("🏁 Process Finished")
