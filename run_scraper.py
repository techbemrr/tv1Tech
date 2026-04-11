import sys
import os
import time
import json
import random
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import gspread
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)

# ---------------- CONFIG ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "500"))
START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW = START_ROW + SHARD_SIZE
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_day_{SHARD_INDEX}.txt")

EXPECTED_COUNT = 26
BATCH_SIZE = 100
RESTART_EVERY_ROWS = 20
MAX_RETRIES = 2  
COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")
CHROME_DRIVER_PATH = ChromeDriverManager().install()

DAY_OUTPUT_START_COL = 3  

# ---------------- UTILS ---------------- #
def col_num_to_letter(n):
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result

DAY_START_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL)
DAY_END_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT - 1)

STATUS_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT)
SHEET_URL_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT + 1)
BROWSER_URL_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT + 2)

def api_retry(func, *args, **kwargs):
    for attempt in range(5):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            wait = (2 ** attempt) + random.random()
            log(f"⚠️ API Issue: {str(e)[:100]}. Retrying in {wait:.1f}s...")
            time.sleep(wait)
    return func(*args, **kwargs)

# ---------------- STATE ---------------- #
if os.path.exists(checkpoint_file):
    try:
        last_i = max(int(open(checkpoint_file).read().strip()), START_ROW)
    except:
        last_i = START_ROW
else:
    last_i = START_ROW

# ---------------- DRIVER ---------------- #
driver = None

def create_driver():
    log(f"🌐 [DAY Shard {SHARD_INDEX}] Initializing browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--incognito")
    opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    drv.set_page_load_timeout(60)

    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            with open(COOKIE_FILE, "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    cookie_data = {k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")}
                    drv.add_cookie(cookie_data)
                except:
                    continue
            drv.refresh()
            time.sleep(2)
        except:
            pass
    return drv

def ensure_driver():
    global driver
    if driver is None:
        driver = create_driver()
    return driver

def restart_driver():
    global driver
    if driver:
        try:
            driver.quit()
        except:
            pass
    driver = None

# ---------------- SCRAPER ---------------- #
def get_values(drv):
    try:
        # Strategy 1: Standard Selenium with expanded selectors
        selectors = [
            "div[class*='valueValue']", 
            "span[class*='valueValue']",
            "div[class*='value-']",
            "span[class*='value-']"
        ]
        
        # Strategy 2: JavaScript Execution (Often bypasses rendering delays)
        js_script = """
        return Array.from(document.querySelectorAll("[class*='valueValue'], [class*='value-']"))
                    .map(el => el.innerText.trim())
                    .filter(txt => txt.length > 0);
        """
        vals = drv.execute_script(js_script)
        
        if not vals:
            elements = drv.find_elements(By.CSS_SELECTOR, ", ".join(selectors))
            vals = [el.text.strip() for el in elements if el.text.strip()]
            
        return vals
    except Exception as e:
        log(f"   get_values error: {e}")
        return []

def scrape_day(url):
    if not url:
        return [""] * EXPECTED_COUNT, "Bad URL", "", ""

    for attempt in range(2):
        try:
            drv = ensure_driver()
            drv.get(url)
            
            # Zoom out to 50% to force more elements to load into view
            drv.execute_script("document.body.style.zoom='50%'")
            
            wait = WebDriverWait(drv, 25)
            # Wait for any element that looks like a data value
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='value']")))
            
            vals = []
            # Intensive check loop
            for _ in range(6):
                vals = get_values(drv)
                if len(vals) >= EXPECTED_COUNT:
                    break
                # Smaller scroll increments to trigger lazy-loaders
                drv.execute_script("window.scrollBy(0, 300);")
                time.sleep(1.5)

            browser_url = drv.current_url
            found_count = len(vals)
            
            if found_count >= EXPECTED_COUNT:
                return vals[:EXPECTED_COUNT], "OK", url, browser_url
            elif found_count > 0:
                padded_vals = (vals + [""] * EXPECTED_COUNT)[:EXPECTED_COUNT]
                return padded_vals, f"Partial ({found_count})", url, browser_url
            else:
                log(f"   ⚠️ Row returned 0 values on attempt {attempt+1}")
                if attempt == 0: 
                    restart_driver() # Force fresh session for retry
                    
        except Exception as e:
            log(f"   ❌ Scrape Attempt {attempt + 1} Failed: {str(e)[:50]}")
            restart_driver()
            
    return [""] * EXPECTED_COUNT, "EMPTY", url, ""

# ---------------- MAIN ---------------- #
def connect_sheets():
    gc = gspread.service_account("credentials.json")
    sh_main = gc.open("STOCKLIST 2").worksheet("Sheet1")
    sh_data = gc.open("MV2 DAY").worksheet("Sheet1")
    return sh_main, sh_data

def process_row(i, company_list, url_list, current_date):
    name = company_list[i].strip() if i < len(company_list) else ""
    url = url_list[i].strip() if i < len(url_list) and "http" in url_list[i] else None
    
    log(f"🔍 [{i + 1}] {name}")
    vals, status, sheet_url_used, browser_url_used = scrape_day(url)
    
    row_idx = i + 1
    row_payload = [
        {"range": f"A{row_idx}", "values": [[name]]},
        {"range": f"B{row_idx}", "values": [[current_date]]},
        {"range": f"{DAY_START_COL_LETTER}{row_idx}:{DAY_END_COL_LETTER}{row_idx}", "values": [vals]},
        {"range": f"{STATUS_COL}{row_idx}", "values": [[status]]},
        {"range": f"{SHEET_URL_COL}{row_idx}", "values": [[sheet_url_used]]},
        {"range": f"{BROWSER_URL_COL}{row_idx}", "values": [[browser_url_used]]}
    ]
    is_success = (status == "OK")
    return row_payload, is_success

try:
    sheet_main, sheet_data = connect_sheets()
    company_list = api_retry(sheet_main.col_values, 1)
    url_list = api_retry(sheet_main.col_values, 4)
    log(f"✅ Processing Rows {last_i + 1} to {min(END_ROW, len(company_list))}")
except Exception as e:
    log(f"❌ Initial Connection Error: {e}")
    sys.exit(1)

failed_queue = []
batch_list = []
current_date = date.today().strftime("%m/%d/%Y")
loop_end = min(END_ROW, len(company_list))

for i in range(last_i, loop_end):
    payload, success = process_row(i, company_list, url_list, current_date)
    batch_list.extend(payload)
    
    if not success:
        failed_queue.append(i)

    with open(checkpoint_file, "w") as f:
        f.write(str(i + 1))

    if (i + 1) % RESTART_EVERY_ROWS == 0:
        restart_driver()

    if len(batch_list) // 6 >= BATCH_SIZE:
        log(f"🚀 Uploading batch of {BATCH_SIZE} rows...")
        api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")
        batch_list = []

if batch_list:
    api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")
    batch_list = []

retry_attempt = 1
while failed_queue and retry_attempt <= MAX_RETRIES:
    log(f"🔁 Starting Retry Pass {retry_attempt} for {len(failed_queue)} symbols...")
    time.sleep(random.randint(15, 25))
    restart_driver()
    
    still_failing = []
    for idx, i in enumerate(failed_queue):
        payload, success = process_row(i, company_list, url_list, current_date)
        batch_list.extend(payload)
        
        if not success:
            still_failing.append(i)
            
        if (idx + 1) % 10 == 0:
            restart_driver()
            
        if len(batch_list) // 6 >= 10:
            api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")
            batch_list = []

    if batch_list:
        api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")
        batch_list = []
        
    failed_queue = still_failing
    retry_attempt += 1

restart_driver()
log("🏁 DAY SHARD COMPLETED.")
