import sys
import os
import time
import json
import random
import re
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

EXPECTED_COUNT = 18
BATCH_SIZE = 50 
RESTART_EVERY_ROWS = 20
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
            log(f"⚠️ API Issue: {str(e)[:50]}. Retrying in {wait:.1f}s...")
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
    log(f"🌐 [Shard {SHARD_INDEX}] Initializing browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    
    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            with open(COOKIE_FILE, "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for c in cookies:
                drv.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
            drv.refresh()
            time.sleep(2)
        except: pass
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
        except: pass
    driver = None

# ---------------- SCRAPER ---------------- #
def extract_legend_values_via_js(drv):
    """JS script to grab text from all legend items directly from the DOM tree."""
    js_script = """
    let results = [];
    
    // 1. Try finding legend values container
    let legendContainers = document.querySelectorAll('[class*="legend-"], [class*="legendMainSource"]');
    
    legendContainers.forEach(container => {
        let spans = container.querySelectorAll('span, div');
        spans.forEach(el => {
            let txt = el.innerText ? el.innerText.trim() : '';
            if (txt && !results.includes(txt)) {
                // Match numerical values or indicators
                if (/^-?\\d+(\\.\\d+)?$/.test(txt) || txt === 'Ø') {
                    results.push(txt);
                }
            }
        });
    });
    
    // 2. Fallback: Scan top-left legend overlay directly
    if (results.length < 18) {
        let allValues = document.querySelectorAll('[class*="value-"], [class*="valueValue"]');
        allValues.forEach(el => {
            let txt = el.innerText ? el.innerText.trim() : '';
            if (txt) {
                results.push(txt);
            }
        });
    }
    
    return results;
    """
    try:
        return drv.execute_script(js_script)
    except:
        return []

def scrape_day(url):
    if not url: return [""] * EXPECTED_COUNT, "NOT OK", "", ""
    
    for attempt in range(2):
        try:
            drv = ensure_driver()
            drv.get(url)
            
            # 1. Wait for canvas or body to ensure the chart is ready
            WebDriverWait(drv, 40).until(
                EC.presence_of_element_located((By.TAG_NAME, "canvas"))
            )
            
            # 2. Allow TradingView layout engine & WebSocket data stream to finish loading
            time.sleep(10)
            
            # 3. Trigger legend expansion if collapsed
            drv.execute_script("""
                let arrow = document.querySelector('[class*="toggleButton"], [class*="pane-legend-icon"]');
                if (arrow) arrow.click();
            """)
            time.sleep(2)

            # 4. Polling for complete legend values
            vals = []
            start_poll = time.time()
            max_poll = 25
            
            while time.time() - start_poll < max_poll:
                vals = extract_legend_values_via_js(drv)
                if len(vals) >= EXPECTED_COUNT:
                    break
                
                # Dispatch mouse movement over canvas to activate dynamic legend update
                drv.execute_script("""
                    let canvas = document.querySelector('canvas');
                    if (canvas) {
                        let rect = canvas.getBoundingClientRect();
                        let evt = new MouseEvent('mousemove', {
                            clientX: rect.left + 300,
                            clientY: rect.top + 150,
                            bubbles: true
                        });
                        canvas.dispatchEvent(evt);
                    }
                """)
                time.sleep(2.0)
            
            browser_url = drv.current_url
            found_count = len(vals)
            
            if found_count >= EXPECTED_COUNT:
                log(f"   ✅ Found {found_count}/{EXPECTED_COUNT}")
                return vals[:EXPECTED_COUNT], "OK", url, browser_url
            else:
                log(f"   ⚠️ Found {found_count}/{EXPECTED_COUNT} (Marking NOT OK)")
                padded = (vals + [""] * EXPECTED_COUNT)[:EXPECTED_COUNT]
                return padded, "NOT OK", url, browser_url
                
        except Exception as e:
            log(f"   ❌ Attempt {attempt + 1} Failed: {str(e)[:60]}")
            restart_driver()
            
    return [""] * EXPECTED_COUNT, "NOT OK", url, ""

# ---------------- MAIN ---------------- #
def connect_sheets():
    gc = gspread.service_account("credentials.json")
    sh_main = gc.open("STOCKLIST 2").worksheet("Sheet1")
    sh_data = gc.open("MV2 DAY").worksheet("Sheet1")
    return sh_main, sh_data

def process_row(i, company_list, url_list, current_date):
    name = company_list[i].strip() if i < len(company_list) else ""
    url = url_list[i].strip() if i < len(url_list) and "http" in url_list[i] else None
    
    log(f"🔍 [{i + 1}] {name} | URL: {url if url else 'No URL'}")
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
    return row_payload, (status == "OK")

try:
    sheet_main, sheet_data = connect_sheets()
    company_list = api_retry(sheet_main.col_values, 1)
    url_list = api_retry(sheet_main.col_values, 4)
    log(f"✅ Starting rows {last_i + 1} to {min(END_ROW, len(company_list))}")
except Exception as e:
    log(f"❌ Connection Error: {e}")
    sys.exit(1)

retry_indices = []
batch_list = []
current_date = date.today().strftime("%m/%d/%Y")
loop_end = min(END_ROW, len(company_list))

# --- FIRST PASS ---
for i in range(last_i, loop_end):
    payload, success = process_row(i, company_list, url_list, current_date)
    batch_list.extend(payload)
    
    if not success:
        retry_indices.append(i)

    with open(checkpoint_file, "w") as f:
        f.write(str(i + 1))

    if (i + 1) % RESTART_EVERY_ROWS == 0:
        restart_driver()

    if len(batch_list) // 6 >= BATCH_SIZE:
        log(f"🚀 Uploading batch...")
        api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")
        batch_list = []

if batch_list:
    api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")
    batch_list = []

# --- RETRY PASS ---
if retry_indices:
    log(f"🔁 Retrying {len(retry_indices)} symbols labeled 'NOT OK'...")
    restart_driver()
    batch_list = []
    
    for idx, i in enumerate(retry_indices):
        payload, success = process_row(i, company_list, url_list, current_date)
        batch_list.extend(payload)
        
        if (idx + 1) % 10 == 0:
            restart_driver()
            api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")
            batch_list = []

    if batch_list:
        api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")

restart_driver()
log("🏁 SCRAPING COMPLETED.")
