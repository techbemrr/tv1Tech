import time
import json
import random
from datetime import date
import gspread
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
EXPECTED_COUNT = 22
DAY_OUTPUT_START_COL = 3
COOKIE_FILE = "cookies.json"
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- LOG ---------------- #
def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)

# ---------------- COLUMN UTILS ---------------- #
def col_num_to_letter(n):
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result

def col_letter_to_num(col):
    num = 0
    for c in col:
        num = num * 26 + (ord(c.upper()) - ord('A') + 1)
    return num

DAY_START_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL)
DAY_END_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT - 1)
STATUS_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT)
SHEET_URL_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT + 1)
BROWSER_URL_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT + 2)

# ---------------- API RETRY ---------------- #
def api_retry(func, *args, **kwargs):
    for attempt in range(5):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            wait = (2 ** attempt) + random.random()
            log(f"⚠️ API retry in {wait:.1f}s...")
            time.sleep(wait)
    return func(*args, **kwargs)

# ---------------- DRIVER ---------------- #
driver = None

def create_driver():
    log("🌐 Starting browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)

    if COOKIE_FILE:
        try:
            drv.get("https://in.tradingview.com/")
            with open(COOKIE_FILE, "r") as f:
                cookies = json.load(f)
            for c in cookies:
                drv.add_cookie({k: v for k, v in c.items() if k in ("name","value","path","secure","expiry")})
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
    elements = drv.find_elements(By.CSS_SELECTOR, "[class*='valueValue']")
    return [el.text.strip() for el in elements if el.text.strip()]

def scrape_day(url):
    if not url:
        return [""] * EXPECTED_COUNT, "NOT OK", "", ""

    for _ in range(2):
        try:
            drv = ensure_driver()
            drv.get(url)

            WebDriverWait(drv, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='valueValue']"))
            )

            time.sleep(3)
            vals = get_values(drv)

            if len(vals) < EXPECTED_COUNT:
                for y in [600, 1200, 2000]:
                    drv.execute_script(f"window.scrollTo(0, {y});")
                    time.sleep(1.5)
                    vals = get_values(drv)
                    if len(vals) >= EXPECTED_COUNT:
                        break

            browser_url = drv.current_url

            if len(vals) >= EXPECTED_COUNT:
                return vals[:EXPECTED_COUNT], "OK", url, browser_url
            else:
                padded = (vals + [""] * EXPECTED_COUNT)[:EXPECTED_COUNT]
                return padded, "NOT OK", url, browser_url

        except:
            restart_driver()

    return [""] * EXPECTED_COUNT, "NOT OK", url, ""

# ---------------- SHEETS ---------------- #
def connect_sheets():
    gc = gspread.service_account("credentials.json")
    sh_main = gc.open("STOCKLIST 2").worksheet("Sheet1")
    sh_data = gc.open("MV2 DAY").worksheet("Sheet1")
    return sh_main, sh_data

# ---------------- FIND NOT OK ---------------- #
def find_not_ok_rows(sheet_data):
    log("🔍 Finding NOT OK rows...")

    status_values = api_retry(
        sheet_data.col_values,
        col_letter_to_num(STATUS_COL)
    )

    indices = []
    for i, val in enumerate(status_values[1:], start=1):
        if val.strip().upper() == "NOT OK":
            indices.append(i)

    log(f"⚠️ Found {len(indices)} rows")
    return indices

# ---------------- PROCESS ---------------- #
def process_row(i, company_list, url_list, current_date):
    name = company_list[i].strip()
    url = url_list[i].strip() if "http" in url_list[i] else None

    log(f"🔁 Fixing [{i+1}] {name}")

    vals, status, sheet_url, browser_url = scrape_day(url)

    row_idx = i + 1

    return [
        {"range": f"A{row_idx}", "values": [[name]]},
        {"range": f"B{row_idx}", "values": [[current_date]]},
        {"range": f"{DAY_START_COL_LETTER}{row_idx}:{DAY_END_COL_LETTER}{row_idx}", "values": [vals]},
        {"range": f"{STATUS_COL}{row_idx}", "values": [[status]]},
        {"range": f"{SHEET_URL_COL}{row_idx}", "values": [[sheet_url]]},
        {"range": f"{BROWSER_URL_COL}{row_idx}", "values": [[browser_url]]}
    ]

# ---------------- MAIN ---------------- #
def main():
    sheet_main, sheet_data = connect_sheets()

    company_list = api_retry(sheet_main.col_values, 1)
    url_list = api_retry(sheet_main.col_values, 4)

    not_ok_rows = find_not_ok_rows(sheet_data)

    if not not_ok_rows:
        log("✅ No NOT OK rows found")
        return

    restart_driver()
    batch = []
    current_date = date.today().strftime("%m/%d/%Y")

    for idx, i in enumerate(not_ok_rows):
        batch.extend(process_row(i - 1, company_list, url_list, current_date))

        if (idx + 1) % 10 == 0:
            restart_driver()
            api_retry(sheet_data.batch_update, batch, value_input_option="RAW")
            batch = []

    if batch:
        api_retry(sheet_data.batch_update, batch, value_input_option="RAW")

    restart_driver()
    log("🏁 CLEANER COMPLETED")

# ---------------- RUN ---------------- #
if __name__ == "__main__":
    main()
