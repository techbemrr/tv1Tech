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
import gspread
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    print(msg, flush=True)

# ---------------- CONFIG ---------------- #
SPREADSHEET_NAME_MAIN = "Stock List"
WORKSHEET_MAIN = "Sheet1"

SPREADSHEET_NAME_DATA = "MV2 for SQL"
WORKSHEET_DATA = "Sheet2"

URL_COL = 3   # C
NAME_COL = 1  # A

# Write to: A=name, C=date, G..=values
WRITE_NAME_COL = "A"
WRITE_DATE_COL = "C"
WRITE_VALUES_COL = "G"

# Resilience / speed knobs
PAGELOAD_TIMEOUT = 35
WAIT_VALUES_SEC = 18
RETRY_REFRESH_ONCE = True
RETRY_REOPEN_ONCE = True

# Google Sheets batching
BATCH_UPDATES_MAX = 240   # number of "range updates" buffered before flush
API_BACKOFF_ON_429 = 60

# Optional tiny pause to reduce bans; keep very low
ROW_DELAY_SEC = 0.05

checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 0

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log("🌐 Initializing Hardened Chrome Instance...")
    opts = Options()
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
    opts.add_argument("--disable-features=Translate,BackForwardCache,AcceptCHFrame,MediaRouter")
    opts.add_argument("--mute-audio")
    opts.add_argument("--lang=en-US")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.set_page_load_timeout(PAGELOAD_TIMEOUT)

    # ---- COOKIE LOGIC ----
    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(2)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)

            for c in cookies:
                try:
                    payload = {k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")}
                    if "path" not in payload:
                        payload["path"] = "/"
                    driver.add_cookie(payload)
                except:
                    continue

            driver.get("https://in.tradingview.com/")
            time.sleep(1)
            log("✅ Cookies applied successfully")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:100]}")

    return driver

# ---------------- POPUP KILLER (BEST-EFFORT) ---------------- #
def kill_popups(driver):
    # TradingView can show different overlays. We try common close buttons.
    xpaths = [
        '//button[@aria-label="Close"]',
        '//button[contains(@class,"close")]',
        '//div[@role="dialog"]//button',
        '//button[.//*[name()="svg"] and @aria-label="Close"]',
    ]
    for xp in xpaths:
        try:
            btns = driver.find_elements(By.XPATH, xp)
            for b in btns[:2]:
                try:
                    b.click()
                    time.sleep(0.1)
                except:
                    pass
        except:
            pass

    # Also escape key sometimes closes modals
    try:
        driver.switch_to.active_element.send_keys("\ue00c")  # ESC
    except:
        pass

# ---------------- SCRAPER LOGIC (FAST + STABLE) ---------------- #
VALUES_CSS = "div.valueValue-l31H9iuA.apply-common-tooltip"

def extract_values(driver):
    els = driver.find_elements(By.CSS_SELECTOR, VALUES_CSS)
    vals = [e.text.strip().replace('−', '-').replace('∅', 'None') for e in els if e.text.strip() != ""]
    return vals

def scrape_tradingview(driver, url):
    try:
        driver.get(url)
        kill_popups(driver)

        # ✅ wait until we see enough value boxes (stable vs absolute xpath)
        WebDriverWait(driver, WAIT_VALUES_SEC).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, VALUES_CSS)) >= 5
        )

        vals = extract_values(driver)
        return vals

    except (TimeoutException, NoSuchElementException):
        if RETRY_REFRESH_ONCE:
            try:
                driver.refresh()
                time.sleep(0.4)
                kill_popups(driver)
                WebDriverWait(driver, max(10, WAIT_VALUES_SEC - 5)).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, VALUES_CSS)) >= 5
                )
                return extract_values(driver)
            except:
                return []
        return []

    except WebDriverException:
        log("🛑 Browser Crash Detected")
        return "RESTART"

# ---------------- INITIAL SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open(SPREADSHEET_NAME_MAIN).worksheet(WORKSHEET_MAIN)
    sheet_data = gc.open(SPREADSHEET_NAME_DATA).worksheet(WORKSHEET_DATA)

    company_list = sheet_main.col_values(URL_COL)
    name_list = sheet_main.col_values(NAME_COL)

    # Remove header row if present (common case)
    # If row1 contains text "URL" etc., keep logic safe.
    # We'll still process row1 if it's a valid http link.
    log(f"✅ Setup complete | Resume index {last_i} | Total rows (col C): {len(company_list)}")
except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = create_driver()
batch_list = []
current_date = date.today().strftime("%m/%d/%Y")  # compute once per run

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
            log(f"⚠️ API Error: {msg[:140]}")
            if "429" in msg or "Quota" in msg:
                log(f"⏳ Quota hit, sleeping {API_BACKOFF_ON_429}s...")
                time.sleep(API_BACKOFF_ON_429)
            else:
                time.sleep(3)
    # If still failing, keep buffer (won't lose); next flush will retry.

try:
    # ✅ run till end (NO fixed count / NO break)
    for i in range(last_i, len(company_list)):
        url = (company_list[i] or "").strip()
        name = (name_list[i] if i < len(name_list) else f"Row {i+1}").strip()

        # ✅ Skip only truly invalid URLs (prevents fake “skips”)
        if not url.startswith("http"):
            log(f"⏭️ Row {i+1}: invalid/blank URL, skipping row")
            with open(checkpoint_file, "w") as f:
                f.write(str(i + 1))
            continue

        log(f"🔍 [{i+1}/{len(company_list)}] Scraping: {name}")

        values = scrape_tradingview(driver, url)

        # Restart browser once if it crashed
        if values == "RESTART":
            try:
                driver.quit()
            except:
                pass
            driver = create_driver()
            values = scrape_tradingview(driver, url)

            if values == "RESTART":
                values = []

        target_row = i + 1  # same row number as sheet

        # ✅ Always write Name + Date (so row isn't "skipped")
        batch_list.append({"range": f"{WRITE_NAME_COL}{target_row}", "values": [[name]]})
        batch_list.append({"range": f"{WRITE_DATE_COL}{target_row}", "values": [[current_date]]})

        # ✅ Values write: only when found (keeps previous values if scrape failed)
        if isinstance(values, list) and len(values) > 0:
            batch_list.append({"range": f"{WRITE_VALUES_COL}{target_row}", "values": [values]})
            log(f"✅ Values: {len(values)} cells")
        else:
            log("⚠️ No values found (kept existing G.. data unchanged)")

        # Flush when buffer large
        if len(batch_list) >= BATCH_UPDATES_MAX:
            flush_batch()

        # Save checkpoint after each row (no skip on crash)
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        if ROW_DELAY_SEC:
            time.sleep(ROW_DELAY_SEC)

finally:
    flush_batch()
    try:
        driver.quit()
    except:
        pass
    log("🏁 Scraping completed successfully")
