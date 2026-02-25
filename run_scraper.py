import sys
import os
import time
import json
import random
from datetime import date, datetime
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
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 0

FAILED_FILE = os.getenv("FAILED_FILE", f"failed_{SHARD_INDEX}.txt")
_failed_seen = set()

def mark_failed(i, reason="NO_VALUES"):
    if i in _failed_seen:
        return
    _failed_seen.add(i)
    try:
        with open(FAILED_FILE, "a") as f:
            f.write(f"{i}|{reason}\n")
    except:
        pass

# ✅ resolve chromedriver path ONCE
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

    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    driver.set_page_load_timeout(45)
    driver.set_script_timeout(30)

    # ---- COOKIE LOGIC ----
    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(2)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)

            allow = ("name", "value", "domain", "path", "secure", "expiry", "httpOnly", "sameSite")
            ok = 0
            for c in cookies:
                try:
                    driver.add_cookie({k: v for k, v in c.items() if k in allow})
                    ok += 1
                except:
                    continue

            driver.refresh()
            time.sleep(1)
            log(f"✅ Cookies applied successfully ({ok}/{len(cookies)})")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:160]}")

    return driver

# ---------------- SAFETY HELPERS (NO LOGIC CHANGE) ---------------- #
def is_blocked_html(html_lower: str) -> bool:
    return ("captcha" in html_lower) or ("access denied" in html_lower) or ("sign in" in html_lower)

def safe_get(driver, url) -> bool:
    """Prevents driver.get() from hanging forever."""
    try:
        driver.get(url)
        return True
    except TimeoutException:
        try:
            driver.execute_script("window.stop();")
        except:
            pass
        return False
    except WebDriverException:
        return False

# ---------------- SCRAPER LOGIC ---------------- #
# ✅ DO NOT CHANGE: exact XPATH + exact class parsing kept as-is
TV_XPATH = '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
TV_CLASS = "valueValue-l31H9iuA apply-common-tooltip"

def scrape_tradingview(driver, url):
    try:
        ok = safe_get(driver, url)
        if not ok:
            log("⚠️ driver.get() timeout/crash -> RESTART")
            return "RESTART"

        # ✅ fast blocked check BEFORE waiting 45s
        html = driver.page_source.lower()
        if is_blocked_html(html):
            return "RESTART"

        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((By.XPATH, TV_XPATH))
        )

        # ✅ blocked check AFTER wait too
        html = driver.page_source.lower()
        if is_blocked_html(html):
            return "RESTART"

        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_=TV_CLASS)
        ]
        return values

    except (TimeoutException, NoSuchElementException):
        return []
    except WebDriverException:
        log("🛑 Browser Crash Detected")
        return "RESTART"

# ✅ retries wrapper (same concept, just adds deadline + logs)
def scrape_with_retries(driver, url, tries=7, row_deadline_sec=120):
    start = time.time()
    delay = 1.0

    for attempt in range(1, tries + 1):
        if time.time() - start > row_deadline_sec:
            log(f"⏱️ Row deadline hit ({row_deadline_sec}s) -> giving up this row")
            return []

        log(f"   ↪ attempt {attempt}/{tries}")
        values = scrape_tradingview(driver, url)

        if values == "RESTART":
            return "RESTART"

        if isinstance(values, list) and values:
            return values

        # backoff + jitter
        try:
            driver.refresh()
        except:
            pass
        time.sleep(delay + random.uniform(0.1, 0.7))
        delay = min(delay * 1.7, 12)

    return []

# ---------------- INITIAL SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")

    company_list = sheet_main.col_values(3)  # URLs
    name_list = sheet_main.col_values(1)     # Names

    log(f"✅ Setup complete | Shard {SHARD_INDEX}/{SHARD_STEP} | Resume index {last_i} | Total {len(company_list)}")
except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = create_driver()
batch_list = []

BATCH_SIZE = 300   # NOTE: this is UPDATE REQUEST count, not row count
current_date = date.today().strftime("%m/%d/%Y")
ROW_SLEEP = 0.05

def flush_batch():
    global batch_list
    if not batch_list:
        return
    for attempt in range(5):
        try:
            sheet_data.batch_update(batch_list)
            log(f"🚀 Saved {len(batch_list)} updates")
            batch_list = []
            return
        except Exception as e:
            msg = str(e)
            log(f"⚠️ API Error: {msg[:160]}")
            if "429" in msg or "quota" in msg.lower() or "rate" in msg.lower():
                sleep_s = 60 + (SHARD_INDEX * 7)
                log(f"⏳ Quota hit, sleeping {sleep_s}s...")
                time.sleep(sleep_s)
            else:
                time.sleep(3 + attempt)

try:
    skipped = 0

    for i in range(last_i, len(company_list)):

        # shard skip (this can look like “stuck”, so log it occasionally)
        if i % SHARD_STEP != SHARD_INDEX:
            skipped += 1
            if skipped % 5000 == 0:
                log(f"…skipping due to shard ({skipped} skipped). current i={i+1}")
            continue
        skipped = 0

        url = (company_list[i] or "").strip()
        name = (name_list[i] if i < len(name_list) else f"Row {i+1}").strip()

        if not url.startswith("http"):
            log(f"⏭️ Row {i+1}: invalid/blank URL -> skipped")
            mark_failed(i, "BAD_URL")
            with open(checkpoint_file, "w") as f:
                f.write(str(i + 1))
            continue

        log(f"🔍 [{i+1}/{len(company_list)}] Scraping: {name}")

        values = scrape_with_retries(driver, url, tries=7, row_deadline_sec=120)

        if values == "RESTART":
            log("🔄 Restarting driver...")
            try:
                driver.quit()
            except:
                pass
            driver = create_driver()
            values = scrape_with_retries(driver, url, tries=5, row_deadline_sec=90)
            if values == "RESTART":
                values = []

        target_row = i + 1

        if isinstance(values, list) and values:
            batch_list.append({"range": f"A{target_row}", "values": [[name]]})
            batch_list.append({"range": f"J{target_row}", "values": [[current_date]]})
            batch_list.append({"range": f"K{target_row}", "values": [values]})
            log(f"✅ Values: {len(values)} cells | Buffered: {len(batch_list)}/{BATCH_SIZE}")
        else:
            batch_list.append({"range": f"A{target_row}", "values": [[name]]})
            batch_list.append({"range": f"J{target_row}", "values": [[current_date]]})
            mark_failed(i, "NO_VALUES")
            log(f"⚠️ No values found for {name} (A/J updated only)")

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
    log(f"📌 Failed rows file: {FAILED_FILE}")
