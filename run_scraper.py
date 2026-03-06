import sys
import os
import time
import json
import re
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
import gspread
from webdriver_manager.chrome import ChromeDriverManager


def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)


# ---------------- CONFIG & RANGE CALCULATION ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "500"))

START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW = START_ROW + SHARD_SIZE

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
if os.path.exists(checkpoint_file):
    try:
        last_i = max(int(open(checkpoint_file).read().strip()), START_ROW)
    except Exception:
        last_i = START_ROW
else:
    last_i = START_ROW

CHROME_DRIVER_PATH = ChromeDriverManager().install()


# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Range {START_ROW+1}-{END_ROW} | Initializing fresh browser...")
    opts = Options()
    opts.page_load_strategy = "eager"

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    drv.set_page_load_timeout(60)

    if os.path.exists("cookies.json"):
        try:
            drv.get("https://in.tradingview.com/")
            time.sleep(2)

            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)

            for c in cookies:
                try:
                    drv.add_cookie({
                        k: v for k, v in c.items()
                        if k in ("name", "value", "path", "secure", "expiry")
                    })
                except Exception:
                    continue

            drv.refresh()
            time.sleep(2)
            log("✅ Cookies applied.")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:80]}")
    return drv


# ---------------- DRIVER HELPERS ---------------- #
driver = None


def ensure_driver():
    global driver
    if driver is None:
        driver = create_driver()
    return driver


def restart_driver():
    global driver
    try:
        if driver:
            log("♻️ Closing browser after batch...")
            driver.quit()
    except Exception as e:
        log(f"⚠️ Browser close issue: {str(e)[:80]}")
    driver = None
    time.sleep(3)


# ---------------- EXTRACTION HELPERS ---------------- #
STRICT_VALUE_SELECTORS = [
    "div[data-name='legend-source-item'] div[class*='valueValue']",
    "div[data-name='legend-source-item'] span[class*='valueValue']",
    "div[class*='legend-source-item'] div[class*='valueValue']",
    "div[class*='legend-source-item'] span[class*='valueValue']",
]

STRICT_CONTAINER_SELECTORS = [
    "div[data-name='legend-source-item']",
    "div[class*='legend-source-item']",
]


def clean_text(text):
    return (
        text.replace("\u202f", " ")
        .replace("\xa0", " ")
        .replace("−", "-")
        .strip()
    )


def is_small_int(text):
    t = clean_text(text).replace(",", "")
    return re.fullmatch(r"\d+", t) is not None and int(t) <= 500


def is_numberish(text):
    t = clean_text(text).replace(",", "")
    return re.fullmatch(r"[+-]?\d+(\.\d+)?", t) is not None


def looks_like_junk_prefix(values):
    if len(values) < 4:
        return False
    # catches patterns like 1,3,6 or 1,50,50 before real-looking values
    return (
        is_small_int(values[0]) and
        is_small_int(values[1]) and
        is_small_int(values[2]) and
        is_numberish(values[3])
    )


def extract_strict_values(drv):
    values = []

    for sel in STRICT_VALUE_SELECTORS:
        try:
            elems = drv.find_elements(By.CSS_SELECTOR, sel)
            current = []
            for el in elems:
                try:
                    if el.is_displayed():
                        txt = clean_text(el.text)
                        if txt:
                            current.append(txt)
                except Exception:
                    pass
            if current:
                values = current
                break
        except Exception:
            pass

    if values:
        return values

    # JS fallback from exact legend container only
    js = """
    const containerSelectors = arguments[0];
    const valueSelectors = arguments[1];

    function visible(el) {
        if (!el) return false;
        const st = window.getComputedStyle(el);
        return st && st.display !== 'none' && st.visibility !== 'hidden' && el.offsetParent !== null;
    }

    for (const csel of containerSelectors) {
        const containers = Array.from(document.querySelectorAll(csel));
        for (const c of containers) {
            if (!visible(c)) continue;
            for (const vsel of valueSelectors) {
                const nodes = Array.from(c.querySelectorAll(vsel));
                const vals = nodes
                    .filter(n => visible(n))
                    .map(n => (n.innerText || n.textContent || '').trim())
                    .filter(Boolean);
                if (vals.length) return vals;
            }
        }
    }
    return [];
    """
    try:
        vals = drv.execute_script(js, STRICT_CONTAINER_SELECTORS, ["div[class*='valueValue']", "span[class*='valueValue']"])
        return [clean_text(x) for x in vals if clean_text(x)]
    except Exception:
        return []


def wait_for_strict_values(drv, timeout=18):
    def _ready(d):
        for sel in STRICT_VALUE_SELECTORS:
            try:
                elems = d.find_elements(By.CSS_SELECTOR, sel)
                visible = [e for e in elems if e.is_displayed() and clean_text(e.text)]
                if visible:
                    return True
            except Exception:
                pass
        return False

    WebDriverWait(drv, timeout).until(lambda d: _ready(d))


def nudge_chart(drv):
    try:
        drv.execute_script("window.scrollTo(0, 250);")
        time.sleep(0.6)
        drv.execute_script("window.scrollTo(0, 0);")
        time.sleep(1.2)
        ActionChains(drv).move_by_offset(20, 20).click().perform()
        time.sleep(0.8)
    except Exception:
        pass


# ---------------- SCRAPER ---------------- #
def scrape_tradingview(url, url_type=""):
    if not url:
        return []

    log(f"   📡 Navigating {url_type}: {url}")

    for attempt in range(3):
        try:
            drv = ensure_driver()
            drv.get(url)

            try:
                wait_for_strict_values(drv, timeout=18)
            except Exception:
                pass

            nudge_chart(drv)

            final_values = extract_strict_values(drv)

            if final_values and looks_like_junk_prefix(final_values):
                log(f"   ⚠️ {url_type} junk-looking values detected: {final_values[:8]}...")
                final_values = []

            if final_values:
                log(f"   📊 Found {len(final_values)} values for {url_type}")
                log(f"   📝 Data Preview: {final_values[:8]}...")
                return final_values

            log(f"   ⚠️ {url_type} No strict values found. Refreshing...")
            try:
                drv.refresh()
            except Exception:
                restart_driver()
            time.sleep(3)

        except Exception as e:
            log(f"   ❌ {url_type} ERROR: {str(e)[:100]}")
            restart_driver()
            time.sleep(2)

    return []


# ---------------- SHEETS SETUP ---------------- #
def connect_sheets():
    log("📊 Connecting to Google Sheets...")
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")
    return sheet_main, sheet_data


try:
    sheet_main, sheet_data = connect_sheets()

    company_list = sheet_main.col_values(1)
    url_d_list = sheet_main.col_values(4)   # D
    url_h_list = sheet_main.col_values(8)   # H

    log(f"✅ Data Ready. Starting from Row {last_i + 1}")
except Exception as e:
    log(f"❌ Connection Error: {e}")
    sys.exit(1)


# ---------------- MAIN LOOP ---------------- #
batch_list = []
buffered_rows = 0
BATCH_SIZE = 50
current_date = date.today().strftime("%m/%d/%Y")


def flush_batch():
    global batch_list, buffered_rows, sheet_data

    if not batch_list:
        return True

    log(f"🚀 UPLOADING BATCH: Sending {buffered_rows} rows to Google Sheets...")

    for attempt in range(3):
        try:
            sheet_data.batch_update(batch_list, value_input_option='RAW')
            log("✅ SUCCESS: Batch successfully written to Sheet2.")
            batch_list = []
            buffered_rows = 0
            return True
        except Exception as e:
            log(f"⚠️ API Retry {attempt+1}: {str(e)[:120]}")
            time.sleep(8 + (attempt * 5))
            try:
                _, sheet_data = connect_sheets()
            except Exception as inner_e:
                log(f"⚠️ Reconnect failed: {str(inner_e)[:120]}")

    return False


try:
    loop_end = min(END_ROW, len(company_list))

    for i in range(last_i, loop_end):
        name = company_list[i].strip()
        log(f"--- [ROW {i+1}] Processing: {name} ---")

        u_d = url_d_list[i].strip() if i < len(url_d_list) and url_d_list[i].startswith("http") else None
        u_h = url_h_list[i].strip() if i < len(url_h_list) and url_h_list[i].startswith("http") else None

        vals_d = scrape_tradingview(u_d, "DAILY") if u_d else []
        vals_h = scrape_tradingview(u_h, "HOURLY") if u_h else []
        combined = vals_d + vals_h

        row_idx = i + 1
        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"J{row_idx}", "values": [[current_date]]})
        batch_list.append({"range": f"K{row_idx}", "values": [combined] if combined else [[]]})

        buffered_rows += 1

        if combined:
            log(f"   📥 Buffered {len(combined)} values into batch.")
        else:
            log("   ⚠️ No values found. Blank K cell buffered.")

        log(f"📈 Shard Progress: {i+1}/{loop_end} | Batch Buffer: {buffered_rows}/{BATCH_SIZE}")

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        if buffered_rows >= BATCH_SIZE:
            ok = flush_batch()

            restart_driver()
            try:
                sheet_main, sheet_data = connect_sheets()
                company_list = sheet_main.col_values(1)
                url_d_list = sheet_main.col_values(4)
                url_h_list = sheet_main.col_values(8)
                log("✅ Fresh start ready for next batch.")
            except Exception as e:
                log(f"❌ Failed to reconnect sheets after batch: {e}")
                break

            if not ok:
                log("❌ Batch upload failed after retries.")
                break

        time.sleep(0.4)

finally:
    if batch_list:
        flush_batch()
    restart_driver()
    log("🏁 Shard Completed.")
