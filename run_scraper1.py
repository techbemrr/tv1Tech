import sys
import os
import time
import json
import random
import traceback
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
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
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_week_{SHARD_INDEX}.txt")

EXPECTED_COUNT = 24
BATCH_SIZE = 100
RESTART_EVERY_ROWS = 20
COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")
CHROME_DRIVER_PATH = ChromeDriverManager().install()

WEEK_OUTPUT_START_COL = 3

# NEW: optional. If set to the indicator's title as shown in the chart legend
# (like "DAY" in the daily chart), only that indicator's values are read.
# If left empty, values are read exactly as before (every visible value on
# the page, in order), so the columns written to the sheet don't change.
INDICATOR_TITLE = os.getenv("INDICATOR_TITLE", "").strip()

# How long to wait for all values to render (seconds)
VALUES_TIMEOUT = int(os.getenv("VALUES_TIMEOUT", "30"))
PAGE_LOAD_TIMEOUT = int(os.getenv("PAGE_LOAD_TIMEOUT", "60"))

# NEW: stop early if this many symbols in a row fail (0 = never stop early)
MAX_CONSECUTIVE_FAILS = int(os.getenv("MAX_CONSECUTIVE_FAILS", "5"))

# NEW: screenshots + page HTML of failures (upload as a workflow artifact)
DEBUG_DIR = os.getenv("DEBUG_DIR", "debug_week")
MAX_DEBUG_SAVES = int(os.getenv("MAX_DEBUG_SAVES", "5"))

# OPTIONAL: "−" -> "-" and "1,176" -> "1176". Off by default (values unchanged).
CLEAN_NUMBERS = os.getenv("CLEAN_NUMBERS", "0") == "1"

# Used only when INDICATOR_TITLE is set. Matches on the "valueValue-" prefix,
# so TradingView changing the random suffix (e.g. "-quatTGAC") doesn't matter.
GET_VALUES_JS = """
const want = arguments[0];
const titles = [...document.querySelectorAll('[class*="title-"]')]
  .filter(e => (e.innerText || '').trim() === want);
if (!titles.length) {
  return {found: false, values: []};
}
let node = titles[0];
while (node && node.querySelectorAll('[class*="valueValue-"]').length === 0) {
  node = node.parentElement;
}
if (!node) {
  return {found: true, values: []};
}
const values = [...node.querySelectorAll('[class*="valueValue-"]')]
  .map(e => (e.innerText || '').trim());
return {found: true, values: values};
"""

# ---------------- UTILS ---------------- #
def col_num_to_letter(n):
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result

WEEK_START_COL_LETTER = col_num_to_letter(WEEK_OUTPUT_START_COL)
WEEK_END_COL_LETTER = col_num_to_letter(WEEK_OUTPUT_START_COL + EXPECTED_COUNT - 1)

def api_retry(func, *args, **kwargs):
    for attempt in range(5):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            wait = (2 ** attempt) + random.random()
            log(f"⚠️ API Issue: {str(e)[:100]}. Retrying in {wait:.1f}s...")
            time.sleep(wait)
    return func(*args, **kwargs)

def describe_error(e):
    # CHANGED: Selenium's TimeoutException has an empty message; always show the type
    msg = str(e).strip().replace("\n", " ")
    return f"{type(e).__name__}: {msg[:150] if msg else '(no message)'}"

_debug_saves = 0

def save_debug(drv, label):
    # NEW: save what the browser actually loaded
    global _debug_saves
    if drv is None or _debug_saves >= MAX_DEBUG_SAVES:
        return
    _debug_saves += 1
    try:
        os.makedirs(DEBUG_DIR, exist_ok=True)
        safe = "".join(ch if ch.isalnum() else "_" for ch in label)[:60]
        base = os.path.join(DEBUG_DIR, f"{_debug_saves:02d}_{safe}")
        try:
            log(f"   🧾 Page title: {drv.title!r}")
            log(f"   🧾 Current URL: {drv.current_url}")
        except Exception:
            pass
        drv.save_screenshot(base + ".png")
        with open(base + ".html", "w", encoding="utf-8") as f:
            f.write(drv.page_source)
        log(f"   🧾 Saved debug files: {base}.png / .html")
    except Exception as e:
        log(f"   ⚠️ Could not save debug files: {describe_error(e)}")

# ---------------- STATE ---------------- #
if os.path.exists(checkpoint_file):
    try:
        last_i = max(int(open(checkpoint_file).read().strip()), START_ROW)
    except Exception:
        last_i = START_ROW
else:
    last_i = START_ROW

# ---------------- DRIVER ---------------- #
driver = None

def create_driver():
    log(f"🌐 [WEEK Shard {SHARD_INDEX}] Initializing browser...")
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
    # CHANGED: removed the hard-coded "Chrome/120.0.0.0" user agent (years older
    # than the Chrome GitHub installs). The real version is set below instead.

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    drv.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

    try:
        ua = drv.execute_script("return navigator.userAgent").replace("HeadlessChrome", "Chrome")
        drv.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": ua})
        log(f"   🧭 Browser: {ua}")
    except Exception as e:
        log(f"   ⚠️ Could not set user agent: {describe_error(e)}")

    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            with open(COOKIE_FILE, "r", encoding="utf-8") as f:
                cookies = json.load(f)
            added = failed = expired = 0
            now = time.time()
            for c in cookies:
                cookie = {k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")}
                exp = c.get("expiry") or c.get("expirationDate")
                if exp and float(exp) < now:
                    expired += 1
                if "expiry" in cookie:
                    try:
                        cookie["expiry"] = int(cookie["expiry"])
                    except Exception:
                        cookie.pop("expiry", None)
                try:
                    drv.add_cookie(cookie)
                    added += 1
                except Exception:
                    failed += 1
            # CHANGED: report cookie loading instead of failing silently
            log(f"   🍪 Cookies: {added} added, {failed} failed, {expired} already expired (site: {drv.current_url})")
            if expired:
                log("   ⚠️ Some cookies are expired. Refresh the cookies secret if values are missing.")
            drv.refresh()
            time.sleep(2)
        except Exception as e:
            log(f"   ⚠️ Cookie loading failed: {describe_error(e)}")
    else:
        log(f"   ⚠️ No cookie file found at {COOKIE_FILE}")
    return drv

def ensure_driver():
    global driver
    if driver is None: driver = create_driver()
    return driver

def restart_driver():
    global driver
    if driver:
        try: driver.quit()
        except Exception: pass
    driver = None

# ---------------- SCRAPER ---------------- #
def clean_value(v):
    if not CLEAN_NUMBERS:
        return v
    return v.replace("−", "-").replace(",", "")

def get_values(drv):
    """Returns (values, indicator_found). indicator_found is None in default mode."""
    try:
        if INDICATOR_TITLE:
            result = drv.execute_script(GET_VALUES_JS, INDICATOR_TITLE) or {}
            vals = [v for v in result.get("values", []) if v]
            return vals, bool(result.get("found"))
        # Default: same as the original script
        elements = drv.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")
        return [el.text.strip() for el in elements if el.text.strip()], None
    except Exception as e:
        log(f"   ⚠️ Could not read values: {describe_error(e)}")
        return [], False

def scrape_week(url, label=""):
    if not url: return [], False
    for attempt in range(2):
        drv = None
        try:
            drv = ensure_driver()
            drv.get(url)

            # CHANGED: wait until all values have rendered (instead of waiting for
            # one element, sleeping and scrolling - the chart doesn't scroll)
            try:
                WebDriverWait(drv, VALUES_TIMEOUT, poll_frequency=1).until(
                    lambda d: len(get_values(d)[0]) >= EXPECTED_COUNT
                )
            except TimeoutException:
                pass  # reported below

            vals, found = get_values(drv)

            if len(vals) >= EXPECTED_COUNT:
                return [clean_value(v) for v in vals[:EXPECTED_COUNT]], True

            # NEW: say why it failed
            if found is False:
                reason = f"indicator title '{INDICATOR_TITLE}' not on page (wrong page, logged out, blocked, or not loaded)"
            elif not vals:
                reason = "no values on page (wrong page, logged out, blocked, or not loaded)"
            else:
                reason = f"only {len(vals)}/{EXPECTED_COUNT} values"
            log(f"   ⚠️ Attempt {attempt+1}: {reason}")
            save_debug(drv, f"{label}_attempt{attempt+1}")

            if attempt == 0:
                continue  # try once more with the same browser
            return [clean_value(v) for v in vals], False  # Partially found
        except Exception as e:
            # CHANGED: full error type + traceback instead of 50 characters
            log(f"   ❌ Scrape Attempt {attempt+1} Failed: {describe_error(e)}")
            log(traceback.format_exc().strip()[-800:])
            save_debug(drv, f"{label}_error{attempt+1}")
            restart_driver()  # only a crash needs a fresh browser
    return [], False

# ---------------- CORE LOGIC ---------------- #
def process_row(i, company_list, url_list, current_date):
    name = company_list[i].strip() if i < len(company_list) else "Unknown"
    url = url_list[i].strip() if i < len(url_list) and "http" in url_list[i] else None

    log(f"🔍 [{i+1}] {name}")
    vals, is_success = scrape_week(url, label=f"{i+1}_{name}")

    row_idx = i + 1
    padded_vals = (vals + [""] * EXPECTED_COUNT)[:EXPECTED_COUNT]

    row_payload = [
        {"range": f"A{row_idx}", "values": [[name]]},
        {"range": f"B{row_idx}", "values": [[current_date]]},
        {"range": f"{WEEK_START_COL_LETTER}{row_idx}:{WEEK_END_COL_LETTER}{row_idx}", "values": [padded_vals]}
    ]
    # rows without a URL don't count as scraping failures
    return row_payload, is_success, bool(url)

# ---------------- MAIN ---------------- #
def connect_sheets():
    gc = gspread.service_account("credentials.json")
    sh_main = gc.open("Stock List").worksheet("Sheet1")
    sh_data = gc.open("MV2 WEEK").worksheet("Sheet1")
    return sh_main, sh_data

try:
    sheet_main, sheet_data = connect_sheets()
    company_list = api_retry(sheet_main.col_values, 1)
    url_list = api_retry(sheet_main.col_values, 8) # Column H
    loop_end = min(END_ROW, len(company_list))
    log(f"✅ Ready. Processing Rows {last_i + 1} to {loop_end}")
    log(f"   Mode: {'indicator ' + repr(INDICATOR_TITLE) if INDICATOR_TITLE else 'all values on page (original behaviour)'}")
except Exception as e:
    log(f"❌ Initial Connection Error: {e}"); sys.exit(1)

retry_indices = []
batch_list = []
current_date = date.today().strftime("%m/%d/%Y")
consecutive_fails = 0
streak_start = None
aborted = False

# --- FIRST PASS ---
try:
    for i in range(last_i, loop_end):
        payload, success, had_url = process_row(i, company_list, url_list, current_date)
        batch_list.extend(payload)

        if not success:
            retry_indices.append(i)

        # NEW: count failures in a row
        if had_url and not success:
            if consecutive_fails == 0:
                streak_start = i
            consecutive_fails += 1
        elif success:
            consecutive_fails = 0
            streak_start = None

        with open(checkpoint_file, "w") as f: f.write(str(i + 1))

        if MAX_CONSECUTIVE_FAILS and consecutive_fails >= MAX_CONSECUTIVE_FAILS:
            log(f"🛑 {consecutive_fails} symbols in a row failed. Stopping early - check the debug screenshots in the workflow artifacts.")
            # point the checkpoint back so a rerun doesn't skip these symbols
            with open(checkpoint_file, "w") as f: f.write(str(streak_start))
            aborted = True
            break

        if (i + 1) % RESTART_EVERY_ROWS == 0: restart_driver()

        if len(batch_list) // 3 >= BATCH_SIZE:
            log(f"🚀 Uploading batch of {BATCH_SIZE}...")
            api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")
            batch_list = []
finally:
    if batch_list:
        api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")
        batch_list = []

if aborted:
    restart_driver()
    log("❌ WEEK SHARD ABORTED (too many failures in a row).")
    sys.exit(1)  # makes the GitHub run show as failed

# --- RETRY PASS ---
if retry_indices:
    log(f"🔁 Starting Retry Pass for {len(retry_indices)} symbols...")
    restart_driver()

    for idx, i in enumerate(retry_indices):
        payload, success, _ = process_row(i, company_list, url_list, current_date)
        batch_list.extend(payload)

        # In retry pass, restart driver more often (every 10 rows) for stability
        if (idx + 1) % 10 == 0: restart_driver()

        if len(batch_list) // 3 >= 10: # Smaller batch for retries
            api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")
            batch_list = []

    if batch_list:
        api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")

restart_driver()
log("🏁 WEEK SHARD COMPLETED.")
