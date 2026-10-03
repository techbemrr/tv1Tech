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

checkpoint_file = os.getenv(
    "CHECKPOINT_FILE",
    f"checkpoint_day_{SHARD_INDEX}.txt"
)

EXPECTED_COUNT = 18
BATCH_SIZE = 50
RESTART_EVERY_ROWS = 20

COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")

CHROME_DRIVER_PATH = ChromeDriverManager().install()

DAY_OUTPUT_START_COL = 3

# CHANGED: no longer depends on the auto-generated "-quatTGAC" suffix.
# The values are found by the indicator's title as shown in the chart legend.
# If you rename the indicator in your TradingView layout, change this
# (or set the INDICATOR_TITLE env variable in the workflow).
INDICATOR_TITLE = os.getenv("INDICATOR_TITLE", "DAY")

# How long to wait for all 18 values to render (seconds)
VALUES_TIMEOUT = int(os.getenv("VALUES_TIMEOUT", "30"))

# Max time for a page load before giving up (seconds)
PAGE_LOAD_TIMEOUT = int(os.getenv("PAGE_LOAD_TIMEOUT", "45"))

# NEW: stop the run early if this many symbols in a row fail,
# instead of running for hours and writing empty rows.
# Set to 0 to disable.
MAX_CONSECUTIVE_FAILS = int(os.getenv("MAX_CONSECUTIVE_FAILS", "5"))

# NEW: on failure, save a screenshot + page HTML here (upload it as an
# artifact in the workflow to see what the browser actually loaded).
DEBUG_DIR = os.getenv("DEBUG_DIR", "debug")
MAX_DEBUG_SAVES = int(os.getenv("MAX_DEBUG_SAVES", "5"))

# OPTIONAL: convert TradingView's "−" (Unicode minus) to "-" and remove
# thousands commas ("1,176" -> "1176"). Off by default so the values written
# to the sheet stay exactly as before. Set CLEAN_NUMBERS=1 to turn on.
CLEAN_NUMBERS = os.getenv("CLEAN_NUMBERS", "0") == "1"


# JavaScript that runs inside the chart page:
# 1. finds the legend title equal to INDICATOR_TITLE (e.g. "DAY")
# 2. walks up to the nearest box that also contains values
# 3. returns those values in order
# Matching on the "valueValue-" prefix survives TradingView changing the
# random suffix (e.g. "-quatTGAC") in a future site update.
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


# ---------------- COLUMN UTILS ---------------- #

def col_num_to_letter(n):
    result = ""

    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result

    return result


DAY_START_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL)

DAY_END_COL_LETTER = col_num_to_letter(
    DAY_OUTPUT_START_COL + EXPECTED_COUNT - 1
)

STATUS_COL = col_num_to_letter(
    DAY_OUTPUT_START_COL + EXPECTED_COUNT
)

SHEET_URL_COL = col_num_to_letter(
    DAY_OUTPUT_START_COL + EXPECTED_COUNT + 1
)

BROWSER_URL_COL = col_num_to_letter(
    DAY_OUTPUT_START_COL + EXPECTED_COUNT + 2
)


# ---------------- API RETRY ---------------- #

def api_retry(func, *args, **kwargs):

    for attempt in range(5):

        try:
            return func(*args, **kwargs)

        except Exception as e:

            wait = (2 ** attempt) + random.random()

            log(
                f"⚠️ API Issue: {str(e)[:100]}. "
                f"Retrying in {wait:.1f}s..."
            )

            time.sleep(wait)

    return func(*args, **kwargs)


# ---------------- STATE ---------------- #

if os.path.exists(checkpoint_file):

    try:

        last_i = max(
            int(open(checkpoint_file).read().strip()),
            START_ROW
        )

    except Exception:

        last_i = START_ROW

else:

    last_i = START_ROW


# ---------------- ERROR / DEBUG HELPERS ---------------- #

def describe_error(e):
    # CHANGED: Selenium's TimeoutException has an empty message, which is why
    # the log only showed "Failed: Message:". Always include the error type.
    msg = str(e).strip().replace("\n", " ")
    return f"{type(e).__name__}: {msg[:150] if msg else '(no message)'}"


_debug_saves = 0


def save_debug(drv, label):
    # NEW: save what the browser actually saw, so a block page, login page
    # or changed layout is visible in the workflow artifacts.
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


# ---------------- DRIVER ---------------- #

driver = None


def create_driver():

    log(
        f"🌐 [Shard {SHARD_INDEX}] "
        f"Initializing browser..."
    )

    opts = Options()

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(
        "--disable-blink-features=AutomationControlled"
    )

    # CHANGED: removed the hard-coded "Chrome/120.0.0.0" user agent.
    # Chrome 120 is years older than the Chrome that GitHub installs, and a
    # site can treat an outdated browser differently. The real version is
    # used below instead.

    drv = webdriver.Chrome(
        service=Service(CHROME_DRIVER_PATH),
        options=opts
    )

    drv.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

    # Use the real browser version, minus the "HeadlessChrome" marker,
    # so the user agent matches the browser actually running.
    try:
        ua = drv.execute_script("return navigator.userAgent")
        ua = ua.replace("HeadlessChrome", "Chrome")
        drv.execute_cdp_cmd(
            "Network.setUserAgentOverride",
            {"userAgent": ua}
        )
        log(f"   🧭 Browser: {ua}")
    except Exception as e:
        log(f"   ⚠️ Could not set user agent: {describe_error(e)}")

    # ---------------- LOAD COOKIES ---------------- #

    if os.path.exists(COOKIE_FILE):

        try:

            drv.get("https://in.tradingview.com/")

            with open(
                COOKIE_FILE,
                "r",
                encoding="utf-8"
            ) as f:

                cookies = json.load(f)

            added = 0
            expired = 0
            failed = 0
            now = time.time()

            for c in cookies:

                cookie = {
                    k: v
                    for k, v in c.items()
                    if k in (
                        "name",
                        "value",
                        "path",
                        "secure",
                        "expiry"
                    )
                }

                # NEW: count expired cookies (an expired login is a common
                # reason for values to suddenly disappear)
                exp = c.get("expiry") or c.get("expirationDate")
                if exp and float(exp) < now:
                    expired += 1

                # Selenium expects an integer expiry
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
            log(
                f"   🍪 Cookies: {added} added, "
                f"{failed} failed, {expired} already expired "
                f"(site: {drv.current_url})"
            )

            if expired:
                log(
                    "   ⚠️ Some cookies are expired. "
                    "Refresh the cookies secret if values are missing."
                )

            drv.refresh()

            time.sleep(2)

        except Exception as e:

            log(
                f"⚠️ Cookie loading failed: "
                f"{describe_error(e)}"
            )

    else:

        log(f"   ⚠️ No cookie file found at {COOKIE_FILE}")

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

        except Exception:
            pass

    driver = None


# ---------------- SCRAPER ---------------- #

def clean_value(v):

    if not CLEAN_NUMBERS:
        return v

    return v.replace("−", "-").replace(",", "")


def get_values(drv):
    # CHANGED: reads only the values of the INDICATOR_TITLE indicator,
    # not every value in the legend (the old selector could also pick up
    # the stock's own O/H/L/C/Vol values).

    try:

        result = drv.execute_script(
            GET_VALUES_JS,
            INDICATOR_TITLE
        ) or {}

        vals = [
            v for v in result.get("values", [])
            if v
        ]

        return vals, bool(result.get("found"))

    except Exception as e:

        log(
            f"⚠️ Could not read values: "
            f"{describe_error(e)}"
        )

        return [], False


def scrape_day(url, label=""):

    if not url:

        return (
            [""] * EXPECTED_COUNT,
            "NOT OK",
            "",
            ""
        )

    for attempt in range(2):

        drv = None

        try:

            drv = ensure_driver()

            log(
                f"   🌐 Opening URL "
                f"(attempt {attempt + 1})"
            )

            drv.get(url)

            # CHANGED: wait until all 18 indicator values have rendered,
            # instead of waiting for one element and then sleeping/scrolling.
            # (The chart doesn't scroll, so the scroll loop was removed.)
            try:

                WebDriverWait(
                    drv,
                    VALUES_TIMEOUT,
                    poll_frequency=1
                ).until(
                    lambda d: len(get_values(d)[0]) >= EXPECTED_COUNT
                )

            except TimeoutException:

                pass  # handled below with a clear message

            vals, title_found = get_values(drv)

            browser_url = drv.current_url

            found_count = len(vals)

            # ---------------- RESULT ---------------- #

            if found_count >= EXPECTED_COUNT:

                log(
                    f"   ✅ Found "
                    f"{found_count}/{EXPECTED_COUNT}"
                )

                return (
                    [clean_value(v) for v in vals[:EXPECTED_COUNT]],
                    "OK",
                    url,
                    browser_url
                )

            # NEW: say *why* it failed
            if not title_found:
                reason = (
                    f"indicator title '{INDICATOR_TITLE}' not on page "
                    f"(wrong page, logged out, blocked, or not loaded)"
                )
            else:
                reason = (
                    f"indicator found but only "
                    f"{found_count}/{EXPECTED_COUNT} values"
                )

            log(
                f"   ⚠️ Attempt {attempt + 1}: {reason}"
            )

            save_debug(drv, f"{label}_attempt{attempt + 1}")

            if attempt == 0:
                # page may just have been slow; try once more
                # with the same browser
                continue

            padded = (
                [clean_value(v) for v in vals] +
                [""] * EXPECTED_COUNT
            )[:EXPECTED_COUNT]

            log(
                f"   ⚠️ Found "
                f"{found_count}/{EXPECTED_COUNT} "
                f"(Marking NOT OK)"
            )

            return (
                padded,
                "NOT OK",
                url,
                browser_url
            )

        except Exception as e:

            # CHANGED: log the error type + full traceback, not just
            # the (often empty) message
            log(
                f"   ❌ Attempt "
                f"{attempt + 1} Failed: "
                f"{describe_error(e)}"
            )

            log(traceback.format_exc().strip()[-800:])

            save_debug(drv, f"{label}_error{attempt + 1}")

            # Only a browser crash needs a fresh browser
            restart_driver()

    return (
        [""] * EXPECTED_COUNT,
        "NOT OK",
        url,
        ""
    )


# ---------------- GOOGLE SHEETS ---------------- #

def connect_sheets():

    gc = gspread.service_account(
        "credentials.json"
    )

    sh_main = gc.open(
        "STOCKLIST 2"
    ).worksheet("Sheet1")

    sh_data = gc.open(
        "MV2 DAY"
    ).worksheet("Sheet1")

    return sh_main, sh_data


# ---------------- PROCESS ROW ---------------- #

def process_row(
    i,
    company_list,
    url_list,
    current_date
):

    name = (
        company_list[i].strip()
        if i < len(company_list)
        else ""
    )

    url = (
        url_list[i].strip()
        if i < len(url_list)
        and "http" in url_list[i]
        else None
    )

    log(
        f"🔍 [{i + 1}] {name} | "
        f"URL: {url if url else 'No URL'}"
    )

    vals, status, sheet_url_used, browser_url_used = scrape_day(
        url,
        label=f"{i + 1}_{name}"
    )

    row_idx = i + 1

    row_payload = [

        {
            "range": f"A{row_idx}",
            "values": [[name]]
        },

        {
            "range": f"B{row_idx}",
            "values": [[current_date]]
        },

        {
            "range": (
                f"{DAY_START_COL_LETTER}{row_idx}:"
                f"{DAY_END_COL_LETTER}{row_idx}"
            ),
            "values": [vals]
        },

        {
            "range": f"{STATUS_COL}{row_idx}",
            "values": [[status]]
        },

        {
            "range": f"{SHEET_URL_COL}{row_idx}",
            "values": [[sheet_url_used]]
        },

        {
            "range": f"{BROWSER_URL_COL}{row_idx}",
            "values": [[browser_url_used]]
        }

    ]

    # A row with no URL is not a scraping failure
    return row_payload, (status == "OK"), bool(url)


# ---------------- CONNECT ---------------- #

try:

    sheet_main, sheet_data = connect_sheets()

    company_list = api_retry(
        sheet_main.col_values,
        1
    )

    url_list = api_retry(
        sheet_main.col_values,
        4
    )

    log(
        f"✅ Starting rows "
        f"{last_i + 1} to "
        f"{min(END_ROW, len(company_list))}"
    )

except Exception as e:

    log(
        f"❌ Connection Error: {e}"
    )

    sys.exit(1)


# ---------------- PROCESSING ---------------- #

retry_indices = []

batch_list = []

current_date = date.today().strftime(
    "%m/%d/%Y"
)

loop_end = min(
    END_ROW,
    len(company_list)
)

consecutive_fails = 0
streak_start = None
aborted = False


# ---------------- FIRST PASS ---------------- #

for i in range(
    last_i,
    loop_end
):

    payload, success, had_url = process_row(
        i,
        company_list,
        url_list,
        current_date
    )

    batch_list.extend(payload)

    if not success:

        retry_indices.append(i)

    # NEW: track failures in a row (rows without a URL don't count)
    if had_url and not success:
        if consecutive_fails == 0:
            streak_start = i
        consecutive_fails += 1
    elif success:
        consecutive_fails = 0
        streak_start = None

    # Save checkpoint

    with open(
        checkpoint_file,
        "w"
    ) as f:

        f.write(str(i + 1))

    if (
        MAX_CONSECUTIVE_FAILS
        and consecutive_fails >= MAX_CONSECUTIVE_FAILS
    ):

        log(
            f"🛑 {consecutive_fails} symbols in a row failed. "
            f"Stopping early - check the debug screenshots "
            f"in the workflow artifacts."
        )

        # Point the checkpoint back at the first failed row so a rerun
        # doesn't skip these symbols
        with open(checkpoint_file, "w") as f:
            f.write(str(streak_start))

        aborted = True
        break

    # Restart browser periodically

    if (i + 1) % RESTART_EVERY_ROWS == 0:

        restart_driver()

    # Upload batch

    if len(batch_list) // 6 >= BATCH_SIZE:

        log(
            "🚀 Uploading batch..."
        )

        api_retry(
            sheet_data.batch_update,
            batch_list,
            value_input_option="RAW"
        )

        batch_list = []


# ---------------- FINAL FIRST PASS UPLOAD ---------------- #

if batch_list:

    api_retry(
        sheet_data.batch_update,
        batch_list,
        value_input_option="RAW"
    )

    batch_list = []


if aborted:

    restart_driver()

    log("❌ SCRAPING ABORTED (too many failures in a row).")

    # Non-zero exit so the GitHub run shows as failed
    sys.exit(1)


# ---------------- RETRY PASS ---------------- #

if retry_indices:

    log(
        f"🔁 Retrying "
        f"{len(retry_indices)} "
        f"symbols labeled 'NOT OK'..."
    )

    restart_driver()

    batch_list = []

    for idx, i in enumerate(
        retry_indices
    ):

        payload, success, _ = process_row(
            i,
            company_list,
            url_list,
            current_date
        )

        batch_list.extend(payload)

        if (idx + 1) % 10 == 0:

            restart_driver()

            api_retry(
                sheet_data.batch_update,
                batch_list,
                value_input_option="RAW"
            )

            batch_list = []

    if batch_list:

        api_retry(
            sheet_data.batch_update,
            batch_list,
            value_input_option="RAW"
        )


# ---------------- CLEANUP ---------------- #

restart_driver()

log(
    "🏁 SCRAPING COMPLETED."
)
