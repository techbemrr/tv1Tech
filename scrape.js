// ✅ COMBINED FINAL UPDATED VERSION
// - Adds browser launcher (local visible + GitHub Actions headless)
// - Auto bring tab to front
// - Optional debug pause to inspect page
// - Keeps your popup killer + scraping logic

import puppeteer from "puppeteer";

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * ✅ Launch Browser (Local Debug + CI Headless)
 * Use env:
 *   HEADLESS=true   -> for GitHub Actions
 *   HEADLESS=false  -> for local debugging (shows browser)
 *   DEBUG_PAUSE=1   -> pause after opening each url for manual inspection
 */
export async function launchBrowser() {
  const HEADLESS = String(process.env.HEADLESS ?? "false").toLowerCase() === "true";
  const DEBUG_PAUSE = String(process.env.DEBUG_PAUSE ?? "0") === "1";

  const browser = await puppeteer.launch({
    headless: HEADLESS ? "new" : false,
    slowMo: HEADLESS ? 0 : 80,
    defaultViewport: HEADLESS ? { width: 1920, height: 1080 } : null,
    devtools: HEADLESS ? false : true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-blink-features=AutomationControlled",
      "--disable-infobars",
      ...(HEADLESS ? [] : ["--start-maximized"]),
    ],
  });

  const page = await browser.newPage();

  // ✅ Modern UA
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
  );

  await page.setExtraHTTPHeaders({ "accept-language": "en-US,en;q=0.9" });

  // Helpful logs in terminal
  page.on("console", (msg) => console.log("[PAGE]", msg.text()));
  page.on("pageerror", (err) => console.log("[PAGE ERROR]", err.message));
  page.on("requestfailed", (req) =>
    console.log("[REQ FAILED]", req.url(), req.failure()?.errorText)
  );

  return { browser, page, HEADLESS, DEBUG_PAUSE };
}

async function safeGoto(page, url, retries = 3, debugPause = false) {
  for (let i = 0; i < retries; i++) {
    try {
      console.log(`[Navigation] Attempt ${i + 1}: ${url}`);

      await page.goto(url, { waitUntil: "load", timeout: 60000 });

      // ✅ Always bring to front (useful when NOT headless)
      try { await page.bringToFront(); } catch {}

      // Immediate burst to kill early blockers
      await killPopups(page);

      // ✅ Optional: pause so you can SEE the page and inspect
      if (debugPause) {
        console.log("🛑 DEBUG PAUSE (30s) — Inspect the browser tab now...");
        await delay(30000);
      }

      // Wait for the Chart Engine (Canvas) to actually render
      await page.waitForFunction(() => {
        const canvas = document.querySelector("canvas");
        return canvas && canvas.offsetWidth > 0;
      }, { timeout: 25000 });

      // Small buffer for technical indicators to calculate and display values
      await delay(4000);

      // Final sweep to catch delayed popups
      await killPopups(page);

      return true;
    } catch (err) {
      console.warn(`[Warning] Attempt ${i + 1} failed: ${err.message}`);
      await killPopups(page).catch(() => {});
      if (i === retries - 1) return false;
      await delay(5000);
    }
  }
}

async function killPopups(page) {
  try {
    // Escape key closes many TradingView native modals
    await page.keyboard.press("Escape");
    await page.keyboard.press("Escape");

    await page.evaluate(() => {
      // 1) Force scrollability
      document.documentElement.style.setProperty("overflow", "auto", "important");
      document.body.style.setProperty("overflow", "auto", "important");

      // 2) Remove common overlay/popup roots
      const selectors = [
        "#overlap-manager-root",
        '[class*="overlap-manager"]',
        '[class*="dialog-"]',
        ".tv-dialog__close",
        ".js-dialog__close",
        'button[name="close"]',
        '[data-role="toast-container"]',
        ".modal-backdrop",
      ];

      selectors.forEach((sel) => {
        document.querySelectorAll(sel).forEach((el) => el.remove());
      });

      // 3) Auto "Accept Cookies"
      const buttons = Array.from(document.querySelectorAll("button"));
      const consentBtn = buttons.find((b) => {
        const t = (b.innerText || "").toLowerCase();
        return t.includes("accept") || t.includes("agree") || t.includes("got it");
      });
      if (consentBtn) consentBtn.click();
    });
  } catch {
    // silent
  }
}

export async function scrapeChart(page, url, opts = {}) {
  const EXPECTED_VALUE_COUNT = 25;
  const debugPause = !!opts.debugPause;

  try {
    await page.setViewport({ width: 1920, height: 1080 });

    const success = await safeGoto(page, url, 3, debugPause);
    if (!success) {
      console.error(`[Error] Navigation failed permanently for: ${url}`);
      return ["", "", ...fixedLength(["NAVIGATION FAILED"], EXPECTED_VALUE_COUNT)];
    }

    await page.waitForSelector('[data-qa-id="legend"]', { timeout: 15000 });

    const now = new Date();
    const dateString = buildDate(now.getDate(), now.getMonth() + 1, now.getFullYear());

    const values = await page.$$eval(
      '[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA',
      (sections) => {
        const target = [...sections].find((section) => {
          const title = section.querySelector(
            '[data-qa-id="legend-source-title"] .title-l31H9iuA'
          );
          const text = title?.innerText?.trim().toLowerCase();
          return text === "clubbed" || text === "l";
        });

        if (!target) return ["INDICATOR NOT FOUND"];

        const spans = target.querySelectorAll(".valueValue-l31H9iuA");
        const results = [...spans].map((s) => (s.innerText || "").trim());

        return results.length > 0 ? results : ["NO VALUES"];
      }
    );

    console.log(`[Success] Scraped ${values.length} values from ${url}`);
    return ["", "", dateString, ...fixedLength(values, EXPECTED_VALUE_COUNT - 1)];
  } catch (err) {
    console.error(`[Fatal] Scrape Error on ${url}:`, err.message);
    return ["", "", ...fixedLength(["ERROR"], EXPECTED_VALUE_COUNT)];
  }
}

function fixedLength(arr, len, fill = "") {
  if (arr.length >= len) return arr.slice(0, len);
  return arr.concat(Array(len - arr.length).fill(fill));
}

function buildDate(day, month, year) {
  return `${String(day).padStart(2, "0")}/${String(month).padStart(2, "0")}/${year}`;
}

/**
 * ✅ Example runner (optional)
 * node yourfile.js
 */
if (import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    const { browser, page, DEBUG_PAUSE } = await launchBrowser();

    const testUrl = process.env.TEST_URL;
    if (!testUrl) {
      console.log("Set TEST_URL env var to test. Example:");
      console.log('TEST_URL="https://www.tradingview.com/chart/...." DEBUG_PAUSE=1 node yourfile.js');
      await browser.close();
      process.exit(0);
    }

    const data = await scrapeChart(page, testUrl, { debugPause: DEBUG_PAUSE });
    console.log("RESULT:", data);

    await browser.close();
  })();
}
