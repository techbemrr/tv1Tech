// ✅ UPDATED VERSION: Enhanced Cookie Application & Force Calculation

import fs from 'fs';
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function safeGoto(page, url, retries = 3) {
  // 1. Precise Cookie Loading
  try {
    if (fs.existsSync('./cookies.json')) {
      const cookiesString = fs.readFileSync('./cookies.json', 'utf8');
      let cookies = JSON.parse(cookiesString);
      
      // Ensure cookies are formatted for the TradingView domain
      const formattedCookies = cookies.map(c => ({
        ...c,
        domain: c.domain || '.tradingview.com',
        path: c.path || '/'
      }));
      
      await page.setCookie(...formattedCookies);
      console.log("[Status] SUCCESS: Cookies applied to session.");
    }
  } catch (err) {
    console.warn("[Status] ERROR: Cookie application failed:", err.message);
  }

  await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36");

  for (let i = 0; i < retries; i++) {
    try {
      console.log(`[Status] Navigating: Attempt ${i + 1}`);
      await page.goto(url, { waitUntil: "load", timeout: 60000 });

      // 2. Verified Login Check (More robust selectors)
      const loginData = await page.evaluate(() => {
        const userBtn = document.querySelector('button[name="header-user-menu-button"], .tv-header__user-menu-button--user, [data-name="header-user-menu-button"]');
        return {
          isLoggedIn: !!userBtn,
          htmlSnippet: userBtn ? "User Button Found" : "Not Found"
        };
      });
      console.log(`[Status] Login Verified: ${loginData.isLoggedIn ? "YES" : "NO (Guest Mode)"}`);

      await killPopups(page);

      // 3. Chart Activation
      await page.waitForSelector('canvas', { timeout: 20000 });
      
      // Force a "Click" on the indicator legend to wake up the engine
      await page.click('[data-qa-id="legend"]').catch(() => {});
      
      // Interaction Simulation
      await page.setViewport({ width: 1921, height: 1081 });
      await delay(500);
      await page.setViewport({ width: 1920, height: 1080 });
      
      const view = page.viewport();
      await page.mouse.click(view.width / 2, view.height / 2);
      console.log("[Status] Chart Engine: Activated");

      // 4. Wait for Data Calculation
      console.log("[Status] Calculating Indicator Values...");
      const dataReady = await page.waitForFunction(() => {
        const studies = document.querySelectorAll('[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA');
        const clubbed = Array.from(studies).find(s => {
          const title = s.querySelector('[data-qa-id="legend-source-title"] .title-l31H9iuA')?.innerText?.toLowerCase();
          return title === "clubbed" || title === "l";
        });
        if (!clubbed) return false;
        
        // Look for any of the value spans to contain a number
        const vals = Array.from(clubbed.querySelectorAll(".valueValue-l31H9iuA"));
        return vals.some(v => /[0-9.-]/.test(v.innerText));
      }, { timeout: 35000, polling: 1000 }).catch(() => false);

      if (!dataReady) {
        console.log("[Status] Calculation Failure: Values timed out.");
        if (i < retries - 1) {
            console.log("[Status] Retrying entire navigation...");
            continue;
        }
      } else {
        console.log("[Status] Calculation: SUCCESS");
        return true;
      }
    } catch (err) {
      console.error(`[Status] Navigation Error: ${err.message}`);
      if (i === retries - 1) return false;
      await delay(5000);
    }
  }
}

async function killPopups(page) {
  try {
    await page.keyboard.press("Escape");
    await page.evaluate(() => {
      const selectors = ['#overlap-manager-root', '[class*="overlap-manager"]', '[class*="dialog-"]', '.tv-dialog__close', '.js-dialog__close', '.modal-backdrop'];
      document.querySelectorAll(selectors.join(',')).forEach(el => el.remove());
    });
  } catch (e) {}
}

export async function scrapeChart(page, url) {
  const EXPECTED_VALUE_COUNT = 25;
  try {
    await page.setViewport({ width: 1920, height: 1080 });
    const success = await safeGoto(page, url);

    if (!success) {
      return ["", "", ...fixedLength(["FAILED"], EXPECTED_VALUE_COUNT)];
    }

    const now = new Date();
    const dateString = buildDate(now.getDate(), now.getMonth() + 1, now.getFullYear());

    const values = await page.$$eval(
      '[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA',
      (sections) => {
        const target = [...sections].find((section) => {
          const title = section.querySelector('[data-qa-id="legend-source-title"] .title-l31H9iuA');
          const text = title?.innerText?.trim().toLowerCase();
          return text === "clubbed" || text === "l";
        });

        if (!target) return ["NOT_FOUND"];
        const spans = target.querySelectorAll(".valueValue-l31H9iuA");
        return [...spans].map(s => {
          const val = s.innerText.trim();
          return (val === "∅" || val === "" || val === "n/a") ? "None" : val;
        });
      }
    );

    console.log(`[Status] Scraped successfully: ${values[0]}`);
    return ["", "", dateString, ...fixedLength(values, EXPECTED_VALUE_COUNT - 1)];
  } catch (err) {
    console.error(`[Status] Fatal Error:`, err.message);
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
