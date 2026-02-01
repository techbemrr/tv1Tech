// ✅ ES MODULE VERSION: Fixed "require is not defined" error
// Optimized for GitHub Actions with Step-by-Step Status Logging

import fs from 'fs';
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function safeGoto(page, url, retries = 3) {
  // 1. Validate and Load Cookies
  try {
    if (fs.existsSync('./cookies.json')) {
      const cookiesString = fs.readFileSync('./cookies.json', 'utf8');
      const cookies = JSON.parse(cookiesString);
      await page.setCookie(...cookies);
      console.log("[Status] SUCCESS: Cookies loaded from cookies.json");
    } else {
      console.log("[Status] INFO: No cookies.json found. Proceeding as Guest.");
    }
  } catch (err) {
    console.warn("[Status] WARNING: Failed to parse cookies.json.");
  }

  // Set real-world headers
  await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36");

  for (let i = 0; i < retries; i++) {
    try {
      console.log(`[Status] Navigating: Attempt ${i + 1} for ${url}`);
      await page.goto(url, { waitUntil: "load", timeout: 60000 });

      // 2. Check Login State
      const isLoggedIn = await page.evaluate(() => {
        // Look for the user profile button instead of the "Sign In" button
        return !!document.querySelector('button[name="header-user-menu-button"]') || 
               !!document.querySelector('.tv-header__user-menu-button--user');
      });
      console.log(`[Status] Login Verified: ${isLoggedIn ? "YES (Authenticated)" : "NO (Guest Mode)"}`);

      await killPopups(page);

      // 3. Wait for Chart Engine
      await page.waitForSelector('canvas', { timeout: 20000 });
      console.log("[Status] Chart Engine: Loaded");

      // 4. Force Activation (Simulation)
      await page.setViewport({ width: 1921, height: 1081 });
      await delay(500);
      await page.setViewport({ width: 1920, height: 1080 });
      
      const view = page.viewport();
      await page.mouse.move(view.width / 2, view.height / 2);
      await page.mouse.click(view.width / 2, view.height / 2);
      console.log("[Status] Interaction: Chart Activated");

      // 5. Wait for Data Calculation
      console.log("[Status] Calculating Indicator Values...");
      const dataReady = await page.waitForFunction(() => {
        const studies = document.querySelectorAll('[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA');
        const clubbed = Array.from(studies).find(s => {
          const title = s.querySelector('[data-qa-id="legend-source-title"] .title-l31H9iuA')?.innerText?.toLowerCase();
          return title === "clubbed" || title === "l";
        });
        if (!clubbed) return false;
        const firstVal = clubbed.querySelector(".valueValue-l31H9iuA")?.innerText || "";
        return /[0-9.-]/.test(firstVal); // True if it contains a digit
      }, { timeout: 30000, polling: 1000 }).catch(() => false);

      if (!dataReady) {
        console.log("[Status] Data Failure: Values stayed ∅. Retrying...");
        if (i < retries - 1) continue;
      } else {
        console.log("[Status] Calculation: Success");
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
      document.body.style.overflow = 'auto';
    });
  } catch (e) {}
}

export async function scrapeChart(page, url) {
  const EXPECTED_VALUE_COUNT = 25;

  try {
    await page.setViewport({ width: 1920, height: 1080 });
    const success = await safeGoto(page, url);

    if (!success) {
      return ["", "", ...fixedLength(["NAVIGATION FAILED"], EXPECTED_VALUE_COUNT)];
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

        if (!target) return ["INDICATOR NOT FOUND"];

        const spans = target.querySelectorAll(".valueValue-l31H9iuA");
        return [...spans].map(s => {
          const val = s.innerText.trim();
          return (val === "∅" || val === "" || val === "n/a") ? "None" : val;
        });
      }
    );

    console.log(`[Status] Row Success: ${values[0]}, ${values[1]}...`);
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
