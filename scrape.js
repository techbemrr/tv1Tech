// ✅ FINAL PRODUCTION VERSION: ES Module
import fs from 'fs';
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function safeGoto(page, url, retries = 3) {
  // --- STEP 1: COOKIE INJECTION ---
  try {
    if (fs.existsSync('./cookies.json')) {
      const cookiesString = fs.readFileSync('./cookies.json', 'utf8');
      const cookies = JSON.parse(cookiesString);
      await page.setCookie(...cookies);
      console.log("[Status] SUCCESS: Cookies applied to browser.");
    } else {
      console.warn("[Status] ALERT: No cookies.json found. Running in Guest Mode.");
    }
  } catch (err) {
    console.error("[Status] ERROR: Cookie format is invalid.");
  }

  await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36");

  for (let i = 0; i < retries; i++) {
    try {
      console.log(`[Status] Navigating: Attempt ${i + 1}`);
      await page.goto(url, { waitUntil: "load", timeout: 60000 });

      // --- STEP 2: LOGIN VERIFICATION ---
      const authStatus = await page.evaluate(() => {
        // Look for the user profile icon (only exists if logged in)
        return !!document.querySelector('button[name="header-user-menu-button"]') || 
               !!document.querySelector('.tv-header__user-menu-button--user');
      });
      console.log(`[Status] Login Verified: ${authStatus ? "YES" : "NO"}`);

      await killPopups(page);

      // --- STEP 3: CHART ACTIVATION ---
      await page.waitForSelector('canvas', { timeout: 20000 });
      
      // Resize trick to force chart redraw
      await page.setViewport({ width: 1921, height: 1081 });
      await delay(500);
      await page.setViewport({ width: 1920, height: 1080 });

      // Click center of chart to focus
      const view = page.viewport();
      await page.mouse.click(view.width / 2, view.height / 2);
      
      // --- STEP 4: DATA CALCULATION (Fix for ∅) ---
      console.log("[Status] Waiting for Indicator values to load...");
      const dataReady = await page.waitForFunction(() => {
        const studies = document.querySelectorAll('[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA');
        const target = Array.from(studies).find(s => {
          const title = s.querySelector('[data-qa-id="legend-source-title"]')?.innerText?.toLowerCase();
          return title === "clubbed" || title === "l";
        });
        
        if (!target) return false;

        // Force a click on the indicator title to refresh values
        const titleEl = target.querySelector('.title-l31H9iuA');
        if (titleEl && !window.hasRefreshed) {
          titleEl.click();
          window.hasRefreshed = true;
        }

        const vals = Array.from(target.querySelectorAll(".valueValue-l31H9iuA"));
        // Success if at least one value is a number (0-9)
        return vals.some(v => /[0-9.-]/.test(v.innerText));
      }, { timeout: 40000, polling: 1000 }).catch(() => false);

      if (dataReady) {
        console.log("[Status] SUCCESS: Values loaded.");
        return true;
      }
      
      console.log("[Status] FAILED: Values stayed ∅. Retrying...");
    } catch (err) {
      console.error(`[Status] Loop Error: ${err.message}`);
    }
  }
  return false;
}

async function killPopups(page) {
  try {
    await page.keyboard.press("Escape");
    await page.evaluate(() => {
      const selectors = ['#overlap-manager-root', '[class*="dialog-"]', '.tv-dialog__close', '.js-dialog__close', '.modal-backdrop'];
      selectors.forEach(s => document.querySelectorAll(s).forEach(el => el.remove()));
    });
  } catch (e) {}
}

export async function scrapeChart(page, url) {
  const EXPECTED_COLUMNS = 25;
  try {
    const success = await safeGoto(page, url);
    
    const now = new Date();
    const dateStr = `${String(now.getDate()).padStart(2, '0')}/${String(now.getMonth() + 1).padStart(2, '0')}/${now.getFullYear()}`;

    if (!success) return ["", "", dateStr, ...Array(EXPECTED_COLUMNS - 3).fill("TIMEOUT")];

    const data = await page.$$eval('[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA', (items) => {
      const club = items.find(i => {
        const t = i.querySelector('[data-qa-id="legend-source-title"]')?.innerText?.toLowerCase();
        return t === "clubbed" || t === "l";
      });
      if (!club) return ["NOT_FOUND"];
      return Array.from(club.querySelectorAll(".valueValue-l31H9iuA")).map(v => {
        const txt = v.innerText.trim();
        return (txt === "∅" || txt === "") ? "None" : txt;
      });
    });

    console.log(`[Status] EXTRACTION: ${data[0]}`);
    return ["", "", dateStr, ...fixedLength(data, EXPECTED_COLUMNS - 3)];
  } catch (err) {
    return ["", "", "ERROR", ...Array(EXPECTED_COLUMNS - 3).fill("ERROR")];
  }
}

function fixedLength(arr, len, fill = "") {
  if (arr.length >= len) return arr.slice(0, len);
  return arr.concat(Array(len - arr.length).fill(fill));
}
