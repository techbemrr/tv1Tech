import fs from 'fs';
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function safeGoto(page, url, retries = 3) {
  // 1. Load Cookies from your login.js output
  try {
    if (fs.existsSync('./cookies.json')) {
      const cookiesString = fs.readFileSync('./cookies.json', 'utf8');
      const cookies = JSON.parse(cookiesString);
      await page.setCookie(...cookies);
      console.log("[Status] SUCCESS: Cookies applied to session.");
    }
  } catch (err) {
    console.warn("[Status] Cookie load skipped/failed.");
  }

  await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36");

  for (let i = 0; i < retries; i++) {
    try {
      console.log(`[Status] Navigating: Attempt ${i + 1} for ${url}`);
      await page.goto(url, { waitUntil: "load", timeout: 60000 });

      // 2. Kill Popups
      await killPopups(page);

      // 3. Verify Login
      const isLoggedIn = await page.evaluate(() => {
        return !!document.querySelector('button[name="header-user-menu-button"]') || 
               !!document.querySelector('.tv-header__user-menu-button--user');
      });
      console.log(`[Status] Login Verified: ${isLoggedIn ? "YES" : "NO (Guest Mode)"}`);

      // 4. Force Chart to wake up
      await page.waitForSelector('canvas', { timeout: 20000 });
      
      // Click the Legend area specifically to focus the study engine
      await page.click('[data-qa-id="legend"]').catch(() => {});
      
      // Simulate activity
      const view = page.viewport();
      await page.mouse.move(view.width / 2, view.height / 2);
      await page.mouse.click(view.width / 2, view.height / 2);
      
      // 5. Wait for specific calculation
      console.log("[Status] Calculating Indicator Values...");
      const dataReady = await page.waitForFunction(() => {
        const studies = document.querySelectorAll('[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA');
        const clubbed = Array.from(studies).find(s => {
          const title = s.querySelector('[data-qa-id="legend-source-title"] .title-l31H9iuA')?.innerText?.toLowerCase();
          return title === "clubbed" || title === "l";
        });
        if (!clubbed) return false;
        
        // Try to click the study title inside the browser to force it to refresh
        const titleEl = clubbed.querySelector('.title-l31H9iuA');
        if (titleEl && !window.hasClicked) {
            titleEl.click();
            window.hasClicked = true; 
        }

        const vals = Array.from(clubbed.querySelectorAll(".valueValue-l31H9iuA"));
        return vals.some(v => /[0-9.-]/.test(v.innerText));
      }, { timeout: 35000, polling: 1000 }).catch(() => false);

      if (!dataReady) {
        console.log("[Status] Data stayed ∅. Retrying...");
        if (i < retries - 1) continue;
      } else {
        console.log("[Status] Calculation: SUCCESS");
        return true;
      }
    } catch (err) {
      console.error(`[Status] Error: ${err.message}`);
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

    console.log(`[Status] Scraped: ${values[0]}...`);
    return ["", "", dateString, ...fixedLength(values, EXPECTED_VALUE_COUNT - 1)];
  } catch (err) {
    console.error(`[Status] Fatal:`, err.message);
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
