import fs from 'fs';
import path from 'path';

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Ensure debug directory exists
const debugDir = './debug';
if (!fs.existsSync(debugDir)) {
  fs.mkdirSync(debugDir);
}

async function safeGoto(page, url, retries = 3) {
  // 1. Inject Cookies
  try {
    if (fs.existsSync('./cookies.json')) {
      const cookies = JSON.parse(fs.readFileSync('./cookies.json', 'utf8'));
      await page.setCookie(...cookies);
      console.log("[Status] Cookies applied.");
    }
  } catch (err) {
    console.warn("[Status] Cookie error:", err.message);
  }

  await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36");

  for (let i = 0; i < retries; i++) {
    try {
      console.log(`[Status] Navigation Attempt ${i + 1}`);
      await page.goto(url, { waitUntil: "networkidle2", timeout: 60000 });

      // 2. Verified Login Check
      const isLoggedIn = await page.evaluate(() => {
        return !!document.querySelector('button[name="header-user-menu-button"], .tv-header__user-menu-button--user');
      });

      console.log(`[Status] Login Verified: ${isLoggedIn ? "YES ✅" : "NO ❌"}`);

      // 📸 Save Screenshot into /debug folder
      const screenshotName = `login_check_attempt_${i}_${isLoggedIn ? 'success' : 'failed'}.png`;
      await page.screenshot({ path: path.join(debugDir, screenshotName), fullPage: true });
      console.log(`[Debug] Screenshot saved to ${debugDir}/${screenshotName}`);

      if (!isLoggedIn && i < retries - 1) continue;

      await page.waitForSelector('canvas', { timeout: 20000 });
      await page.click('[data-qa-id="legend"]').catch(() => {});
      
      const dataReady = await page.waitForFunction(() => {
        const items = document.querySelectorAll('[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA');
        const target = Array.from(items).find(s => {
          const t = s.querySelector('[data-qa-id="legend-source-title"]')?.innerText?.toLowerCase();
          return t === "clubbed" || t === "l";
        });
        if (!target) return false;
        const firstVal = target.querySelector(".valueValue-l31H9iuA")?.innerText;
        return /[0-9.-]/.test(firstVal);
      }, { timeout: 30000 }).catch(() => false);

      if (dataReady) return true;
    } catch (err) {
      console.error(`[Error] ${err.message}`);
    }
  }
  return false;
}

export async function scrapeChart(page, url) {
  const COLS = 25;
  try {
    const success = await safeGoto(page, url);
    const now = new Date();
    const dateStr = `${String(now.getDate()).padStart(2, '0')}/${String(now.getMonth() + 1).padStart(2, '0')}/${now.getFullYear()}`;

    if (!success) return ["", "", dateStr, ...Array(COLS - 3).fill("FAILED")];

    const data = await page.$$eval('[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA', (items) => {
      const club = items.find(i => {
        const t = i.querySelector('[data-qa-id="legend-source-title"]')?.innerText?.toLowerCase();
        return t === "clubbed" || t === "l";
      });
      return club ? Array.from(club.querySelectorAll(".valueValue-l31H9iuA")).map(v => v.innerText.trim()) : ["NOT_FOUND"];
    });

    return ["", "", dateStr, ...data.map(v => (v === "∅" || v === "") ? "None" : v).slice(0, COLS - 3)];
  } catch (err) {
    return ["", "", "ERROR", ...Array(COLS - 3).fill("ERROR")];
  }
}
