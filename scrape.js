import fs from 'fs';
import path from 'path';

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function safeGoto(page, url, retries = 3) {
  // --- 1. SESSION INJECTION ---
  try {
    if (fs.existsSync('./cookies.json')) {
      const cookiesString = fs.readFileSync('./cookies.json', 'utf8');
      const cookies = JSON.parse(cookiesString);
      await page.setCookie(...cookies);
      console.log("[Status] SUCCESS: Cookies loaded.");
    }
  } catch (err) {
    console.warn("[Status] Cookie load failed, continuing as guest.");
  }

  await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36");

  for (let i = 0; i < retries; i++) {
    try {
      console.log(`[Status] Navigating: Attempt ${i + 1}`);
      await page.goto(url, { waitUntil: "load", timeout: 60000 });

      await killPopups(page);

      // --- 2. LOGIN VERIFICATION & DEBUGGING ---
      const loginInfo = await page.evaluate(() => {
        const userBtn = document.querySelector('button[name="header-user-menu-button"], .tv-header__user-menu-button--user');
        return {
          isLoggedIn: !!userBtn,
          currentUrl: window.location.href,
          bodySnippet: document.body.innerText.substring(0, 100).replace(/\n/g, ' ')
        };
      });

      console.log(`[Status] Login Verified: ${loginInfo.isLoggedIn ? "YES ✅" : "NO ❌"}`);
      
      if (!loginInfo.isLoggedIn) {
        console.log(`[Debug] Current URL: ${loginInfo.currentUrl}`);
        // Take a screenshot so you can see why login failed (e.g., CAPTCHA)
        await page.screenshot({ path: `debug_login_failed_${i}.png` });
        console.log(`[Debug] Screenshot saved: debug_login_failed_${i}.png`);
      }

      // --- 3. FORCE DATA ENGINE ---
      await page.waitForSelector('canvas', { timeout: 20000 });
      
      // Interaction to wake up "None" values
      const view = page.viewport();
      await page.mouse.click(view.width / 2, view.height / 2);
      
      console.log("[Status] Waiting for calculation (digits)...");
      const dataReady = await page.waitForFunction(() => {
        const items = document.querySelectorAll('[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA');
        const target = Array.from(items).find(s => {
          const t = s.querySelector('[data-qa-id="legend-source-title"]')?.innerText?.toLowerCase();
          return t === "clubbed" || t === "l";
        });
        if (!target) return false;

        // Force click the title to refresh
        const title = target.querySelector('.title-l31H9iuA');
        if (title && !window.doneClick) { title.click(); window.doneClick = true; }

        const vals = Array.from(target.querySelectorAll(".valueValue-l31H9iuA"));
        return vals.some(v => /[0-9.-]/.test(v.innerText));
      }, { timeout: 40000, polling: 1000 }).catch(() => false);

      if (dataReady) {
        console.log("[Status] SUCCESS: Numbers detected.");
        return true;
      }
      
      console.log("[Status] RETRY: Values stayed ∅.");
    } catch (err) {
      console.error(`[Error] ${err.message}`);
    }
  }
  return false;
}

async function killPopups(page) {
  try {
    await page.keyboard.press("Escape");
    await page.evaluate(() => {
      const selectors = ['#overlap-manager-root', '[class*="dialog-"]', '.tv-dialog__close', '.modal-backdrop'];
      selectors.forEach(s => document.querySelectorAll(s).forEach(el => el.remove()));
    });
  } catch (e) {}
}

export async function scrapeChart(page, url) {
  const COLS = 25;
  try {
    await page.setViewport({ width: 1920, height: 1080 });
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

    const cleanData = data.map(v => (v === "∅" || v === "") ? "None" : v);
    console.log(`[Status] DATA: ${cleanData.slice(0, 3).join(' | ')}`);
    return ["", "", dateStr, ...fixedLength(cleanData, COLS - 3)];
  } catch (err) {
    return ["", "", "ERROR", ...Array(COLS - 3).fill("ERROR")];
  }
}

function fixedLength(arr, len) {
  return arr.length >= len ? arr.slice(0, len) : arr.concat(Array(len - arr.length).fill(""));
}
