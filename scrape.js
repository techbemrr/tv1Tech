import fs from 'fs';
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function safeGoto(page, url, retries = 3) {
  // 1. Forced Cookie Injection
  try {
    if (fs.existsSync('./cookies.json')) {
      const cookies = JSON.parse(fs.readFileSync('./cookies.json', 'utf8'));
      const formatted = cookies.map(c => ({
        ...c,
        domain: c.domain.startsWith('.') ? c.domain : `.${c.domain}`,
        sameSite: "Lax"
      }));
      await page.setCookie(...formatted);
      console.log("[Status] SUCCESS: Cookies applied.");
    }
  } catch (err) {
    console.warn("[Status] Cookie Injection Error:", err.message);
  }

  await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36");

  for (let i = 0; i < retries; i++) {
    try {
      console.log(`[Status] Navigating Attempt ${i + 1}`);
      await page.goto(url, { waitUntil: "networkidle2", timeout: 60000 });

      // 2. Verified Login Check
      const isLoggedIn = await page.evaluate(() => {
        return !!document.querySelector('button[name="header-user-menu-button"], .tv-header__user-menu-button--user');
      });

      console.log(`[Status] Login Verified: ${isLoggedIn ? "YES ✅" : "NO ❌"}`);

      // ⚠️ Take screenshot immediately if login fails
      if (!isLoggedIn) {
        const screenshotPath = `login_failed_batch_${Date.now()}.png`;
        await page.screenshot({ path: screenshotPath, fullPage: true });
        console.log(`[Debug] Login failed screenshot saved: ${screenshotPath}`);
      }

      await page.waitForSelector('canvas', { timeout: 20000 });
      
      // Force interaction to wake up the ∅ values
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
      }, { timeout: 30000, polling: 1000 }).catch(() => false);

      if (dataReady) return true;
      console.log("[Status] Values stayed ∅, retrying...");
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
      if (!club) return ["NOT_FOUND"];
      return Array.from(club.querySelectorAll(".valueValue-l31H9iuA")).map(v => v.innerText.trim());
    });

    return ["", "", dateStr, ...data.map(v => (v === "∅" || v === "") ? "None" : v).slice(0, COLS - 3)];
  } catch (err) {
    return ["", "", "ERROR", ...Array(COLS - 3).fill("ERROR")];
  }
}
