const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function fixedLength(arr, len, fill = "") {
  if (arr.length >= len) return arr.slice(0, len);
  return arr.concat(Array(len - arr.length).fill(fill));
}

function two(n) {
  return String(n).padStart(2, "0");
}

async function killPopups(page) {
  try {
    await page.keyboard.press("Escape");
    await page.keyboard.press("Escape");

    await page.evaluate(() => {
      document.documentElement.style.setProperty("overflow", "auto", "important");
      document.body.style.setProperty("overflow", "auto", "important");

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

      const btns = Array.from(document.querySelectorAll("button"));
      const consent = btns.find((b) => {
        const t = (b.innerText || "").toLowerCase();
        return t.includes("accept") || t.includes("agree") || t.includes("got it");
      });
      if (consent) consent.click();
    });
  } catch {}
}

async function isLoggedOutWall(page) {
  const url = page.url();
  if (url.includes("/accounts/signin") || url.includes("/login")) return true;

  return await page.evaluate(() => {
    const t = (document.body?.innerText || "").toLowerCase();
    if (t.includes("sign in") && t.includes("email")) return true;
    return false;
  });
}

async function safeGoto(page, url, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      console.log(`[Navigation] Attempt ${i + 1}: ${url}`);

      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });

      try { await page.bringToFront(); } catch {}

      await killPopups(page);

      await page.waitForFunction(() => {
        const c = document.querySelector("canvas");
        return c && c.offsetWidth > 0 && c.offsetHeight > 0;
      }, { timeout: 25000 });

      await delay(2000);
      await killPopups(page);

      return true;
    } catch (err) {
      console.warn(`[Warning] Attempt ${i + 1} failed: ${err.message}`);
      await killPopups(page).catch(() => {});
      if (i === retries - 1) return false;
      await delay(4000);
    }
  }
}

export async function scrapeChart(page, url) {
  const EXPECTED_VALUE_COUNT = 25;
  const now = new Date();
  const month = two(now.getMonth() + 1);
  const day = two(now.getDate());

  const INDICATOR_NAME = String(process.env.INDICATOR_NAME || "").trim().toLowerCase();
  const INDICATOR_PARTIAL = INDICATOR_NAME.length > 0;

  try {
    await page.setViewport({ width: 1920, height: 1080 });

    const ok = await safeGoto(page, url, 3);
    if (!ok) {
      return [month, day, ...fixedLength(["NAVIGATION FAILED"], EXPECTED_VALUE_COUNT)];
    }

    if (await isLoggedOutWall(page)) {
      return [month, day, ...fixedLength(["LOGGED OUT / SESSION EXPIRED"], EXPECTED_VALUE_COUNT)];
    }

    await page.waitForSelector('[data-qa-id="legend"]', { timeout: 20000 });

    const values = await page.$eval('[data-qa-id="legend"]', (legend, meta) => {
      // get all legend items that represent studies/indicators
      const items = Array.from(
        legend.querySelectorAll('[class*="item"][class*="study"], [class*="study"]')
      );

      const pickValuesFromItem = (item) => {
        const spans = Array.from(item.querySelectorAll('[class*="valueValue"]'));
        const out = spans.map((s) => (s.innerText || "").trim()).filter(Boolean);
        return out;
      };

      const getTitle = (item) => {
        const tEl = item.querySelector('[data-qa-id="legend-source-title"]');
        return (tEl?.innerText || "").trim().toLowerCase();
      };

      // 1) Try match indicator name (if provided)
      if (meta.indicatorPartial) {
        for (const item of items) {
          const title = getTitle(item);
          if (title && title.includes(meta.indicatorName)) {
            const out = pickValuesFromItem(item);
            if (out.length) return out;
          }
        }
      }

      // 2) Fallback: first item that has any values
      for (const item of items) {
        const out = pickValuesFromItem(item);
        if (out.length) return out;
      }

      // 3) If no study items, try any value nodes inside legend
      const any = Array.from(legend.querySelectorAll('[class*="valueValue"]'))
        .map((n) => (n.innerText || "").trim())
        .filter(Boolean);

      return any.length ? any : ["NO VALUES IN LEGEND"];
    }, { indicatorName: INDICATOR_NAME, indicatorPartial: INDICATOR_PARTIAL });

    console.log(`[Success] Scraped ${values.length} values from ${url}`);
    return [month, day, ...fixedLength(values, EXPECTED_VALUE_COUNT)];
  } catch (err) {
    return [month, day, ...fixedLength([`ERROR: ${err.message}`], EXPECTED_VALUE_COUNT)];
  }
}
