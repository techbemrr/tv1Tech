// index.js
import puppeteer from "puppeteer";
import fs from "fs";
import { login } from "./login.js";
import { scrapeChart } from "./scrape.js";
import { getChartLinks, writeValuesToNewSheet } from "./sheets.js";

const COOKIE_PATH = "./cookies.json";

if (process.env.COOKIES_BASE64 && !fs.existsSync(COOKIE_PATH)) {
  const decoded = Buffer.from(process.env.COOKIES_BASE64, "base64").toString(
    "utf-8"
  );
  fs.writeFileSync(COOKIE_PATH, decoded);
  console.log("✅ cookies.json restored from Base64");
}

async function saveCookies(cookies) {
  fs.writeFileSync(COOKIE_PATH, JSON.stringify(cookies, null, 2));
}

async function loadCookies(page) {
  if (fs.existsSync(COOKIE_PATH)) {
    const cookies = JSON.parse(fs.readFileSync(COOKIE_PATH));
    await page.setCookie(...cookies);
    return true;
  }
  return false;
}

(async () => {
  const browser = await puppeteer.launch({
    headless: "new", // Login page requires UI rendering
    slowMo: 50,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  const page = await browser.newPage();

  const hasCookies = await loadCookies(page);
  if (!hasCookies) {
    try {
      const cookies = await login(page);
      await saveCookies(cookies);
    } catch (err) {
      console.error("❌ Login failed:", err.message);
      //   try {
      //     await page.screenshot({ path: "fatal-login-error.png" });
      //   } catch (screenshotError) {
      //     console.warn("⚠ Could not take screenshot:", screenshotError.message);
      //   }

      await browser.close();
      process.exit(1);
    }
  }

  const chartLinks = await getChartLinks();

  const batchIndex = parseInt(process.argv[2] || "0", 10);
  const BATCH_SIZE = 340;
  const start = batchIndex * BATCH_SIZE;
  const end = start + BATCH_SIZE;
  const batchLinks = chartLinks.slice(start, end);
  console.log(`🔢 Processing batch ${batchIndex}: ${batchLinks.length} charts`);

  for (let i = 0; i < batchLinks.length; i++) {
    const url = batchLinks[i];
    if (!url) continue;

    const originalIndex = start + i;

    console.log(`📈 Scraping Row ${originalIndex + 2}: ${url}`);
    try {
      const values = await scrapeChart(page, url);
      const rowData = [url, ...values];
      await writeValuesToNewSheet(originalIndex, rowData);
    } catch (err) {
      console.error(`⚠️ Error scraping ${url}:`, err.message);
    }
    if ((i + 1) % 200 === 0) {
      console.log(" Restarting page to clear memory...");
      await page.close();
      page = await browser.newPage();
      await loadCookies(page);
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }

  await browser.close();
})();
