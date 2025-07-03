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

  for (let i = 0; i < chartLinks.length; i++) {
    const url = chartLinks[i];
    if (!url) continue;

    console.log(`📈 Scraping Row ${i + 2}: ${url}`);
    try {
      const values = await scrapeChart(page, url);
      const rowData = [url, ...values];
      await writeValuesToNewSheet(i, rowData);
    } catch (err) {
      console.error(`⚠️ Error scraping ${url}:`, err.message);
    }
  }

  await browser.close();
})();
