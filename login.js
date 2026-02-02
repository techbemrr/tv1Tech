import dotenv from "dotenv";
dotenv.config();

const EMAIL = process.env.EMAIL;
const PASSWORD = process.env.PASSWORD;

const delay = (ms) => new Promise((res) => setTimeout(res, ms));

async function clickButtonByText(page, text) {
  const handle = await page.evaluateHandle((t) => {
    const btns = Array.from(document.querySelectorAll("button"));
    return (
      btns.find((b) =>
        (b.textContent || "").trim().toLowerCase().includes(t.toLowerCase())
      ) || null
    );
  }, text);

  const el = handle.asElement();
  if (!el) return false;
  await el.click();
  return true;
}

async function isReallyLoggedIn(page) {
  return await page.evaluate(() => {
    return !!document.querySelector(
      '[data-name="header-user-menu"], [data-qa-id="header-user-menu"], button[aria-label*="Profile"], a[href*="logout"]'
    );
  });
}

export async function login(page) {
  if (!EMAIL || !PASSWORD) {
    throw new Error("Missing EMAIL/PASSWORD env variables");
  }

  console.log("Navigating to TradingView Login...");

  await page.goto("https://www.tradingview.com/accounts/signin/", {
    waitUntil: "domcontentloaded",
    timeout: 60000,
  });

  await delay(1500);

  if (await isReallyLoggedIn(page)) {
    console.log("Already logged in!");
    return await page.cookies();
  }

  // open email form (TradingView UI varies)
  await clickButtonByText(page, "Email").catch(() => {});
  await clickButtonByText(page, "Continue with email").catch(() => {});
  await delay(1200);

  // username/email
  const usernameSelectors = [
    'input[name="id_username"]',
    'input[type="email"]',
    'input[name="username"]',
    'input[name="email"]',
    'input[placeholder*="email" i]',
    'input[placeholder*="username" i]',
  ];

  let uSel = null;
  for (const sel of usernameSelectors) {
    const h = await page.$(sel);
    if (h) { uSel = sel; break; }
  }
  if (!uSel) {
    await page.screenshot({ path: "login_username_not_found.png", fullPage: true });
    throw new Error("Could not find username/email input field");
  }

  await page.click(uSel, { clickCount: 3 });
  await page.keyboard.press("Backspace");
  await page.type(uSel, EMAIL, { delay: 35 });

  // password
  const passwordSelectors = [
    'input[name="id_password"]',
    'input[type="password"]',
    'input[name="password"]',
    'input[placeholder*="password" i]',
  ];

  let pSel = null;
  for (const sel of passwordSelectors) {
    const h = await page.$(sel);
    if (h) { pSel = sel; break; }
  }
  if (!pSel) {
    await page.screenshot({ path: "login_password_not_found.png", fullPage: true });
    throw new Error("Could not find password input field");
  }

  await page.click(pSel, { clickCount: 3 });
  await page.keyboard.press("Backspace");
  await page.type(pSel, PASSWORD, { delay: 35 });

  // submit
  const clicked =
    (await clickButtonByText(page, "Sign in")) ||
    (await clickButtonByText(page, "Log in")) ||
    (await clickButtonByText(page, "Continue"));

  if (!clicked) await page.keyboard.press("Enter");

  console.log("Waiting for login to complete...");

  try {
    await page.waitForFunction(() => {
      return !!document.querySelector(
        '[data-name="header-user-menu"], [data-qa-id="header-user-menu"], button[aria-label*="Profile"], a[href*="logout"]'
      );
    }, { timeout: 45000 });
  } catch {
    await page.screenshot({ path: "login_failed_debug.png", fullPage: true });
    throw new Error("Login failed: user menu not found (2FA/captcha/blocked?)");
  }

  console.log("Login successful!");
  return await page.cookies();
}
