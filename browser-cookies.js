import { chromium, devices } from "playwright";
import fs from "fs/promises";
import path from "path";
import { BrowserFingerprint } from "./browserFingerprint.js";

// Device settings
const iphone13 = devices["iPhone 13"];

// Constants
const COOKIES_FILE = "cookies.json";
const CONFIG = {
  COOKIE_REFRESH_INTERVAL: 24 * 60 * 60 * 1000, // 24 hours
  PAGE_TIMEOUT: 45000,
  MAX_RETRIES: 5,
  RETRY_DELAY: 10000,
  CHALLENGE_TIMEOUT: 10000,
};

let browser = null;

/**
 * Gets a random location for browser fingerprinting
 */
function getRandomLocation() {
  const locations = [
    {
      locale: "en-US",
      timezone: "America/Los_Angeles",
      latitude: 34.052235,
      longitude: -118.243683,
    },
    {
      locale: "en-US",
      timezone: "America/New_York",
      latitude: 40.712776,
      longitude: -74.005974,
    },
    {
      locale: "en-US",
      timezone: "America/Chicago",
      latitude: 41.878113,
      longitude: -87.629799,
    },
    {
      locale: "en-US",
      timezone: "America/Denver",
      latitude: 39.739235,
      longitude: -104.99025,
    },
    {
      locale: "en-CA",
      timezone: "America/Toronto",
      latitude: 43.65107,
      longitude: -79.347015,
    },
    {
      locale: "en-GB",
      timezone: "Europe/London",
      latitude: 51.507351,
      longitude: -0.127758,
    },
  ];

  return locations[Math.floor(Math.random() * locations.length)];
}

/**
 * Generate a realistic iPhone user agent
 */
function getRealisticIphoneUserAgent() {
  const iOSVersions = [
    "15_0",
    "15_1",
    "15_2",
    "15_3",
    "15_4",
    "15_5",
    "15_6",
    "16_0",
    "16_1",
    "16_2",
  ];
  const version = iOSVersions[Math.floor(Math.random() * iOSVersions.length)];
  return `Mozilla/5.0 (iPhone; CPU iPhone OS ${version} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/${
    version.split("_")[0]
  }.0 Mobile/15E148 Safari/604.1`;
}

/**
 * Enhance fingerprint with more browser properties
 */
function enhancedFingerprint() {
  const baseFingerprint = BrowserFingerprint.generate();

  // Add additional properties to make fingerprint more realistic
  return {
    ...baseFingerprint,
    webgl: {
      vendor: "Apple Inc.",
      renderer: "Apple GPU",
    },
    fonts: [
      "Arial",
      "Courier New",
      "Georgia",
      "Times New Roman",
      "Trebuchet MS",
      "Verdana",
    ],
    plugins: [
      "PDF Viewer",
      "Chrome PDF Viewer",
      "Chromium PDF Viewer",
      "Microsoft Edge PDF Viewer",
      "WebKit built-in PDF",
    ],
    screen: {
      width: 390,
      height: 844,
      availWidth: 390,
      availHeight: 844,
      colorDepth: 24,
      pixelDepth: 24,
    },
    timezone: {
      offset: new Date().getTimezoneOffset(),
    },
  };
}

/**
 * Simulate various mobile interactions to appear more human-like
 */
async function simulateMobileInteractions(page) {
  try {
    // Get viewport size
    const viewportSize = page.viewportSize();
    if (!viewportSize) return;

    // Random scroll amounts
    const scrollOptions = [
      { direction: "down", amount: 300 },
      { direction: "down", amount: 500 },
      { direction: "down", amount: 800 },
      { direction: "up", amount: 200 },
      { direction: "up", amount: 400 },
    ];

    // Pick 2-3 random scroll actions
    const scrollCount = 2 + Math.floor(Math.random() * 2);
    for (let i = 0; i < scrollCount; i++) {
      const option =
        scrollOptions[Math.floor(Math.random() * scrollOptions.length)];

      // Scroll with a dynamic speed
      const scrollY =
        option.direction === "down" ? option.amount : -option.amount;
      await page.evaluate((y) => {
        window.scrollBy({
          top: y,
          behavior: "smooth",
        });
      }, scrollY);

      // Random pause between scrolls (500-2000ms)
      await page.waitForTimeout(500 + Math.floor(Math.random() * 1500));
    }

    // Simulate random taps/clicks (1-2 times)
    const tapCount = 1 + Math.floor(Math.random() * 2);
    for (let i = 0; i < tapCount; i++) {
      // Random position within viewport
      const x = 50 + Math.floor(Math.random() * (viewportSize.width - 100));
      const y = 150 + Math.floor(Math.random() * (viewportSize.height - 300));

      await page.mouse.click(x, y);
      await page.waitForTimeout(500 + Math.floor(Math.random() * 1000));
    }
  } catch (error) {
    console.warn("Error during mobile interaction simulation:", error.message);
  }
}

/**
 * Initialize the browser with enhanced fingerprinting
 */
async function initBrowser(proxy) {
  let context = null;

  try {
    // Get randomized human-like properties
    const location = getRandomLocation();

    // For persisting browser sessions, use same browser if possible
    if (!browser || !browser.isConnected()) {
      // Launch options
      const launchOptions = {
        headless: false,
        args: [
          "--disable-blink-features=AutomationControlled",
          "--disable-features=IsolateOrigins,site-per-process",
          "--disable-web-security",
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--no-first-run",
          "--no-default-browser-check",
          "--disable-infobars",
          "--disable-notifications",
        ],
        timeout: 60000,
      };

      if (proxy && typeof proxy === "object" && proxy.proxy) {
        try {
          // Extract hostname and port from proxy string
          const proxyString = proxy.proxy;

          // Ensure proxyString is a string before using string methods
          if (typeof proxyString !== "string") {
            throw new Error(
              "Invalid proxy format: proxy.proxy must be a string, got " +
                typeof proxyString
            );
          }

          // Check if proxy string is in correct format (host:port)
          if (!proxyString.includes(":")) {
            throw new Error("Invalid proxy format: " + proxyString);
          }

          const [hostname, portStr] = proxyString.split(":");
          const port = parseInt(portStr) || 80;

          launchOptions.proxy = {
            server: `http://${hostname}:${port}`,
            username: proxy.username,
            password: proxy.password,
          };

          console.log(`Configuring browser with proxy: ${hostname}:${port}`);
        } catch (error) {
          console.warn(
            "Invalid proxy configuration, launching without proxy:",
            error
          );
        }
      }

      // Launch browser
      browser = await chromium.launch(launchOptions);
    }

    // Create new context with enhanced fingerprinting
    context = await browser.newContext({
      ...iphone13,
      userAgent: getRealisticIphoneUserAgent(),
      locale: location.locale,
      colorScheme: ["dark", "light"][Math.floor(Math.random() * 2)],
      timezoneId: location.timezone,
      geolocation: {
        latitude: location.latitude,
        longitude: location.longitude,
        accuracy: 100 * Math.random() + 50,
      },
      permissions: ["geolocation", "notifications", "microphone", "camera"],
      deviceScaleFactor: 2 + Math.random() * 0.5,
      hasTouch: true,
      isMobile: true,
      javaScriptEnabled: true,
      acceptDownloads: true,
      ignoreHTTPSErrors: true,
      bypassCSP: true,
      extraHTTPHeaders: {
        Accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Language": `${location.locale},en;q=0.9`,
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        DNT: Math.random() > 0.5 ? "1" : "0",
        "Upgrade-Insecure-Requests": "1",
        Pragma: "no-cache",
      },
      viewport: {
        width: [375, 390, 414][Math.floor(Math.random() * 3)],
        height: [667, 736, 812, 844][Math.floor(Math.random() * 4)],
      },
    });

    // Create a new page and simulate human behavior
    const page = await context.newPage();
    await page.waitForTimeout(1000 + Math.random() * 2000);
    await simulateMobileInteractions(page);

    return { context, fingerprint: enhancedFingerprint(), page, browser };
  } catch (error) {
    console.error("Error initializing browser:", error.message);

    // Cleanup on error
    if (context) await context.close().catch(() => {});

    throw error;
  }
}

/**
 * Handle Ticketmaster challenge pages (CAPTCHA, etc.)
 */
async function handleTicketmasterChallenge(page) {
  const startTime = Date.now();

  try {
    const challengePresent = await page
      .evaluate(() => {
        return document.body.textContent.includes(
          "Your Browsing Activity Has Been Paused"
        ) || document.body.textContent.includes(
          "Are you a robot"
        ) || document.body.textContent.includes(
          "Verify you are human"
        ) || document.body.textContent.includes(
          "I'm not a robot"
        );
      })
      .catch(() => false); // Catch any navigation errors

    if (challengePresent) {
      console.log("Detected Ticketmaster challenge, attempting resolution...");
      await page.waitForTimeout(1000 + Math.random() * 1000);

      try {
        const viewportSize = page.viewportSize();
        if (viewportSize) {
          await page.mouse.move(
            Math.floor(Math.random() * viewportSize.width),
            Math.floor(Math.random() * viewportSize.height),
            { steps: 5 }
          );
        }
      } catch (moveError) {
        console.warn(
          "Mouse movement error in challenge, continuing:",
          moveError.message
        );
      }

      // Check for checkbox challenges first
      try {
        const checkboxSelectors = [
          'input[type="checkbox"]',
          '.cf-turnstile',
          '.g-recaptcha',
          '.h-captcha',
          '[data-callback]',
          '.captcha-checkbox',
          '.verification-checkbox'
        ];
        
        let checkboxFound = false;
        for (const selector of checkboxSelectors) {
          const checkbox = await page.$(selector).catch(() => null);
          if (checkbox) {
            console.log(`Found checkbox challenge with selector: ${selector}`);
            
            // Add human-like delay before clicking
            await page.waitForTimeout(1500 + Math.random() * 1000);
            
            // Move mouse to checkbox area first
            const box = await checkbox.boundingBox();
            if (box) {
              await page.mouse.move(
                box.x + box.width / 2 + (Math.random() * 10 - 5),
                box.y + box.height / 2 + (Math.random() * 10 - 5),
                { steps: 3 + Math.floor(Math.random() * 5) }
              );
              await page.waitForTimeout(200 + Math.random() * 300);
            }
            
            await checkbox.click();
            checkboxFound = true;
            console.log("Clicked checkbox challenge");
            
            // Wait for potential verification
            await page.waitForTimeout(3000 + Math.random() * 2000);
            break;
          }
        }
        
        if (checkboxFound) {
          // Check if challenge is resolved
          await page.waitForTimeout(2000);
          const stillChallenged = await page
            .evaluate(() => {
              return document.body.textContent.includes(
                "Your Browsing Activity Has Been Paused"
              ) || document.body.textContent.includes(
                "Are you a robot"
              ) || document.body.textContent.includes(
                "Verify you are human"
              );
            })
            .catch(() => false);
            
          if (!stillChallenged) {
            console.log("Checkbox challenge resolved successfully");
            return true;
          }
        }
      } catch (checkboxError) {
        console.warn("Checkbox handling error:", checkboxError.message);
      }

      // Fallback to button-based challenge handling
      const buttons = await page.$$("button").catch(() => []);
      let buttonClicked = false;

      for (const button of buttons) {
        if (Date.now() - startTime > CONFIG.CHALLENGE_TIMEOUT) {
          console.warn("Challenge timeout, continuing without resolution");
          return false;
        }

        try {
          const text = await button.textContent();
          if (
            text?.toLowerCase().includes("continue") ||
            text?.toLowerCase().includes("verify") ||
            text?.toLowerCase().includes("proceed") ||
            text?.toLowerCase().includes("submit")
          ) {
            await page.waitForTimeout(500 + Math.random() * 500);
            await button.click();
            buttonClicked = true;
            console.log(`Clicked challenge button: ${text}`);
            break;
          }
        } catch (buttonError) {
          console.warn("Button click error, continuing:", buttonError.message);
          continue;
        }
      }

      if (!buttonClicked) {
        console.warn(
          "Could not find challenge button, continuing without resolution"
        );
        return false;
      }

      await page.waitForTimeout(2000);
      const stillChallenged = await page
        .evaluate(() => {
          return document.body.textContent.includes(
            "Your Browsing Activity Has Been Paused"
          ) || document.body.textContent.includes(
            "Are you a robot"
          ) || document.body.textContent.includes(
            "Verify you are human"
          );
        })
        .catch(() => false);

      if (stillChallenged) {
        console.warn("Challenge not resolved, continuing without resolution");
        return false;
      }
    }
    return true;
  } catch (error) {
    console.warn("Challenge handling failed, continuing:", error.message);
    return false;
  }
}

/**
 * Check for Ticketmaster challenge page
 */
async function checkForTicketmasterChallenge(page) {
  try {
    // Check for various challenge selectors
    const challengeSelectors = [
      "#challenge-running",
      ".cf-turnstile",
      ".g-recaptcha",
      ".h-captcha",
      '[data-callback]',
      '.captcha-checkbox',
      '.verification-checkbox',
      'input[type="checkbox"]'
    ];
    
    for (const selector of challengeSelectors) {
      const isChallengePresent = (await page.$(selector).catch(() => null)) !== null;
      if (isChallengePresent) {
        console.warn(`Challenge detected with selector: ${selector}`);
        return true;
      }
    }

    const challengePresent = await page
      .evaluate(() => {
        return document.body.textContent.includes(
          "Your Browsing Activity Has Been Paused"
        ) || document.body.textContent.includes(
          "Are you a robot"
        ) || document.body.textContent.includes(
          "Verify you are human"
        ) || document.body.textContent.includes(
          "I'm not a robot"
        ) || document.body.textContent.includes(
          "Please verify"
        ) || document.body.textContent.includes(
          "Security check"
        );
      })
      .catch(() => false);

    return challengePresent;
  } catch (error) {
    console.error("Error checking for Ticketmaster challenge:", error);
    return false;
  }
}

/**
 * Capture cookies from the browser
 */
async function captureCookies(page, fingerprint) {
  let retryCount = 0;
  const MAX_RETRIES = 5;

  while (retryCount < MAX_RETRIES) {
    try {
      const challengePresent = await page
        .evaluate(() => {
          return document.body.textContent.includes(
            "Your Browsing Activity Has Been Paused"
          );
        })
        .catch(() => false);

      if (challengePresent) {
        console.log(
          `Attempt ${retryCount + 1}: Challenge detected during cookie capture`
        );

        const challengeResolved = await handleTicketmasterChallenge(page);
        if (!challengeResolved) {
          if (retryCount === MAX_RETRIES - 1) {
            console.log("Max retries reached during challenge resolution");
            return { cookies: null, fingerprint };
          }
          await page.waitForTimeout(CONFIG.RETRY_DELAY);
          retryCount++;
          continue;
        }
      }

      // Get context from page's browser context
      const context = page.context();
      if (!context) {
        throw new Error("Cannot access browser context from page");
      }

      let cookies = await context.cookies().catch(() => []);

      if (!cookies?.length) {
        console.log(`Attempt ${retryCount + 1}: No cookies captured`);
        if (retryCount === MAX_RETRIES - 1) {
          return { cookies: null, fingerprint };
        }
        await page.waitForTimeout(CONFIG.RETRY_DELAY);
        retryCount++;
        continue;
      }

      // Filter out reCAPTCHA Google cookies
      cookies = cookies.filter(
        (cookie) =>
          !cookie.name.includes("_grecaptcha") &&
          !cookie.domain.includes("google.com")
      );

      // Check if we have enough cookies from ticketmaster.com
      const ticketmasterCookies = cookies.filter(
        (cookie) =>
          cookie.domain.includes("ticketmaster.com") ||
          cookie.domain.includes(".ticketmaster.com")
      );

      if (ticketmasterCookies.length < 3) {
        console.log(
          `Attempt ${retryCount + 1}: Not enough Ticketmaster cookies`
        );
        if (retryCount === MAX_RETRIES - 1) {
          return { cookies: null, fingerprint };
        }
        await page.waitForTimeout(CONFIG.RETRY_DELAY);
        retryCount++;
        continue;
      }

      // Check JSON size
      const cookiesJson = JSON.stringify(cookies, null, 2);
      const lineCount = cookiesJson.split("\n").length;

      if (lineCount < 200) {
        console.log(
          `Attempt ${
            retryCount + 1
          }: Cookie JSON too small (${lineCount} lines)`
        );
        if (retryCount === MAX_RETRIES - 1) {
          return { cookies: null, fingerprint };
        }
        await page.waitForTimeout(CONFIG.RETRY_DELAY);
        retryCount++;
        continue;
      }

      const oneHourFromNow = Date.now() + CONFIG.COOKIE_REFRESH_INTERVAL;
      cookies = cookies.map((cookie) => ({
        ...cookie,
        expires: oneHourFromNow / 1000,
        expiry: oneHourFromNow / 1000,
      }));

      // Add cookies one at a time with error handling
      for (const cookie of cookies) {
        try {
          await context.addCookies([cookie]);
        } catch (error) {
          console.warn(`Error adding cookie ${cookie.name}:`, error.message);
        }
      }

      // Save cookies to file
      await saveCookiesToFile(cookies);
      console.log(`Successfully captured cookies on attempt ${retryCount + 1}`);
      return { cookies, fingerprint };
    } catch (error) {
      console.error(
        `Error capturing cookies on attempt ${retryCount + 1}:`,
        error
      );
      if (retryCount === MAX_RETRIES - 1) {
        return { cookies: null, fingerprint };
      }
      await page.waitForTimeout(CONFIG.RETRY_DELAY);
      retryCount++;
    }
  }

  return { cookies: null, fingerprint };
}

/**
 * Save cookies to a file
 */
async function saveCookiesToFile(cookies) {
  try {
    // Format the cookies with updated expiration
    const cookieData = cookies.map((cookie) => ({
      ...cookie,
      expires: cookie.expires || Date.now() + CONFIG.COOKIE_REFRESH_INTERVAL,
      expiry: cookie.expiry || Date.now() + CONFIG.COOKIE_REFRESH_INTERVAL,
    }));

    await fs.writeFile(COOKIES_FILE, JSON.stringify(cookieData, null, 2));
    console.log(`Saved ${cookies.length} cookies to ${COOKIES_FILE}`);
    return true;
  } catch (error) {
    console.error(`Error saving cookies to file: ${error.message}`);
    return false;
  }
}

/**
 * Load cookies from file
 */
async function loadCookiesFromFile() {
  try {
    const cookiesFile = path.join(process.cwd(), COOKIES_FILE);

    // Check if file exists
    try {
      await fs.access(cookiesFile);
    } catch (error) {
      console.log("Cookies file does not exist");
      return null;
    }

    // Read and parse
    const fileData = await fs.readFile(cookiesFile, "utf8");
    const cookies = JSON.parse(fileData);

    if (!Array.isArray(cookies) || cookies.length === 0) {
      console.log("Invalid or empty cookies file");
      return null;
    }

    console.log(`Loaded ${cookies.length} cookies from file`);
    return cookies;
  } catch (error) {
    console.error(`Error loading cookies from file: ${error.message}`);
    return null;
  }
}

/**
 * Get fresh cookies by opening a browser and navigating to Ticketmaster
 */
async function refreshCookies(eventId, proxy = null, existingBrowserInstance = null, existingContext = null, existingPage = null) {
  let localContext = existingContext;
  let page = existingPage;
  let browserInstance = existingBrowserInstance;
  let succeeded = false; // Flag to indicate success

  try {
    console.log(`Refreshing cookies using event ${eventId}`);

    // Try to load existing cookies first
    const existingCookies = await loadCookiesFromFile();
    if (existingCookies && existingCookies.length >= 3) {
      const cookieAge = existingCookies[0]?.expiry
        ? existingCookies[0].expiry * 1000 - Date.now()
        : 0;

      if (cookieAge > 0 && cookieAge < CONFIG.COOKIE_REFRESH_INTERVAL) {
        console.log(
          `Using existing cookies (age: ${Math.floor(
            cookieAge / 1000 / 60
          )} minutes)`
        );
        return {
          cookies: existingCookies,
          fingerprint: BrowserFingerprint.generate(),
          lastRefresh: Date.now(),
        };
      }
    }

    // Initialize browser with improved error handling
    let initAttempts = 0;
    let initSuccess = false;
    let initError = null;

    if (browserInstance && localContext && page && typeof browserInstance.isConnected === 'function' && browserInstance.isConnected()) {
      console.log("Reusing provided browser instance, context, and page.");
      // Update the global browser variable to prevent initBrowser from creating a new one
      browser = browserInstance;
      initSuccess = true;
      // Ensure the page is not closed if it's being reused
      if (page.isClosed()) {
        console.warn("Provided page was closed, attempting to create a new one from context.");
        if (localContext.newPage) { // Check if context is still valid for newPage
            try {
                page = await localContext.newPage();
            } catch (e) {
                console.error("Failed to create new page from reused context:", e.message);
                initSuccess = false; // Force re-initialization
                browser = null; // Reset global browser as well
            }
        } else {
            initSuccess = false; // Force re-initialization if context is also unusable
            browser = null; // Reset global browser as well
        }
      }
    } else if (browserInstance && localContext && typeof browserInstance.isConnected === 'function' && browserInstance.isConnected()) {
      console.log("Reusing provided browser instance and context, creating new page.");
      // Update the global browser variable to prevent initBrowser from creating a new one
      browser = browserInstance;
      try {
        page = await localContext.newPage();
        initSuccess = true;
      } catch (e) {
        console.error("Failed to create new page from reused context:", e.message);
        // Fall through to full initialization if new page creation fails
        browserInstance = null; // Reset to allow full init
        localContext = null;
        browser = null; // Reset global browser as well
      }
    } else {
      // Clear any potentially stale passed-in instances if they are not fully usable
      browserInstance = null;
      localContext = null;
      page = null;
      browser = null; // Reset global browser as well
    }

    if (!initSuccess) {
      console.log("Proceeding with new browser initialization.");
      // Resetting these to null ensures initBrowser starts fresh if previous checks failed partially
      browserInstance = null;
      localContext = null;
      page = null;
      while (initAttempts < 3 && !initSuccess) {
      try {
        const result = await initBrowser(proxy);
        if (!result || !result.context || !result.fingerprint) {
          throw new Error(
            "Failed to initialize browser or generate fingerprint"
          );
        }

        browserInstance = result.browser;
        localContext = result.context;
        page = result.page;
        // Update global browser variable when creating new browser
        browser = result.browser;

        initSuccess = true;
      } catch (error) {
        initAttempts++;
        initError = error;
        console.error(
          `Browser init attempt ${initAttempts} failed:`,
          error.message
        );
        await new Promise((resolve) =>
          setTimeout(resolve, 1000 * initAttempts)
        );
      }
    }

    if (!initSuccess) {
      console.error("All browser initialization attempts failed");
      throw initError || new Error("Failed to initialize browser");
    }

    // Navigate to event page
    const url = `https://www.ticketmaster.com/event/${eventId}`;
    console.log(`Navigating to ${url}`);

    await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: CONFIG.PAGE_TIMEOUT,
    });

    // Check if the page loaded properly
    const currentUrl = page.url();
    const pageLoadSuccessful = currentUrl.includes(`/event/${eventId}`);

    if (!pageLoadSuccessful) {
      console.warn(`Failed to load event page, URL: ${currentUrl}`);

      // Try refreshing the page
      console.log("Attempting to reload the page...");
      await page.reload({
        waitUntil: "domcontentloaded",
        timeout: CONFIG.PAGE_TIMEOUT,
      });

      const newUrl = page.url();
      const reloadSuccessful = newUrl.includes(`/event/${eventId}`);

      if (!reloadSuccessful) {
        console.warn(`Reload failed, URL: ${newUrl}`);
        throw new Error("Failed to load Ticketmaster event page");
      }
    }

    console.log(`Successfully loaded page for event ${eventId}`);

    // Check for Ticketmaster challenge
    const isChallengePresent = await checkForTicketmasterChallenge(page);
    if (isChallengePresent) {
      console.warn(
        "Detected Ticketmaster challenge page, attempting to resolve..."
      );
      await handleTicketmasterChallenge(page);
    }

    // Simulate human behavior
    await simulateMobileInteractions(page);

    // Wait for cookies to be set
    await page.waitForTimeout(2000);

    // Capture cookies
    const fingerprint = BrowserFingerprint.generate();
    const { cookies } = await captureCookies(page, fingerprint);

    if (!cookies || cookies.length === 0) {
      throw new Error("Failed to capture cookies");
    }

    // Mark refresh as successful before returning
    succeeded = true; // Set flag before returning
    return {
      cookies,
      fingerprint,
      lastRefresh: Date.now(),
      browserInstanceToReuse: browserInstance, // Pass the browser instance back
      contextToReuse: localContext, // Pass the context back
      pageToReuse: page // Pass the page back
    };
  }
    // Error is re-thrown, cleanup happens in finally
    throw error;
  } finally {
    if (!succeeded) { // If not successful, clean up everything
      if (page) {
        try { await page.close(); } catch (e) { console.warn('Error closing page on refresh failure:', e.message); }
      }
      if (localContext) {
        try { await localContext.close(); } catch (e) { console.warn('Error closing context on refresh failure:', e.message); }
      }
      if (browserInstance) {
        try { await browserInstance.close(); } catch (e) { console.warn('Error closing browser on refresh failure:', e.message); }
      }
    }
    // If succeeded is true, browserInstance, localContext, and page were returned and should not be closed here.
    // The calling function is now responsible for them.
  }
}


/**
 * Clean up browser resources
 * Note: This function is modified to preserve browser instances that are being reused
 */
async function cleanup() {
  // Don't close the browser if it's being reused for cookie refresh
  // The browser will be managed by the cookie refresh system
  console.log("Cleanup called - browser instance will be preserved for reuse");
  // Commenting out browser.close() to prevent closing reusable browser instances
  // if (browser) {
  //   try {
  //     await browser.close();
  //     browser = null;
  //   } catch (error) {
  //     console.warn("Error closing browser:", error.message);
  //   }
  // }
}

export {
  initBrowser,
  captureCookies,
  refreshCookies,
  loadCookiesFromFile,
  saveCookiesToFile,
  cleanup,
  handleTicketmasterChallenge,
  checkForTicketmasterChallenge,
  enhancedFingerprint,
  getRandomLocation,
  getRealisticIphoneUserAgent,
  simulateMobileInteractions,
};
