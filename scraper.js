import { createRequire } from "module";
const require = createRequire(import.meta.url);
const axios = require("axios");
const { HttpsProxyAgent } = require("https-proxy-agent");
const fs = require("fs");
import { firefox } from "playwright";
import proxyArray from "./helpers/proxy.js";
import { AttachRowSection } from "./helpers/seatBatch.js";
import GenerateNanoPlaces from "./helpers/seats.js";
import crypto from "crypto";
import { BrowserFingerprint } from "./browserFingerprint.js";
import { simulateHumanBehavior } from "./helpers/humanBehavior.js";

const COOKIES_FILE = "cookies.json";
const CONFIG = {
  COOKIE_REFRESH_INTERVAL: 24 * 60 * 60 * 1000, // 24 hours
  PAGE_TIMEOUT: 30000,
  MAX_RETRIES: 3,
  RETRY_DELAY: 30000,
  CHALLENGE_TIMEOUT: 10000,
};

let browser = null;
let context = null;
let capturedState = {
  cookies: null,
  fingerprint: null,
  lastRefresh: null,
  proxy: null,
};
// Flag to track if we're currently refreshing cookies
let isRefreshingCookies = false;
// Queue for pending cookie refresh requests
let cookieRefreshQueue = [];

function generateCorrelationId() {
  return crypto.randomUUID();
}

async function handleTicketmasterChallenge(page) {
  const startTime = Date.now();

  try {
    const challengePresent = await page.evaluate(() => {
      return document.body.textContent.includes(
        "Your Browsing Activity Has Been Paused"
      );
    });

    if (challengePresent) {
      console.log("Detected Ticketmaster challenge, attempting resolution...");
      await page.waitForTimeout(1000 + Math.random() * 1000);

      const viewportSize = page.viewportSize();
      if (viewportSize) {
        await page.mouse.move(
          Math.floor(Math.random() * viewportSize.width),
          Math.floor(Math.random() * viewportSize.height),
          { steps: 5 }
        );
      }

      const buttons = await page.$$("button");
      let buttonClicked = false;

      for (const button of buttons) {
        if (Date.now() - startTime > CONFIG.CHALLENGE_TIMEOUT) {
          throw new Error("Challenge timeout");
        }

        const text = await button.textContent();
        if (
          text?.toLowerCase().includes("continue") ||
          text?.toLowerCase().includes("verify")
        ) {
          await button.click();
          buttonClicked = true;
          break;
        }
      }

      if (!buttonClicked) {
        throw new Error("Could not find challenge button");
      }

      await page.waitForTimeout(2000);
      const stillChallenged = await page.evaluate(() => {
        return document.body.textContent.includes(
          "Your Browsing Activity Has Been Paused"
        );
      });

      if (stillChallenged) {
        throw new Error("Challenge not resolved");
      }
    }
    return true;
  } catch (error) {
    console.error("Challenge handling failed:", error.message);
    resetCapturedState();
    throw error;
  }
}

async function initBrowser(proxy) {
  try {
    if (!proxy?.proxy) {
      const { proxy: newProxy } = GetProxy();
      proxy = newProxy;
    }

    const fingerprint = BrowserFingerprint.generate();
    const userAgent = BrowserFingerprint.generateUserAgent(fingerprint);

    if (browser) {
      await cleanup(browser, context);
    }

    const proxyUrl = new URL(`http://${proxy.proxy}`);

    browser = await firefox.launch({
      headless: true,
      proxy: {
        server: `http://${proxyUrl.hostname}:${proxyUrl.port || 80}`,
        username: proxy.username,
        password: proxy.password,
      },
    });

    context = await browser.newContext({
      viewport: fingerprint.screen,
      userAgent,
      locale: fingerprint.language,
      deviceScaleFactor: fingerprint.devicePixelRatio,
      colorScheme: "light",
      timezoneId: fingerprint.timezone,
    });

    return { context, fingerprint };
  } catch (error) {
    await cleanup(browser, context);
    console.error("Error initializing browser:", error);
    throw error;
  }
}

async function captureCookies(page, fingerprint) {
  for (let attempt = 1; attempt <= CONFIG.MAX_RETRIES; attempt++) {
    try {
      const challengePresent = await page.evaluate(() => {
        return document.body.textContent.includes(
          "Your Browsing Activity Has Been Paused"
        );
      });

      if (challengePresent) {
        console.log(
          `Attempt ${attempt}: Challenge detected during cookie capture`
        );

        const challengeResolved = await handleTicketmasterChallenge(page);
        if (!challengeResolved) {
          if (attempt === CONFIG.MAX_RETRIES) {
            console.log("Max retries reached during challenge resolution");
            return { cookies: null, fingerprint };
          }
          await page.waitForTimeout(CONFIG.RETRY_DELAY);
          continue;
        }
      }

      let cookies = await context.cookies();

      if (!cookies?.length) {
        console.log(`Attempt ${attempt}: No cookies captured`);
        if (attempt === CONFIG.MAX_RETRIES) {
          return { cookies: null, fingerprint };
        }
        await page.waitForTimeout(CONFIG.RETRY_DELAY);
        continue;
      }

      const oneHourFromNow = Date.now() + CONFIG.COOKIE_REFRESH_INTERVAL;
      cookies = cookies.map((cookie) => ({
        ...cookie,
        expires: oneHourFromNow / 1000,
        expiry: oneHourFromNow / 1000,
      }));

      await Promise.all(
        cookies.map((cookie) =>
          context.addCookies([
            {
              ...cookie,
              expires: oneHourFromNow / 1000,
              expiry: oneHourFromNow / 1000,
            },
          ])
        )
      );

      fs.writeFileSync(COOKIES_FILE, JSON.stringify(cookies, null, 2));
      console.log(`Successfully captured cookies on attempt ${attempt}`);
      return { cookies, fingerprint };
    } catch (error) {
      console.error(`Error capturing cookies (attempt ${attempt}):`, error);
      if (attempt === CONFIG.MAX_RETRIES) {
        return { cookies: null, fingerprint };
      }
      await page.waitForTimeout(CONFIG.RETRY_DELAY);
    }
  }

  return { cookies: null, fingerprint };
}

async function loadCookiesFromFile() {
  try {
    if (fs.existsSync(COOKIES_FILE)) {
      // Check file size first
      const stats = await fs.promises.stat(COOKIES_FILE);
      const fileSize = stats.size;
      
      // Verify file size meets minimum requirements
      if (fileSize < 1000) {
        console.error(`Cookies file is too small: ${fileSize} bytes (minimum 1000 bytes required)`);
        // Delete the invalid file
        await fs.promises.unlink(COOKIES_FILE);
        console.error("Deleted invalid cookies file - will get new cookies");
        return null;
      }
      
      // File size is valid, proceed with loading
      const cookiesData = fs.readFileSync(COOKIES_FILE, "utf8");
      const cookies = JSON.parse(cookiesData);
      
      // Also validate that we have cookies
      if (!cookies || !Array.isArray(cookies) || cookies.length === 0) {
        console.error("Cookies file exists but contains no valid cookies");
        await fs.promises.unlink(COOKIES_FILE);
        return null;
      }
      
      console.log(`Loaded valid cookies file (${fileSize} bytes) with ${cookies.length} cookies`);
      return cookies;
    }
  } catch (error) {
    console.error("Error loading cookies from file:", error);
    // Try to delete the file if there was an error parsing it
    try {
      if (fs.existsSync(COOKIES_FILE)) {
        await fs.promises.unlink(COOKIES_FILE);
        console.error("Deleted corrupted cookies file");
      }
    } catch (deleteError) {
      console.error("Error deleting corrupted cookies file:", deleteError);
    }
  }
  return null;
}

async function getCapturedData(eventId, proxy, forceRefresh = false) {
  const currentTime = Date.now();

  // If we don't have cookies, try to load them from file first
  if (!capturedState.cookies) {
    const cookiesFromFile = await loadCookiesFromFile();
    if (cookiesFromFile) {
      const lastRefresh = cookiesFromFile[0]?.expires * 1000 || 0;
      if (currentTime - lastRefresh <= CONFIG.COOKIE_REFRESH_INTERVAL) {
        capturedState.cookies = cookiesFromFile;
        capturedState.lastRefresh = lastRefresh;
        if (!capturedState.fingerprint) {
          capturedState.fingerprint = BrowserFingerprint.generate();
        }
        if (!capturedState.proxy) {
          capturedState.proxy = proxy;
        }
      }
    }
  }

  // Check if we need to refresh cookies
  const needsRefresh =
    !capturedState.cookies ||
    !capturedState.fingerprint ||
    !capturedState.lastRefresh ||
    !capturedState.proxy ||
    currentTime - capturedState.lastRefresh > CONFIG.COOKIE_REFRESH_INTERVAL ||
    forceRefresh;

  if (needsRefresh) {
    // Use the refreshHeaders function with locking mechanism
    return await refreshHeaders(eventId, proxy);
  }

  return capturedState;
}

async function refreshHeaders(eventId, proxy, existingCookies = null) {
  // If cookies are already being refreshed, add this request to the queue
  if (isRefreshingCookies) {
    console.log(
      `Cookies are already being refreshed, queueing request for event ${eventId}`
    );
    return new Promise((resolve, reject) => {
      cookieRefreshQueue.push({ resolve, reject });
    });
  }

  let localContext = null;
  let localBrowser = null;

  try {
    isRefreshingCookies = true;

    if (
      capturedState.cookies?.length &&
      capturedState.lastRefresh &&
      Date.now() - capturedState.lastRefresh <= CONFIG.COOKIE_REFRESH_INTERVAL
    ) {
      console.log("Using existing cookies from memory");
      return capturedState;
    }

    if (existingCookies !== null) {
      console.log(`Using provided cookies for event ${eventId}`);

      if (!capturedState.fingerprint) {
        capturedState.fingerprint = BrowserFingerprint.generate();
      }

      capturedState = {
        cookies: existingCookies,
        fingerprint: capturedState.fingerprint,
        lastRefresh: Date.now(),
        proxy: capturedState.proxy || proxy,
      };

      return capturedState;
    }

    const cookiesFromFile = await loadCookiesFromFile();
    if (cookiesFromFile) {
      console.log("Using cookies from file");
      if (!capturedState.fingerprint) {
        capturedState.fingerprint = BrowserFingerprint.generate();
      }

      capturedState = {
        cookies: cookiesFromFile,
        fingerprint: capturedState.fingerprint || BrowserFingerprint.generate(),
        lastRefresh: Date.now(),
        proxy: capturedState.proxy || proxy,
      };

      return capturedState;
    }

    console.log(
      `No valid cookies found, getting new cookies using event ${eventId}`
    );

    const { context: newContext, fingerprint } = await initBrowser(proxy);
    if (!newContext || !fingerprint) {
      throw new Error("Failed to initialize browser or generate fingerprint");
    }

    localContext = newContext;

    // Create page in try-catch block
    let page;
    try {
      page = await localContext.newPage();
    } catch (pageError) {
      console.error("Failed to create new page:", pageError);
      throw new Error("Failed to create new page: " + pageError.message);
    }

    if (!page) {
      throw new Error("Failed to create page");
    }

    const url = `https://www.ticketmaster.com/event/${eventId}`;

    try {
      console.log(`Navigating to ${url} for event ${eventId}`);

      // Use domcontentloaded instead of networkidle for faster, more reliable loading
      await page.goto(url, {
        waitUntil: "domcontentloaded",
        timeout: 60000, // 60 second timeout
      });

      console.log(`Successfully loaded page for event ${eventId}`);

      // Check for Ticketmaster challenge (e.g., CAPTCHA)
      const isChallengePresent = await checkForTicketmasterChallenge(page);
      if (isChallengePresent) {
        console.warn(
          "Ticketmaster challenge detected, skipping cookie capture"
        );
        throw new Error("Ticketmaster challenge detected");
      }

      try {
        await page.waitForTimeout(2000);
        await simulateHumanBehavior(page);
      } catch (behaviorError) {
        console.warn(
          "Human behavior simulation failed:",
          behaviorError.message
        );
        // Continue anyway - not critical
      }

      const data = await captureCookies(page, fingerprint);

      if (data?.cookies && data.cookies.length > 0) {
        capturedState = {
          cookies: data.cookies,
          fingerprint: fingerprint,
          lastRefresh: Date.now(),
          proxy: proxy,
        };
        console.log(
          `Successfully captured ${data.cookies.length} cookies for event ${eventId}`
        );

        // Save cookies to JSON file and verify it's valid
        const cookiesValid = await saveCookiesToFile(data.cookies);
        if (!cookiesValid) {
          console.error("Failed to save valid cookies file - aborting scrape");
          resetCapturedState();
          throw new Error("Invalid cookies file - too small or corrupted");
        }
        console.log("Cookies saved to file and validated");
      } else {
        console.error(`No cookies captured for event ${eventId}`);
        resetCapturedState();
      }

      // Wait for proper page closure
      try {
        await page.close();
      } catch (closeError) {
        console.warn("Error closing page:", closeError.message);
      }

      return capturedState;
    } catch (navigationError) {
      console.error(
        `Navigation error for ${eventId}:`,
        navigationError.message
      );

      // Try direct API approach as fallback
      try {
        console.log("Attempting direct API approach for cookies");
        const directData = await getDirectAPICookies(eventId);
        if (directData) {
          return directData;
        }
      } catch (apiError) {
        console.error("Direct API approach failed:", apiError.message);
      }

      throw navigationError;
    } finally {
      // Ensure page is closed
      if (page) {
        try {
          await page.close().catch(() => {});
        } catch (e) {
          console.warn("Error closing page in finally block");
        }
      }
    }
  } catch (error) {
    console.error("Error in refreshHeaders:", error);
    resetCapturedState();
    throw error;
  } finally {
    // Clean up local instances, not affecting global ones
    await cleanup(localBrowser, localContext);

    // Set the refreshing flag back to false
    isRefreshingCookies = false;

    // Process any queued refresh requests
    while (cookieRefreshQueue.length > 0) {
      const { resolve, reject } = cookieRefreshQueue.shift();
      if (capturedState.cookies) {
        resolve(capturedState);
      } else {
        reject(new Error("Failed to refresh cookies"));
      }
    }
  }
}

// Function to check for Ticketmaster challenge (e.g., CAPTCHA)
async function checkForTicketmasterChallenge(page) {
  try {
    // Check for CAPTCHA or other blocking mechanisms
    const challengeSelector = "#challenge-running"; // Example selector for CAPTCHA
    const isChallengePresent = (await page.$(challengeSelector)) !== null;

    if (isChallengePresent) {
      console.warn("Ticketmaster challenge detected");
      return true;
    }

    return false;
  } catch (error) {
    console.error("Error checking for Ticketmaster challenge:", error);
    return false;
  }
}

// Function to save cookies to a JSON file
async function saveCookiesToFile(cookies) {
  try {
    const filePath = "./cookies.json";
    
    // Only save if we have cookies
    if (!cookies || cookies.length === 0) {
      console.error("No cookies to save - skipping file write");
      return false;
    }
    
    // Write cookies to file
    await fs.promises.writeFile(filePath, JSON.stringify(cookies, null, 2));
    console.log("Cookies saved to file:", filePath);
    
    // Verify file size/content meets minimum requirements
    try {
      const stats = await fs.promises.stat(filePath);
      const fileSize = stats.size;
      
      // Check if file is too small (less than 1000 bytes)
      if (fileSize < 1000) {
        console.error(`Cookies file is too small: ${fileSize} bytes (minimum 1000 bytes required)`);
        // Delete the invalid file
        await fs.promises.unlink(filePath);
        console.error("Deleted invalid cookies file - will try to get new cookies on next request");
        return false;
      }
      
      console.log(`Cookies file size: ${fileSize} bytes (valid size)`);
      return true;
    } catch (statError) {
      console.error("Error checking cookies file size:", statError);
      return false;
    }
  } catch (error) {
    console.error("Error saving cookies to file:", error);
    return false;
  }
}

const GetData = async (headers, proxyAgent, url, eventId) => {
  let abortController = new AbortController();

  try {
    const timeout = setTimeout(() => {
      abortController.abort();
      console.log("Request aborted due to timeout");
      console.log(eventId, "eventId");
    }, CONFIG.PAGE_TIMEOUT);

    try {
      const response = await axios.get(url, {
        httpsAgent: proxyAgent,
        headers: {
          ...headers,
          "Accept-Encoding": "gzip, deflate, br",
          Connection: "keep-alive",
        },
        timeout: CONFIG.PAGE_TIMEOUT,
        signal: abortController.signal,
        validateStatus: (status) => status === 200,
      });

      clearTimeout(timeout);
      return response.data;
    } catch (error) {
      clearTimeout(timeout);
      console.log(`Request failed: ${error.message}`);
      return false;
    }
  } catch (error) {
    console.log(error, "error");
    return false;
  }
};

const GetProxy = () => {
  let _proxy = [...proxyArray?.proxies];
  const randomProxy = Math.floor(Math.random() * _proxy.length);
  _proxy = _proxy[randomProxy];

  if (!_proxy?.proxy || !_proxy?.username || !_proxy?.password) {
    throw new Error("Invalid proxy configuration");
  }

  try {
    const proxyUrl = new URL(`http://${_proxy.proxy}`);
    const proxyURl = `http://${_proxy.username}:${_proxy.password}@${
      proxyUrl.hostname
    }:${proxyUrl.port || 80}`;
    const proxyAgent = new HttpsProxyAgent(proxyURl);
    return { proxyAgent, proxy: _proxy };
  } catch (error) {
    console.error("Invalid proxy URL format:", error);
    throw new Error("Invalid proxy URL format");
  }
};

const ScrapeEvent = async (event, externalProxyAgent = null, externalProxy = null) => {
  try {
    // Determine event ID from either object or simple ID
    const eventId = event?.eventId || event;
    
    // Use provided proxy if available, otherwise get a new one
    let proxyAgent, proxy;
    if (externalProxyAgent && externalProxy) {
      console.log(`Using provided proxy ${externalProxy.proxy} for event ${eventId}`);
      proxyAgent = externalProxyAgent;
      proxy = externalProxy;
    } else {
      const proxyData = GetProxy();
      proxyAgent = proxyData.proxyAgent;
      proxy = proxyData.proxy;
    }
    
    // If headers are provided (for batch processing), use them directly
    let cookieString, userAgent, fingerprint;
    
    let useProvidedHeaders = false;
    if (event?.headers) {
      console.log(`Processing headers for event ${eventId} (batch processing)`);
      cookieString = event.headers.Cookie || event.headers.cookie;
      userAgent = event.headers["User-Agent"] || event.headers["user-agent"];
      
      // Check if we have valid headers to use
      if (cookieString && userAgent) {
        console.log(`Reusing existing headers for batch processing of event ${eventId}`);
        useProvidedHeaders = true;
        // Initialize fingerprint with at least a language property
        fingerprint = { language: "en-US" };
      } else {
        console.log(`Incomplete headers for event ${eventId}, falling back to standard flow`);
      }
    }
    
    if (!useProvidedHeaders) {
      // Standard flow - get cookies and headers
      const correlationId = generateCorrelationId();
      const capturedData = await getCapturedData(eventId, proxy);

      if (!capturedData?.cookies?.length) {
        throw new Error("Failed to capture cookies");
      }

      cookieString = capturedData.cookies
        .map((cookie) => `${cookie.name}=${cookie.value}`)
        .join("; ");

      userAgent = BrowserFingerprint.generateUserAgent(
        capturedData.fingerprint
      );
      
      fingerprint = capturedData.fingerprint;
    }

    const MapHeader = {
      "User-Agent": userAgent,
      Accept: "*/*",
      Origin: "https://www.ticketmaster.com",
      Referer: "https://www.ticketmaster.com/",
      "Content-Encoding": "gzip",
      Cookie: cookieString,
    };

    const FacetHeader = {
      Accept: "*/*",
      "Accept-Language": fingerprint?.language || "en-US",
      "Accept-Encoding": "gzip, deflate, br, zstd",
      "User-Agent": userAgent,
      Referer: "https://www.ticketmaster.com/",
      Origin: "https://www.ticketmaster.com",
      Cookie: cookieString,
      "tmps-correlation-id": generateCorrelationId(),
      "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe",
    };

    console.log(`Starting event scraping for ${eventId} with${event?.headers ? " shared" : ""} cookies...`);

    const mapUrl = `https://mapsapi.tmol.io/maps/geometry/3/event/${eventId}/placeDetailNoKeys?useHostGrids=true&app=CCP&sectionLevel=true&systemId=HOST`;
    const facetUrl = `https://services.ticketmaster.com/api/ismds/event/${eventId}/facets?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse`;

    const [DataMap, DataFacets] = await Promise.all([
      GetData(MapHeader, proxyAgent, mapUrl, eventId),
      GetData(FacetHeader, proxyAgent, facetUrl, eventId),
    ]);

    if (!DataFacets || !DataMap) {
      console.log(`Failed to get data from APIs for event ${eventId}`);
      return false;
    }

    console.log(`API requests completed successfully for event ${eventId}`);
    const dataGet = GenerateNanoPlaces(DataFacets?.facets);
    const finalData = AttachRowSection(
      dataGet,
      DataMap,
      DataFacets?._embedded?.offer,
      { eventId, inHandDate: event?.inHandDate },
      DataFacets?._embedded?.description
    );

    return finalData;
  } catch (error) {
    console.error(`Scraping error for event ${event?.eventId || event}:`, error);
    return false;
  } finally {
    await cleanup(browser, context);
  }
};

async function cleanup(browser, context) {
  try {
    if (context) await context.close().catch(() => {});
    if (browser) await browser.close().catch(() => {});
  } catch (error) {
    console.warn("Cleanup error:", error);
  }
}

function resetCapturedState() {
  capturedState = {
    cookies: null,
    fingerprint: null,
    lastRefresh: null,
    proxy: null,
  };
}

export { ScrapeEvent, refreshHeaders };
