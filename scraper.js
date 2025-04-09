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
  // If cookies are already being refreshed, add this request to the queue with a timeout
  if (isRefreshingCookies) {
    console.log(
      `Cookies are already being refreshed, queueing request for event ${eventId}`
    );
    return new Promise((resolve, reject) => {
      // Add a timeout to prevent requests from getting stuck in the queue forever
      const timeoutId = setTimeout(() => {
        // Find and remove this request from the queue
        const index = cookieRefreshQueue.findIndex(item => item.timeoutId === timeoutId);
        if (index !== -1) {
          cookieRefreshQueue.splice(index, 1);
        }
        
        // Generate fallback headers since we timed out waiting
        console.log(`Queue timeout for event ${eventId}, using fallback headers`);
        const fallbackHeaders = generateFallbackHeaders();
        resolve({
          cookies: null,
          fingerprint: BrowserFingerprint.generate(),
          lastRefresh: Date.now(),
          headers: fallbackHeaders,
          proxy: proxy
        });
      }, 30000); // 30 second timeout
      
      cookieRefreshQueue.push({ resolve, reject, eventId, timeoutId });
    });
  }

  let localContext = null;
  let localBrowser = null;

  try {
    isRefreshingCookies = true;
    
    // Set up a cleanup function to ensure we always reset the flag and process the queue
    const cleanupRefreshProcess = (error = null) => {
      isRefreshingCookies = false;
      
      // Process any queued refresh requests
      while (cookieRefreshQueue.length > 0) {
        const { resolve, reject, timeoutId } = cookieRefreshQueue.shift();
        clearTimeout(timeoutId); // Clear the timeout for this request
        
        if (error) {
          reject(error);
        } else if (capturedState.cookies || capturedState.headers) {
          resolve(capturedState);
        } else {
          // If we don't have cookies or headers, use fallback headers
          const fallbackHeaders = generateFallbackHeaders();
          const fallbackState = {
            cookies: null,
            fingerprint: BrowserFingerprint.generate(),
            lastRefresh: Date.now(),
            headers: fallbackHeaders,
            proxy: proxy
          };
          resolve(fallbackState);
        }
      }
    };
    
    // Add a global timeout for the entire refresh process
    const globalTimeoutId = setTimeout(() => {
      console.error(`Global timeout reached when refreshing cookies for event ${eventId}`);
      cleanupRefreshProcess(new Error("Global timeout for cookie refresh"));
    }, 60000); // 60 second timeout for the entire process

    // Check if we have valid cookies in memory first
    if (
      capturedState.cookies?.length &&
      capturedState.lastRefresh &&
      Date.now() - capturedState.lastRefresh <= CONFIG.COOKIE_REFRESH_INTERVAL
    ) {
      console.log("Using existing cookies from memory");
      clearTimeout(globalTimeoutId);
      cleanupRefreshProcess();
      return capturedState;
    }

    // If specific cookies are provided, use them
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
      
      clearTimeout(globalTimeoutId);
      cleanupRefreshProcess();
      return capturedState;
    }

    // Try to load cookies from file with improved validation
    try {
      const cookiesFromFile = await loadCookiesFromFile();
      if (cookiesFromFile && cookiesFromFile.length >= 3) { // Ensure we have enough cookies
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
        
        clearTimeout(globalTimeoutId);
        cleanupRefreshProcess();
        return capturedState;
      }
    } catch (err) {
      console.error("Error loading cookies from file:", err);
    }

    console.log(
      `No valid cookies found, getting new cookies using event ${eventId}`
    );

    // Generate fallback headers if we can't get proper ones
    const fallbackHeaders = generateFallbackHeaders();
    
    // Initialize browser with improved error handling
    let initAttempts = 0;
    let initSuccess = false;
    let initError = null;
    
    while (initAttempts < 3 && !initSuccess) {
      try {
        const { context: newContext, fingerprint } = await initBrowser(proxy);
        if (!newContext || !fingerprint) {
          throw new Error("Failed to initialize browser or generate fingerprint");
        }
        
        localContext = newContext;
        initSuccess = true;
      } catch (error) {
        initAttempts++;
        initError = error;
        console.error(`Browser init attempt ${initAttempts} failed:`, error.message);
        await new Promise(resolve => setTimeout(resolve, 1000 * initAttempts));
      }
    }
    
    if (!initSuccess) {
      console.error("All browser initialization attempts failed");
      
      // Return fallback headers since we couldn't initialize browser
      capturedState = {
        cookies: null,
        fingerprint: BrowserFingerprint.generate(),
        lastRefresh: Date.now(),
        headers: fallbackHeaders,
        proxy: capturedState.proxy || proxy,
      };
      
      clearTimeout(globalTimeoutId);
      cleanupRefreshProcess();
      return capturedState;
    }

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
        timeout: 30000, // 30 second timeout
      });

      console.log(`Successfully loaded page for event ${eventId}`);

      // Check for Ticketmaster challenge (e.g., CAPTCHA)
      const isChallengePresent = await checkForTicketmasterChallenge(page);
      if (isChallengePresent) {
        console.warn(
          "Detected Ticketmaster challenge page, attempting to resolve..."
        );
        await handleTicketmasterChallenge(page);
      }

      // Simulate human behavior
      await simulateHumanBehavior(page);

      // Additional wait for cookies to be fully set
      await page.waitForTimeout(1000);

      // Capture cookies
      const { cookies, fingerprint: newFingerprint } = await captureCookies(
        page,
        capturedState.fingerprint || BrowserFingerprint.generate()
      );

      if (!cookies || cookies.length === 0) {
        console.error("Failed to capture cookies for event", eventId);
        
        // Use fallback headers if we couldn't get cookies
        capturedState = {
          cookies: null,
          fingerprint: newFingerprint,
          lastRefresh: Date.now(),
          headers: fallbackHeaders,
          proxy: capturedState.proxy || proxy,
        };
        
        // Process queued refresh requests with fallback data
        while (cookieRefreshQueue.length > 0) {
          const { resolve } = cookieRefreshQueue.shift();
          resolve(capturedState);
        }
        
        isRefreshingCookies = false;
        return capturedState;
      }

      // Update captured state with new cookies and fingerprint
      capturedState = {
        cookies,
        fingerprint: newFingerprint,
        lastRefresh: Date.now(),
        proxy: capturedState.proxy || proxy,
      };

      // Save cookies to file in the background
      saveCookiesToFile(cookies).catch((error) => {
        console.error("Error saving cookies to file:", error);
      });
      
      // Process queued refresh requests
      while (cookieRefreshQueue.length > 0) {
        const { resolve } = cookieRefreshQueue.shift();
        resolve(capturedState);
      }

      return capturedState;
    } catch (error) {
      console.error("Error refreshing headers:", error);
      
      // Use fallback headers on error
      capturedState = {
        cookies: null,
        fingerprint: BrowserFingerprint.generate(),
        lastRefresh: Date.now(),
        headers: fallbackHeaders,
        proxy: capturedState.proxy || proxy,
      };
      
      // Process queued refresh requests with fallback data
      while (cookieRefreshQueue.length > 0) {
        const { resolve } = cookieRefreshQueue.shift();
        resolve(capturedState);
      }
      
      throw error;
    } finally {
      if (page) {
        try {
          await page.close();
        } catch (e) {
          console.error("Error closing page:", e);
        }
      }
      
      await cleanup(localBrowser, localContext);
      clearTimeout(globalTimeoutId);
      cleanupRefreshProcess();
    }
  } catch (error) {
    console.error("Error in refreshHeaders:", error);
    
    // Make sure we clean up on error
    if (localBrowser) {
      await cleanup(localBrowser, localContext);
    }
    
    // Reset the flag to allow new requests
    isRefreshingCookies = false;
    
    // In case of error, reject all queued requests
    while (cookieRefreshQueue.length > 0) {
      const { reject, timeoutId } = cookieRefreshQueue.shift();
      clearTimeout(timeoutId); // Clear the timeout
      reject(error);
    }
    
    throw error;
  }
}

// Generate fallback headers to use when proper cookies can't be obtained
function generateFallbackHeaders() {
  const fingerprint = BrowserFingerprint.generate();
  const userAgent = BrowserFingerprint.generateUserAgent(fingerprint);
  
  return {
    "User-Agent": userAgent,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.ticketmaster.com/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "x-tm-api-key": "b462oi7fic6pehcdkzony5bxhe",
  };
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
      
      // Check different header formats for compatibility
      if (typeof event.headers === 'object') {
        // If it's a complete headers object with all required fields
        if (event.headers.headers) {
          // Extract from nested headers property if available
          cookieString = event.headers.headers.Cookie || event.headers.headers.cookie;
          userAgent = event.headers.headers["User-Agent"] || event.headers.headers["user-agent"];
        } else {
          // Try direct properties
          cookieString = event.headers.Cookie || event.headers.cookie;
          userAgent = event.headers["User-Agent"] || event.headers["user-agent"];
        }
        
        // For backwards compatibility, check if headers contains capturedState format
        if (event.headers.cookies && Array.isArray(event.headers.cookies)) {
          cookieString = event.headers.cookies
            .map((cookie) => `${cookie.name}=${cookie.value}`)
            .join("; ");
        }
      }
      
      // Check if we have valid headers to use
      if (cookieString && userAgent) {
        console.log(`Reusing existing headers for batch processing of event ${eventId}`);
        useProvidedHeaders = true;
        // Initialize fingerprint with at least basic properties
        fingerprint = event.headers.fingerprint || { 
          language: "en-US",
          timezone: "America/New_York",
          screen: { width: 1920, height: 1080 }
        };
      } else {
        console.log(`Incomplete headers for event ${eventId}, falling back to standard flow`);
      }
    }
    
    if (!useProvidedHeaders) {
      // Standard flow - get cookies and headers
      const correlationId = generateCorrelationId();
      const capturedData = await getCapturedData(eventId, proxy);

      if (!capturedData) {
        throw new Error("Failed to get captured data");
      }
      
      // Use headers from capturedData if available (fallback mechanism)
      if (capturedData.headers && !capturedData.cookies) {
        console.log(`Using fallback headers for event ${eventId}`);
        return callTicketmasterAPI(capturedData.headers, proxyAgent, eventId, event);
      }

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

    return callTicketmasterAPI(FacetHeader, proxyAgent, eventId, event, MapHeader);
  } catch (error) {
    console.error(`Scraping error for event ${event?.eventId || event}:`, error);
    return false;
  } finally {
    await cleanup(browser, context);
  }
};

// Extracted API call logic to reduce duplicate code
async function callTicketmasterAPI(facetHeader, proxyAgent, eventId, event, mapHeader = null) {
  // Maximum retries and delay configuration
  const maxRetries = 3;
  const baseDelayMs = 500;
  const maxDelayMs = 2000;
  const jitterFactor = 0.2; // 20% jitter
  
  // Track success metrics for this call
  let attempts = 0;
  let lastError = null;
  const startTime = Date.now();
  
  // IP rotation strategies for 403s (reuse from proxy pool)
  let alternativeProxies = [];
  try {
    // Pre-select a few alternative proxies to try if we get blocked
    for (let i = 0; i < 2; i++) {
      const { proxyAgent: altAgent, proxy: altProxy } = GetProxy();
      if (altAgent && altProxy) {
        alternativeProxies.push({ proxyAgent: altAgent, proxy: altProxy });
      }
    }
  } catch (err) {
    console.log(`Could not prepare alternative proxies: ${err.message}`);
  }
  
  // Enhanced version with more effective randomization techniques for 403s
  const generateAlternativeHeaders = () => {
    const fp = BrowserFingerprint.generate();
    const ua = BrowserFingerprint.generateUserAgent(fp);
    
    // Extract just the essential cookie values if we have them
    let cookieParts = [];
    if (facetHeader.Cookie) {
      try {
        // Try to preserve critical authentication cookies like session ID, auth tokens
        const cookieStr = facetHeader.Cookie;
        const cookiesSplit = cookieStr.split(';');
        const essentialCookies = ['SESSION', 'TM_TKTS', 'TMPS', 'aud', 'TMRANDOM', 'tmTrackID', 'at'];
        
        cookiesSplit.forEach(cookie => {
          const name = cookie.split('=')[0]?.trim();
          if (essentialCookies.some(essential => name?.includes(essential))) {
            cookieParts.push(cookie.trim());
          }
        });
      } catch (error) {
        console.log(`Error extracting cookies: ${error.message}`);
      }
    }
    
    // Add subtle timezone and language variations
    const languages = ["en-US", "en-GB", "en-CA", "en"];
    const timezones = ["America/New_York", "America/Chicago", "America/Los_Angeles", "Europe/London"];
    
    // Randomize accepted formats slightly
    const accepts = [
      "*/*",
      "application/json, text/plain, */*",
      "application/json, text/javascript, */*; q=0.01"
    ];
    
    // Common mobile and desktop User-Agent patterns
    const useNewUa = Math.random() > 0.5;
    const userAgent = useNewUa ? ua : [
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    ][Math.floor(Math.random() * 4)];
    
    return {
      "User-Agent": userAgent,
      "Accept": accepts[Math.floor(Math.random() * accepts.length)],
      "Accept-Language": languages[Math.floor(Math.random() * languages.length)],
      "Accept-Encoding": "gzip, deflate, br",
      "Referer": "https://www.ticketmaster.com/",
      "Origin": "https://www.ticketmaster.com",
      "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe",
      "x-tm-api-key": "b462oi7fic6pehcdkzony5bxhe",
      "tmps-correlation-id": generateCorrelationId(),
      "Cache-Control": "no-cache",
      "Pragma": "no-cache",
      "sec-ch-ua": `"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"`,
      "sec-ch-ua-mobile": "?0",
      "sec-ch-ua-platform": Math.random() > 0.2 ? "Windows" : "macOS",
      "sec-fetch-dest": "empty",
      "sec-fetch-mode": "cors",
      "sec-fetch-site": "same-site",
      "Cookie": cookieParts.length > 0 ? cookieParts.join('; ') : undefined
    };
  };
  
  // Helper to create an optimized version of the request with retries
  const makeRequestWithRetry = async (url, headers, agent, attemptNum = 0, isRetryWithNewProxy = false) => {
    // Add jitter to prevent thundering herd pattern and appear more human
    const jitter = 1 + (Math.random() * jitterFactor * 2 - jitterFactor);
    const delay = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attemptNum)) * jitter;
    
    if (attemptNum > 0) {
      // Wait with exponential backoff before retry
      await new Promise(resolve => setTimeout(resolve, delay));
      console.log(`Retry attempt ${attemptNum} for ${eventId} (${url.includes('mapsapi') ? 'map' : 'facet'} API) after ${delay.toFixed(0)}ms`);
    }
    
    // Create a fresh abort controller for each attempt
    let abortController = new AbortController();
    const timeout = setTimeout(() => {
      abortController.abort();
      console.log(`Request aborted due to timeout for ${eventId}`);
    }, CONFIG.PAGE_TIMEOUT);
    
    try {
      // Add slight randomization to headers to avoid fingerprinting
      const modifiedHeaders = { ...headers };
      
      // IMPORTANT: Trim cookie length if it's too large (Ticketmaster has limits)
      if (modifiedHeaders.Cookie && modifiedHeaders.Cookie.length > 2000) {
        modifiedHeaders.Cookie = trimCookieHeader(modifiedHeaders.Cookie);
        console.log(`Trimmed cookie header to ${modifiedHeaders.Cookie.length} chars for ${eventId}`);
      }
      
      // Slightly randomize headers while keeping essential values
      if (attemptNum > 0) {
        // Add "random" parameters that won't affect functionality
        modifiedHeaders['X-Request-ID'] = generateCorrelationId();
        modifiedHeaders['X-Attempt-Num'] = attemptNum.toString();
        
        // IMPORTANT: Remove If-Modified-Since header to avoid 304 errors
        // The server may return 304 Not Modified if we send this with a future date
        delete modifiedHeaders['If-Modified-Since'];
        
        // Add subtle browser-like variations
        if (Math.random() > 0.5) {
          modifiedHeaders['X-Requested-With'] = 'XMLHttpRequest';
        }
        
        // Add random viewport size to appear more like a real browser
        const viewportWidth = 1200 + Math.floor(Math.random() * 400);
        const viewportHeight = 800 + Math.floor(Math.random() * 300);
        modifiedHeaders['X-Viewport-Size'] = `${viewportWidth}x${viewportHeight}`;
        
        // Mimic browser behavior by changing headers slightly based on destination
        if (url.includes('mapsapi')) {
          modifiedHeaders['Sec-Fetch-Site'] = 'cross-site';
        } else {
          modifiedHeaders['Sec-Fetch-Site'] = 'same-site';
        }
      }
      
      // Make the request using browser-like settings
      const response = await axios.get(url, {
        httpsAgent: agent,
        headers: modifiedHeaders,
        timeout: CONFIG.PAGE_TIMEOUT,
        signal: abortController.signal,
        // Update validateStatus to accept 304 Not Modified as a valid response
        validateStatus: (status) => status === 200 || status === 304,
      });
      
      clearTimeout(timeout);
      
      // Handle 304 Not Modified responses
      if (response.status === 304) {
        console.log(`Received 304 Not Modified for ${eventId} - this is OK, using cached data`);
        // For 304 responses, we need to get data from cache or make a fresh request
        // without the If-Modified-Since header
        if (attemptNum === 0) {
          // Try again without cache validation headers
          const cleanHeaders = { ...modifiedHeaders };
          // Remove all cache-related headers
          delete cleanHeaders['If-Modified-Since'];
          delete cleanHeaders['If-None-Match'];
          return makeRequestWithRetry(url, cleanHeaders, agent, attemptNum + 1);
        }
      }
      
      return response.data;
    } catch (error) {
      clearTimeout(timeout);
      
      // Check if this is a 403 error
      const is403Error = error.response && error.response.status === 403;
      const is429Error = error.response && error.response.status === 429;
      const is304Error = error.response && error.response.status === 304;
      const is400Error = error.response && error.response.status === 400;
      const isNetworkError = !error.response && error.code;
      
      // Special handling for 400 Bad Request - likely caused by malformed request
      if (is400Error) {
        console.log(`400 Bad Request for ${eventId} - likely invalid request format or cookie issue`);
        
        // Try with minimal headers and essential cookies only
        if (attemptNum < maxRetries) {
          // Create minimal headers with only essential cookies
          const minimalHeaders = { 
            "User-Agent": headers["User-Agent"] || "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.ticketmaster.com/",
            "Origin": "https://www.ticketmaster.com",
            "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe"
          };
          
          // Extract just essential cookies if we have them
          if (headers.Cookie) {
            const essentialCookieString = extractEssentialCookies(headers.Cookie);
            if (essentialCookieString) {
              minimalHeaders["Cookie"] = essentialCookieString;
            }
          }
          
          console.log(`Retrying ${eventId} with minimal headers after 400 error`);
          return makeRequestWithRetry(url, minimalHeaders, agent, attemptNum + 1);
        }
      }
      
      // Special handling for 304 Not Modified errors
      if (is304Error) {
        console.log(`304 Not Modified for ${eventId} - trying with fresh request`);
        // Make a new request without cache validation headers
        const cleanHeaders = { ...headers };
        delete cleanHeaders['If-Modified-Since'];
        delete cleanHeaders['If-None-Match'];
        delete cleanHeaders['If-Match'];
        delete cleanHeaders['Cache-Control'];
        // Add explicit cache-busting
        cleanHeaders['Cache-Control'] = 'no-cache, no-store, must-revalidate';
        cleanHeaders['Pragma'] = 'no-cache';
        cleanHeaders['Expires'] = '0';
        
        // Add a random query parameter to avoid cache
        const cacheBuster = `?_=${Date.now()}`;
        const urlWithBuster = url.includes('?') 
          ? `${url}&_=${Date.now()}` 
          : `${url}?_=${Date.now()}`;
          
        return makeRequestWithRetry(urlWithBuster, cleanHeaders, agent, attemptNum + 1);
      }
      
      // On 403/429 errors, we need to try a different approach
      if (is403Error || is429Error) {
        console.log(`${error.response.status} error for ${eventId} (${url.includes('mapsapi') ? 'map' : 'facet'} API)`);
        
        if (attemptNum < maxRetries) {
          // Try with alternative headers or proxy if available
          if (is403Error && alternativeProxies.length > 0 && (attemptNum > 0 || isRetryWithNewProxy)) {
            // On second+ retry with 403, try a different proxy
            const altProxyData = alternativeProxies.shift();
            console.log(`Switching to alternative proxy for ${eventId} retry`);
            
            // Try with the alternative proxy and completely fresh headers
            const altHeaders = generateAlternativeHeaders();
            return makeRequestWithRetry(url, altHeaders, altProxyData.proxyAgent, attemptNum + 1, true);
          } else {
            // Try with fresh headers but same proxy
            const altHeaders = generateAlternativeHeaders();
            
            // For stubborn 403s, try completely different approach
            if (attemptNum > 1) {
              // Further randomize request properties on persistent 403s
              // Add small delay to appear more human-like
              await new Promise(resolve => setTimeout(resolve, 500 + Math.random() * 1000));
              
              // Try a different approach to the request
              console.log(`Using alternative request approach for persistent 403s on ${eventId}`);
              
              // Construct a more browser-like request to attempt to bypass protections
              const browserLikeHeaders = {
                ...altHeaders,
                "Referer": `https://www.ticketmaster.com/event/${eventId}`,
                "Host": new URL(url).hostname,
                "Connection": "keep-alive",
                "TE": "trailers"
              };
              
              return makeRequestWithRetry(url, browserLikeHeaders, agent, attemptNum + 1);
            }
            
            return makeRequestWithRetry(url, altHeaders, agent, attemptNum + 1);
          }
        }
      } else if (isNetworkError && attemptNum < maxRetries) {
        // For network errors, simply retry with the same parameters
        console.log(`Network error for ${eventId}: ${error.message}, retrying...`);
        return makeRequestWithRetry(url, headers, agent, attemptNum + 1);
      }
      
      throw error;
    }
  };
  
  // Function to trim cookie header to a reasonable size
  // This prevents 400 Bad Request errors from overly large cookies
  function trimCookieHeader(cookieString) {
    if (!cookieString || cookieString.length < 2000) return cookieString;
    
    // Most important cookies for Ticketmaster API functionality
    const essentialCookieNames = [
      'TMUO', 'TMPS', 'TM_TKTS', 'SESSION', 'audit', 'CMPS', 'CMID', 
      'MUID', 'au_id', 'aud', 'tmTrackID', 'TapAd_DID', 'uid'
    ];
    
    // Split cookie string into individual cookies
    const cookies = cookieString.split(';').map(c => c.trim());
    
    // First pass: keep only essential cookies
    let result = cookies.filter(cookie => {
      const name = cookie.split('=')[0]?.trim();
      return essentialCookieNames.some(essential => name === essential);
    });
    
    // If we don't have enough essential cookies, add some of the first cookies
    // to ensure we have at least a few cookies in the request
    if (result.length < 3) {
      // Add some of the first cookies from the original string (up to 5)
      result = result.concat(cookies.slice(0, 5).filter(c => !result.includes(c)));
    }
    
    return result.join('; ');
  }
  
  // Function to extract just the essential cookies from a cookie string
  function extractEssentialCookies(cookieString) {
    if (!cookieString) return '';
    
    // Most critical cookies for authentication and functionality
    const criticalCookies = ['TMUO', 'TMPS', 'TM_TKTS', 'SESSION', 'audit'];
    
    // Try to extract just these cookies
    const cookies = cookieString.split(';').map(c => c.trim());
    const essential = cookies.filter(cookie => {
      const name = cookie.split('=')[0]?.trim();
      return criticalCookies.some(critical => name === critical);
    });
    
    // Return minimal set of cookies
    return essential.join('; ');
  }
  
  try {
    const mapUrl = `https://mapsapi.tmol.io/maps/geometry/3/event/${eventId}/placeDetailNoKeys?useHostGrids=true&app=CCP&sectionLevel=true&systemId=HOST`;
    const facetUrl = `https://services.ticketmaster.com/api/ismds/event/${eventId}/facets?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse`;

    // If mapHeader is not provided, use facetHeader with adjustments
    const effectiveMapHeader = mapHeader || {
      ...facetHeader,
      Accept: "*/*",
      "Content-Encoding": "gzip"
    };
    
    // Randomize request order slightly to appear more human-like
    const randomizeOrder = Math.random() > 0.5;
    
    // Stagger requests by a small random amount (20-100ms)
    const staggerDelay = 20 + Math.floor(Math.random() * 80);
    
    // Sometimes add a fake visit to the event page first to establish session context
    if (Math.random() > 0.6) {
      try {
        const eventPageUrl = `https://www.ticketmaster.com/event/${eventId}`;
        console.log(`Making context-establishing request to ${eventPageUrl} for ${eventId}`);
        
        // Make a HEAD request to the event page to establish browser context
        await axios({
          method: 'head',
          url: eventPageUrl,
          headers: {
            ...facetHeader,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1"
          },
          httpsAgent: proxyAgent,
          timeout: 5000, // Short timeout as we don't need a full response
        }).catch(e => {
          // Ignore errors - this is just to establish some context
          console.log(`Context request for ${eventId} failed (this is okay): ${e.message}`);
        });
        
        // Small delay after the context request
        await new Promise(resolve => setTimeout(resolve, 100 + Math.random() * 200));
      } catch (contextError) {
        // Ignore any errors from the context-establishing request
      }
    }
    
    // Make both requests with automatic retry logic
    let DataMap, DataFacets;
    
    if (randomizeOrder) {
      // Map API first
      DataMap = await makeRequestWithRetry(mapUrl, effectiveMapHeader, proxyAgent);
      
      // Small delay between requests
      await new Promise(resolve => setTimeout(resolve, staggerDelay));
      
      // Then Facet API
      DataFacets = await makeRequestWithRetry(facetUrl, facetHeader, proxyAgent);
    } else {
      // Facet API first
      DataFacets = await makeRequestWithRetry(facetUrl, facetHeader, proxyAgent);
      
      // Small delay between requests
      await new Promise(resolve => setTimeout(resolve, staggerDelay));
      
      // Then Map API
      DataMap = await makeRequestWithRetry(mapUrl, effectiveMapHeader, proxyAgent);
    }

    if (!DataFacets || !DataMap) {
      console.log(`Failed to get data from APIs for event ${eventId}`);
      return false;
    }

    console.log(`API requests completed successfully for event ${eventId} in ${Date.now() - startTime}ms`);
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
    console.error(`API error for event ${eventId}:`, error);
    // Track errors by type to identify patterns
    const errorType = error.response?.status || error.code || 'unknown';
    console.error(`Error type: ${errorType}, attempt ${attempts}/${maxRetries}`);
    return false;
  }
}

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
