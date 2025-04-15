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

// Enhanced cookie management
const COOKIE_MANAGEMENT = {
  ESSENTIAL_COOKIES: [
    'TMUO', 'TMPS', 'TM_TKTS', 'SESSION', 'audit', 'CMPS', 'CMID',
    'MUID', 'au_id', 'aud', 'tmTrackID', 'TapAd_DID', 'uid'
  ],
  AUTH_COOKIES: ['TMUO', 'TMPS', 'TM_TKTS', 'SESSION', 'audit'],
  MAX_COOKIE_LENGTH: 4000, // Increased from 2000
  COOKIE_REFRESH_INTERVAL: 30 * 60 * 1000, // 30 minutes
};

// Enhanced cookie handling
const handleCookies = {
  // Extract and validate essential cookies
  extractEssentialCookies: (cookies) => {
    if (!cookies) return '';
    
    const cookieMap = new Map();
    cookies.split(';').forEach(cookie => {
      const [name, value] = cookie.trim().split('=');
      if (name && value) {
        cookieMap.set(name, value);
      }
    });
    
    // Prioritize auth cookies
    const essentialCookies = [];
    COOKIE_MANAGEMENT.AUTH_COOKIES.forEach(name => {
      if (cookieMap.has(name)) {
        essentialCookies.push(`${name}=${cookieMap.get(name)}`);
        cookieMap.delete(name);
      }
    });
    
    // Add other essential cookies if we have space
    COOKIE_MANAGEMENT.ESSENTIAL_COOKIES.forEach(name => {
      if (cookieMap.has(name) && essentialCookies.length < 10) {
        essentialCookies.push(`${name}=${cookieMap.get(name)}`);
        cookieMap.delete(name);
      }
    });
    
    return essentialCookies.join('; ');
  },
  
  // Validate cookie freshness
  areCookiesFresh: (cookies) => {
    if (!cookies) return false;
    
    const cookieMap = new Map();
    cookies.split(';').forEach(cookie => {
      const [name, value] = cookie.trim().split('=');
      if (name && value) {
        cookieMap.set(name, value);
      }
    });
    
    // Check for essential auth cookies
    const hasAuthCookies = COOKIE_MANAGEMENT.AUTH_COOKIES.every(name => 
      cookieMap.has(name) && cookieMap.get(name).length > 0
    );
    
    return hasAuthCookies;
  },
  
  // Merge cookies from different sources
  mergeCookies: (existingCookies, newCookies) => {
    if (!existingCookies) return newCookies;
    if (!newCookies) return existingCookies;
    
    const cookieMap = new Map();
    
    // Add existing cookies first
    existingCookies.split(';').forEach(cookie => {
      const [name, value] = cookie.trim().split('=');
      if (name && value) {
        cookieMap.set(name, value);
      }
    });
    
    // Update with new cookies
    newCookies.split(';').forEach(cookie => {
      const [name, value] = cookie.trim().split('=');
      if (name && value) {
        cookieMap.set(name, value);
      }
    });
    
    // Convert back to string
    return Array.from(cookieMap.entries())
      .map(([name, value]) => `${name}=${value}`)
      .join('; ');
  }
};

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
      const data = await fs.promises.readFile(COOKIES_FILE, 'utf8');
      const cookies = JSON.parse(data);
      
      // Validate cookie freshness
      const cookieString = cookies
        .map(cookie => `${cookie.name}=${cookie.value}`)
        .join('; ');
      
      if (handleCookies.areCookiesFresh(cookieString)) {
        return cookies;
      }
    }
    return null;
  } catch (error) {
    console.error('Error loading cookies:', error);
    return null;
  }
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
    const cookieData = cookies.map(cookie => ({
      ...cookie,
      expires: cookie.expires || Date.now() + COOKIE_MANAGEMENT.COOKIE_REFRESH_INTERVAL
    }));
    
    await fs.promises.writeFile(
      COOKIES_FILE,
      JSON.stringify(cookieData, null, 2)
    );
  } catch (error) {
    console.error('Error saving cookies:', error);
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

// Enhanced browser fingerprinting
const generateEnhancedFingerprint = () => {
  const baseFingerprint = BrowserFingerprint.generate();
  
  // Add more realistic browser characteristics
  return {
    ...baseFingerprint,
    screen: {
      ...baseFingerprint.screen,
      colorDepth: Math.random() > 0.5 ? 24 : 30,
      pixelRatio: Math.random() > 0.5 ? 1 : 2,
    },
    language: baseFingerprint.language,
    timezone: baseFingerprint.timezone,
    platform: Math.random() > 0.5 ? 'Win32' : 'MacIntel',
    deviceMemory: Math.random() > 0.5 ? 8 : 16,
    hardwareConcurrency: Math.random() > 0.5 ? 4 : 8,
    webglVendor: Math.random() > 0.5 ? 'Google Inc.' : 'Intel Inc.',
    webglRenderer: Math.random() > 0.5 ? 'ANGLE (Intel, Intel(R) UHD Graphics Direct3D11 vs_5_0 ps_5_0, D3D11)' : 'ANGLE (Google, Vulkan 1.3.0 (SwiftShader Device (Subzero) (0x0000C0DE)), SwiftShader driver)',
  };
};

// Enhanced human behavior simulation
const simulateEnhancedHumanBehavior = async (page) => {
  try {
    // Random delays between actions
    const delays = [100, 200, 300, 400, 500];
    const randomDelay = () => delays[Math.floor(Math.random() * delays.length)];
    
    // Random mouse movements
    const viewportSize = page.viewportSize();
    if (viewportSize) {
      const steps = 5 + Math.floor(Math.random() * 5);
      for (let i = 0; i < steps; i++) {
        await page.mouse.move(
          Math.floor(Math.random() * viewportSize.width),
          Math.floor(Math.random() * viewportSize.height),
          { steps: 3 + Math.floor(Math.random() * 3) }
        );
        await page.waitForTimeout(randomDelay());
      }
    }
    
    // Random scrolling
    const scrollAmount = Math.floor(Math.random() * 500) + 100;
    const scrollSteps = 3 + Math.floor(Math.random() * 3);
    const stepSize = scrollAmount / scrollSteps;
    
    for (let i = 0; i < scrollSteps; i++) {
      await page.mouse.wheel(0, stepSize);
      await page.waitForTimeout(randomDelay());
    }
    
    // Random keyboard activity
    if (Math.random() > 0.7) {
      await page.keyboard.press('Tab');
      await page.waitForTimeout(randomDelay());
    }
    
    // Random viewport resizing
    if (Math.random() > 0.8) {
      const newWidth = 1024 + Math.floor(Math.random() * 500);
      const newHeight = 768 + Math.floor(Math.random() * 300);
      await page.setViewportSize({ width: newWidth, height: newHeight });
      await page.waitForTimeout(randomDelay());
    }
  } catch (error) {
    console.error('Error in human behavior simulation:', error);
  }
};

// Enhanced request headers
const generateEnhancedHeaders = (fingerprint, cookies) => {
  try {
    // Ensure we have valid inputs
    if (!fingerprint) {
      console.warn('No fingerprint provided, using default values');
      fingerprint = {
        language: 'en-US',
        timezone: 'America/New_York',
        screen: { width: 1920, height: 1080 }
      };
    }

    if (!cookies) {
      console.warn('No cookies provided, using empty string');
      cookies = '';
    }

    const userAgent = BrowserFingerprint.generateUserAgent(fingerprint);
    const acceptLanguages = ['en-US,en;q=0.9', 'en-GB,en;q=0.9', 'en-CA,en;q=0.9'];
    const acceptEncodings = ['gzip, deflate, br', 'gzip, deflate', 'br'];
    
    // Generate a realistic sec-ch-ua string based on the browser
    const browser = fingerprint.browser || { name: 'Chrome', version: '120.0.0.0' };
    const secChUa = `"Not_A Brand";v="8", "${browser.name}";v="${browser.version.split('.')[0]}"`;
    
    // Generate a realistic sec-ch-ua-platform based on the platform
    const platform = fingerprint.platform || { name: 'Windows', version: '10' };
    const secChUaPlatform = platform.name === 'Windows' ? 'Windows' : 
                           platform.name === 'Macintosh' ? 'macOS' : 
                           platform.name === 'iPhone' ? 'iOS' : 
                           platform.name === 'Android' ? 'Android' : 'Windows';

    // Generate a realistic sec-ch-ua-mobile based on the device type
    const isMobile = fingerprint.platform?.type === 'mobile' || fingerprint.platform?.type === 'tablet';
    const secChUaMobile = isMobile ? '?1' : '?0';

    return {
      'User-Agent': userAgent,
      'Accept': '*/*',
      'Accept-Language': acceptLanguages[Math.floor(Math.random() * acceptLanguages.length)],
      'Accept-Encoding': acceptEncodings[Math.floor(Math.random() * acceptEncodings.length)],
      'Connection': 'keep-alive',
      'Referer': 'https://www.ticketmaster.com/',
      'Origin': 'https://www.ticketmaster.com',
      'Sec-Fetch-Dest': 'empty',
      'Sec-Fetch-Mode': 'cors',
      'Sec-Fetch-Site': 'same-site',
      'Pragma': 'no-cache',
      'Cache-Control': 'no-cache',
      'Cookie': cookies,
      'X-Requested-With': 'XMLHttpRequest',
      'X-Api-Key': 'b462oi7fic6pehcdkzony5bxhe',
      'sec-ch-ua': secChUa,
      'sec-ch-ua-mobile': secChUaMobile,
      'sec-ch-ua-platform': secChUaPlatform,
      'DNT': Math.random() > 0.5 ? '1' : '0',
      'Upgrade-Insecure-Requests': '1',
      'TE': 'trailers',
    };
  } catch (error) {
    console.error('Error generating enhanced headers:', error);
    // Return a basic set of headers as fallback
    return {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Accept': '*/*',
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'gzip, deflate, br',
      'Connection': 'keep-alive',
      'Referer': 'https://www.ticketmaster.com/',
      'Origin': 'https://www.ticketmaster.com',
      'X-Api-Key': 'b462oi7fic6pehcdkzony5bxhe',
      'Cookie': cookies || '',
    };
  }
};

// Enhanced API call with better retry logic
async function callTicketmasterAPI(facetHeader, proxyAgent, eventId, event, mapHeader = null) {
  const maxRetries = 3;
  const baseDelayMs = 1000; // Increased base delay
  const maxDelayMs = 5000; // Increased max delay
  const jitterFactor = 0.3; // Increased jitter
  
  let attempts = 0;
  let lastError = null;
  const startTime = Date.now();
  
  // Enhanced proxy rotation
  let alternativeProxies = [];
  try {
    for (let i = 0; i < 3; i++) { // Increased alternative proxies
      const { proxyAgent: altAgent, proxy: altProxy } = GetProxy();
      if (altAgent && altProxy) {
        alternativeProxies.push({ proxyAgent: altAgent, proxy: altProxy });
      }
    }
  } catch (err) {
    console.log(`Could not prepare alternative proxies: ${err.message}`);
  }
  
  // Enhanced request with better error handling
  const makeRequestWithRetry = async (url, headers, agent, attemptNum = 0, isRetryWithNewProxy = false) => {
    const jitter = 1 + (Math.random() * jitterFactor * 2 - jitterFactor);
    const delay = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attemptNum)) * jitter;
    
    if (attemptNum > 0) {
      await new Promise(resolve => setTimeout(resolve, delay));
      console.log(`Retry attempt ${attemptNum} for ${eventId} after ${delay.toFixed(0)}ms`);
    }
    
    try {
      // Add random delay before request
      await new Promise(resolve => setTimeout(resolve, 100 + Math.random() * 400));
      
      const response = await axios.get(url, {
        httpsAgent: agent,
        headers: {
          ...headers,
          'X-Request-ID': generateCorrelationId(),
          'X-Attempt-Num': attemptNum.toString(),
        },
        timeout: 30000,
        validateStatus: (status) => status === 200 || status === 304,
      });
      
      return response.data;
    } catch (error) {
      const is403Error = error.response?.status === 403;
      const is429Error = error.response?.status === 429;
      
      if ((is403Error || is429Error) && attemptNum < maxRetries) {
        if (isRetryWithNewProxy && alternativeProxies.length > 0) {
          const altProxyData = alternativeProxies.shift();
          console.log(`Switching to alternative proxy for ${eventId} retry`);
          return makeRequestWithRetry(url, headers, altProxyData.proxyAgent, attemptNum + 1, true);
        }
        
        // Try with fresh headers
        const newFingerprint = generateEnhancedFingerprint();
        const newHeaders = generateEnhancedHeaders(newFingerprint, headers.Cookie);
        return makeRequestWithRetry(url, newHeaders, agent, attemptNum + 1);
      }
      
      throw error;
    }
  };
  
  try {
    const mapUrl = `https://mapsapi.tmol.io/maps/geometry/3/event/${eventId}/placeDetailNoKeys?useHostGrids=true&app=CCP&sectionLevel=true&systemId=HOST`;
    const facetUrl = `https://services.ticketmaster.com/api/ismds/event/${eventId}/facets?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse`;
    
    // Randomize request order
    const randomizeOrder = Math.random() > 0.5;
    let DataMap, DataFacets;
    
    if (randomizeOrder) {
      DataMap = await makeRequestWithRetry(mapUrl, mapHeader || facetHeader, proxyAgent);
      await new Promise(resolve => setTimeout(resolve, 500 + Math.random() * 1000));
      DataFacets = await makeRequestWithRetry(facetUrl, facetHeader, proxyAgent);
    } else {
      DataFacets = await makeRequestWithRetry(facetUrl, facetHeader, proxyAgent);
      await new Promise(resolve => setTimeout(resolve, 500 + Math.random() * 1000));
      DataMap = await makeRequestWithRetry(mapUrl, mapHeader || facetHeader, proxyAgent);
    }
    
    if (!DataFacets || !DataMap) {
      throw new Error('Failed to get data from APIs');
    }
    
    console.log(`API requests completed successfully for event ${eventId} in ${Date.now() - startTime}ms`);
    return AttachRowSection(
      GenerateNanoPlaces(DataFacets?.facets),
      DataMap,
      DataFacets?._embedded?.offer,
      { eventId, inHandDate: event?.inHandDate },
      DataFacets?._embedded?.description
    );
  } catch (error) {
    console.error(`API error for event ${eventId}:`, error);
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
