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
  MAX_RETRIES: 5,
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
  MAX_COOKIE_LENGTH: 8000, // Increased from 4000 for more robust storage
  COOKIE_REFRESH_INTERVAL: 12 * 60 * 60 * 1000, // 12 hours (more conservative refresh)
  MAX_COOKIE_AGE: 7 * 24 * 60 * 60 * 1000, // 7 days maximum cookie lifetime
  COOKIE_ROTATION: {
    ENABLED: true,
    MAX_STORED_COOKIES: 100, // Keep multiple cookie sets
    ROTATION_INTERVAL: 4 * 60 * 60 * 1000, // 4 hours between rotations
    LAST_ROTATION: Date.now()
  }
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
      if (cookieMap.has(name) && essentialCookies.length < 20) { // Increased from 10
        essentialCookies.push(`${name}=${cookieMap.get(name)}`);
        cookieMap.delete(name);
      }
    });

    // Add any remaining cookies if they fit
    if (essentialCookies.join('; ').length < COOKIE_MANAGEMENT.MAX_COOKIE_LENGTH) {
      for (const [name, value] of cookieMap.entries()) {
        const potentialCookie = `${name}=${value}`;
        if (essentialCookies.join('; ').length + potentialCookie.length + 2 < COOKIE_MANAGEMENT.MAX_COOKIE_LENGTH) {
          essentialCookies.push(potentialCookie);
        }
      }
    }
    
    return essentialCookies.join('; ');
  },
  
  // Validate cookie freshness with improved logic
  areCookiesFresh: (cookies) => {
    if (!cookies) return false;
    
    const cookieMap = new Map();
    cookies.split(';').forEach(cookie => {
      const [name, value] = cookie.trim().split('=');
      if (name && value) {
        cookieMap.set(name, value);
      }
    });
    
    // More lenient check: require at least 3 auth cookies
    const authCookiesPresent = COOKIE_MANAGEMENT.AUTH_COOKIES.filter(name => 
      cookieMap.has(name) && cookieMap.get(name).length > 0
    );
    
    return authCookiesPresent.length >= 3; // Need at least 3 auth cookies
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
      const parsedData = JSON.parse(data);
      
      // Check if we're using the new format with cookie rotation
      if (parsedData.cookieSets && Array.isArray(parsedData.cookieSets)) {
        // Sort sets by timestamp (newest first)
        const sortedSets = parsedData.cookieSets.sort((a, b) => b.timestamp - a.timestamp);
        
        // Try each cookie set until we find a usable one
        for (const set of sortedSets) {
          const cookies = set.cookies;
          if (cookies && cookies.length > 0) {
            // Validate cookie freshness
            const cookieString = cookies
              .map(cookie => `${cookie.name}=${cookie.value}`)
              .join('; ');
            
            if (handleCookies.areCookiesFresh(cookieString)) {
              console.log('Using cookie set from', new Date(set.timestamp).toISOString());
              return cookies;
            }
          }
        }
        return null; // No valid cookie sets found
      } 
      else if (Array.isArray(parsedData)) {
        // Legacy format - single cookie array
        const cookies = parsedData;
        
        // Validate cookie freshness
        const cookieString = cookies
          .map(cookie => `${cookie.name}=${cookie.value}`)
          .join('; ');
        
        if (handleCookies.areCookiesFresh(cookieString)) {
          return cookies;
        }
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
      // For more reliable cookie freshness, we now depend on explicit expiry
      // rather than just checking when they were last refreshed
      const cookieAge = cookiesFromFile[0]?.expiry ? 
                       (cookiesFromFile[0].expiry * 1000 - currentTime) : 
                       COOKIE_MANAGEMENT.MAX_COOKIE_AGE;
      
      // If cookies are still valid (not expired and not too old)
      if (cookieAge > 0 && cookieAge < COOKIE_MANAGEMENT.MAX_COOKIE_AGE) {
        capturedState.cookies = cookiesFromFile;
        capturedState.lastRefresh = currentTime - (COOKIE_MANAGEMENT.MAX_COOKIE_AGE - cookieAge);
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
    currentTime - capturedState.lastRefresh > COOKIE_MANAGEMENT.COOKIE_REFRESH_INTERVAL ||
    forceRefresh;

  if (needsRefresh) {
    // Add jitter to prevent all refreshes happening at the same time
    const jitter = Math.random() * 600000 - 300000; // ±5 minutes
    const effectiveInterval = COOKIE_MANAGEMENT.COOKIE_REFRESH_INTERVAL + jitter;
    
    // Check again with jitter applied
    const needsRefreshWithJitter = 
      !capturedState.cookies ||
      !capturedState.fingerprint ||
      !capturedState.lastRefresh ||
      !capturedState.proxy ||
      currentTime - capturedState.lastRefresh > effectiveInterval ||
      forceRefresh;
    
    if (needsRefreshWithJitter) {
      console.log(`Refreshing cookies with jitter of ${Math.round(jitter/60000)}min for event ${eventId}`);
      return await refreshHeaders(eventId, proxy);
    }
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
      Date.now() - capturedState.lastRefresh <= COOKIE_MANAGEMENT.COOKIE_REFRESH_INTERVAL
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
    // Get current cookies from file if they exist
    let existingCookieSets = [];
    try {
      if (fs.existsSync(COOKIES_FILE)) {
        const fileContent = await fs.promises.readFile(COOKIES_FILE, 'utf8');
        const fileData = JSON.parse(fileContent);
        if (Array.isArray(fileData)) {
          existingCookieSets = fileData;
        } else if (Array.isArray(fileData.cookieSets)) {
          existingCookieSets = fileData.cookieSets;
        }
      }
    } catch (err) {
      console.error('Error reading existing cookies:', err);
    }

    // Format the new cookies with updated expiration
    const cookieData = cookies.map(cookie => ({
      ...cookie,
      expires: cookie.expires || Date.now() + COOKIE_MANAGEMENT.COOKIE_REFRESH_INTERVAL,
      expiry: cookie.expiry || Date.now() + COOKIE_MANAGEMENT.COOKIE_REFRESH_INTERVAL
    }));

    // If cookie rotation is enabled
    if (COOKIE_MANAGEMENT.COOKIE_ROTATION.ENABLED) {
      // Check if enough time has passed since last rotation
      const shouldRotate = Date.now() - COOKIE_MANAGEMENT.COOKIE_ROTATION.LAST_ROTATION > 
                          COOKIE_MANAGEMENT.COOKIE_ROTATION.ROTATION_INTERVAL;
      
      if (shouldRotate) {
        // Add current cookies as a new set with timestamp
        const newCookieSet = {
          timestamp: Date.now(),
          cookies: cookieData
        };
        
        // Add new set to existing sets
        existingCookieSets.push(newCookieSet);
        
        // Keep only the newest sets up to MAX_STORED_COOKIES
        existingCookieSets.sort((a, b) => b.timestamp - a.timestamp);
        existingCookieSets = existingCookieSets.slice(0, COOKIE_MANAGEMENT.COOKIE_ROTATION.MAX_STORED_COOKIES);
        
        // Update last rotation time
        COOKIE_MANAGEMENT.COOKIE_ROTATION.LAST_ROTATION = Date.now();
        
        // Save all sets to file
        await fs.promises.writeFile(
          COOKIES_FILE,
          JSON.stringify({
            lastUpdated: Date.now(),
            cookieSets: existingCookieSets
          }, null, 2)
        );
      } else {
        // Just update the most recent set
        if (existingCookieSets.length > 0) {
          existingCookieSets[0].cookies = cookieData;
          existingCookieSets[0].timestamp = Date.now();
          
          await fs.promises.writeFile(
            COOKIES_FILE,
            JSON.stringify({
              lastUpdated: Date.now(),
              cookieSets: existingCookieSets
            }, null, 2)
          );
        } else {
          // No existing sets, create first one
          await fs.promises.writeFile(
            COOKIES_FILE,
            JSON.stringify({
              lastUpdated: Date.now(),
              cookieSets: [{
                timestamp: Date.now(),
                cookies: cookieData
              }]
            }, null, 2)
          );
        }
      }
    } else {
      // Simple cookie storage without rotation
      await fs.promises.writeFile(
        COOKIES_FILE,
        JSON.stringify(cookieData, null, 2)
      );
    }
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
  try {
    // Use ProxyManager global instance if available
    if (global.proxyManager) {
      return global.proxyManager.getProxyForEvent('random');
    }
    
    // Fallback to old method
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
  } catch (error) {
    console.error("Error getting proxy:", error);
    // Last resort fallback if everything fails
    const fallbackProxy = proxyArray.proxies[0];
    const proxyUrl = new URL(`http://${fallbackProxy.proxy}`);
    const proxyURl = `http://${fallbackProxy.username}:${fallbackProxy.password}@${
      proxyUrl.hostname
    }:${proxyUrl.port || 80}`;
    const proxyAgent = new HttpsProxyAgent(proxyURl);
    return { proxyAgent, proxy: fallbackProxy };
  }
};

const ScrapeEvent = async (event, externalProxyAgent = null, externalProxy = null) => {
  try {
    // Determine event ID from either object or simple ID
    const eventId = event?.eventId || event;
    const startTime = Date.now();
    const correlationId = generateCorrelationId();
    let proxyAgent = externalProxyAgent;
    let proxy = externalProxy;
    
    // Ensure we have a valid event ID
    if (!eventId) {
      console.error("Missing event ID in ScrapeEvent call");
      return false;
    }
    
    console.log(`Starting event ${eventId} processing with correlation ID: ${correlationId}`);
    
    // Initialize static tracking variables if needed
    if (!ScrapeEvent.rateLimits) {
      ScrapeEvent.rateLimits = {
        hourlyCount: 0,
        lastHour: new Date().getHours(),
        maxPerHour: 1000,
        recentEvents: new Set(),
        blockedUntil: 0
      };
    }
    
    if (!ScrapeEvent.headerCache) {
      ScrapeEvent.headerCache = new Map();
    }
    
    if (!ScrapeEvent.resultCache) {
      ScrapeEvent.resultCache = new Map();
      // Set up automatic cache cleanup
      setInterval(() => {
        const now = Date.now();
        // Keep results for up to 30 minutes
        const maxAge = 30 * 60 * 1000;
        
        for (const [key, value] of ScrapeEvent.resultCache.entries()) {
          if (now - value.timestamp > maxAge) {
            ScrapeEvent.resultCache.delete(key);
          }
        }
      }, 5 * 60 * 1000); // Run cleanup every 5 minutes
    }
    
    // Check result cache first (if this exact event was processed recently)
    const resultCacheKey = `result_${eventId}`;
    if (ScrapeEvent.resultCache.has(resultCacheKey)) {
      const cachedResult = ScrapeEvent.resultCache.get(resultCacheKey);
      if (cachedResult && cachedResult.data && Date.now() - cachedResult.timestamp < 5 * 60 * 1000) { // 5 min cache
        console.log(`Using cached result for event ${eventId} from ${new Date(cachedResult.timestamp).toISOString()}`);
        return cachedResult.data;
      }
    }
    
    // Check rate limits to avoid overwhelming external services
    const currentHour = new Date().getHours();
    if (currentHour !== ScrapeEvent.rateLimits.lastHour) {
      ScrapeEvent.rateLimits.hourlyCount = 0;
      ScrapeEvent.rateLimits.lastHour = currentHour;
    }
    
    if (ScrapeEvent.rateLimits.hourlyCount >= ScrapeEvent.rateLimits.maxPerHour) {
      console.warn(`Hourly rate limit reached (${ScrapeEvent.rateLimits.hourlyCount}/${ScrapeEvent.rateLimits.maxPerHour})`);
      throw new Error("Rate limit exceeded");
    }
    
    // Check global block (if we've detected we're being blocked by the server)
    if (ScrapeEvent.rateLimits.blockedUntil > Date.now()) {
      const waitTime = Math.ceil((ScrapeEvent.rateLimits.blockedUntil - Date.now()) / 1000);
      console.warn(`Global block in effect for ${waitTime} more seconds`);
      throw new Error(`Service temporarily unavailable for ${waitTime} seconds`);
    }
    
    // Check if this specific event was processed recently to avoid duplicates
    if (ScrapeEvent.rateLimits.recentEvents.has(eventId)) {
      console.log(`Event ${eventId} was processed recently, skipping duplicated request`);
      
      // Wait for a cached result if one is being processed
      if (ScrapeEvent.pendingEvents && ScrapeEvent.pendingEvents.has(eventId)) {
        console.log(`Waiting for pending result of event ${eventId}`);
        try {
          // Wait up to 10 seconds for the result
          const waitStart = Date.now();
          while (Date.now() - waitStart < 10000) {
            await new Promise(resolve => setTimeout(resolve, 500));
            if (ScrapeEvent.resultCache.has(resultCacheKey)) {
              const result = ScrapeEvent.resultCache.get(resultCacheKey);
              if (result && result.data) {
                console.log(`Got pending result for event ${eventId}`);
                return result.data;
              }
            }
            if (!ScrapeEvent.pendingEvents.has(eventId)) {
              console.log(`Event ${eventId} is no longer pending`);
              break;
            }
          }
        } catch (error) {
          console.error(`Error waiting for pending event ${eventId}:`, error.message);
        }
      }
    }
    
    // Track this event as recently processed
    ScrapeEvent.rateLimits.recentEvents.add(eventId);
    // Auto-clear events after 30 seconds to allow reprocessing
    setTimeout(() => {
      ScrapeEvent.rateLimits.recentEvents.delete(eventId);
    }, 30 * 1000);
    
    // Track as pending for deduplication
    if (!ScrapeEvent.pendingEvents) {
      ScrapeEvent.pendingEvents = new Set();
    }
    ScrapeEvent.pendingEvents.add(eventId);
    
    // Use provided proxy if available, otherwise get a new one
    try {
      if (proxyAgent && proxy) {
        console.log(`Using provided proxy ${proxy.proxy || 'unknown'} for event ${eventId}`);
      } else {
        // If we have a ProxyManager instance, use it to get an event-specific proxy
        if (global.proxyManager) {
          const proxyData = global.proxyManager.getProxyForEvent(eventId);
          if (proxyData) {
            const proxyAgentData = global.proxyManager.createProxyAgent(proxyData);
            proxyAgent = proxyAgentData.proxyAgent;
            proxy = proxyData;
            console.log(`Using dedicated proxy ${proxy.proxy} for event ${eventId} from ProxyManager`);
          } else {
            // Fallback to random proxy selection
            const proxyData = GetProxy();
            proxyAgent = proxyData.proxyAgent;
            proxy = proxyData.proxy;
          }
        } else {
          // Fallback to old method if no proxy manager is available
          const proxyData = GetProxy();
          proxyAgent = proxyData.proxyAgent;
          proxy = proxyData.proxy;
        }
      }
    } catch (proxyError) {
      console.error(`Proxy error for event ${eventId}:`, proxyError.message);
      // Try again with a different proxy selection mechanism
      console.log(`Attempting alternative proxy selection for event ${eventId}`);
      await new Promise(resolve => setTimeout(resolve, 1000));
      const proxyData = GetProxy();
      proxyAgent = proxyData.proxyAgent;
      proxy = proxyData.proxy;
    }
    
    // Log memory usage as needed
    const memUsage = process.memoryUsage();
    if (memUsage.heapUsed > 1024 * 1024 * 1024) { // Over 1GB
      console.warn(`High memory usage (${Math.round(memUsage.heapUsed / 1024 / 1024)}MB) during event ${eventId} processing`);
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
      // Use memory cache for frequently used events to optimize performance
      const cacheKey = `header_${eventId}_${proxy?.proxy || 'default'}`;
      if (ScrapeEvent.headerCache.has(cacheKey)) {
        const cachedHeaders = ScrapeEvent.headerCache.get(cacheKey);
        if (cachedHeaders && Date.now() - cachedHeaders.timestamp < 60 * 60 * 1000) { // 1 hour cache
          console.log(`Using cached headers for event ${eventId}`);
          cookieString = cachedHeaders.cookieString;
          userAgent = cachedHeaders.userAgent;
          fingerprint = cachedHeaders.fingerprint;
        } else {
          ScrapeEvent.headerCache.delete(cacheKey); // Remove expired cache entry
        }
      }
      
      if (!cookieString) {
        try {
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
          
          // Store in memory cache
          ScrapeEvent.headerCache.set(cacheKey, {
            cookieString,
            userAgent,
            fingerprint,
            timestamp: Date.now()
          });
        } catch (error) {
          console.error(`Error getting cookies for event ${eventId}:`, error.message);
          // Use fallback headers if available
          const fallbackHeaders = generateFallbackHeaders();
          if (fallbackHeaders) {
            console.log(`Using fallback headers for event ${eventId} after error`);
            return callTicketmasterAPI(fallbackHeaders, proxyAgent, eventId, event);
          }
          throw error;
        }
      }
    }

    // Generate headers with enhanced request ID for tracing
    const traceId = generateCorrelationId() + `-${Date.now()}`;
    
    // Create safe header objects with no references to other objects to prevent circular JSON issues
    const MapHeader = {
      "User-Agent": userAgent,
      "Accept": "*/*",
      "Origin": "https://www.ticketmaster.com",
      "Referer": "https://www.ticketmaster.com/",
      "Content-Encoding": "gzip",
      "Cookie": cookieString,
      "X-Request-ID": traceId,
      "X-Correlation-ID": correlationId
    };

    const FacetHeader = {
      "Accept": "*/*",
      "Accept-Language": fingerprint?.language || "en-US",
      "Accept-Encoding": "gzip, deflate, br, zstd",
      "User-Agent": userAgent,
      "Referer": "https://www.ticketmaster.com/",
      "Origin": "https://www.ticketmaster.com",
      "Cookie": cookieString,
      "tmps-correlation-id": correlationId,
      "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe",
      "X-Request-ID": traceId,
      "X-Correlation-ID": correlationId
    };

    console.log(`Starting event scraping for ${eventId} with${event?.headers ? " shared" : ""} cookies...`);

    // Measure API call time for performance monitoring
    const apiStartTime = Date.now();
    const result = await callTicketmasterAPI(FacetHeader, proxyAgent, eventId, event, MapHeader);
    const apiDuration = Date.now() - apiStartTime;
    
    console.log(`Event ${eventId} processing completed in ${Date.now() - startTime}ms (API: ${apiDuration}ms)`);
    
    // Store successful result in cache
    if (result) {
      ScrapeEvent.resultCache.set(resultCacheKey, {
        data: result,
        timestamp: Date.now()
      });
    }
    
    // If API call was too fast, it might be suspicious (rate limited or blocked)
    if (apiDuration < 100 && !result) {
      console.warn(`Suspiciously fast API failure for event ${eventId} (${apiDuration}ms). Possible rate limiting detected.`);
      // Implement a temporary rate limiting backoff (1 minute)
      ScrapeEvent.rateLimits.blockedUntil = Date.now() + 60 * 1000;
    }
    
    return result;
  } catch (error) {
    console.error(`Scraping error for event ${event?.eventId || event}:`, error.message);
    
    // Clear pending status
    const eventId = event?.eventId || event;
    if (ScrapeEvent.pendingEvents) {
      ScrapeEvent.pendingEvents.delete(eventId);
    }
    
    // Implement automatic retry mechanism for recoverable errors
    const recoverable = error.message && (
      error.message.includes("proxy") || 
      error.message.includes("timeout") || 
      error.message.includes("network") ||
      error.message.includes("ECONNRESET") ||
      error.message.includes("ETIMEDOUT") ||
      error.message.includes("circular") ||
      error.message.includes("JSON") ||
      error.message.includes("403") // Also retry 403 errors with a new proxy
    );
    
    if (recoverable) {
      // Track failed attempts to prevent infinite retries
      if (!ScrapeEvent.failedAttempts) {
        ScrapeEvent.failedAttempts = new Map();
      }
      
      const eventId = event?.eventId || event;
      const attempts = (ScrapeEvent.failedAttempts.get(eventId) || 0) + 1;
      ScrapeEvent.failedAttempts.set(eventId, attempts);
      
      // Only retry if we haven't exceeded max attempts
      if (attempts <= 3) { // Max 4 total attempts (1 original + 3 retries)
        console.log(`Retrying event ${eventId} after recoverable error (attempt ${attempts})`);
        // Add exponential backoff
        const backoff = Math.pow(2, attempts) * 2000 + Math.random() * 1000;
        await new Promise(resolve => setTimeout(resolve, backoff));
        
        // Try with a fresh proxy - ensure we get a different one than before
        let newProxy, newProxyAgent;
        
        if (global.proxyManager) {
          // Release the old proxy with error
          if (externalProxy && externalProxy.proxy) {
            global.proxyManager.releaseProxy(eventId, false, error);
          }
          
          // Get a fresh proxy from ProxyManager
          const proxyData = global.proxyManager.getProxyForEvent(eventId);
          if (proxyData) {
            const proxyAgentData = global.proxyManager.createProxyAgent(proxyData);
            newProxyAgent = proxyAgentData.proxyAgent;
            newProxy = proxyData;
          } else {
            // Fallback
            const proxyData = GetProxy();
            newProxyAgent = proxyData.proxyAgent;
            newProxy = proxyData.proxy;
          }
        } else {
          // Fallback to old method
          const proxyData = GetProxy();
          newProxyAgent = proxyData.proxyAgent;
          newProxy = proxyData.proxy;
        }
        
        // Force cookie refresh on second retry
        if (attempts >= 2) {
          // Force refresh by clearing header cache
          const cacheKey = `header_${eventId}_${newProxy?.proxy || 'default'}`;
          if (ScrapeEvent.headerCache) {
            ScrapeEvent.headerCache.delete(cacheKey);
          }
        }
        
        return ScrapeEvent(event, newProxyAgent, newProxy);
      } else {
        // Clean up failed attempts tracking after some time
        setTimeout(() => {
          ScrapeEvent.failedAttempts.delete(eventId);
        }, 10 * 60 * 1000); // 10 minutes
        
        // Log final failure
        console.error(`Exhausted all retry attempts for event ${eventId}`);
      }
    }
    
    // Release proxy with error if we have a proxy manager
    if (global.proxyManager && externalProxy && externalProxy.proxy) {
      global.proxyManager.releaseProxy(event?.eventId || event, false, error);
    }
    
    return false;
  }
};

// Enhanced API call with better retry logic
async function callTicketmasterAPI(facetHeader, proxyAgent, eventId, event, mapHeader = null, retryCount = 0) {
  const maxRetries = 3;
  const baseDelayMs = 1000; // Increased base delay
  const maxDelayMs = 5000; // Increased max delay
  const jitterFactor = 0.3; // Increased jitter
  
  let attempts = 0;
  let lastError = null;
  let result = null;
  
  // Enhanced proxy rotation with better error tracking
  let alternativeProxies = [];
  let proxyErrors = new Map();
  
  try {
    for (let i = 0; i < 3; i++) { // Increased alternative proxies
      try {
        const { proxyAgent: altAgent, proxy: altProxy } = GetProxy();
        if (altAgent && altProxy) {
          alternativeProxies.push({ proxyAgent: altAgent, proxy: altProxy });
        }
      } catch (proxyError) {
        console.error(`Error getting alternative proxy ${i}:`, proxyError.message);
      }
    }
  } catch (err) {
    console.log(`Could not prepare alternative proxies: ${err.message}`);
  }
  
  // Track API rate limits globally
  if (!callTicketmasterAPI.rateLimits) {
    callTicketmasterAPI.rateLimits = {
      window: 60 * 1000, // 1 minute window
      maxPerWindow: 120, // Maximum 120 requests per minute
      requests: [], // Array to track request timestamps
      lastWarning: 0
    };
  }
  
  // Add current request to tracking
  callTicketmasterAPI.rateLimits.requests.push(Date.now());
  
  // Remove requests older than the window
  const windowStart = Date.now() - callTicketmasterAPI.rateLimits.window;
  callTicketmasterAPI.rateLimits.requests = callTicketmasterAPI.rateLimits.requests.filter(
    timestamp => timestamp >= windowStart
  );
  
  // Check if we're approaching rate limits
  const requestCount = callTicketmasterAPI.rateLimits.requests.length;
  const limitPercentage = requestCount / callTicketmasterAPI.rateLimits.maxPerWindow;
  
  if (limitPercentage > 0.8 && Date.now() - callTicketmasterAPI.rateLimits.lastWarning > 30000) {
    console.warn(`API rate limit threshold approaching: ${requestCount}/${callTicketmasterAPI.rateLimits.maxPerWindow} requests (${Math.round(limitPercentage * 100)}%)`);
    callTicketmasterAPI.rateLimits.lastWarning = Date.now();
  }
  
  // Add dynamic delay based on current rate limit usage
  if (limitPercentage > 0.5) {
    const dynamicDelay = Math.floor(limitPercentage * 2000); // up to 2 seconds delay at high usage
    await new Promise(resolve => setTimeout(resolve, dynamicDelay));
  }
  
  // Enhanced request with better error handling
  const makeRequestWithRetry = async (url, headers, agent, attemptNum = 0, isRetryWithNewProxy = false) => {
    const jitter = 1 + (Math.random() * jitterFactor * 2 - jitterFactor);
    const delay = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attemptNum)) * jitter;
    
    if (attemptNum > 0) {
      await new Promise(resolve => setTimeout(resolve, delay));
      console.log(`Retry attempt ${attemptNum} for event ${eventId} after ${delay.toFixed(0)}ms`);
    }
    
    try {
      // Add random delay before request
      await new Promise(resolve => setTimeout(resolve, 100 + Math.random() * 400));
      
      // Construct a safe copy of headers without any possible circular references
      const safeHeaders = {};
      for (const [key, value] of Object.entries(headers)) {
        if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
          safeHeaders[key] = value;
        }
      }
      
      // Add jitter to user agent to reduce fingerprinting
      if (safeHeaders['User-Agent'] && Math.random() > 0.7) {
        // Slightly modify user agent string to reduce tracking
        const ua = safeHeaders['User-Agent'];
        if (ua.includes('Chrome/')) {
          const parts = ua.split('Chrome/');
          const version = parts[1].split(' ')[0];
          const versionParts = version.split('.');
          if (versionParts.length > 2) {
            // Slightly adjust patch version
            const patchVersion = parseInt(versionParts[2], 10);
            const newPatch = Math.max(0, patchVersion + Math.floor(Math.random() * 5) - 2);
            versionParts[2] = newPatch.toString();
            const newVersion = versionParts.join('.');
            safeHeaders['User-Agent'] = parts[0] + 'Chrome/' + newVersion + ' ' + parts[1].split(' ').slice(1).join(' ');
          }
        }
      }
      
      const response = await axios.get(url, {
        httpsAgent: agent,
        headers: safeHeaders,
        timeout: 30000,
        validateStatus: (status) => status === 200 || status === 304,
      });
      
      return response.data;
    } catch (error) {
      const is403Error = error.response?.status === 403;
      const is429Error = error.response?.status === 429;
      const is500Error = error.response?.status >= 500 && error.response?.status < 600;
      
      // Track proxy errors
      if (agent && (is403Error || is429Error)) {
        // Create a non-circular key for the proxy without JSON.stringify
        const proxyKey = agent.proxy?.host || 'unknown-proxy';
        proxyErrors.set(proxyKey, (proxyErrors.get(proxyKey) || 0) + 1);
      }
      
      // If we've hit a rate limit, throw immediately
      if (is429Error) {
        throw error;
      }
      
      // For 403 errors, we should try with a new proxy before giving up
      if (is403Error && !isRetryWithNewProxy && attemptNum < 2) {
        console.log(`403 error with proxy ${agent.proxy?.host || 'unknown'}, retrying with new proxy`);
        
        try {
          // Get a new proxy
          let newAgent;
          
          if (global.proxyManager) {
            const proxyData = global.proxyManager.getProxyForEvent(eventId);
            if (proxyData) {
              const proxyAgentData = global.proxyManager.createProxyAgent(proxyData);
              newAgent = proxyAgentData.proxyAgent;
            } else {
              const proxyData = GetProxy();
              newAgent = proxyData.proxyAgent;
            }
          } else {
            const proxyData = GetProxy();
            newAgent = proxyData.proxyAgent;
          }
          
          // Wait before retry
          await new Promise(resolve => setTimeout(resolve, 1000 + Math.random() * 2000));
          
          // Try again with new proxy
          return makeRequestWithRetry(url, headers, newAgent, attemptNum + 1, true);
        } catch (proxyError) {
          console.error(`Failed to get new proxy for 403 retry: ${proxyError.message}`);
          throw error; // Rethrow original error if we can't get a new proxy
        }
      }
      
      // For network errors or server errors, retry a few times
      if ((error.code === 'ECONNRESET' || error.code === 'ETIMEDOUT' || is500Error) && attemptNum < 2) {
        console.log(`Network/server error (${error.message}), retrying request (${attemptNum + 1})`);
        
        // Add back-off delay
        const delay = 1000 * Math.pow(2, attemptNum) + Math.random() * 1000;
        await new Promise(resolve => setTimeout(resolve, delay));
        
        return makeRequestWithRetry(url, headers, agent, attemptNum + 1, isRetryWithNewProxy);
      }
      
      // Otherwise, rethrow
      throw error;
    }
  };
  
  try {
    const mapUrl = `https://mapsapi.tmol.io/maps/geometry/3/event/${eventId}/placeDetailNoKeys?useHostGrids=true&app=CCP&sectionLevel=true&systemId=HOST`;
    const facetUrl = `https://services.ticketmaster.com/api/ismds/event/${eventId}/facets?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse`;
    
    // Ensure no circular references in headers by creating new objects with primitive values only
    let safeMapHeader = null;
    if (mapHeader) {
      safeMapHeader = {};
      for (const [key, value] of Object.entries(mapHeader)) {
        if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
          safeMapHeader[key] = value;
        }
      }
    }
    
    let safeFacetHeader = {};
    for (const [key, value] of Object.entries(facetHeader)) {
      if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
        safeFacetHeader[key] = value;
      }
    }
    
    // Randomize request order to be less predictable
    const randomizeOrder = Math.random() > 0.5;
    let DataMap, DataFacets;
    
    // Wrap each API call in a try-catch to handle them independently
    if (randomizeOrder) {
      try {
        DataMap = await makeRequestWithRetry(mapUrl, safeMapHeader || safeFacetHeader, proxyAgent);
      } catch (mapError) {
        console.error(`Map API error for event ${eventId}:`, mapError.message);
        DataMap = null;
      }
      
      // Add variable delay between requests to appear more human-like
      await new Promise(resolve => setTimeout(resolve, 500 + Math.random() * 2000));
      
      try {
        DataFacets = await makeRequestWithRetry(facetUrl, safeFacetHeader, proxyAgent);
      } catch (facetError) {
        console.error(`Facet API error for event ${eventId}:`, facetError.message);
        DataFacets = null;
      }
    } else {
      try {
        DataFacets = await makeRequestWithRetry(facetUrl, safeFacetHeader, proxyAgent);
      } catch (facetError) {
        console.error(`Facet API error for event ${eventId}:`, facetError.message);
        DataFacets = null;
      }
      
      await new Promise(resolve => setTimeout(resolve, 500 + Math.random() * 2000));
      
      try {
        DataMap = await makeRequestWithRetry(mapUrl, safeMapHeader || safeFacetHeader, proxyAgent);
      } catch (mapError) {
        console.error(`Map API error for event ${eventId}:`, mapError.message);
        DataMap = null;
      }
    }
    
    if (!DataFacets && !DataMap) {
      throw new Error('Both API calls failed - no data available');
    }
    
    // Allow processing to continue even if one API call failed
    if (!DataFacets) {
      console.warn(`Proceeding with event ${eventId} using only map data`);
    }
    
    if (!DataMap) {
      console.warn(`Proceeding with event ${eventId} using only facet data`);
    }
    
    console.log(`API requests completed for event ${eventId} in ${Date.now() - startTime}ms`);
    
    // Handle the case where we have partial data
    try {
      return AttachRowSection(
        DataFacets ? GenerateNanoPlaces(DataFacets?.facets) : [],
        DataMap || {},
        DataFacets?._embedded?.offer || [],
        { eventId, inHandDate: event?.inHandDate },
        DataFacets?._embedded?.description || {}
      );
    } catch (processError) {
      console.error(`Error processing API response for event ${eventId}:`, processError.message);
      // If we can't process the data, return null so calling function knows to handle it
      return null;
    }
  } catch (error) {
    console.error(`API error for event ${eventId}:`, error.message);
    return null;
  }
}

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
