import { createRequire } from "module";
const require = createRequire(import.meta.url);
import got from 'got';
const { HttpsProxyAgent } = require("https-proxy-agent");
const fs = require("fs");
import { chromium,devices } from "playwright";
import proxyArray from "./helpers/proxy.js";
import { AttachRowSection } from "./helpers/seatBatch.js";
import GenerateNanoPlaces from "./helpers/seats.js";
import crypto from "crypto";
import { BrowserFingerprint } from "./browserFingerprint.js";
import { simulateHumanBehavior } from "./helpers/humanBehavior.js";
import pThrottle from 'p-throttle';
import randomUseragent from 'random-useragent';
import delay from 'delay-async';
import pRetry from 'p-retry';
import { CookieManager } from './helpers/CookieManager.js';
import scraperManager from './scraperManager.js';
import CookieRefreshTracker from './helpers/CookieRefreshTracker.js';
// Initialize CookieManager instance
const cookieManager = new CookieManager();
cookieManager.persistedPage = null;
cookieManager.persistedContext = null;

const iphone13 = devices["iPhone 13"];

const COOKIES_FILE = "cookies.json";
const CONFIG = {
  COOKIE_REFRESH_INTERVAL: 24 * 60 * 60 * 1000, // 24 hours
  PAGE_TIMEOUT: 30000,
  MAX_RETRIES: 5,
  RETRY_DELAY: 10000,
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
// Flag to track if periodic refresh has been started
let isPeriodicRefreshStarted = false;

// Enhanced cookie management
const COOKIE_MANAGEMENT = {
  ESSENTIAL_COOKIES: [
    "TMUO",
    "TMPS",
    "TM_TKTS",
    "SESSION",
    "audit",
    "CMPS",
    "CMID",
    "MUID",
    "au_id",
    "aud",
    "tmTrackID",
    "TapAd_DID",
    "uid",
  ],
  AUTH_COOKIES: ["TMUO", "TMPS", "TM_TKTS", "SESSION", "audit"],
  MAX_COOKIE_LENGTH: 8000, // Increased from 4000 for more robust storage
  COOKIE_REFRESH_INTERVAL: 30 * 60 * 1000, // 30 minutes
  MAX_COOKIE_AGE: 7 * 24 * 60 * 60 * 1000, // 7 days maximum cookie lifetime
  COOKIE_ROTATION: {
    ENABLED: true,
    MAX_STORED_COOKIES: 100, // Keep multiple cookie sets
    ROTATION_INTERVAL: 4 * 60 * 60 * 1000, // 4 hours between rotations
    LAST_ROTATION: Date.now(),
  },
};

// Enhanced cookie handling
const handleCookies = {
  // Extract and validate essential cookies
  extractEssentialCookies: (cookies) => {
    if (!cookies) return "";

    const cookieMap = new Map();
    cookies.split(";").forEach((cookie) => {
      const [name, value] = cookie.trim().split("=");
      if (name && value) {
        cookieMap.set(name, value);
      }
    });

    // Prioritize auth cookies
    const essentialCookies = [];
    COOKIE_MANAGEMENT.AUTH_COOKIES.forEach((name) => {
      if (cookieMap.has(name)) {
        essentialCookies.push(`${name}=${cookieMap.get(name)}`);
        cookieMap.delete(name);
      }
    });

    // Add other essential cookies if we have space
    COOKIE_MANAGEMENT.ESSENTIAL_COOKIES.forEach((name) => {
      if (cookieMap.has(name) && essentialCookies.length < 20) {
        // Increased from 10
        essentialCookies.push(`${name}=${cookieMap.get(name)}`);
        cookieMap.delete(name);
      }
    });

    // Add any remaining cookies if they fit
    if (
      essentialCookies.join("; ").length < COOKIE_MANAGEMENT.MAX_COOKIE_LENGTH
    ) {
      for (const [name, value] of cookieMap.entries()) {
        const potentialCookie = `${name}=${value}`;
        if (
          essentialCookies.join("; ").length + potentialCookie.length + 2 <
          COOKIE_MANAGEMENT.MAX_COOKIE_LENGTH
        ) {
          essentialCookies.push(potentialCookie);
        }
      }
    }

    return essentialCookies.join("; ");
  },

  // Validate cookie freshness with improved logic
  areCookiesFresh: (cookies) => {
    if (!cookies) return false;

    const cookieMap = new Map();
    cookies.split(";").forEach((cookie) => {
      const [name, value] = cookie.trim().split("=");
      if (name && value) {
        cookieMap.set(name, value);
      }
    });

    // More lenient check: require at least 3 auth cookies
    const authCookiesPresent = COOKIE_MANAGEMENT.AUTH_COOKIES.filter(
      (name) => cookieMap.has(name) && cookieMap.get(name).length > 0
    );

    return authCookiesPresent.length >= 3; // Need at least 3 auth cookies
  },

  // Merge cookies from different sources
  mergeCookies: (existingCookies, newCookies) => {
    if (!existingCookies) return newCookies;
    if (!newCookies) return existingCookies;

    const cookieMap = new Map();

    // Add existing cookies first
    existingCookies.split(";").forEach((cookie) => {
      const [name, value] = cookie.trim().split("=");
      if (name && value) {
        cookieMap.set(name, value);
      }
    });

    // Update with new cookies
    newCookies.split(";").forEach((cookie) => {
      const [name, value] = cookie.trim().split("=");
      if (name && value) {
        cookieMap.set(name, value);
      }
    });

    // Convert back to string
    return Array.from(cookieMap.entries())
      .map(([name, value]) => `${name}=${value}`)
      .join("; ");
  },
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
    }).catch(() => false); // Catch any navigation errors

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
        console.warn("Mouse movement error in challenge, continuing:", moveError.message);
      }

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
            text?.toLowerCase().includes("verify")
          ) {
            await button.click();
            buttonClicked = true;
            break;
          }
        } catch (buttonError) {
          console.warn("Button click error, continuing:", buttonError.message);
          continue;
        }
      }

      if (!buttonClicked) {
        console.warn("Could not find challenge button, continuing without resolution");
        return false;
      }

      await page.waitForTimeout(2000);
      const stillChallenged = await page.evaluate(() => {
        return document.body.textContent.includes(
          "Your Browsing Activity Has Been Paused"
        );
      }).catch(() => false);

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

// Enhanced location generation
function getRandomLocation() {
  const locations = [
    { timezone: "America/New_York", latitude: 40.7128 + (Math.random() * 0.1 - 0.05), longitude: -74.006 + (Math.random() * 0.1 - 0.05), locale: "en-US" },
    { timezone: "America/Chicago", latitude: 41.8781 + (Math.random() * 0.1 - 0.05), longitude: -87.6298 + (Math.random() * 0.1 - 0.05), locale: "en-US" },
    { timezone: "America/Denver", latitude: 39.7392 + (Math.random() * 0.1 - 0.05), longitude: -104.9903 + (Math.random() * 0.1 - 0.05), locale: "en-US" },
    { timezone: "America/Los_Angeles", latitude: 34.0522 + (Math.random() * 0.1 - 0.05), longitude: -118.2437 + (Math.random() * 0.1 - 0.05), locale: "en-US" },
    { timezone: "America/Phoenix", latitude: 33.4484 + (Math.random() * 0.1 - 0.05), longitude: -112.0740 + (Math.random() * 0.1 - 0.05), locale: "en-US" },
    { timezone: "America/Toronto", latitude: 43.6532 + (Math.random() * 0.1 - 0.05), longitude: -79.3832 + (Math.random() * 0.1 - 0.05), locale: "en-CA" },
    { timezone: "America/Vancouver", latitude: 49.2827 + (Math.random() * 0.1 - 0.05), longitude: -123.1207 + (Math.random() * 0.1 - 0.05), locale: "en-CA" },
    { timezone: "Europe/London", latitude: 51.5074 + (Math.random() * 0.1 - 0.05), longitude: -0.1278 + (Math.random() * 0.1 - 0.05), locale: "en-GB" },
    { timezone: "Europe/Paris", latitude: 48.8566 + (Math.random() * 0.1 - 0.05), longitude: 2.3522 + (Math.random() * 0.1 - 0.05), locale: "fr-FR" },
    { timezone: "Europe/Berlin", latitude: 52.5200 + (Math.random() * 0.1 - 0.05), longitude: 13.4050 + (Math.random() * 0.1 - 0.05), locale: "de-DE" },
  ];

  const weightedLocations = [
    ...locations.slice(0, 6),
    ...locations.slice(0, 6),
    ...locations
  ];

  const random = Math.floor(Math.random() * weightedLocations.length);
  return weightedLocations[random];
}

// Generate realistic iPhone user agent
function getRealisticIphoneUserAgent() {
  const iOSVersions = ['15_0', '15_1', '15_2', '15_3', '15_4', '15_5', '15_6', '16_0', '16_1', '16_2'];
  const safariVersions = ['15.0', '15.1', '15.2', '15.3', '15.4', '16.0', '16.1'];
  
  const iOSVersion = iOSVersions[Math.floor(Math.random() * iOSVersions.length)];
  const safariVersion = safariVersions[Math.floor(Math.random() * safariVersions.length)];
  
  return `Mozilla/5.0 (iPhone; CPU iPhone OS ${iOSVersion} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/${safariVersion} Mobile/15E148 Safari/604.1`;
}

// Enhanced browser fingerprint generation
function enhancedFingerprint() {
  const baseFingerprint = BrowserFingerprint.generate();
  
  const hardwareValues = [4, 6, 8];
  const memoryValues = [2, 4, 8];
  
  return {
    ...baseFingerprint,
    hardwareConcurrency: hardwareValues[Math.floor(Math.random() * hardwareValues.length)],
    deviceMemory: memoryValues[Math.floor(Math.random() * memoryValues.length)],
    screenResolution: {
      width: [375, 390, 414][Math.floor(Math.random() * 3)],
      height: [667, 736, 812, 844][Math.floor(Math.random() * 4)]
    },
    colorDepth: 24,
    touchPoints: Math.floor(Math.random() * 3) + 3
  };
}

// Simulate mobile interactions
async function simulateMobileInteractions(page) {
  const viewport = page.viewportSize();
  if (!viewport) return;

  const startX = viewport.width / 2;
  const startY = viewport.height * (0.7 + Math.random() * 0.2);
  const endY = viewport.height * (0.3 + Math.random() * 0.2);
  
  await page.touchscreen.tap(startX, startY);
  await page.waitForTimeout(100 + Math.random() * 300);
  
  await page.mouse.move(startX, startY);
  await page.mouse.down();
  
  const steps = 10 + Math.floor(Math.random() * 10);
  const timePerStep = 10 + Math.floor(Math.random() * 20);
  
  for (let i = 0; i <= steps; i++) {
    const currentY = startY - ((startY - endY) * (i / steps));
    await page.mouse.move(startX, currentY);
    await page.waitForTimeout(timePerStep);
  }
  
  await page.mouse.up();
  await page.waitForTimeout(300 + Math.random() * 500);
}

// Enhanced browser initialization
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
          '--disable-blink-features=AutomationControlled',
          '--disable-features=IsolateOrigins,site-per-process',
          '--disable-web-security',
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--no-first-run',
          '--no-default-browser-check',
          '--disable-infobars',
          '--disable-notifications'
        ],
        timeout: 60000,
      };

      if (proxy && typeof proxy === 'object' && proxy.proxy) {
        try {
          // Extract hostname and port from proxy string
          const proxyString = proxy.proxy;
          
          // Ensure proxyString is a string before using string methods
          if (typeof proxyString !== 'string') {
            throw new Error('Invalid proxy format: proxy.proxy must be a string, got ' + typeof proxyString);
          }
          
          // Check if proxy string is in correct format (host:port)
          if (!proxyString.includes(':')) {
            throw new Error('Invalid proxy format: ' + proxyString);
          }
          
          const [hostname, portStr] = proxyString.split(':');
          const port = parseInt(portStr) || 80;
          
          launchOptions.proxy = {
            server: `http://${hostname}:${port}`,
            username: proxy.username,
            password: proxy.password,
          };
          
          console.log(`Configuring browser with proxy: ${hostname}:${port}`);
        } catch (error) {
          console.warn('Invalid proxy configuration, launching without proxy:', error);
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
      permissions: [
        "geolocation",
        "notifications",
        "microphone",
        "camera",
      ],
      deviceScaleFactor: 2 + Math.random() * 0.5,
      hasTouch: true,
      isMobile: true,
      javaScriptEnabled: true,
      acceptDownloads: true,
      ignoreHTTPSErrors: true,
      bypassCSP: true,
      extraHTTPHeaders: {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Language": `${location.locale},en;q=0.9`,
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "DNT": Math.random() > 0.5 ? "1" : "0",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache"
      },
      viewport: {
        width: [375, 390, 414][Math.floor(Math.random() * 3)],
        height: [667, 736, 812, 844][Math.floor(Math.random() * 4)]
      }
    });
    
    // Create a new page and simulate human behavior
    const page = await context.newPage();
    await page.waitForTimeout(1000 + Math.random() * 2000);
    await simulateMobileInteractions(page);
    
    return { context, fingerprint: enhancedFingerprint(), page };
  } catch (error) {
    console.error("Error initializing browser:", error.message);
    
    // Cleanup on error
    if (context) await context.close().catch(() => {});
    
    throw error;
  }
}

async function captureCookies(page, fingerprint) {
  let retryCount = 0;
  const MAX_RETRIES = 5;
  
  while (retryCount < MAX_RETRIES) {
    try {
      const challengePresent = await page.evaluate(() => {
        return document.body.textContent.includes(
          "Your Browsing Activity Has Been Paused"
        );
      }).catch(() => false);

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
      cookies = cookies.filter(cookie => !cookie.name.includes('_grecaptcha') && 
                                      !cookie.domain.includes('google.com'));

      // Check if we have enough cookies from ticketmaster.com
      const ticketmasterCookies = cookies.filter(cookie => 
        cookie.domain.includes('ticketmaster.com') || 
        cookie.domain.includes('.ticketmaster.com')
      );

      if (ticketmasterCookies.length < 3) {
        console.log(`Attempt ${retryCount + 1}: Not enough Ticketmaster cookies`);
        if (retryCount === MAX_RETRIES - 1) {
          return { cookies: null, fingerprint };
        }
        await page.waitForTimeout(CONFIG.RETRY_DELAY);
        retryCount++;
        continue;
      }

      // Check JSON size
      const cookiesJson = JSON.stringify(cookies, null, 2);
      const lineCount = cookiesJson.split('\n').length;
      
      if (lineCount < 200) {
        console.log(`Attempt ${retryCount + 1}: Cookie JSON too small (${lineCount} lines)`);
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

      fs.writeFileSync(COOKIES_FILE, JSON.stringify(cookies, null, 2));
      console.log(`Successfully captured cookies on attempt ${retryCount + 1}`);
      return { cookies, fingerprint };
    } catch (error) {
      console.error(`Error capturing cookies on attempt ${retryCount + 1}:`, error);
      if (retryCount === MAX_RETRIES - 1) {
        return { cookies: null, fingerprint };
      }
      await page.waitForTimeout(CONFIG.RETRY_DELAY);
      retryCount++;
    }
  }
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
  if (!cookieManager.capturedState.cookies) {
    const cookiesFromFile = await cookieManager.loadCookiesFromFile();
    if (cookiesFromFile) {
      const cookieAge = cookiesFromFile[0]?.expiry ? 
                       (cookiesFromFile[0].expiry * 1000 - currentTime) : 
                       CookieManager.CONFIG.MAX_COOKIE_AGE;
      
      if (cookieAge > 0 && cookieAge < CookieManager.CONFIG.MAX_COOKIE_AGE) {
        cookieManager.capturedState.cookies = cookiesFromFile;
        cookieManager.capturedState.lastRefresh = currentTime - (CookieManager.CONFIG.MAX_COOKIE_AGE - cookieAge);
        if (!cookieManager.capturedState.fingerprint) {
          cookieManager.capturedState.fingerprint = BrowserFingerprint.generate();
        }
        if (!cookieManager.capturedState.proxy) {
          cookieManager.capturedState.proxy = proxy;
        }
      }
    }
  }

  // Check if we need to refresh cookies
  const needsRefresh =
    !cookieManager.capturedState.cookies ||
    !cookieManager.capturedState.fingerprint ||
    !cookieManager.capturedState.lastRefresh ||
    !cookieManager.capturedState.proxy ||
    currentTime - cookieManager.capturedState.lastRefresh > CookieManager.CONFIG.COOKIE_REFRESH_INTERVAL ||
    forceRefresh;

  if (needsRefresh) {
    const jitter = Math.random() * 600000 - 300000; // ±5 minutes
    const effectiveInterval = CookieManager.CONFIG.COOKIE_REFRESH_INTERVAL + jitter;
    
    const needsRefreshWithJitter = 
      !cookieManager.capturedState.cookies ||
      !cookieManager.capturedState.fingerprint ||
      !cookieManager.capturedState.lastRefresh ||
      !cookieManager.capturedState.proxy ||
      currentTime - cookieManager.capturedState.lastRefresh > effectiveInterval ||
      forceRefresh;
    
    if (needsRefreshWithJitter) {
      console.log(`Refreshing cookies with jitter of ${Math.round(jitter/60000)}min for event ${eventId}`);
      return await refreshHeaders(eventId, proxy);
    }
  }

  return cookieManager.capturedState;
}

async function refreshHeaders(eventId, proxy, existingCookies = null) {
  // If cookies are already being refreshed, add this request to the queue with a timeout
  if (cookieManager.isRefreshingCookies) {
    console.log(
      `Cookies are already being refreshed, queueing request for event ${eventId}`
    );
    return new Promise((resolve, reject) => {
      // Add a timeout to prevent requests from getting stuck in the queue forever
      const timeoutId = setTimeout(() => {
        // Find and remove this request from the queue
        const index = cookieManager.cookieRefreshQueue.findIndex(item => item.timeoutId === timeoutId);
        if (index !== -1) {
          cookieManager.cookieRefreshQueue.splice(index, 1);
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
      
      cookieManager.cookieRefreshQueue.push({ resolve, reject, eventId, timeoutId });
    });
  }

  let localContext = null;
  let page = null;

  try {
    cookieManager.isRefreshingCookies = true;
    
    // Set up a cleanup function to ensure we always reset the flag and process the queue
    const cleanupRefreshProcess = async (error = null) => {
      cookieManager.isRefreshingCookies = false;
      
      // Process any queued refresh requests
      while (cookieManager.cookieRefreshQueue.length > 0) {
        const { resolve, reject, timeoutId } = cookieManager.cookieRefreshQueue.shift();
        clearTimeout(timeoutId); // Clear the timeout for this request
        
        if (error) {
          reject(error);
        } else if (cookieManager.capturedState.cookies || cookieManager.capturedState.headers) {
          resolve(cookieManager.capturedState);
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
      
      // Don't close context or page to keep the browser session alive
    };
    
    // Add a global timeout for the entire refresh process
    const globalTimeoutId = setTimeout(() => {
      console.error(`Global timeout reached when refreshing cookies for event ${eventId}`);
      cleanupRefreshProcess(new Error("Global timeout for cookie refresh"));
    }, 60000); // 60 second timeout for the entire process

    // Check if we have valid cookies in memory first
    if (
      cookieManager.capturedState.cookies?.length &&
      cookieManager.capturedState.lastRefresh &&
      Date.now() - cookieManager.capturedState.lastRefresh <= COOKIE_MANAGEMENT.COOKIE_REFRESH_INTERVAL
    ) {
      console.log("Using existing cookies from memory");
      clearTimeout(globalTimeoutId);
      await cleanupRefreshProcess();
      return cookieManager.capturedState;
    }

    // If specific cookies are provided, use them
    if (existingCookies !== null) {
      console.log(`Using provided cookies for event ${eventId}`);

      if (!cookieManager.capturedState.fingerprint) {
        cookieManager.capturedState.fingerprint = BrowserFingerprint.generate();
      }

      cookieManager.capturedState = {
        cookies: existingCookies,
        fingerprint: cookieManager.capturedState.fingerprint,
        lastRefresh: Date.now(),
        proxy: cookieManager.capturedState.proxy || proxy,
      };
      
      clearTimeout(globalTimeoutId);
      await cleanupRefreshProcess();
      return cookieManager.capturedState;
    }

    // Try to load cookies from file with improved validation
    try {
      const cookiesFromFile = await loadCookiesFromFile();
      if (cookiesFromFile && cookiesFromFile.length >= 3) { // Ensure we have enough cookies
        console.log("Using cookies from file");
        if (!cookieManager.capturedState.fingerprint) {
          cookieManager.capturedState.fingerprint = BrowserFingerprint.generate();
        }

        cookieManager.capturedState = {
          cookies: cookiesFromFile,
          fingerprint: cookieManager.capturedState.fingerprint || BrowserFingerprint.generate(),
          lastRefresh: Date.now(),
          proxy: cookieManager.capturedState.proxy || proxy,
        };
        
        clearTimeout(globalTimeoutId);
        await cleanupRefreshProcess();
        return cookieManager.capturedState;
      }
    } catch (err) {
      console.error("Error loading cookies from file:", err);
    }

    console.log(
      `No valid cookies found, getting new cookies using event ${eventId}`
    );

    // Generate fallback headers if we can't get proper ones
    const fallbackHeaders = generateFallbackHeaders();
    
    // Check if we have a persisted page and context first
    if (cookieManager.persistedPage && cookieManager.persistedContext) {
      console.log('Using existing browser page for refresh');
      page = cookieManager.persistedPage;
      localContext = cookieManager.persistedContext;
    } else {
      // Initialize browser with improved error handling
      let initAttempts = 0;
      let initSuccess = false;
      let initError = null;
      
      while (initAttempts < 3 && !initSuccess) {
        try {
          const { context: newContext, fingerprint, page: newPage } = await initBrowser(proxy, true); // Pass true for cookie capture
          if (!newContext || !fingerprint) {
            throw new Error("Failed to initialize browser or generate fingerprint");
          }
          
          localContext = newContext;
          page = newPage;
          
          // Store the page and context for future use
          cookieManager.persistedPage = page;
          cookieManager.persistedContext = localContext;
          
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
        cookieManager.capturedState = {
          cookies: null,
          fingerprint: BrowserFingerprint.generate(),
          lastRefresh: Date.now(),
          headers: fallbackHeaders,
          proxy: cookieManager.capturedState.proxy || proxy,
        };
        
        clearTimeout(globalTimeoutId);
        cleanupRefreshProcess();
        return cookieManager.capturedState;
      }
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

      // Only capture cookies if the page URL still contains the event ID (i.e., real event data)
      const currentUrl = page.url();
      if (!currentUrl.includes(`/event/${eventId}`)) {
        console.warn(`Page did not load event data for ${eventId}, URL: ${currentUrl}`);
        // Abort and return fallback state without cookies
        cookieManager.capturedState = {
          cookies: null,
          fingerprint: cookieManager.capturedState.fingerprint || BrowserFingerprint.generate(),
          lastRefresh: Date.now(),
          headers: fallbackHeaders,
          proxy: proxy
        };
        clearTimeout(globalTimeoutId);
        cleanupRefreshProcess();
        return cookieManager.capturedState;
      }

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
        cookieManager.capturedState.fingerprint || BrowserFingerprint.generate()
      );

      if (!cookies || cookies.length === 0) {
        console.error("Failed to capture cookies for event", eventId);
        
        // Use fallback headers if we couldn't get cookies
        cookieManager.capturedState = {
          cookies: null,
          fingerprint: newFingerprint,
          lastRefresh: Date.now(),
          headers: fallbackHeaders,
          proxy: cookieManager.capturedState.proxy || proxy,
        };
        
        // Process queued refresh requests with fallback data
        while (cookieManager.cookieRefreshQueue.length > 0) {
          const { resolve } = cookieManager.cookieRefreshQueue.shift();
          resolve(cookieManager.capturedState);
        }
        
        cookieManager.isRefreshingCookies = false;
        return cookieManager.capturedState;
      }

      // Update captured state with new cookies and fingerprint
      cookieManager.capturedState = {
        cookies,
        fingerprint: newFingerprint,
        lastRefresh: Date.now(),
        proxy: cookieManager.capturedState.proxy || proxy,
      };

      // Save cookies to file in the background
      cookieManager.saveCookiesToFile(cookies).catch((error) => {
        console.error("Error saving cookies to file:", error);
      });
      
      // Process queued refresh requests
      while (cookieManager.cookieRefreshQueue.length > 0) {
        const { resolve } = cookieManager.cookieRefreshQueue.shift();
        resolve(cookieManager.capturedState);
      }

      return cookieManager.capturedState;
    } catch (error) {
      console.error("Error refreshing headers:", error);
      
      // Use fallback headers on error
      cookieManager.capturedState = {
        cookies: null,
        fingerprint: BrowserFingerprint.generate(),
        lastRefresh: Date.now(),
        headers: fallbackHeaders,
        proxy: cookieManager.capturedState.proxy || proxy,
      };
      
      // Process queued refresh requests with fallback data
      while (cookieManager.cookieRefreshQueue.length > 0) {
        const { resolve } = cookieManager.cookieRefreshQueue.shift();
        resolve(cookieManager.capturedState);
      }
      
      throw error;
    } finally {
      // Don't close page or context - keep them for reuse
      clearTimeout(globalTimeoutId);
      cleanupRefreshProcess();
    }
  } catch (error) {
    console.error("Error in refreshHeaders:", error);
    
    // Reset the flag to allow new requests
    cookieManager.isRefreshingCookies = false;
    
    // In case of error, reject all queued requests
    while (cookieManager.cookieRefreshQueue.length > 0) {
      const { reject, timeoutId } = cookieManager.cookieRefreshQueue.shift();
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

// New throttled request function to limit API calls (max 5 requests per 10 seconds)
const throttle = pThrottle({
  limit: 5,
  interval: 10000
});

const throttledRequest = throttle(async (options) => {
  // Add random delay to make requests look more human
  const humanDelay = Math.floor(Math.random() * 1000) + 500;
  await delay(humanDelay);
  return got(options);
});

// Replace the GetData function with this improved version
const GetData = async (headers, proxyAgent, url, eventId) => {
  let abortController = new AbortController();

  try {
    const timeout = setTimeout(() => {
      abortController.abort();
      console.log("Request aborted due to timeout");
      console.log(eventId, "eventId");
    }, CONFIG.PAGE_TIMEOUT);

    try {
      // Add subtle variations to headers to look more human
      const modifiedHeaders = { ...headers };
      
      // Occasionally modify accept-language with slight variations
      if (Math.random() > 0.7 && modifiedHeaders['Accept-Language']) {
        const languages = ['en-US', 'en-GB', 'en-CA', 'en'];
        const weights = [0.8, 0.9, 0.7, 0.6];
        const baseLanguage = languages[Math.floor(Math.random() * languages.length)];
        const weight = weights[Math.floor(Math.random() * weights.length)];
        modifiedHeaders['Accept-Language'] = `${baseLanguage},en;q=${weight}`;
      }
      
      // Use an unpredictable order for cache directives
      if (Math.random() > 0.5) {
        modifiedHeaders['Cache-Control'] = 'no-cache, no-store, must-revalidate';
      } else {
        modifiedHeaders['Cache-Control'] = 'no-store, no-cache, must-revalidate';
      }
      
      // Randomly adjust connection setting
      modifiedHeaders['Connection'] = Math.random() > 0.3 ? 'keep-alive' : 'close';
      
      const response = await pRetry(() => throttledRequest({
        url,
        agent: {
          https: proxyAgent
        },
        headers: modifiedHeaders,
        timeout: {
          request: CONFIG.PAGE_TIMEOUT
        },
        retry: {
          limit: 2,
          methods: ['GET'],
          statusCodes: [408, 413, 429, 500, 502, 503, 504],
          errorCodes: ['ETIMEDOUT', 'ECONNRESET', 'EADDRINUSE', 'ECONNREFUSED', 'EPIPE', 'ENOTFOUND', 'ENETUNREACH', 'EAI_AGAIN']
        },
        throwHttpErrors: false,
        signal: abortController.signal
      }), {
        retries: 3,
        onFailedAttempt: error => {
          console.log(`Request attempt ${error.attemptNumber} failed for ${eventId}. ${error.retriesLeft} retries left.`);
          // Longer delays between retries
          return delay(error.attemptNumber * 2000 + Math.random() * 1000);
        }
      });

      clearTimeout(timeout);
      
      if (response.statusCode !== 200) {
        console.log(`Request failed with status code ${response.statusCode} for ${eventId}`);
        return false;
      }
      
      return JSON.parse(response.body);
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

const GetProxy = async () => {
  try {
    // Use ProxyManager global instance if available
    if (global.proxyManager) {
      // Try multiple times to get a healthy proxy
      let attempts = 0;
      const maxAttempts = 3;
      let lastError = null;

      // Add diagnostic information about proxy health
      const healthyCount = global.proxyManager.getAvailableProxyCount();
      const totalProxies = global.proxyManager.proxies.length;
      console.log(`Proxy health status: ${healthyCount}/${totalProxies} healthy proxies available`);
      
      // If no healthy proxies but we have total proxies, try to reset some
      if (healthyCount === 0 && totalProxies > 0) {
        console.log('No healthy proxies available, attempting to reset a few for emergency use');
        // Try to reset health of a few proxies for emergency use without changing core logic
        if (global.proxyManager.healthManager) {
          global.proxyManager.healthManager.resetHealthMetrics(true); // Force reset some proxies
          console.log(`After emergency reset: ${global.proxyManager.getAvailableProxyCount()}/${totalProxies} healthy proxies`);
        }
      }

      while (attempts < maxAttempts) {
        try {
          const proxyData = global.proxyManager.getProxyForEvent('random');
          if (proxyData) {
            // Test the proxy with a simple connection before returning
            try {
              const proxyUrl = new URL(`http://${proxyData.proxy}`);
              const testUrl = `http://${proxyData.username}:${proxyData.password}@${proxyUrl.hostname}:${proxyUrl.port || 80}`;
              const testAgent = new HttpsProxyAgent(testUrl);
              
              // Quick test connection with short timeout
              await new Promise((resolve, reject) => {
                const testReq = require('https').get({
                  host: 'www.ticketmaster.com',
                  port: 443,
                  agent: testAgent,
                  timeout: 5000 // Short 5s timeout for test
                }, resolve);
                testReq.on('error', reject);
                testReq.on('timeout', () => reject(new Error('Proxy test timeout')));
              });
              
              // If we get here, the proxy is responsive
              const proxyAgent = new HttpsProxyAgent(testUrl);
              return { proxyAgent, proxy: proxyData };
            } catch (testError) {
              console.warn(`Proxy test failed for ${proxyData.proxy}: ${testError.message}`);
              global.proxyManager.recordProxyFailure(proxyData.proxy, testError);
              attempts++;
              lastError = testError;
              continue;
            }
          }
        } catch (error) {
          console.warn(`Failed to get proxy (attempt ${attempts + 1}): ${error.message}`);
          attempts++;
          lastError = error;
          await new Promise(resolve => setTimeout(resolve, 1000 * attempts));
          continue;
        }
      }

      console.error(`Failed to get healthy proxy after ${maxAttempts} attempts:`, lastError?.message);
    }
    
    // Fallback to old method with similar health checks
    let _proxy = [...proxyArray?.proxies];
    let attempts = 0;
    const maxAttempts = 3;

    while (attempts < maxAttempts) {
      try {
        const randomProxy = Math.floor(Math.random() * _proxy.length);
        const selectedProxy = _proxy[randomProxy];

        if (!selectedProxy?.proxy || !selectedProxy?.username || !selectedProxy?.password) {
          throw new Error("Invalid proxy configuration");
        }

        const proxyUrl = new URL(`http://${selectedProxy.proxy}`);
        const proxyURl = `http://${selectedProxy.username}:${selectedProxy.password}@${
          proxyUrl.hostname
        }:${proxyUrl.port || 80}`;
        
        // Test the proxy before returning
        try {
          const testAgent = new HttpsProxyAgent(proxyURl);
          await new Promise((resolve, reject) => {
            const testReq = require('https').get({
              host: 'www.ticketmaster.com',
              port: 443,
              agent: testAgent,
              timeout: 5000
            }, resolve);
            testReq.on('error', reject);
            testReq.on('timeout', () => reject(new Error('Proxy test timeout')));
          });
          
          // If we get here, proxy is good
          const proxyAgent = new HttpsProxyAgent(proxyURl);
          return { proxyAgent, proxy: selectedProxy };
        } catch (testError) {
          console.warn(`Proxy test failed for ${selectedProxy.proxy}: ${testError.message}`);
          // Remove failed proxy from the list for this session
          _proxy = _proxy.filter(p => p.proxy !== selectedProxy.proxy);
          attempts++;
          continue;
        }
      } catch (error) {
        console.warn(`Failed to get proxy (attempt ${attempts + 1}): ${error.message}`);
        attempts++;
        await new Promise(resolve => setTimeout(resolve, 1000 * attempts));
        continue;
      }
    }

    // Last resort fallback
    console.warn("Using fallback proxy after all attempts failed");
    const fallbackProxy = proxyArray.proxies[0];
    const proxyUrl = new URL(`http://${fallbackProxy.proxy}`);
    const proxyURl = `http://${fallbackProxy.username}:${fallbackProxy.password}@${
      proxyUrl.hostname
    }:${proxyUrl.port || 80}`;
    const proxyAgent = new HttpsProxyAgent(proxyURl);
    return { proxyAgent, proxy: fallbackProxy };
  } catch (error) {
    console.error("Critical error in GetProxy:", error);
    throw new Error(`Failed to get any working proxy: ${error.message}`);
  }
};

const ScrapeEvent = async (
  event,
  externalProxyAgent = null,
  externalProxy = null
) => {
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

    console.log(
      `Starting event ${eventId} processing with correlation ID: ${correlationId}`
    );

    // Initialize rate limiting tracking
    if (!ScrapeEvent.rateLimits) {
      ScrapeEvent.rateLimits = {
        hourlyCount: 0,
        lastHour: new Date().getHours(),
        maxPerHour: 1000,
        blockedUntil: 0,
      };
    }

    // Check rate limits to avoid overwhelming external services
    const currentHour = new Date().getHours();
    if (currentHour !== ScrapeEvent.rateLimits.lastHour) {
      ScrapeEvent.rateLimits.hourlyCount = 0;
      ScrapeEvent.rateLimits.lastHour = currentHour;
    }

    if (
      ScrapeEvent.rateLimits.hourlyCount >= ScrapeEvent.rateLimits.maxPerHour
    ) {
      console.warn(
        `Hourly rate limit reached (${ScrapeEvent.rateLimits.hourlyCount}/${ScrapeEvent.rateLimits.maxPerHour})`
      );
      throw new Error("Rate limit exceeded");
    }

    // Check global block (if we've detected we're being blocked by the server)
    if (ScrapeEvent.rateLimits.blockedUntil > Date.now()) {
      const waitTime = Math.ceil(
        (ScrapeEvent.rateLimits.blockedUntil - Date.now()) / 1000
      );
      console.warn(`Global block in effect for ${waitTime} more seconds`);
      throw new Error(
        `Service temporarily unavailable for ${waitTime} seconds`
      );
    }

    // Use provided proxy if available, otherwise get a new one
    try {
      if (proxyAgent && proxy) {
        console.log(
          `Using provided proxy ${
            proxy.proxy || "unknown"
          } for event ${eventId}`
        );
      } else {
        // If we have a ProxyManager instance, use it to get an event-specific proxy
        if (global.proxyManager) {
          const proxyData = global.proxyManager.getProxyForEvent(eventId);
          if (proxyData) {
            const proxyAgentData =
              global.proxyManager.createProxyAgent(proxyData);
            proxyAgent = proxyAgentData.proxyAgent;
            proxy = proxyData;
            console.log(
              `Using dedicated proxy ${proxy.proxy} for event ${eventId} from ProxyManager`
            );
          } else {
            // Fallback to random proxy selection
            const proxyData = await GetProxy();
            proxyAgent = proxyData.proxyAgent;
            proxy = proxyData.proxy;
          }
        } else {
          // Fallback to old method if no proxy manager is available
          const proxyData = await GetProxy();
          proxyAgent = proxyData.proxyAgent;
          proxy = proxyData.proxy;
        }
      }
    } catch (proxyError) {
      console.error(`Proxy error for event ${eventId}:`, proxyError.message);
      // Try again with a different proxy selection mechanism
      console.log(
        `Attempting alternative proxy selection for event ${eventId}`
      );
      await new Promise((resolve) => setTimeout(resolve, 1000));
      const proxyData = await GetProxy();
      proxyAgent = proxyData.proxyAgent;
      proxy = proxyData.proxy;
    }

    // Log memory usage as needed
    const memUsage = process.memoryUsage();
    if (memUsage.heapUsed > 1024 * 1024 * 1024) {
      // Over 1GB
      console.warn(
        `High memory usage (${Math.round(
          memUsage.heapUsed / 1024 / 1024
        )}MB) during event ${eventId} processing`
      );
    }

    // If headers are provided (for batch processing), use them directly
    let cookieString, userAgent, fingerprint;
    let useProvidedHeaders = false;

    if (event?.headers) {
      console.log(`Processing headers for event ${eventId} (batch processing)`);

      // Check different header formats for compatibility
      if (typeof event.headers === "object") {
        // If it's a complete headers object with all required fields
        if (event.headers.headers) {
          // Extract from nested headers property if available
          cookieString =
            event.headers.headers.Cookie || event.headers.headers.cookie;
          userAgent =
            event.headers.headers["User-Agent"] ||
            event.headers.headers["user-agent"];
        } else {
          // Try direct properties
          cookieString = event.headers.Cookie || event.headers.cookie;
          userAgent =
            event.headers["User-Agent"] || event.headers["user-agent"];
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
        console.log(
          `Reusing existing headers for batch processing of event ${eventId}`
        );
        useProvidedHeaders = true;
        // Initialize fingerprint with at least basic properties
        fingerprint =
          event.headers.fingerprint || generateEnhancedFingerprint();
      } else {
        console.log(
          `Incomplete headers for event ${eventId}, falling back to standard flow`
        );
      }
    }

    if (!useProvidedHeaders) {
      // Standard flow - get cookies and headers
      try {
        const capturedData = await getCapturedData(eventId, proxy);

        if (!capturedData) {
          throw new Error("Failed to get captured data");
        }

        // Use headers from capturedData if available (fallback mechanism)
        if (capturedData.headers && !capturedData.cookies) {
          console.log(`Using fallback headers for event ${eventId}`);
          return callTicketmasterAPI(
            capturedData.headers,
            proxyAgent,
            eventId,
            event,
            0,
            startTime
          );
        }

        if (!capturedData?.cookies?.length) {
          throw new Error("Failed to capture cookies");
        }

        cookieString = capturedData.cookies
          .map((cookie) => `${cookie.name}=${cookie.value}`)
          .join("; ");

        // Generate enhanced fingerprint instead of the basic one
        fingerprint = generateEnhancedFingerprint();
        userAgent =
          fingerprint.browser?.userAgent ||
          randomUseragent.getRandom(
            (ua) => ua.browserName === fingerprint.browser?.name
          ) ||
          BrowserFingerprint.generateUserAgent(fingerprint);
      } catch (error) {
        console.error(
          `Error getting cookies for event ${eventId}:`,
          error.message
        );
        // Use fallback headers if available
        const fallbackHeaders = generateFallbackHeaders();
        if (fallbackHeaders) {
          console.log(
            `Using fallback headers for event ${eventId} after error`
          );
          return callTicketmasterAPI(
            fallbackHeaders,
            proxyAgent,
            eventId,
            event,
            0,
            startTime
          );
        }
        throw error;
      }
    }

    // Generate enhanced headers using our improved function
    const enhancedHeaders = generateEnhancedHeaders(fingerprint, cookieString);

    // Create safe header objects that match the expected format
    const MapHeader = {
      ...enhancedHeaders,
      "X-Request-ID": generateCorrelationId() + `-${Date.now()}`,
      "X-Correlation-ID": correlationId,
    };

    const FacetHeader = {
      ...enhancedHeaders,
      "tmps-correlation-id": correlationId,
      "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe",
      "X-Request-ID": generateCorrelationId() + `-${Date.now()}`,
      "X-Correlation-ID": correlationId,
    };

    console.log(
      `Starting event scraping for ${eventId} with${
        event?.headers ? " shared" : ""
      } cookies and enhanced headers...`
    );

    // Measure API call time for performance monitoring
    const apiStartTime = Date.now();
    const result = await callTicketmasterAPI(
      FacetHeader,
      proxyAgent,
      eventId,
      event,
      MapHeader,
      0,
      startTime
    );
    const apiDuration = Date.now() - apiStartTime;

    console.log(
      `Event ${eventId} processing completed in ${
        Date.now() - startTime
      }ms (API: ${apiDuration}ms)`
    );

    // If API call was too fast, it might be suspicious (rate limited or blocked)
    if (apiDuration < 100 && !result) {
      console.warn(
        `Suspiciously fast API failure for event ${eventId} (${apiDuration}ms). Possible rate limiting detected.`
      );
      // Implement a temporary rate limiting backoff (1 minute)
      ScrapeEvent.rateLimits.blockedUntil = Date.now() + 60 * 1000;
    }

    return result;
  } catch (error) {
    console.error(
      `Scraping error for event ${event?.eventId || event}:`,
      error.message
    );

    // Implement automatic retry mechanism for recoverable errors
    const recoverable =
      error.message &&
      (error.message.includes("proxy") ||
        error.message.includes("timeout") ||
        error.message.includes("network") ||
        error.message.includes("ECONNRESET") ||
        error.message.includes("ETIMEDOUT") ||
        error.message.includes("circular") ||
        error.message.includes("JSON") ||
        error.message.includes("403")); // Also retry 403 errors with a new proxy

    if (recoverable) {
      // Track failed attempts to prevent infinite retries
      if (!ScrapeEvent.failedAttempts) {
        ScrapeEvent.failedAttempts = new Map();
      }

      const eventId = event?.eventId || event;
      const attempts = (ScrapeEvent.failedAttempts.get(eventId) || 0) + 1;
      ScrapeEvent.failedAttempts.set(eventId, attempts);

      // Only retry if we haven't exceeded max attempts
      if (attempts <= 3) {
        // Max 4 total attempts (1 original + 3 retries)
        console.log(
          `Retrying event ${eventId} after recoverable error (attempt ${attempts})`
        );
        // Add exponential backoff
        const backoff = Math.pow(2, attempts) * 2000 + Math.random() * 1000;
        await new Promise((resolve) => setTimeout(resolve, backoff));

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
            const proxyAgentData =
              global.proxyManager.createProxyAgent(proxyData);
            newProxyAgent = proxyAgentData.proxyAgent;
            newProxy = proxyData;
          } else {
            // Fallback
            const proxyData = await GetProxy();
            newProxyAgent = proxyData.proxyAgent;
            newProxy = proxyData.proxy;
          }
        } else {
          // Fallback to old method
          const proxyData = await GetProxy();
          newProxyAgent = proxyData.proxyAgent;
          newProxy = proxyData.proxy;
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
async function callTicketmasterAPI(facetHeader, proxyAgent, eventId, event, mapHeader = null, retryCount = 0, startTime) {
  // Add a fallback for startTime if not provided
  startTime = startTime || Date.now();
  
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
        const { proxyAgent: altAgent, proxy: altProxy } = await GetProxy();
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
    await delay(dynamicDelay);
  }
  
  // Enhanced request with better error handling
  const makeRequestWithRetry = async (url, headers, agent, attemptNum = 0, isRetryWithNewProxy = false) => {
    const jitter = 1 + (Math.random() * jitterFactor * 2 - jitterFactor);
    const delayMs = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attemptNum)) * jitter;
    
    if (attemptNum > 0) {
      await delay(delayMs);
      console.log(`Retry attempt ${attemptNum} for event ${eventId} after ${delayMs.toFixed(0)}ms`);
    }
    
    try {
      // Add random delay before request to mimic human behavior
      await delay(100 + Math.random() * 400);
      
      // Construct a safe copy of headers without any possible circular references
      const safeHeaders = {};
      for (const [key, value] of Object.entries(headers)) {
        if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
          safeHeaders[key] = value;
        }
      }
      
      // Randomly modify browser fingerprint on retries to avoid detection
      if (attemptNum > 0) {
        // Generate a new random user agent
        const randomUA = randomUseragent.getRandom(function(ua) {
          return ua.browserName === 'Chrome' || ua.browserName === 'Firefox';
        });
        
        if (randomUA) {
          safeHeaders['User-Agent'] = randomUA;
        }
        
        // Add realistic browser-specific headers
        const browserSpecificHeaders = Math.random() > 0.5 ? {
          'sec-ch-ua': '"Chromium";v="116", "Google Chrome";v="116", "Not=A?Brand";v="99"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"Windows"'
        } : {
          'sec-ch-ua': '"Firefox";v="115", "Gecko";v="115"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"Windows"'
        };
        
        Object.assign(safeHeaders, browserSpecificHeaders);
        
        // Add small variations in Accept headers to make it look more natural
        const acceptVariations = [
          '*/*',
          'application/json, text/plain, */*',
          'application/json, text/javascript, */*; q=0.01'
        ];
        safeHeaders['Accept'] = acceptVariations[Math.floor(Math.random() * acceptVariations.length)];
        
        // Vary the order of common headers to avoid fingerprinting
        const headerOrder = {};
        const commonHeaderKeys = ['Accept', 'Accept-Language', 'Accept-Encoding', 'User-Agent', 'Referer', 'Origin', 'X-Api-Key'];
        const shuffledKeys = commonHeaderKeys.sort(() => Math.random() - 0.5);
        
        shuffledKeys.forEach(key => {
          if (safeHeaders[key]) {
            headerOrder[key] = safeHeaders[key];
            delete safeHeaders[key];
          }
        });
        
        // Re-add the headers in a random order
        Object.assign(safeHeaders, headerOrder);
      }
      
      // Use throttled request to make calls look more natural
      return await throttledRequest({
        url,
        agent: {
          https: agent
        },
        headers: safeHeaders,
        timeout: {
          request: 30000
        },
        responseType: 'json',
        retry: {
          limit: 1, // We handle retries ourselves
          methods: ['GET'],
          statusCodes: [408, 413, 429, 500, 502, 503, 504],
          errorCodes: ['ETIMEDOUT', 'ECONNRESET', 'EADDRINUSE', 'ECONNREFUSED', 'EPIPE', 'ENOTFOUND']
        }
      });
    } catch (error) {
      const is403Error = error.response?.statusCode === 403;
      const is429Error = error.response?.statusCode === 429;
      const is500Error = error.response?.statusCode >= 500 && error.response?.statusCode < 600;
      
      // Track proxy errors
      if (agent && (is403Error || is429Error)) {
        // Create a non-circular key for the proxy without JSON.stringify
        const proxyKey = agent.proxy?.host || 'unknown-proxy';
        proxyErrors.set(proxyKey, (proxyErrors.get(proxyKey) || 0) + 1);
      }
      
      // If we've hit a rate limit, wait longer before retrying
      if (is429Error) {
        const retryAfter = error.response?.headers?.['retry-after'] || 30; 
        const waitTime = parseInt(retryAfter, 10) * 1000 || 30000;
        console.log(`Rate limited (429) for event ${eventId}, waiting ${waitTime/1000}s before retry`);
        await delay(waitTime);
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
              const proxyData = await GetProxy();
              newAgent = proxyData.proxyAgent;
            }
          } else {
            const proxyData = await GetProxy();
            newAgent = proxyData.proxyAgent;
          }
          
          // Wait before retry with increasing backoff
          await delay(2000 + Math.random() * 2000 + attemptNum * 1000);
          
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
    
    // Add unique query parameters to bust cache and appear more like a browser
    const cacheBuster = Date.now();
    const randomId = Math.floor(Math.random() * 1000000);
    
    // Append cache busting differently for each URL
    const mapUrlWithParams = `${mapUrl}${mapUrl.includes('?') ? '&' : '?'}_=${cacheBuster}`;
    const facetUrlWithParams = `${facetUrl}${facetUrl.includes('?') ? '&' : '?'}_=${cacheBuster+1}&t=${randomId}`;
    
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
        const mapResponse = await makeRequestWithRetry(mapUrlWithParams, safeMapHeader || safeFacetHeader, proxyAgent);
        DataMap = mapResponse.body || mapResponse;
      } catch (mapError) {
        console.error(`Map API error for event ${eventId}:`, mapError.message);
        DataMap = null;
      }
      
      // Add variable delay between requests to appear more human-like
      await delay(500 + Math.random() * 2000);
      
      try {
        const facetResponse = await makeRequestWithRetry(facetUrlWithParams, safeFacetHeader, proxyAgent);
        DataFacets = facetResponse.body || facetResponse;
      } catch (facetError) {
        console.error(`Facet API error for event ${eventId}:`, facetError.message);
        DataFacets = null;
      }
    } else {
      try {
        const facetResponse = await makeRequestWithRetry(facetUrlWithParams, safeFacetHeader, proxyAgent);
        DataFacets = facetResponse.body || facetResponse;
      } catch (facetError) {
        console.error(`Facet API error for event ${eventId}:`, facetError.message);
        DataFacets = null;
      }
      
      await delay(500 + Math.random() * 2000);
      
      try {
        const mapResponse = await makeRequestWithRetry(mapUrlWithParams, safeMapHeader || safeFacetHeader, proxyAgent);
        DataMap = mapResponse.body || mapResponse;
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
  
  // Device types for realistic fingerprinting
  const devices = [
    // Windows devices
    {
      platform: 'Win32',
      os: 'Windows',
      osVersion: ['10.0', '11.0'],
      browserNames: ['Chrome', 'Firefox', 'Edge'],
      screenResolutions: [
        {width: 1920, height: 1080},
        {width: 1366, height: 768},
        {width: 2560, height: 1440},
        {width: 1280, height: 720}
      ],
      languages: ['en-US', 'en-GB', 'en'],
      timezones: ['America/New_York', 'America/Chicago', 'America/Los_Angeles', 'Europe/London'],
      colorDepths: [24, 30],
      pixelRatios: [1, 1.25, 1.5, 2],
      vendors: ['Google Inc.', 'Microsoft Corporation', 'Intel Inc.'],
      renderers: [
        'ANGLE (Intel, Intel(R) UHD Graphics Direct3D11 vs_5_0 ps_5_0, D3D11)',
        'ANGLE (NVIDIA, NVIDIA GeForce GTX 1650 Direct3D11 vs_5_0 ps_5_0, D3D11)',
        'ANGLE (AMD, AMD Radeon RX 580 Direct3D11 vs_5_0 ps_5_0, D3D11)'
      ]
    },
    // Mac devices
    {
      platform: 'MacIntel',
      os: 'Mac OS X',
      osVersion: ['10.15.7', '11.6.8', '12.5.1', '13.4.1'],
      browserNames: ['Chrome', 'Firefox', 'Safari'],
      screenResolutions: [
        {width: 1440, height: 900},
        {width: 2560, height: 1600},
        {width: 2880, height: 1800},
        {width: 1280, height: 800}
      ],
      languages: ['en-US', 'en-GB', 'en-CA', 'en'],
      timezones: ['America/New_York', 'America/Chicago', 'America/Los_Angeles', 'Europe/London'],
      colorDepths: [24, 30],
      pixelRatios: [1, 2],
      vendors: ['Apple Inc.', 'Google Inc.', 'Intel Inc.'],
      renderers: [
        'Apple GPU',
        'Intel(R) Iris(TM) Plus Graphics',
        'AMD Radeon Pro 5500M OpenGL Engine'
      ]
    }
  ];
  
  // Get random values for consistency across the fingerprint
  const randomIndex = Math.floor(Math.random() * devices.length);
  const device = devices[randomIndex];
  
  // Choose a browser
  const browserIndex = Math.floor(Math.random() * device.browserNames.length);
  const browserName = device.browserNames[browserIndex];
  
  // Choose random values for other properties
  const screenRes = device.screenResolutions[Math.floor(Math.random() * device.screenResolutions.length)];
  const language = device.languages[Math.floor(Math.random() * device.languages.length)];
  const timezone = device.timezones[Math.floor(Math.random() * device.timezones.length)];
  const colorDepth = device.colorDepths[Math.floor(Math.random() * device.colorDepths.length)];
  const pixelRatio = device.pixelRatios[Math.floor(Math.random() * device.pixelRatios.length)];
  const vendor = device.vendors[Math.floor(Math.random() * device.vendors.length)];
  const renderer = device.renderers[Math.floor(Math.random() * device.renderers.length)];
  const osVersion = device.osVersion[Math.floor(Math.random() * device.osVersion.length)];
  
  // Random hardware values that make sense for the device
  const memoryOptions = [4, 8, 16, 32];
  const cpuOptions = [2, 4, 6, 8, 10, 12];
  const memory = memoryOptions[Math.floor(Math.random() * memoryOptions.length)];
  const cpuCount = cpuOptions[Math.floor(Math.random() * cpuOptions.length)];
  
  // Generate a realistic timestamp for browser cookies
  const browserCookieCreated = Date.now() - Math.floor(Math.random() * 30 * 24 * 60 * 60 * 1000); // 0-30 days ago
  
  // Random browser capabilities - not all browsers support all features
  const supportsSpeechSynthesis = browserName !== 'Safari' && Math.random() > 0.1;
  const supportsPaymentRequest = browserName === 'Chrome' || (browserName === 'Edge' && Math.random() > 0.3);
  const supportsWebGL2 = browserName !== 'Safari' || Math.random() > 0.4;
  
  // Enhanced fingerprint object
  return {
    ...baseFingerprint, // Keep base fingerprint as fallback
    // Browser identification
    browser: {
      name: browserName,
      version: getRandomBrowserVersion(browserName),
      userAgent: randomUseragent.getRandom(ua => ua.browserName === browserName) || baseFingerprint.userAgent,
      language: language,
      cookies: true,
      localStorage: true,
      sessionStorage: true,
      cookiesCreatedAt: browserCookieCreated,
    },
    // Operating system
    platform: {
      name: device.os,
      version: osVersion,
      type: 'desktop',
      architecture: Math.random() > 0.5 ? 'x86_64' : 'arm64', 
    },
    // Screen and display
    screen: {
      width: screenRes.width,
      height: screenRes.height,
      colorDepth: colorDepth,
      pixelRatio: pixelRatio,
      orientation: 'landscape'
    },
    // Connection info
    connection: {
      type: Math.random() > 0.3 ? 'wifi' : 'ethernet',
      downlink: 5 + Math.floor(Math.random() * 95), // 5-100 Mbps
      rtt: 5 + Math.floor(Math.random() * 95), // 5-100ms
    },
    // Hardware
    hardware: {
      deviceMemory: memory,
      hardwareConcurrency: cpuCount,
    },
    // Graphics
    graphics: {
      vendor: vendor,
      renderer: renderer,
      webGLVendor: vendor,
      webGLRenderer: renderer,
      supportsWebGL: true,
      supportsWebGL2: supportsWebGL2,
    },
    // Capabilities
    capabilities: {
      audio: true,
      video: true,
      webRTC: Math.random() > 0.2,
      geolocation: Math.random() > 0.4,
      speechSynthesis: supportsSpeechSynthesis,
      paymentRequest: supportsPaymentRequest,
    },
    language: language,
    timezone: timezone,
  };
};

// Helper function to generate realistic browser versions
function getRandomBrowserVersion(browserName) {
  const browserVersions = {
    'Chrome': ['111.0.5563.111', '112.0.5615.50', '113.0.5672.92', '114.0.5735.106', '115.0.5790.98', '116.0.5845.97'],
    'Firefox': ['111.0', '112.0.2', '113.0.1', '114.0.2', '115.0', '116.0'],
    'Edge': ['111.0.1661.51', '112.0.1722.48', '113.0.1774.42', '114.0.1823.51', '115.0.1901.183', '116.0.1938.62'],
    'Safari': ['16.3', '16.4', '16.5', '16.6', '17.0']
  };
  
  const versions = browserVersions[browserName] || browserVersions['Chrome'];
  return versions[Math.floor(Math.random() * versions.length)];
}

// Enhanced human behavior simulation
const simulateEnhancedHumanBehavior = async (page) => {
  try {
    // Random delays between actions
    const delayOptions = [100, 200, 300, 400, 500];
    const randomDelay = () => delayOptions[Math.floor(Math.random() * delayOptions.length)];
    
    // Random mouse movements with error handling
    try {
      const viewportSize = page.viewportSize();
      if (viewportSize) {
        const steps = 3 + Math.floor(Math.random() * 3);
        for (let i = 0; i < steps; i++) {
          try {
            await page.mouse.move(
              Math.floor(Math.random() * viewportSize.width),
              Math.floor(Math.random() * viewportSize.height),
              { steps: 4 + Math.floor(Math.random() * 3) }
            );
            await page.waitForTimeout(randomDelay());
          } catch (moveError) {
            console.warn("Mouse movement error, continuing:", moveError.message);
            continue;
          }
        }
      }
    } catch (error) {
      console.warn("Viewport error in human behavior, continuing:", error.message);
    }
    
    // Random scrolling with error handling
    try {
      const scrollAmount = Math.floor(Math.random() * 500) + 200;
      const scrollSteps = 3 + Math.floor(Math.random() * 2);
      const stepSize = scrollAmount / scrollSteps;
      
      for (let i = 0; i < scrollSteps; i++) {
        try {
          await page.mouse.wheel(0, stepSize);
          await page.waitForTimeout(randomDelay());
        } catch (scrollError) {
          console.warn("Scroll error, continuing:", scrollError.message);
          continue;
        }
      }
    } catch (error) {
      console.warn("Scrolling error in human behavior, continuing:", error.message);
    }
    
    // Random keyboard activity with error handling
    if (Math.random() > 0.6) {
      try {
        await page.keyboard.press('Tab');
        await page.waitForTimeout(randomDelay());
      } catch (keyboardError) {
        console.warn("Keyboard error, continuing:", keyboardError.message);
      }
    }
    
    // Random viewport resizing with error handling
    if (Math.random() > 0.7) {
      try {
        const viewportSize = page.viewportSize();
        if (viewportSize) {
          const newWidth = Math.max(800, viewportSize.width + Math.floor(Math.random() * 100) - 50);
          const newHeight = Math.max(600, viewportSize.height + Math.floor(Math.random() * 100) - 50);
          await page.setViewportSize({ width: newWidth, height: newHeight });
          await page.waitForTimeout(randomDelay());
        }
      } catch (resizeError) {
        console.warn("Viewport resize error, continuing:", resizeError.message);
      }
    }
  } catch (error) {
    console.warn('Error in human behavior simulation, continuing:', error.message);
  }
}

// Enhanced request headers
const generateEnhancedHeaders = (fingerprint, cookies) => {
  try {
    // Ensure we have valid inputs
    if (!fingerprint) {
      fingerprint = generateEnhancedFingerprint();
    }

    if (!cookies) {
      cookies = '';
    }

    // Get appropriate user agent - either from fingerprint or generate new one
    let userAgent;
    if (fingerprint.browser?.userAgent) {
      userAgent = fingerprint.browser.userAgent;
    } else if (fingerprint.browser?.name) {
      userAgent = randomUseragent.getRandom(ua => ua.browserName === fingerprint.browser.name);
    } else {
      userAgent = randomUseragent.getRandom(ua => ua.browserName === 'Chrome');
    }

    const browserName = fingerprint.browser?.name || 'Chrome';
    const browserVersion = fingerprint.browser?.version || '116.0.0.0';
    const platformName = fingerprint.platform?.name || 'Windows';
    
    // Generate appropriate sec-ch-ua headers based on browser
    let secChUa;
    if (browserName === 'Chrome') {
      secChUa = `"Not A(Brand";v="99", "Google Chrome";v="${browserVersion.split('.')[0]}", "Chromium";v="${browserVersion.split('.')[0]}"`;
    } else if (browserName === 'Firefox') {
      secChUa = `"Firefox";v="${browserVersion.split('.')[0]}"`;
    } else if (browserName === 'Edge') {
      secChUa = `"Microsoft Edge";v="${browserVersion.split('.')[0]}", "Chromium";v="${browserVersion.split('.')[0]}"`;
    } else if (browserName === 'Safari') {
      secChUa = `"Safari";v="${browserVersion.split('.')[0]}"`;
    } else {
      secChUa = `"Not_A Brand";v="8", "${browserName}";v="${browserVersion.split('.')[0]}"`;
    }
    
    // Generate platform string
    const secChUaPlatform = platformName === 'Windows' ? 'Windows' : 
                           platformName === 'Mac OS X' ? 'macOS' : 
                           platformName === 'iPhone' ? 'iOS' : 
                           platformName === 'Android' ? 'Android' : 'Windows';

    // Mobile detection
    const isMobile = fingerprint.platform?.type === 'mobile' || false;
    const secChUaMobile = isMobile ? '?1' : '?0';

    // Preferred language
    const language = fingerprint.language || 'en-US';
    
    // Choose varied accept language formats
    let acceptLanguage;
    if (language === 'en-US') {
      acceptLanguage = Math.random() > 0.5 ? 'en-US,en;q=0.9' : 'en-US,en;q=0.9,en-GB;q=0.8';
    } else if (language === 'en-GB') {
      acceptLanguage = Math.random() > 0.5 ? 'en-GB,en;q=0.9' : 'en-GB,en;q=0.9,en-US;q=0.8';
    } else {
      acceptLanguage = `${language},en;q=0.9`;
    }

    // Randomize accept encoding based on browser
    let acceptEncoding;
    if (browserName === 'Firefox') {
      acceptEncoding = 'gzip, deflate, br';
    } else if (browserName === 'Safari') {
      acceptEncoding = 'gzip, deflate';
    } else {
      acceptEncoding = 'gzip, deflate, br';
    }

    const headers = {
      'User-Agent': userAgent,
      'Accept': ['*/*', 'application/json, text/plain, */*', 'application/json, text/javascript, */*; q=0.01'][Math.floor(Math.random() * 3)],
      'Accept-Language': acceptLanguage,
      'Accept-Encoding': acceptEncoding,
      'Connection': Math.random() > 0.3 ? 'keep-alive' : 'close',
      'Referer': 'https://www.ticketmaster.com/',
      'Origin': 'https://www.ticketmaster.com',
      'sec-ch-ua': secChUa,
      'sec-ch-ua-mobile': secChUaMobile,
      'sec-ch-ua-platform': `"${secChUaPlatform}"`,
      'Sec-Fetch-Dest': 'empty',
      'Sec-Fetch-Mode': 'cors',
      'Sec-Fetch-Site': 'same-site',
      'Pragma': 'no-cache',
      'Cache-Control': Math.random() > 0.5 ? 'no-cache, no-store, must-revalidate' : 'no-store, max-age=0',
      'Cookie': cookies,
      'X-Requested-With': 'XMLHttpRequest',
      'X-Api-Key': 'b462oi7fic6pehcdkzony5bxhe',
      'DNT': Math.random() > 0.7 ? '1' : '0',
    };
    
    return headers;
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
    // Never close the browser or context to maintain persistent session
    console.log("Keeping browser and context open for reuse");
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

export { ScrapeEvent, refreshHeaders, generateEnhancedHeaders, refreshCookiesPeriodically };

// Function to periodically refresh cookies
async function startPeriodicCookieRefresh() {
  // Prevent multiple starts
  if (isPeriodicRefreshStarted) {
    console.log('Periodic cookie refresh already started, skipping...');
    return;
  }
  
  isPeriodicRefreshStarted = true;
  console.log('Starting periodic cookie refresh service...');
  
  // Initial refresh
  await refreshCookiesPeriodically();
  
  // Set up interval for periodic refresh with proper error handling
  const refreshInterval = setInterval(async () => {
    try {
      await refreshCookiesPeriodically();
    } catch (error) {
      console.error('Error in periodic cookie refresh:', error);
      // Don't retry immediately on error, let the interval handle the next attempt
    }
  }, COOKIE_MANAGEMENT.COOKIE_REFRESH_INTERVAL);
  
  // Store the interval ID so we can clear it if needed
  return refreshInterval;
}

async function refreshCookiesPeriodically() {
  const MAX_RETRIES = 3;
  const RETRY_DELAY = 30000; // 30 seconds
  
  let retryCount = 0;
  let lastError = null;
  let localContext = null;
  let refreshRecord = null;
  
  while (retryCount < MAX_RETRIES) {
    try {
      console.log("Starting periodic cookie refresh...");
      
      // Get a stable event ID to use for refreshing - choose one from active scrapes or use a default
      let eventId = null;
      
      // Try to find an active event ID from scraper manager
      if (typeof ScraperManager !== 'undefined' && ScraperManager.getActiveEvents) {
        const activeEvents = ScraperManager.getActiveEvents();
        if (activeEvents.length > 0) {
          // Choose a random event from active ones
          eventId = activeEvents[Math.floor(Math.random() * activeEvents.length)].id;
          console.log(`Using active event ${eventId} for periodic refresh`);
        }
      }
      
      // If no active events, use a default event ID
      if (!eventId) {
        // Default to a known stable event ID for refreshing cookies
        // Using a popular event that's likely to stay active
        eventId = "0400619496250E05";
        console.log(`Using default event ${eventId} for periodic refresh`);
      }
      
      // Get the proxy to use for refresh
      const proxyData = await GetProxy();
      
      // Start tracking this refresh operation
      if (!refreshRecord) {
        refreshRecord = await CookieRefreshTracker.startRefresh(eventId, proxyData.proxy);
      }
      
      // Call refreshHeaders to get fresh cookies
      // Pass only the proxy object, not the entire proxyData object
      const newState = await refreshHeaders(eventId, proxyData.proxy);
      
      if (newState?.cookies?.length) {
        console.log('Successfully refreshed cookies in periodic refresh');
        
        // Update the captured state
        cookieManager.capturedState = {
          ...newState,
          lastRefresh: Date.now()
        };
        
        // Save to file
        await cookieManager.saveCookiesToFile(newState.cookies);
        
        // Don't close context - keep browser session alive
        
        // Track successful refresh in database
        await CookieRefreshTracker.markSuccess(
          refreshRecord.refreshId, 
          newState.cookies.length, 
          retryCount
        );
        
        return; // Success, exit the retry loop
      } else {
        console.warn('Failed to refresh cookies in periodic refresh - no cookies returned');
        lastError = new Error('No cookies returned from refresh');
        
        // Don't mark as failed yet, we'll retry
      }
    } catch (error) {
      console.error(`Error in periodic cookie refresh (attempt ${retryCount + 1}):`, error.message);
      lastError = error;
      
      // Don't close context - keep browser session alive
    }

    // If we get here, we need to retry
    retryCount++;
    if (retryCount < MAX_RETRIES) {
      console.log(`Waiting ${RETRY_DELAY/1000} seconds before retry...`);
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
    }
  }

  // If we've exhausted all retries, log the final error
  console.error('Failed to refresh cookies after all retries:', lastError?.message);
  
  // Track failed refresh in database
  if (refreshRecord) {
    await CookieRefreshTracker.markFailed(
      refreshRecord.refreshId, 
      lastError?.message || 'Unknown error', 
      retryCount
    );
  }
  
  throw lastError; // Re-throw to be handled by the interval
}

// Start the periodic refresh when the module is loaded
startPeriodicCookieRefresh().catch(error => {
  console.error('Failed to start periodic cookie refresh:', error);
  isPeriodicRefreshStarted = false; // Reset the flag on startup failure
});
