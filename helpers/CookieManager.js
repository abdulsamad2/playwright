import { createRequire } from "module";
const require = createRequire(import.meta.url);
const fs = require("fs");
const path = require("path");
import moment from "moment";
import { setTimeout } from "timers/promises";
import { refreshHeaders } from "../scraper.js";
import config from "../config/scraperConfig.js";
import { BrowserFingerprint } from '../browserFingerprint.js';
import { ScraperManager } from '../scraperManager.js';

/**
 * CookieManager handles cookie and header management for the scraper
 */
export class CookieManager {
  constructor(logger) {
    this.logger = logger;
    this.headersCache = new Map();
    this.headerRefreshTimestamps = new Map();
    this.lastCookieReset = null;
    this.cookiesPath = path.join(process.cwd(), "cookies.json");
    this.COOKIES_FILE = "cookies.json";
    this.capturedState = {
      cookies: null,
      fingerprint: null,
      lastRefresh: null,
      proxy: null,
    };
    this.isRefreshingCookies = false;
    this.cookieRefreshQueue = [];
  }

  // Configuration constants
  static CONFIG = {
    COOKIE_REFRESH_INTERVAL: 24 * 60 * 60 * 1000, // 24 hours
    MAX_COOKIE_LENGTH: 8000,
    MAX_COOKIE_AGE: 7 * 24 * 60 * 60 * 1000, // 7 days
    COOKIE_ROTATION: {
      ENABLED: true,
      MAX_STORED_COOKIES: 100,
      ROTATION_INTERVAL: 4 * 60 * 60 * 1000, // 4 hours
      LAST_ROTATION: Date.now()
    },
    PERIODIC_REFRESH_INTERVAL: 30 * 60 * 1000 // 30 minutes
  };

  // Essential cookies that must be present
  static ESSENTIAL_COOKIES = [
    'TMUO', 'TMPS', 'TM_TKTS', 'SESSION', 'audit', 'CMPS', 'CMID',
    'MUID', 'au_id', 'aud', 'tmTrackID', 'TapAd_DID', 'uid'
  ];

  // Authentication cookies that are critical
  static AUTH_COOKIES = ['TMUO', 'TMPS', 'TM_TKTS', 'SESSION', 'audit'];

  /**
   * Get headers for an event, refreshing if necessary
   */
  async getHeadersForEvent(eventId) {
    return this.refreshEventHeaders(eventId);
  }

  /**
   * Refresh event headers if they're stale
   */
  async refreshEventHeaders(eventId) {
    const lastRefresh = this.headerRefreshTimestamps.get(eventId);
    
    // Only refresh headers if they haven't been refreshed in last 5 minutes
    if (!lastRefresh || moment().diff(lastRefresh) > config.HEADER_REFRESH_INTERVAL) {
      try {
        this.logger.logWithTime(`Refreshing headers for ${eventId}`);
        const headers = await refreshHeaders(eventId);
        if (headers) {
          this.headersCache.set(eventId, headers);
          this.headerRefreshTimestamps.set(eventId, moment());
          return headers;
        }
      } catch (error) {
        this.logger.logWithTime(
          `Failed to refresh headers for ${eventId}: ${error.message}`,
          "error"
        );
      }
    }

    return this.headersCache.get(eventId);
  }

  /**
   * Reset cookies and clear header cache
   */
  async resetCookiesAndHeaders() {
    // Avoid resetting cookies too frequently
    const now = moment();
    if (this.lastCookieReset && now.diff(this.lastCookieReset) < config.COOKIE_RESET_COOLDOWN) {
      this.logger.logWithTime(
        `Skipping cookie reset - last reset was ${moment.duration(now.diff(this.lastCookieReset)).humanize()} ago`,
        "warning"
      );
      return false;
    }
    
    try {
      this.logger.logWithTime("Detected multiple API failures - resetting cookies and headers", "warning");
      
      // Check if cookies.json exists before trying to delete it
      try {
        await fs.promises.access(this.cookiesPath);
        
        // Delete cookies.json
        await fs.promises.unlink(this.cookiesPath);
        this.logger.logWithTime("Deleted cookies.json", "info");
      } catch (e) {
        // File doesn't exist, that's fine
        this.logger.logWithTime("No cookies.json file found to delete", "info");
      }
      
      // Clear all cached headers
      this.headersCache.clear();
      this.headerRefreshTimestamps.clear();
      this.lastCookieReset = now;
      
      // Apply a system-wide cooldown to allow for fresh cookie generation
      this.logger.logWithTime("Applying cooldown to allow for cookie regeneration", "info");
      await setTimeout(config.COOKIE_REGENERATION_DELAY);
      
      return true;
    } catch (error) {
      this.logger.logWithTime(`Error resetting cookies: ${error.message}`, "error");
      return false;
    }
  }
  
  /**
   * Check if headers exist for an event
   */
  hasHeadersForEvent(eventId) {
    return this.headersCache.has(eventId);
  }

  async loadCookiesFromFile() {
    try {
      if (fs.existsSync(this.cookiesPath)) {
        const data = await fs.promises.readFile(this.cookiesPath, 'utf8');
        const parsedData = JSON.parse(data);
        
        if (parsedData.cookieSets && Array.isArray(parsedData.cookieSets)) {
          const sortedSets = parsedData.cookieSets.sort((a, b) => b.timestamp - a.timestamp);
          
          for (const set of sortedSets) {
            const cookies = set.cookies;
            if (cookies && cookies.length > 0) {
              const cookieString = cookies
                .map(cookie => `${cookie.name}=${cookie.value}`)
                .join('; ');
              
              if (this.areCookiesFresh(cookieString)) {
                console.log('Using cookie set from', new Date(set.timestamp).toISOString());
                return cookies;
              }
            }
          }
          return null;
        } 
        else if (Array.isArray(parsedData)) {
          const cookies = parsedData;
          const cookieString = cookies
            .map(cookie => `${cookie.name}=${cookie.value}`)
            .join('; ');
          
          if (this.areCookiesFresh(cookieString)) {
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

  areCookiesFresh(cookies) {
    if (!cookies) return false;
    
    const cookieMap = new Map();
    cookies.split(';').forEach(cookie => {
      const [name, value] = cookie.trim().split('=');
      if (name && value) {
        cookieMap.set(name, value);
      }
    });
    
    const authCookiesPresent = CookieManager.AUTH_COOKIES.filter(name => 
      cookieMap.has(name) && cookieMap.get(name).length > 0
    );
    
    return authCookiesPresent.length >= 3;
  }

  async saveCookiesToFile(cookies) {
    try {
      let existingCookieSets = [];
      try {
        if (fs.existsSync(this.cookiesPath)) {
          const fileContent = await fs.promises.readFile(this.cookiesPath, 'utf8');
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

      const cookieData = cookies.map(cookie => ({
        ...cookie,
        expires: cookie.expires || Date.now() + CookieManager.CONFIG.COOKIE_REFRESH_INTERVAL,
        expiry: cookie.expiry || Date.now() + CookieManager.CONFIG.COOKIE_REFRESH_INTERVAL
      }));

      if (CookieManager.CONFIG.COOKIE_ROTATION.ENABLED) {
        const shouldRotate = Date.now() - CookieManager.CONFIG.COOKIE_ROTATION.LAST_ROTATION > 
                            CookieManager.CONFIG.COOKIE_ROTATION.ROTATION_INTERVAL;
        
        if (shouldRotate) {
          const newCookieSet = {
            timestamp: Date.now(),
            cookies: cookieData
          };
          
          existingCookieSets.push(newCookieSet);
          existingCookieSets.sort((a, b) => b.timestamp - a.timestamp);
          existingCookieSets = existingCookieSets.slice(0, CookieManager.CONFIG.COOKIE_ROTATION.MAX_STORED_COOKIES);
          
          CookieManager.CONFIG.COOKIE_ROTATION.LAST_ROTATION = Date.now();
          
          await fs.promises.writeFile(
            this.cookiesPath,
            JSON.stringify({
              lastUpdated: Date.now(),
              cookieSets: existingCookieSets
            }, null, 2)
          );
        } else {
          if (existingCookieSets.length > 0) {
            existingCookieSets[0].cookies = cookieData;
            existingCookieSets[0].timestamp = Date.now();
            
            await fs.promises.writeFile(
              this.cookiesPath,
              JSON.stringify({
                lastUpdated: Date.now(),
                cookieSets: existingCookieSets
              }, null, 2)
            );
          } else {
            await fs.promises.writeFile(
              this.cookiesPath,
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
        await fs.promises.writeFile(
          this.cookiesPath,
          JSON.stringify(cookieData, null, 2)
        );
      }
    } catch (error) {
      console.error('Error saving cookies:', error);
    }
  }

  extractEssentialCookies(cookies) {
    if (!cookies) return '';
    
    const cookieMap = new Map();
    cookies.split(';').forEach(cookie => {
      const [name, value] = cookie.trim().split('=');
      if (name && value) {
        cookieMap.set(name, value);
      }
    });
    
    const essentialCookies = [];
    CookieManager.AUTH_COOKIES.forEach(name => {
      if (cookieMap.has(name)) {
        essentialCookies.push(`${name}=${cookieMap.get(name)}`);
        cookieMap.delete(name);
      }
    });
    
    CookieManager.ESSENTIAL_COOKIES.forEach(name => {
      if (cookieMap.has(name) && essentialCookies.length < 20) {
        essentialCookies.push(`${name}=${cookieMap.get(name)}`);
        cookieMap.delete(name);
      }
    });

    if (essentialCookies.join('; ').length < CookieManager.CONFIG.MAX_COOKIE_LENGTH) {
      for (const [name, value] of cookieMap.entries()) {
        const potentialCookie = `${name}=${value}`;
        if (essentialCookies.join('; ').length + potentialCookie.length + 2 < CookieManager.CONFIG.MAX_COOKIE_LENGTH) {
          essentialCookies.push(potentialCookie);
        }
      }
    }
    
    return essentialCookies.join('; ');
  }

  mergeCookies(existingCookies, newCookies) {
    if (!existingCookies) return newCookies;
    if (!newCookies) return existingCookies;
    
    const cookieMap = new Map();
    
    existingCookies.split(';').forEach(cookie => {
      const [name, value] = cookie.trim().split('=');
      if (name && value) {
        cookieMap.set(name, value);
      }
    });
    
    newCookies.split(';').forEach(cookie => {
      const [name, value] = cookie.trim().split('=');
      if (name && value) {
        cookieMap.set(name, value);
      }
    });
    
    return Array.from(cookieMap.entries())
      .map(([name, value]) => `${name}=${value}`)
      .join('; ');
  }

  resetCapturedState() {
    this.capturedState = {
      cookies: null,
      fingerprint: null,
      lastRefresh: null,
      proxy: null,
    };
  }

  async startPeriodicCookieRefresh() {
    console.log('Starting periodic cookie refresh service...');
    await this.refreshCookiesPeriodically();
    
    setInterval(async () => {
      await this.refreshCookiesPeriodically();
    }, CookieManager.CONFIG.PERIODIC_REFRESH_INTERVAL);
  }

  async refreshCookiesPeriodically() {
    try {
      console.log('Starting periodic cookie refresh...');
      
      // Get a random active event ID
      const activeEvents = await ScraperManager.getEventsToProcess();
      if (!activeEvents || activeEvents.length === 0) {
        console.warn('No active events found for cookie refresh');
        return;
      }
      
      const randomEventId = activeEvents[Math.floor(Math.random() * activeEvents.length)];
      const { proxy } = GetProxy();
      
      const newState = await refreshHeaders(randomEventId, proxy, null);
      
      if (newState?.cookies?.length) {
        console.log('Successfully refreshed cookies in periodic refresh');
        
        this.capturedState = {
          ...newState,
          lastRefresh: Date.now()
        };
        
        await this.saveCookiesToFile(newState.cookies);
      } else {
        console.warn('Failed to refresh cookies in periodic refresh');
      }
    } catch (error) {
      console.error('Error in periodic cookie refresh:', error);
    }
  }
} 