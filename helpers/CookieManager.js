import path from "path";
import fs from "fs/promises";
import moment from "moment";
import { setTimeout } from "timers/promises";
import { refreshHeaders } from "../scraper.js";
import config from "../config/scraperConfig.js";

/**
 * CookieManager handles cookie and header management for the scraper
 */
class CookieManager {
  constructor(logger) {
    this.logger = logger;
    this.headersCache = new Map();
    this.headerRefreshTimestamps = new Map();
    this.lastCookieReset = null;
    this.cookiesPath = path.join(process.cwd(), "cookies.json");
  }

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
        await fs.access(this.cookiesPath);
        
        // Delete cookies.json
        await fs.unlink(this.cookiesPath);
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
}

export default CookieManager; 