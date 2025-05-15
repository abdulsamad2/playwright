import moment from "moment";
import { setTimeout } from "timers/promises";
import { Event, ErrorLog, ConsecutiveGroup } from "./models/index.js";
import { ScrapeEvent, refreshHeaders, generateEnhancedHeaders } from "./scraper.js";
import { cpus } from "os";
import fs from "fs/promises";
import path from "path";
import ProxyManager from "./helpers/ProxyManager.js";
import { BrowserFingerprint } from "./browserFingerprint.js";
import { v4 as uuidv4 } from 'uuid';
import SyncService from './services/syncService.js';
import nodeFs from 'fs';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

// Add this at the top of the file, after imports
export const ENABLE_CSV_UPLOAD = false; // Set to false to disable all_events_combined.csv upload

const MAX_UPDATE_INTERVAL = 120000; // Strict 2-minute update requirement (reduced from 160000)
const CONCURRENT_LIMIT = Math.max(60, Math.floor(cpus().length * 5)); // Dramatically increased for maximum parallel processing
const MAX_RETRIES = 15; // Updated from 10 per request of user
const SCRAPE_TIMEOUT = 60000; // Increased from 35000 to 60 seconds
const BATCH_SIZE = Math.max(CONCURRENT_LIMIT * 4, 120); // Significantly increased batch size for bulk processing
const RETRY_BACKOFF_MS = 1500; // Reduced base backoff time for faster retries
const MIN_TIME_BETWEEN_EVENT_SCRAPES = 120000; // Minimum 1 minute between scrapes (allowing for 2-minute updates)
const URGENT_THRESHOLD = 30000; // Events needing update within 30 seconds of deadline (tighter window)
const MAX_ALLOWED_UPDATE_INTERVAL = 180000; // Maximum 3 minutes allowed between updates
const EVENT_FAILURE_THRESHOLD = 600000; // 10 minutes without updates before marking an event as stopped/failed

// New cooldown settings - shorter cooldowns to allow for more frequent retries within 10 minute window
const SHORT_COOLDOWNS = [1000, 2000, 4000]; // Shortened cooldowns: 1s, 2s, 4s for more frequent retries
const LONG_COOLDOWN_MINUTES = 1; // Reduced to 1 minute for more frequent retries within the 10 minute window

// Logging levels: 0 = errors only, 1 = warnings + errors, 2 = info + warnings + errors, 3 = all (verbose)
const LOG_LEVEL = 1; // Default to warnings and errors only

// Cookie expiration threshold: refresh cookies every 20 minutes
const COOKIE_EXPIRATION_MS = 20 * 60 * 1000; // 20 minutes (changed from 30 minutes)

// Anti-bot helpers: rotate User-Agent and spoof IP
const USER_AGENT_POOL = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  // Additional Firefox UA for variety
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:115.0) Gecko/20100101 Firefox/115.0"
];
const generateRandomIp = () => Array.from({ length: 4 }, () => Math.floor(Math.random() * 256)).join('.');
// Number of proxy rotation attempts when fetching fresh cookies/headers
const PROXY_ROTATION_ATTEMPTS = 3;

// Only accept cookie sets with at least this many cookies to avoid challenge pages
const MIN_VALID_COOKIES = 3;

// Realistic fingerprint options to mimic human browsers (language, timezone, plugins, screen size, etc.)
const FINGERPRINT_POOL = [
  { language: 'en-US', timezone: 'America/Los_Angeles', platform: 'Win32', screen: { width:1920, height:1080 }, deviceMemory: 8, hardwareConcurrency: 8, plugins: ['Widevine Content Decryption Module','Chrome PDF Viewer','Native Client'] },
  { language: 'en-GB', timezone: 'Europe/London', platform: 'MacIntel', screen: { width:1440, height:900 }, deviceMemory: 8, hardwareConcurrency: 4, plugins: ['PDF Viewer','QuickTime Plug-in 7.7.9','Java(TM) Platform SE 8 U211'] },
  { language: 'fr-FR', timezone: 'Europe/Paris', platform: 'Win32', screen: { width:1366, height:768 }, deviceMemory: 4, hardwareConcurrency: 4, plugins: ['Chrome PDF Viewer','Widevine Content Decryption Module'] },
  { language: 'de-DE', timezone: 'Europe/Berlin', platform: 'Linux x86_64', screen: { width:1920, height:1080 }, deviceMemory: 16, hardwareConcurrency: 8, plugins: ['Flash','QuickTime Plug-in','Java Bridge'] },
  // Firefox fingerprint with common privacy/bot-buster add-ons for human-like behavior
  { language: 'en-US', timezone: 'America/New_York', platform: 'MacIntel', screen: { width:1680, height:1050 }, deviceMemory: 8, hardwareConcurrency: 4, plugins: ['uBlock Origin','Privacy Badger','CanvasBlocker','NoScript','Video DownloadHelper'] }
];
// Helper to pick a random fingerprint
const randomFingerprint = () => FINGERPRINT_POOL[Math.floor(Math.random() * FINGERPRINT_POOL.length)];

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
  MAX_COOKIE_LENGTH: 8000,
  COOKIE_REFRESH_INTERVAL: 20 * 60 * 1000, // 20 minutes (changed from 30 minutes)
  MAX_COOKIE_AGE: 7 * 24 * 60 * 60 * 1000,
  COOKIE_ROTATION: {
    ENABLED: true,
    MAX_STORED_COOKIES: 100,
    ROTATION_INTERVAL: 20 * 60 * 1000, // 20 minutes (changed from 4 hours)
    LAST_ROTATION: Date.now(),
  },
};

// Update constants for large-scale processing
const CONCURRENT_DOMAIN_LIMIT = 25; // Process more events from the same domain concurrently
const DB_FETCH_LIMIT = 500; // Fetch more events at once for efficient DB interaction

// CSV Upload Constants
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const DATA_DIR = path.join(__dirname, 'data');
const BLANK_CSV_PATH = path.join(DATA_DIR, 'blank_csv.csv');
const COMPANY_ID = '702';
const API_TOKEN = 'OaJwtlUQiriMSrnGd7cauDWtIyAMnS363icaz-7t1vJ7bjIBe9ZFjBwgPYY1Q9eKV_Jt';
const CSV_UPLOAD_INTERVAL = 6 * 60 * 1000; // 6 minutes in milliseconds

/**
 * ScraperManager class that maintains the original API while using the modular architecture internally
 */
export class ScraperManager {
  constructor() {
    this.startTime = moment();
    this.lastSuccessTime = null;
    this.successCount = 0;
    this.activeJobs = new Map();
    this.failedEvents = new Set();
    this.retryQueue = [];
    this.isRunning = false;
    this.headersCache = new Map();
    this.eventUpdateTimestamps = new Map();
    this.priorityQueue = new Set();
    this.concurrencySemaphore = CONCURRENT_LIMIT;
    this.processingEvents = new Set(); // Track events currently being processed
    this.headerRefreshTimestamps = new Map(); // Track when headers were last refreshed
    this.cooldownEvents = new Map(); // Events that need to cool down before retry
    this.eventUpdateSchedule = new Map(); // Tracks when each event needs to be updated next
    this.eventFailureCounts = new Map(); // Track consecutive failures per event
    this.eventFailureTimes = new Map(); // Track when failures happened
    this.globalConsecutiveErrors = 0; // Track consecutive errors across all events
    this.lastCookieReset = null; // Track when cookies were last reset
    this.cookiesPath = path.join(process.cwd(), "cookies.json"); // Path to cookies file

    // Initialize the proxy manager
    this.proxyManager = new ProxyManager(this);
    this.batchProxies = new Map(); // Map batch ID to proxy

    // New: Data caching and API error handling
    this.headerRotationPool = []; // Pool of working headers
    this.proxySuccessRates = new Map(); // Track success rates per proxy
    this.apiCircuitBreaker = {
      failures: 0,
      threshold: 10,
      resetTimeout: 30000,
      lastTripped: null,
      tripped: false,
    };
    this.headerSuccessRates = new Map(); // Track which headers work better

    // New: Failed events batching
    this.failureTypeGroups = new Map(); // Group failed events by error type
    this.lastFailedBatchProcess = null; // Last time we processed a batch of failed events
    this.failedEventsProcessingInterval = 5000; // Process failed events every 5 seconds
    this.eventMaxRetries = new Map(); // Track dynamic retry limits per event
    
    // CSV upload tracking
    this.lastCsvUploadTime = null; // Track last successful CSV upload
    this.csvUploadIntervalId = null; // Track the interval ID for clean shutdown
  }

  logWithTime(message, type = "info") {
    // Map log levels to numeric values
    const logLevels = {
      error: 0,
      warning: 1,
      info: 2,
      success: 2,
      debug: 3,
    };

    // Skip logging if level is higher than configured LOG_LEVEL
    if (logLevels[type] > LOG_LEVEL) {
      return;
    }

    const now = moment();
    const formattedTime = now.format("YYYY-MM-DD HH:mm:ss");

    const statusEmoji =
      {
        success: "✅",
        error: "❌",
        warning: "⚠️",
        info: "ℹ️",
        debug: "🔍",
      }[type] || "📝";

    // Only include detailed stats for errors or if we're at the highest verbosity
    if (type === "error" || LOG_LEVEL >= 3) {
      const runningTime = moment.duration(now.diff(this.startTime));
      console.log(
        `${statusEmoji} [${formattedTime}] ${message}\n` +
          `   Runtime: ${Math.floor(
            runningTime.asHours()
          )}h ${runningTime.minutes()}m ${runningTime.seconds()}s\n` +
          `   Active: ${this.activeJobs.size}/${CONCURRENT_LIMIT}, Success: ${this.successCount}, Failed: ${this.failedEvents.size}, Retry Queue: ${this.retryQueue.length}`
      );
    } else {
      console.log(`${statusEmoji} [${formattedTime}] ${message}`);
    }
  }

  async logError(eventId, errorType, error, metadata = {}) {
    try {
      const event = await Event.findOne({ Event_ID: eventId })
        .select("url _id Last_Updated")
        .lean();

      // Fix for missing eventUrl - provide a fallback
      const eventUrl = event?.url || `unknown-url-for-event-${eventId}`;
      const eventObjectId = event?._id || null;

      // Log to console first in case DB logging fails
      console.error(
        `Error for event ${eventId} (${errorType}): ${error.message}`
      );

      // Check if this is a failure that should be logged to the database
      const shouldLogToDB = this.shouldLogErrorToDatabase(eventId, errorType);

      // Only try to log to database if we have minimal required data and it's a significant error
      if (eventObjectId && shouldLogToDB) {
        await ErrorLog.create({
          eventUrl: eventUrl,
          eventId: eventObjectId,
          externalEventId: eventId,
          errorType,
          message: error.message,
          stack: error.stack,
          metadata: {
            ...metadata,
            timestamp: new Date(),
            iteration: metadata.iterationNumber || 1,
          },
        });
      } else if (!eventObjectId) {
        console.error(
          `Cannot log to ErrorLog - event ${eventId} not found in database`
        );
      }
    } catch (err) {
      console.error("Error logging to database:", err);
      console.error("Original error:", error);
    }
  }

  // Helper method to determine if an error should be logged to the database
  shouldLogErrorToDatabase(eventId, errorType) {
    // Always log critical errors like LONG_COOLDOWN
    if (errorType === "LONG_COOLDOWN") {
      return true;
    }

    // Check if the event has been marked as failed due to 10-min threshold
    const lastUpdate = this.eventUpdateTimestamps.get(eventId);
    if (!lastUpdate) {
      return true; // No record of updates, should log
    }

    // Only log errors to database if the event has exceeded the failure threshold (10 minutes)
    const timeSinceUpdate = moment().diff(lastUpdate);
    return timeSinceUpdate >= EVENT_FAILURE_THRESHOLD;
  }

  async acquireSemaphore() {
    while (this.concurrencySemaphore <= 0) {
      await setTimeout(100);
    }
    this.concurrencySemaphore--;
  }

  releaseSemaphore() {
    this.concurrencySemaphore++;
  }

  shouldSkipEvent(eventId) {
    // Check if event is in cooldown period
    if (this.cooldownEvents.has(eventId)) {
      const cooldownUntil = this.cooldownEvents.get(eventId);
      if (moment().isBefore(cooldownUntil)) {
        // Only log at higher verbosity levels
        if (LOG_LEVEL >= 2) {
          const timeLeft = moment
            .duration(cooldownUntil.diff(moment()))
            .asSeconds();
          this.logWithTime(
            `Skipping ${eventId}: In cooldown for ${timeLeft.toFixed(1)}s more`,
            "debug"
          );
        }
        return true;
      } else {
        this.cooldownEvents.delete(eventId);
      }
    }

    // Check if event is already being processed
    if (this.processingEvents.has(eventId)) {
      // Only log at higher verbosity levels
      if (LOG_LEVEL >= 3) {
        this.logWithTime(`Skipping ${eventId}: Already processing`, "debug");
      }
      return true;
    }

    // Check if minimum time between scrapes has elapsed (unless approaching max allowed time)
    const lastUpdate = this.eventUpdateTimestamps.get(eventId);
    if (
      lastUpdate &&
      moment().diff(lastUpdate) < MIN_TIME_BETWEEN_EVENT_SCRAPES
    ) {
      // Check if approaching 3-minute maximum threshold - override minimum time check
      const timeSinceLastUpdate = moment().diff(lastUpdate);
      if (timeSinceLastUpdate > MAX_ALLOWED_UPDATE_INTERVAL - 20000) {
        // Approaching max allowed time, don't skip
        if (LOG_LEVEL >= 2) {
          this.logWithTime(
            `Processing ${eventId} despite minimum time restriction: Approaching max allowed time (${
              timeSinceLastUpdate / 1000
            }s since last update)`,
            "warning"
          );
        }
        return false;
      }

      // Only log at higher verbosity levels
      if (LOG_LEVEL >= 3) {
        this.logWithTime(
          `Skipping ${eventId}: Too soon since last scrape (${moment().diff(
            lastUpdate
          )}ms)`,
          "debug"
        );
      }
      return true;
    }

    return false;
  }

  async refreshEventHeaders(eventId) {
    // Anti-bot: only fetch fresh cookies/headers when expired, add random UA, referer, IP, and request ID
    try {
      // Always get a random event ID from the database for cookie refresh
      let eventToUse;
      try {
        // Get a random active event from the database with specific criteria
        const randomEvent = await Event.aggregate([
          {
            $match: {
              Skip_Scraping: { $ne: true },
              url: { $exists: true, $ne: "" },
            },
          },
          { $sample: { size: 5 } },
          { $project: { Event_ID: 1, url: 1 } },
        ]);

        if (randomEvent && randomEvent.length > 0) {
          const selectedEvent =
            randomEvent[Math.floor(Math.random() * randomEvent.length)];
          eventToUse = selectedEvent.Event_ID;
          if (LOG_LEVEL >= 2) {
            this.logWithTime(
              `Using random event ${eventToUse} for cookie refresh`,
              "debug"
            );
          }
        } else {
          eventToUse = eventId; // Fallback to provided eventId
        }
      } catch (dbError) {
        this.logWithTime(
          `Error getting random event: ${dbError.message}`,
          "warning"
        );
        eventToUse = eventId; // Fallback to provided eventId
      }

      const eventDoc = await Event.findOne({ Event_ID: eventToUse })
        .select("url")
        .lean();
      const refererUrl = eventDoc?.url || "https://www.ticketmaster.com/";
      const requestId =
        Math.random().toString(36).substring(2, 10) +
        "-" +
        Date.now().toString(36);
      const lastRefresh = this.headerRefreshTimestamps.get(eventToUse);

      // Generate a new X-Forwarded-For and X-Request-Id for each request
      const refreshedIpAndId = {
        "X-Forwarded-For": generateRandomIp(),
        "X-Request-Id":
          Math.random().toString(36).substring(2, 10) +
          "-" +
          Date.now().toString(36),
      };

      // Check for stale cached headers - enforce 20-minute refresh interval
      const effectiveExpirationTime = COOKIE_MANAGEMENT.COOKIE_REFRESH_INTERVAL;

      // Only refresh cookies/headers if they have expired
      if (
        !lastRefresh ||
        moment().diff(lastRefresh) > effectiveExpirationTime
      ) {
        if (LOG_LEVEL >= 2) {
          this.logWithTime(`Refreshing headers for ${eventToUse}`, "debug");
        }

        // Clear any stale headers from the cache
        this.headersCache.delete(eventToUse);

        // Try to get fresh headers from rotation pool first
        if (this.headerRotationPool.length > 0) {
          const headerIndex = Math.floor(
            Math.random() * this.headerRotationPool.length
          );
          const cachedHeaders = { ...this.headerRotationPool[headerIndex] };

          // Update dynamic anti-bot fields
          if (cachedHeaders.headers) {
            cachedHeaders.headers = {
              ...cachedHeaders.headers,
              ...refreshedIpAndId,
            };
          }

          // Update timestamp but don't overwrite the original headers
          this.headerRefreshTimestamps.set(eventToUse, moment());
          return cachedHeaders;
        }

        // Get a random proxy for refreshing headers
        const { proxy: cookieProxy } =
          this.proxyManager.getProxyForEvent(eventToUse);

        // Try to refresh headers with the proxy
        let capturedState = null;
        try {
          capturedState = await refreshHeaders(eventToUse, cookieProxy);
        } catch (error) {
          this.logWithTime(
            `Error refreshing headers with proxy: ${error.message}`,
            "warning"
          );
        }

        // If no valid state captured, try without proxy as last resort
        if (
          !capturedState ||
          !capturedState.cookies ||
          capturedState.cookies.length < MIN_VALID_COOKIES
        ) {
          this.logWithTime(
            `Trying to refresh headers without proxy for ${eventToUse}`,
            "warning"
          );
          try {
            capturedState = await refreshHeaders(eventToUse);
          } catch (error) {
            this.logWithTime(
              `Error refreshing headers without proxy: ${error.message}`,
              "warning"
            );
          }
        }

        if (
          capturedState &&
          capturedState.cookies &&
          capturedState.cookies.length >= MIN_VALID_COOKIES
        ) {
          // Build enhanced headers using the shared generator
          const cookieString = capturedState.cookies
            .map((c) => `${c.name}=${c.value}`)
            .join("; ");
          const poolFp = randomFingerprint();
          const mergedFp = { ...capturedState.fingerprint, ...poolFp };
          // Generate varied headers
          const enhanced = generateEnhancedHeaders(mergedFp, cookieString);
          // Inject anti-bot fields
          enhanced["Referer"] = refererUrl;
          enhanced["X-Forwarded-For"] = refreshedIpAndId["X-Forwarded-For"];
          enhanced["X-Request-Id"] = refreshedIpAndId["X-Request-Id"];
          const standardizedHeaders = {
            headers: enhanced,
            cookies: capturedState.cookies,
            fingerprint: mergedFp,
            timestamp: Date.now(),
          };

          // Cache and rotate
          this.headersCache.set(eventToUse, standardizedHeaders);
          this.headerRefreshTimestamps.set(eventToUse, moment());

          // Add to rotation pool if not already present
          if (
            !this.headerRotationPool.some(
              (h) => h.headers.Cookie === cookieString
            )
          ) {
            this.headerRotationPool.push(standardizedHeaders);
            if (this.headerRotationPool.length > 10)
              this.headerRotationPool.shift();
          }

          return standardizedHeaders;
        } else {
          this.logWithTime(
            `Failed to get valid cookies for ${eventToUse}`,
            "warning"
          );
        }
      } else {
        // Use cached headers but refresh dynamic fields
        const cachedHeaders = this.headersCache.get(eventToUse);
        if (cachedHeaders) {
          // Create a deep copy to avoid modifying the cached version
          const refreshedHeaders = JSON.parse(JSON.stringify(cachedHeaders));

          // Update dynamic anti-bot fields
          if (refreshedHeaders.headers) {
            refreshedHeaders.headers = {
              ...refreshedHeaders.headers,
              ...refreshedIpAndId,
            };
          }

          return refreshedHeaders;
        }
      }
    } catch (error) {
      this.logWithTime(`Failed to refresh headers: ${error.message}`, "error");
    }

    // As a fallback, try to use any previously cached headers
    const cachedHeaders = this.headersCache.get(eventId);
    if (cachedHeaders) {
      // Update dynamic anti-bot fields here too
      const refreshedHeaders = JSON.parse(JSON.stringify(cachedHeaders));
      refreshedHeaders.headers["X-Forwarded-For"] = generateRandomIp();
      refreshedHeaders.headers["X-Request-Id"] =
        Math.random().toString(36).substring(2, 10) +
        "-" +
        Date.now().toString(36);
      return refreshedHeaders;
    }

    // If all else fails, try to use headers from rotation pool
    if (this.headerRotationPool.length > 0) {
      const headerIndex = Math.floor(
        Math.random() * this.headerRotationPool.length
      );
      const poolHeaders = JSON.parse(
        JSON.stringify(this.headerRotationPool[headerIndex])
      );

      // Update dynamic anti-bot fields
      if (poolHeaders.headers) {
        poolHeaders.headers["X-Forwarded-For"] = generateRandomIp();
        poolHeaders.headers["X-Request-Id"] =
          Math.random().toString(36).substring(2, 10) +
          "-" +
          Date.now().toString(36);
      }

      return poolHeaders;
    }

    // Ultimate fallback: generate basic headers
    this.logWithTime(`Using fallback headers for ${eventId}`, "warning");
    return {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1",
        Accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        Referer: "https://www.ticketmaster.com/",
        "X-Forwarded-For": generateRandomIp(),
        "X-Request-Id":
          Math.random().toString(36).substring(2, 10) +
          "-" +
          Date.now().toString(36),
      },
    };
  }

  async resetCookiesAndHeaders() {
    // Avoid resetting cookies too frequently (at most once per hour)
    const now = moment();
    if (
      this.lastCookieReset &&
      now.diff(this.lastCookieReset) < COOKIE_EXPIRATION_MS
    ) {
      if (LOG_LEVEL >= 1) {
        this.logWithTime(
          `Skipping cookie reset - last reset was ${moment
            .duration(now.diff(this.lastCookieReset))
            .humanize()} ago`,
          "warning"
        );
      }
      return;
    }

    try {
      this.logWithTime(
        "Detected multiple API failures - resetting cookies and headers",
        "warning"
      );

      // Check if cookies.json exists before trying to delete it
      try {
        await fs.access(this.cookiesPath);
        // Only delete if expired
        const stats = await fs.stat(this.cookiesPath);
        const cookieAge = now.diff(moment(stats.mtime));
        if (cookieAge < COOKIE_EXPIRATION_MS) {
          if (LOG_LEVEL >= 2) {
            this.logWithTime(
              `Cookies not expired yet (age ${cookieAge}ms), skipping fresh retrieval`,
              "info"
            );
          }
          return true;
        }
        // Delete expired cookies
        await fs.unlink(this.cookiesPath);
        if (LOG_LEVEL >= 1) {
          this.logWithTime("Deleted expired cookies.json", "info");
        }
      } catch (e) {
        // File doesn't exist or stat failed, that's fine
        if (LOG_LEVEL >= 2) {
          this.logWithTime("No cookies.json file found to delete", "info");
        }
      }

      // Clear all cached headers
      this.headersCache.clear();
      this.headerRefreshTimestamps.clear();

      // Reset error counters
      this.globalConsecutiveErrors = 0;
      this.lastCookieReset = now;

      // Apply a system-wide cooldown to allow for fresh cookie generation
      this.logWithTime(
        "Applying 30-second cooldown to allow for cookie regeneration",
        "info"
      );
      await setTimeout(30000);

      // Trigger a headers refresh on the next event
      return true;
    } catch (error) {
      this.logWithTime(`Error resetting cookies: ${error.message}`, "error");
      return false;
    }
  }

  async updateEventMetadata(eventId, scrapeResult) {
    const startTime = performance.now();
    const session = await Event.startSession();

    try {
      await session.withTransaction(async () => {
        const event = await Event.findOne({ Event_ID: eventId }).session(
          session
        );
        if (!event) {
          throw new Error(
            `Event ${eventId} not found in database for metadata update`
          );
        }

        // Get price increase percentage and in-hand date from event
        const priceIncreasePercentage = event.priceIncreasePercentage || 25; // Default 25% if not set
        const inHandDate = event.inHandDate || new Date();
        const mapping_id = event.mapping_id; // Get the mapping_id from the event

        if (!mapping_id) {
          throw new Error(`Event ${eventId} is missing required mapping_id`);
        }

        // Filter scrape results to include only groups with at least 2 seats
        const validScrapeResult = scrapeResult.filter(
          (group) =>
            group.seats &&
            group.seats.length >= 2 &&
            group.inventory &&
            group.inventory.quantity >= 2
        );

        const previousTicketCount = event.Available_Seats || 0;
        const currentTicketCount = validScrapeResult.length;

        const metadata = {
          lastUpdate: moment().format("YYYY-MM-DD HH:mm:ss"),
          iterationNumber: (event.metadata?.iterationNumber || 0) + 1,
          scrapeDurationMs: performance.now() - startTime,
          scrapeStartTime: this.startTime.toDate(),
          scrapeEndTime: new Date(),
          scrapeDurationSeconds: moment().diff(this.startTime, "seconds"),
          totalRunningTimeMinutes: moment().diff(this.startTime, "minutes"),
          ticketStats: {
            totalTickets: currentTicketCount,
            ticketCountChange: currentTicketCount - previousTicketCount,
            previousTicketCount,
          },
        };

        // First update the basic event info
        await Event.updateOne(
          { Event_ID: eventId },
          {
            $set: {
              Available_Seats: currentTicketCount,
              Last_Updated: new Date(),
              "metadata.basic": metadata,
            },
          }
        ).session(session);

        if (validScrapeResult?.length > 0) {
          // Check if seats have actually changed before performing database operations
          const currentGroups = await ConsecutiveGroup.find(
            { eventId },
            { section: 1, row: 1, seats: 1, "inventory.listPrice": 1 }
          )
            .lean()
            .session(session);

          // Create hash sets for efficient comparison
          const currentSeatsHash = new Set(
            currentGroups.flatMap((g) =>
              g.seats.map((s) => `${g.section}-${g.row}-${s.number}-${s.price}`)
            )
          );

          const newSeatsHash = new Set(
            validScrapeResult.flatMap((g) =>
              g.seats.map(
                (s) => `${g.section}-${g.row}-${s}-${g.inventory.listPrice}`
              )
            )
          );

          // Check if there are differences in the seats
          const hasChanges =
            currentSeatsHash.size !== newSeatsHash.size ||
            [...currentSeatsHash].some((s) => !newSeatsHash.has(s)) ||
            [...newSeatsHash].some((s) => !currentSeatsHash.has(s));

          if (hasChanges) {
            // Only log at appropriate log level
            if (LOG_LEVEL >= 2) {
              this.logWithTime(
                `Seat changes detected for event ${eventId}, updating database`,
                "info"
              );
            }

            // Bulk delete and insert for better performance
            await ConsecutiveGroup.deleteMany({ eventId }).session(session);

            const groupsToInsert = validScrapeResult.map((group) => {
              // Calculate the increased list price based on the percentage
              const basePrice = parseFloat(group.inventory.listPrice) || 500.0;
              const increasedPrice =
                basePrice * (1 + priceIncreasePercentage / 100);

              // Format in-hand date as YYYY-MM-DD
              const formattedInHandDate =
                inHandDate instanceof Date ? inHandDate : new Date();

              return {
                eventId,
                mapping_id, // Add mapping_id at the top level
                section: group.section,
                row: group.row,
                seatCount: group.inventory.quantity,
                seatRange: `${Math.min(...group.seats)}-${Math.max(
                  ...group.seats
                )}`,
                seats: group.seats.map((seatNumber) => ({
                  number: seatNumber.toString(),
                  inHandDate: formattedInHandDate,
                  price: increasedPrice,
                  mapping_id, // Add mapping_id to each seat
                })),
                inventory: {
                  quantity: group.inventory.quantity,
                  section: group.section,
                  hideSeatNumbers: group.inventory.hideSeatNumbers || true,
                  row: group.row,
                  cost: group.inventory.cost,
                  stockType: group.inventory.stockType || "MOBILE_TRANSFER",
                  lineType: group.inventory.lineType,
                  seatType: group.inventory.seatType,
                  inHandDate: formattedInHandDate,
                  notes:
                    group.inventory.notes ||
                    `These are internal notes. @sec[${group.section}]`,
                  tags: group.inventory.tags || "john drew don",
                  inventoryId: group.inventory.inventoryId,
                  offerId: group.inventory.offerId,
                  splitType: group.inventory.splitType || "CUSTOM",
                  publicNotes:
                    group.inventory.publicNotes || `These are ${group.row} Row`,
                  listPrice: increasedPrice,
                  customSplit:
                    group.inventory.customSplit ||
                    `${Math.ceil(group.inventory.quantity / 2)},${
                      group.inventory.quantity
                    }`,
                  mapping_id, // Add mapping_id to inventory
                  tickets: group.inventory.tickets.map((ticket) => ({
                    id: ticket.id,
                    seatNumber: ticket.seatNumber,
                    notes: ticket.notes,
                    cost: ticket.cost,
                    faceValue: ticket.faceValue,
                    taxedCost: ticket.taxedCost,
                    sellPrice:
                      typeof ticket?.sellPrice === "number" &&
                      !isNaN(ticket?.sellPrice)
                        ? ticket.sellPrice
                        : parseFloat(ticket?.cost || ticket?.faceValue || 0), // Fallback to cost or faceValue if sellPrice is invalid
                    stockType: ticket.stockType,
                    eventId: ticket.eventId,
                    accountId: ticket.accountId,
                    status: ticket.status,
                    auditNote: ticket.auditNote,
                    mapping_id: mapping_id, // Add mapping_id to each ticket
                  })),
                },
              };
            });

            // Use fewer documents in a single batch insert
            const CHUNK_SIZE = 100;
            for (let i = 0; i < groupsToInsert.length; i += CHUNK_SIZE) {
              const chunk = groupsToInsert.slice(i, i + CHUNK_SIZE);
              await ConsecutiveGroup.insertMany(chunk, { session });
            }

            if (LOG_LEVEL >= 2) {
              this.logWithTime(
                `Updated ${groupsToInsert.length} consecutive groups for event ${eventId}`,
                "success"
              );
            }
          } else {
            if (LOG_LEVEL >= 3) {
              this.logWithTime(
                `No seat changes detected for event ${eventId}`,
                "debug"
              );
            }
          }
        }

        // Final metadata update
        await Event.updateOne(
          { Event_ID: eventId },
          { $set: { "metadata.full": metadata } }
        ).session(session);
      });

      // Update the event's update schedule for next update
      this.eventUpdateSchedule.set(
        eventId,
        moment().add(MIN_TIME_BETWEEN_EVENT_SCRAPES, "milliseconds")
      );

      if (LOG_LEVEL >= 2) {
        this.logWithTime(
          `Updated event ${eventId} in ${(
            performance.now() - startTime
          ).toFixed(2)}ms, next update by ${this.eventUpdateSchedule
            .get(eventId)
            .format("HH:mm:ss")}`,
          "success"
        );
      }
    } catch (error) {
      await this.logError(eventId, "DATABASE_ERROR", error);
      throw error;
    } finally {
      session.endSession();
    }
  }

  /**
   * Generate CSV inventory for an event for each scrape
   * @param {string} eventId - The event ID
   * @param {Array} scrapeResult - The scrape result data
   */
  async generateInventoryCsv(eventId, scrapeResult) {
    try {
      // Import the inventory controller
      const inventoryController = (
        await import("./controllers/inventoryController.js")
      ).default;

      // Get event data upfront with price increase percentage and mapping_id
      const event = await Event.findOne({ Event_ID: eventId })
        .select("")
        .lean();

      if (!event) {
        this.logWithTime(
          `Event ${eventId} not found for CSV generation`,
          "error"
        );
        return;
      }

      // Validate required fields
      if (!event.mapping_id) {
        this.logWithTime(
          `No mapping_id found for event ${eventId} in database`,
          "error"
        );
        return;
      }

      if (!event.priceIncreasePercentage) {
        this.logWithTime(
          `No price increase percentage found for event ${eventId} in database`,
          "error"
        );
        return;
      }

      const mapping_id = event.mapping_id;
      const priceIncreasePercentage = event.priceIncreasePercentage;
      const inHandDate = event?.inHandDate;

      // Filter out groups with fewer than 2 seats
      const validGroups = scrapeResult.filter(
        (group) =>
          group.seats &&
          group.seats.length >= 2 &&
          group.inventory &&
          group.inventory.quantity >= 2
      );

      if (validGroups.length === 0) {
        this.logWithTime(
          `No valid groups with at least 2 seats found for event ${eventId}`,
          "warning"
        );
        return;
      }

      // Create a Set to track unique combinations of section, row, and seats
      const uniqueGroups = new Set();

      // Format the scrape results as inventory records, preventing duplicates
      const inventoryRecords = [];
      for (const group of validGroups) {
        // Create a unique key for this group
        const uniqueKey = `${group.section}-${group.row}-${group.seats.join(
          ","
        )}`;

        // Skip if we've already processed this combination
        if (uniqueGroups.has(uniqueKey)) {
          continue;
        }
        uniqueGroups.add(uniqueKey);

        // Calculate the increased list price based on the percentage from database
        const basePrice = parseFloat(group.inventory.listPrice);
        const increasedPrice = (
          basePrice *
          (1 + priceIncreasePercentage / 100)
        ).toFixed(2);

        // Format in-hand date as YYYY-MM-DD
        const formattedInHandDate = event?.inHandDate
          ?.toISOString()
          .split("T")[0];

        // Create a record with the exact required format
        inventoryRecords.push({
          inventory_id: uuidv4(),
          event_name: event?.Event_Name || `Event ${eventId}`,
          venue_name: event?.Venue || "Unknown Venue",
          event_date: event?.Event_DateTime?.toISOString(),
          event_id: mapping_id, // Use mapping_id as event_id
          quantity: group.inventory.quantity,
          section: group.section,
          row: group.row,
          seats: group.seats.join(","),
          barcodes: "",
          internal_notes: `These are internal notes. @sec[${group.section}]`,
          public_notes: `These are ${group.row} Row`,
          tags: "",
          list_price: increasedPrice,
          face_price: group.inventory?.tickets?.[0]?.faceValue || "",
          taxed_cost: group.inventory?.tickets?.[0]?.taxedCost || "",
          cost: group.inventory?.tickets?.[0]?.cost || "",
          hide_seats: "Y",
          in_hand: "N",
          in_hand_date: formattedInHandDate,
          instant_transfer: "N",
          files_available: "N",
          split_type: "NEVERLEAVEONE",
          custom_split: `${Math.ceil(group.inventory.quantity / 2)},${
            group.inventory.quantity
          }`,
          stock_type: "MOBILE_TRANSFER",
          zone: "N",
          shown_quantity: String(Math.ceil(group.inventory.quantity / 2)),
          passthrough: "",
          mapping_id: mapping_id, // Include mapping_id in the record
          source_event_id: eventId, // Store the original Event_ID to ensure proper association
        });
      }

      // Delete old CSV files before adding new records
      inventoryController.deleteOldCsvFiles(eventId);

      // Add the inventory records in bulk
      const result = await inventoryController.addBulkInventory(
        inventoryRecords,
        eventId
      );

      if (result.success) {
        this.logWithTime(
          `Successfully generated CSV for event ${eventId}: ${result.message}`,
          "success"
        );
      } else {
        this.logWithTime(
          `Failed to generate CSV for event ${eventId}: ${result.message}`,
          "error"
        );
      }
    } catch (error) {
      this.logWithTime(
        `Error generating CSV for event ${eventId}: ${error.message}`,
        "error"
      );
    }
  }

  async scrapeEvent(eventId, retryCount = 0, proxyAgent = null, proxy = null) {
    // Skip if the event should be skipped
    if (this.shouldSkipEvent(eventId)) {
      return false;
    }

    // If circuit breaker is tripped, delay non-critical events
    if (this.apiCircuitBreaker.tripped) {
      const lastUpdate = this.eventUpdateTimestamps.get(eventId);
      const timeSinceUpdate = lastUpdate ? moment().diff(lastUpdate) : Infinity;

      // Allow processing of critical events approaching 3-minute maximum, or events approaching 2-minute target
      if (
        timeSinceUpdate < MAX_UPDATE_INTERVAL - 20000 &&
        timeSinceUpdate < MAX_ALLOWED_UPDATE_INTERVAL - 30000
      ) {
        if (LOG_LEVEL >= 2) {
          this.logWithTime(
            `Skipping ${eventId} temporarily: Circuit breaker tripped`,
            "info"
          );
        }
        return false;
      } else if (timeSinceUpdate > MAX_ALLOWED_UPDATE_INTERVAL - 30000) {
        // Log urgent processing despite circuit breaker
        if (LOG_LEVEL >= 1) {
          this.logWithTime(
            `Processing ${eventId} despite circuit breaker: Approaching maximum allowed time (${
              timeSinceUpdate / 1000
            }s since last update)`,
            "warning"
          );
        }
      }
    }

    await this.acquireSemaphore();
    this.processingEvents.add(eventId);
    this.activeJobs.set(eventId, moment());

    try {
      // Only log at higher verbosity levels
      if (LOG_LEVEL >= 2) {
        this.logWithTime(
          `Scraping ${eventId} (Attempt ${retryCount + 1})`,
          "info"
        );
      }

      // Use passed proxy if available, otherwise get a new one
      let proxyToUse = proxy;
      let proxyAgentToUse = proxyAgent;

      if (!proxyAgentToUse || !proxyToUse) {
        const { proxyAgent: newProxyAgent, proxy: newProxy } =
          this.proxyManager.getProxyForBatch([eventId]);
        proxyAgentToUse = newProxyAgent;
        proxyToUse = newProxy;
      }

      // Look up the event first before trying to scrape
      const event = await Event.findOne({ Event_ID: eventId })
        .select("Skip_Scraping inHandDate url")
        .lean();

      if (!event) {
        this.logWithTime(`Event ${eventId} not found in database`, "error");
        // Put event in cooldown to avoid immediate retries
        this.cooldownEvents.set(eventId, moment().add(60, "minutes"));
        return false;
      }

      if (event.Skip_Scraping) {
        if (LOG_LEVEL >= 2) {
          this.logWithTime(`Skipping ${eventId} (flagged)`, "info");
        }
        // Still update the schedule for next time
        this.eventUpdateSchedule.set(
          eventId,
          moment().add(MIN_TIME_BETWEEN_EVENT_SCRAPES, "milliseconds")
        );
        return true;
      }

      // Check if cookies have been refreshed recently
      const lastRefresh = this.headerRefreshTimestamps.get(eventId) || 0;
      const cookieAge = Date.now() - lastRefresh;

      // Force header refresh if cookies are more than 15 minutes old
      // or if this is a retry attempt (to get fresh cookies)
      const shouldRefreshHeaders =
        !this.headersCache.has(eventId) ||
        cookieAge > COOKIE_EXPIRATION_MS / 2 ||
        retryCount > 0;

      if (shouldRefreshHeaders) {
        if (LOG_LEVEL >= 2) {
          this.logWithTime(
            `Getting fresh headers for event ${eventId}${
              retryCount > 0 ? " (retry attempt)" : ""
            }`,
            "debug"
          );
        }
        await this.refreshEventHeaders(eventId);
      } else if (LOG_LEVEL >= 3) {
        this.logWithTime(
          `Using cached headers for event ${eventId} (age: ${Math.floor(
            cookieAge / 1000
          )}s)`,
          "debug"
        );
      }

      // Get the cached headers or defaults
      const headers =
        this.headersCache.get(eventId) ||
        (await this.refreshEventHeaders(eventId));
      if (!headers) {
        throw new Error("Failed to obtain valid headers");
      }

      // Set a longer timeout for the scrape
      const result = await Promise.race([
        ScrapeEvent({ eventId, headers }),
        setTimeout(SCRAPE_TIMEOUT).then(() => {
          throw new Error("Scrape timed out");
        }),
      ]);

      if (!result) {
        throw new Error("Empty scrape result: null or undefined returned");
      }

      if (!Array.isArray(result)) {
        throw new Error(
          `Invalid scrape result: expected array but got ${typeof result}`
        );
      }

      if (result.length === 0) {
        throw new Error("Empty scrape result: array is empty");
      }

      // Mark this header as successful
      if (headers) {
        const headerKey =
          headers.headers.Cookie?.substring(0, 20) ||
          headers.headers["User-Agent"]?.substring(0, 20);
        const currentSuccessRate = this.headerSuccessRates.get(headerKey) || {
          success: 0,
          failure: 0,
        };
        currentSuccessRate.success++;
        this.headerSuccessRates.set(headerKey, currentSuccessRate);

        // Add to rotation pool if not already there
        if (
          !this.headerRotationPool.some(
            (h) =>
              h.headers.Cookie?.substring(0, 20) === headerKey ||
              h.headers["User-Agent"]?.substring(0, 20) === headerKey
          )
        ) {
          this.headerRotationPool.push(headers);
          // Limit pool size
          if (this.headerRotationPool.length > 10) {
            this.headerRotationPool.shift();
          }
        }
      }

      await this.updateEventMetadata(eventId, result);

      // Generate fresh CSV inventory for each scrape
      await this.generateInventoryCsv(eventId, result);

      // Success! If this event was previously failing, update its status in DB
      const recentFailures = this.getRecentFailureCount(eventId);
      if (recentFailures > 0) {
        try {
          // Mark event as active again after successful scrape
          await Event.updateOne(
            { Event_ID: eventId },
            { $set: { Skip_Scraping: false, status: "active" } }
          );
          this.logWithTime(
            `Reactivated previously failing event: ${eventId}`,
            "success"
          );
        } catch (err) {
          console.error(`Failed to update status for event ${eventId}:`, err);
        }
      }

      this.successCount++;
      this.lastSuccessTime = moment();
      this.eventUpdateTimestamps.set(eventId, moment());
      this.failedEvents.delete(eventId);
      this.clearFailureCount(eventId);

      // Reset API error counter on success
      this.apiCircuitBreaker.failures = Math.max(
        0,
        this.apiCircuitBreaker.failures - 1
      );

      // Reset global consecutive error counter on success
      this.globalConsecutiveErrors = 0;

      // Increase retry limit by 5 after a successful scrape
      const prevMax = this.eventMaxRetries.get(eventId) ?? MAX_RETRIES;
      const newMax = prevMax + 3;
      this.eventMaxRetries.set(eventId, newMax);
      this.logWithTime(
        `Increased retry limit for ${eventId} to ${newMax}`,
        "info"
      );

      return true;
    } catch (error) {
      this.failedEvents.add(eventId);

      // New: Try to handle the API error with smart strategies first
      const errorHandled = await this.handleApiError(
        eventId,
        error,
        this.headersCache.get(eventId)
      );

      if (errorHandled) {
        // If we handled the error with our special strategies, use a very short cooldown
        // and don't increment the failure count (we're trying a different approach)
        const shortCooldown = 1000; // 1 second
        this.cooldownEvents.set(
          eventId,
          moment().add(shortCooldown, "milliseconds")
        );

        // Put directly into retry queue with same retry count (don't increment)
        this.retryQueue.push({
          eventId,
          retryCount: retryCount, // Keep same retry count since we're trying a different approach
          retryAfter: moment().add(shortCooldown, "milliseconds"),
        });

        this.logWithTime(
          `API error for ${eventId} handled with special strategy, retrying in 1s`,
          "warning"
        );

        return false;
      }

      // Normal error handling for non-API errors
      this.incrementFailureCount(eventId);

      await this.logError(eventId, "SCRAPE_ERROR", error, { retryCount });

      // Get current failure count for this event
      const recentFailures = this.getRecentFailureCount(eventId);

      // Apply the new short cooldown strategy
      let backoffTime;
      let shouldMarkStopped = false;

      // Use API-specific message for API errors
      const isApiError =
        error.message.includes("403") ||
        error.message.includes("400") ||
        error.message.includes("429") ||
        error.message.includes("API");

      if (retryCount < SHORT_COOLDOWNS.length) {
        // Use the short, progressive cooldowns for initial retries
        backoffTime = SHORT_COOLDOWNS[retryCount];

        // Log only at appropriate levels
        if (LOG_LEVEL >= 1) {
          this.logWithTime(
            `${isApiError ? "API error" : "Error"} for ${eventId}: ${
              error.message
            }. Short cooldown for ${backoffTime / 1000}s`,
            "warning"
          );
        }
      } else {
        // For persistent failures, use a longer cooldown
        backoffTime = LONG_COOLDOWN_MINUTES * 60 * 1000; // Convert minutes to ms

        // Only mark as stopped if the event hasn't been updated in 10 minutes
        const lastUpdate = this.eventLastUpdates.get(eventId);
        const timeSinceUpdate = lastUpdate ? moment().diff(lastUpdate) : 0;
        shouldMarkStopped = timeSinceUpdate >= EVENT_FAILURE_THRESHOLD;

        if (shouldMarkStopped) {
          this.logWithTime(
            `Persistent ${
              isApiError ? "API errors" : "errors"
            } for ${eventId}: ${
              error.message
            }. Event hasn't been successfully updated for ${
              timeSinceUpdate / 1000
            }s. Marking as stopped.`,
            "error"
          );
        } else {
          this.logWithTime(
            `Persistent ${
              isApiError ? "API errors" : "errors"
            } for ${eventId}: ${
              error.message
            }. ${LONG_COOLDOWN_MINUTES} minute cooldown before retry.`,
            "warning"
          );
        }

        // Log long cooldown to error logs
        await this.logError(
          eventId,
          "LONG_COOLDOWN",
          new Error(
            `Event put in ${LONG_COOLDOWN_MINUTES} minute cooldown after persistent failures`
          ),
          {
            cooldownDuration: LONG_COOLDOWN_MINUTES * 60 * 1000,
            isApiError,
            originalError: error.message,
            failureCount: recentFailures,
            retryCount,
          }
        );
      }

      // If we've had 3 consecutive API errors, trigger a cookie reset
      if (isApiError) {
        this.globalConsecutiveErrors++;
        if (this.globalConsecutiveErrors >= 3) {
          // Don't await here to prevent blocking the current event processing
          this.resetCookiesAndHeaders().catch((e) =>
            console.error("Error during cookie reset:", e)
          );
        }
      }

      // Set the cooldown
      const cooldownUntil = moment().add(backoffTime, "milliseconds");
      this.cooldownEvents.set(eventId, cooldownUntil);

      // Only update the database if the event has been inactive for 10 minutes
      const lastUpdate = this.eventUpdateTimestamps.get(eventId);
      const timeSinceUpdate = lastUpdate ? moment().diff(lastUpdate) : 0;
      const shouldMarkStoppedInDB = timeSinceUpdate >= EVENT_FAILURE_THRESHOLD;

      if (shouldMarkStoppedInDB) {
        // Mark event as stopped in database
        try {
          await Event.updateOne(
            { Event_ID: eventId },
            {
              $set: {
                Skip_Scraping: true,
                status: "stopped",
                stopReason: isApiError ? "API Error" : "Persistent Failure",
                lastErrorMessage: error.message,
                lastErrorTime: new Date(),
              },
            }
          );
          if (LOG_LEVEL >= 1) {
            this.logWithTime(
              `Marked event ${eventId} as stopped in database`,
              "warning"
            );
          }
        } catch (err) {
          console.error(`Failed to update status for event ${eventId}:`, err);
        }
      }

      // Still update the event schedule for 2-minute compliance
      this.eventUpdateSchedule.set(
        eventId,
        moment().add(MIN_TIME_BETWEEN_EVENT_SCRAPES, "milliseconds")
      );

      const maxRetriesForEvent =
        this.eventMaxRetries.get(eventId) ?? MAX_RETRIES;

      // Instead of using retryCount < maxRetriesForEvent, we'll use time-based condition
      // Only stop retrying if we've hit the failure threshold
      if (!shouldMarkStopped) {
        this.retryQueue.push({
          eventId,
          retryCount: retryCount + 1,
          retryAfter: cooldownUntil,
        });

        if (LOG_LEVEL >= 2) {
          this.logWithTime(
            `Queued for retry: ${eventId} (after ${
              backoffTime / 1000
            }s cooldown)`,
            "info"
          );
        }
      } else {
        this.logWithTime(
          `Stopping retries for ${eventId} after ${
            timeSinceUpdate / 1000
          }s without successful updates`,
          "error"
        );
      }
    } finally {
      this.activeJobs.delete(eventId);
      this.processingEvents.delete(eventId);
      this.releaseSemaphore();

      // Only release proxy if we created it in this method (not if it was passed in)
      if (!proxyAgent && !proxy) {
        this.proxyManager.releaseProxy(eventId);
      }
    }
  }

  getRecentFailureCount(eventId) {
    const count = this.eventFailureCounts.get(eventId) || 0;
    return count;
  }

  incrementFailureCount(eventId) {
    const currentCount = this.eventFailureCounts.get(eventId) || 0;
    this.eventFailureCounts.set(eventId, currentCount + 1);
    this.eventFailureTimes.set(eventId, moment());
  }

  clearFailureCount(eventId) {
    this.eventFailureCounts.delete(eventId);
    this.eventFailureTimes.delete(eventId);
  }

  async processBatch(eventIds) {
    // For very large batches, process parallel but with throttling
    const results = [];
    const failed = [];

    if (eventIds.length <= 0) {
      return { failed };
    }

    // Get available proxy count to better determine batch sizing
    const availableProxies = this.proxyManager.getAvailableProxyCount();

    // Dynamic chunk sizing based on available resources and event count
    const optimalChunkSize = Math.min(
      Math.max(25, Math.ceil(eventIds.length / Math.max(1, availableProxies))),
      50 // Maximum chunk size to prevent memory issues
    );

    if (LOG_LEVEL >= 1) {
      this.logWithTime(
        `Processing ${eventIds.length} events using ${availableProxies} proxies with chunk size ${optimalChunkSize}`,
        "info"
      );
    }

    // Split into chunks for better control
    const chunks = [];
    for (let i = 0; i < eventIds.length; i += optimalChunkSize) {
      chunks.push(eventIds.slice(i, i + optimalChunkSize));
    }

    // Process chunks with controlled parallelism and better error handling
    let successCount = 0;
    let failureCount = 0;

    // Process in batches with maximum parallelism
    const batchSize = Math.min(16, Math.ceil(chunks.length / 4)); // Process up to 16 chunks in parallel
    const batchCount = Math.ceil(chunks.length / batchSize);

    for (let batchIndex = 0; batchIndex < batchCount; batchIndex++) {
      const batchStart = batchIndex * batchSize;
      const batchEnd = Math.min(batchStart + batchSize, chunks.length);
      const batchChunks = chunks.slice(batchStart, batchEnd);

      // Process each chunk in the batch
      const batchProcesses = batchChunks.map(async (chunk) => {
        const batchId =
          Date.now().toString(36) + Math.random().toString(36).substr(2, 5);

        try {
          // Get proxy data for this chunk
          const proxyData = await this.proxyManager.getProxyForBatch(chunk);

          if (!proxyData || !proxyData.proxyAgent) {
            throw new Error("Failed to obtain proxy for batch");
          }

          // Process the chunk efficiently
          const result = await this.processBatchEfficiently(
            chunk,
            batchId,
            proxyData.proxyAgent,
            proxyData.proxy,
            proxyData.eventProxyMap
          );

          successCount += result.results.length;
          failureCount += result.failed.length;
          results.push(...result.results);
          failed.push(...result.failed);

          // Update proxy health based on results
          if (result.results.length > 0 && proxyData.proxy) {
            this.proxyManager.updateProxyHealth(proxyData.proxy.proxy, true);
          }

          if (
            result.failed.length > result.results.length &&
            result.failed.length > 0 &&
            proxyData.proxy
          ) {
            this.proxyManager.updateProxyHealth(proxyData.proxy.proxy, false);
          }
        } catch (error) {
          this.logWithTime(
            `Error processing batch ${batchId}: ${error.message}`,
            "error"
          );

          // Handle proxy errors
          if (this.batchProxies.has(batchId)) {
            const { proxy, chunk } = this.batchProxies.get(batchId);
            if (proxy && proxy.proxy) {
              this.proxyManager.updateProxyHealth(proxy.proxy, false);
              this.proxyManager.releaseProxyBatch(chunk);
            }
          }

          // Add failed events to retry queue
          failed.push(
            ...chunk.map((eventId) => ({
              eventId,
              error: new Error(`Batch processing error: ${error.message}`),
            }))
          );
          failureCount += chunk.length;
        } finally {
          // Clean up batch tracking
          this.batchProxies.delete(batchId);
        }
      });

      // Wait for this batch of chunks to complete
      await Promise.all(batchProcesses);

      // Add a small delay between batches to prevent rate limiting
      if (batchIndex < batchCount - 1) {
        await setTimeout(500); // Increased delay between batches
      }
    }

    // Log final statistics
    if (LOG_LEVEL >= 1) {
      this.logWithTime(
        `Completed processing ${eventIds.length} events: ${successCount} successful, ${failureCount} failed`,
        "info"
      );
    }

    return { results, failed };
  }

  // New method to process a batch of events efficiently by handling API requests together
  async processBatchEfficiently(
    eventIds,
    batchId,
    proxyAgent,
    proxy,
    eventProxyMap
  ) {
    const results = [];
    const failed = [];

    // Define sharedHeaders at the top level of the function
    let sharedHeaders = null;

    // Skip excessive logging for small batches to reduce log noise
    const shouldLogDetails = LOG_LEVEL >= 2 && eventIds.length > 5;

    // Pre-filter events that should be skipped to avoid unnecessary DB lookups
    const filteredIds = eventIds.filter(
      (eventId) => !this.shouldSkipEvent(eventId)
    );

    if (filteredIds.length === 0) {
      if (LOG_LEVEL >= 1) {
        this.logWithTime(
          `All events in batch ${batchId} should be skipped, skipping validation`,
          "warning"
        );
      }
      return { results, failed };
    }

    // Group events by domain for better efficiency
    const eventsByDomain = {};

    // Preload all event validations in parallel to save time
    const validationPromises = filteredIds.map(async (eventId) => {
      try {
        const event = await Event.findOne({ Event_ID: eventId })
          .select("Skip_Scraping inHandDate url")
          .lean();

        if (!event) {
          this.cooldownEvents.set(eventId, moment().add(60, "minutes"));
          return { eventId, valid: false, reason: "not_found" };
        }

        if (event.Skip_Scraping) {
          this.eventUpdateSchedule.set(
            eventId,
            moment().add(MIN_TIME_BETWEEN_EVENT_SCRAPES, "milliseconds")
          );
          return { eventId, valid: false, reason: "flagged" };
        }

        // Group by domain
        if (event.url) {
          try {
            const url = new URL(event.url);
            const domain = url.hostname;
            if (!eventsByDomain[domain]) {
              eventsByDomain[domain] = [];
            }
            eventsByDomain[domain].push(eventId);
          } catch (e) {
            // Invalid URL, just add to default group
            if (!eventsByDomain["default"]) {
              eventsByDomain["default"] = [];
            }
            eventsByDomain["default"].push(eventId);
          }
        } else {
          // No URL, add to default group
          if (!eventsByDomain["default"]) {
            eventsByDomain["default"] = [];
          }
          eventsByDomain["default"].push(eventId);
        }

        return { eventId, valid: true, url: event.url };
      } catch (error) {
        return { eventId, valid: false, reason: "error", error };
      }
    });

    const validations = await Promise.all(validationPromises);

    // Filter to valid events
    const validEventDetails = validations.filter((v) => v.valid);

    if (validEventDetails.length === 0) {
      return { results, failed };
    }

    // Get a shared set of headers for this batch to reduce header generation overhead
    try {
      sharedHeaders = await this.refreshEventHeaders(
        validEventDetails[0].eventId
      );
      if (!sharedHeaders) {
        this.logWithTime(
          "Primary header retrieval failed, trying alternative events",
          "warning"
        );

        // Try up to 3 other events in case the first one fails
        for (let i = 1; i < Math.min(validEventDetails.length, 4); i++) {
          try {
            sharedHeaders = await this.refreshEventHeaders(
              validEventDetails[i].eventId
            );
            if (sharedHeaders) {
              this.logWithTime(
                `Successfully obtained headers from alternative event ${validEventDetails[i].eventId}`,
                "info"
              );
              break;
            }
          } catch (err) {
            this.logWithTime(
              `Alternative header attempt ${i} failed: ${err.message}`,
              "warning"
            );
          }
        }

        // If still no headers, create basic fallback headers
        if (!sharedHeaders) {
          this.logWithTime("Creating fallback headers for batch", "warning");
          sharedHeaders = {
            headers: {
              "User-Agent":
                "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1",
              Accept:
                "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
              "Accept-Language": "en-US,en;q=0.5",
              Referer: "https://www.ticketmaster.com/",
              "X-Forwarded-For": generateRandomIp(),
              "X-Request-Id":
                Math.random().toString(36).substring(2, 10) +
                "-" +
                Date.now().toString(36),
            },
            cookies: [],
            fingerprint: {},
          };
        }
      }
    } catch (error) {
      this.logWithTime(
        `Failed to obtain shared headers for batch: ${error.message}`,
        "error"
      );

      // Create emergency fallback headers instead of failing the whole batch
      this.logWithTime(
        "Creating emergency fallback headers after error",
        "warning"
      );
      sharedHeaders = {
        headers: {
          "User-Agent":
            "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1",
          Accept:
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
          "Accept-Language": "en-US,en;q=0.5",
          Referer: "https://www.ticketmaster.com/",
          "X-Forwarded-For": generateRandomIp(),
          "X-Request-Id":
            Math.random().toString(36).substring(2, 10) +
            "-" +
            Date.now().toString(36),
        },
        cookies: [],
        fingerprint: {},
      };
    }

    // Process domains in parallel for better efficiency
    await Promise.all(
      Object.entries(eventsByDomain).map(async ([domain, domainEvents]) => {
        // Get domain-specific headers if possible
        let domainHeaders = sharedHeaders;
        if (domain !== "default" && domainEvents.length > 0) {
          try {
            domainHeaders = await this.refreshEventHeaders(domainEvents[0]);
            if (!domainHeaders) {
              // Fall back to shared headers
              domainHeaders = sharedHeaders;
            }
          } catch (error) {
            // Fall back to shared headers
            this.logWithTime(
              `Failed to get domain-specific headers: ${error.message}`,
              "warning"
            );
            domainHeaders = sharedHeaders;
          }
        }

        // Process up to 20 events at a time per domain for maximum throughput
        const domainConcurrentLimit = 20; // Doubled from 10 to 20

        // Split domain events into smaller groups
        for (let i = 0; i < domainEvents.length; i += domainConcurrentLimit) {
          const concurrentBatch = domainEvents.slice(
            i,
            i + domainConcurrentLimit
          );

          // Process events in this domain batch concurrently
          await Promise.all(
            concurrentBatch.map(async (eventId) => {
              // Skip if invalidated
              if (!validations.find((v) => v.eventId === eventId && v.valid)) {
                return;
              }

              try {
                // Get event-specific proxy from map if available
                let eventProxyAgent = proxyAgent;
                let eventProxy = proxy;
                let usingEventSpecificProxy = false;

                if (eventProxyMap && eventProxyMap.has(eventId)) {
                  const eventProxyData = eventProxyMap.get(eventId);
                  if (eventProxyData && eventProxyData.proxyAgent) {
                    eventProxyAgent = eventProxyData.proxyAgent;
                    eventProxy = eventProxyData.proxy;
                    usingEventSpecificProxy = true;

                    if (LOG_LEVEL >= 3) {
                      this.logWithTime(
                        `Using dedicated proxy ${eventProxy.proxy.substring(
                          0,
                          15
                        )}... for event ${eventId}`,
                        "debug"
                      );
                    }
                  }
                }

                if (!usingEventSpecificProxy && LOG_LEVEL >= 3) {
                  this.logWithTime(
                    `No dedicated proxy for event ${eventId}, using shared proxy`,
                    "debug"
                  );
                }

                // Use domain-specific headers if available
                const result = await this.scrapeEvent(
                  eventId,
                  0, // retryCount = 0 for initial attempt
                  eventProxyAgent,
                  eventProxy,
                  domainHeaders // Pass domain-specific headers
                );

                if (result !== false) {
                  results.push({ eventId, result });

                  // Mark this proxy as successful
                  if (usingEventSpecificProxy && eventProxy) {
                    this.proxyManager.updateProxyHealth(eventProxy.proxy, true);
                  }
                } else {
                  // If using a dedicated proxy for this event and it failed, mark it as having issues
                  if (usingEventSpecificProxy && eventProxy) {
                    this.proxyManager.updateProxyHealth(
                      eventProxy.proxy,
                      false
                    );
                  }
                }
              } catch (error) {
                failed.push({ eventId, error });

                // Mark this proxy as having issues if using a dedicated one
                if (eventProxyMap && eventProxyMap.has(eventId)) {
                  const eventProxyData = eventProxyMap.get(eventId);
                  if (eventProxyData && eventProxyData.proxy) {
                    this.proxyManager.updateProxyHealth(
                      eventProxyData.proxy.proxy,
                      false
                    );
                  }
                }
              } finally {
                // Release the proxy for this event
                if (eventProxyMap && eventProxyMap.has(eventId)) {
                  this.proxyManager.releaseProxy(eventId);
                }
              }
            })
          );

          // Add a small delay between batches within the same domain
          if (i + domainConcurrentLimit < domainEvents.length) {
            await setTimeout(500);
          }
        }
      })
    );

    return { results, failed };
  }

  // Helper method to handle event errors consistently
  async handleEventError(eventId, error, retryCount, failedList) {
    this.failedEvents.add(eventId);

    // Track failures by event ID
    if (!this.eventFailures.has(eventId)) {
      this.eventFailures.set(eventId, []);
    }

    // Keep only the most recent failures (sliding window)
    const failures = this.eventFailures.get(eventId);
    failures.push({
      time: moment(),
      error: error.message,
      isApiError: error.isApiError || false,
    });

    // Limit failure history to last 20 failures
    if (failures.length > 20) {
      failures.shift();
    }

    // Get the last update time for the event
    const lastUpdate = this.eventLastUpdates.get(eventId);
    const timeSinceUpdate = lastUpdate ? moment().diff(lastUpdate) : 0;

    // Determine if this is a persistent failure that should be marked as stopped
    const shouldMarkStopped = timeSinceUpdate >= EVENT_FAILURE_THRESHOLD;

    // Apply the cooldown strategy
    let backoffTime;

    // Use API-specific message for API errors
    const isApiError =
      error.message.includes("403") ||
      error.message.includes("400") ||
      error.message.includes("429") ||
      error.message.includes("API");

    const recentFailures = this.getRecentFailureCount(eventId);

    if (retryCount < SHORT_COOLDOWNS.length) {
      // Use the short, progressive cooldowns for initial retries
      backoffTime = SHORT_COOLDOWNS[retryCount];

      // Log only at appropriate levels
      if (LOG_LEVEL >= 1) {
        this.logWithTime(
          `${isApiError ? "API error" : "Error"} for ${eventId}: ${
            error.message
          }. Aggressive retry in ${backoffTime / 1000}s`,
          "warning"
        );
      }
    } else {
      // For persistent failures, use a shorter cooldown
      backoffTime = LONG_COOLDOWN_MINUTES * 60 * 1000; // 1 minute cooldown

      if (shouldMarkStopped) {
        this.logWithTime(
          `Persistent ${isApiError ? "API errors" : "errors"} for ${eventId}: ${
            error.message
          }. Event hasn't been successfully updated for ${
            timeSinceUpdate / 1000
          }s. Marking as stopped.`,
          "error"
        );
      } else {
        this.logWithTime(
          `Persistent ${isApiError ? "API errors" : "errors"} for ${eventId}: ${
            error.message
          }. Aggressive retry in ${LONG_COOLDOWN_MINUTES} minute.`,
          "warning"
        );
      }

      // Log long cooldown to error logs if needed
      await this.logError(
        eventId,
        "AGGRESSIVE_RETRY",
        new Error(
          `Event put in ${LONG_COOLDOWN_MINUTES} minute aggressive retry after persistent failures`
        ),
        {
          cooldownDuration: LONG_COOLDOWN_MINUTES * 60 * 1000,
          isApiError,
          originalError: error.message,
          failureCount: recentFailures,
          retryCount,
        }
      );
    }

    // If we've had 2 consecutive API errors, trigger a cookie reset (reduced from 3)
    if (isApiError) {
      this.globalConsecutiveErrors++;
      if (this.globalConsecutiveErrors >= 2) {
        // Don't await here to prevent blocking the current event processing
        this.resetCookiesAndHeaders().catch((e) =>
          console.error("Error during cookie reset:", e)
        );
      }
    }

    // Set the cooldown
    const cooldownUntil = moment().add(backoffTime, "milliseconds");
    this.cooldownEvents.set(eventId, cooldownUntil);

    // Mark event as stopped in database if it's a persistent failure
    if (shouldMarkStopped) {
      try {
        await Event.updateOne(
          { Event_ID: eventId },
          {
            $set: {
              Skip_Scraping: true,
              status: "stopped",
              stopReason: isApiError ? "API Error" : "Persistent Failure",
              lastErrorMessage: error.message,
              lastErrorTime: new Date(),
            },
          }
        );
        if (LOG_LEVEL >= 1) {
          this.logWithTime(
            `Marked event ${eventId} as stopped in database`,
            "warning"
          );
        }
      } catch (err) {
        console.error(`Failed to update status for event ${eventId}:`, err);
      }
    }

    // Still update the event schedule for 2-minute compliance
    this.eventUpdateSchedule.set(
      eventId,
      moment().add(Math.min(backoffTime, 60000), "milliseconds")
    );

    // Only add to retry queue if we haven't hit the failure threshold
    if (!shouldMarkStopped) {
      this.retryQueue.push({
        eventId,
        retryCount: retryCount + 1,
        retryAfter: cooldownUntil,
        priority: Math.min(retryCount + 1, 10), // Higher priority for fewer retries
      });

      if (LOG_LEVEL >= 2) {
        this.logWithTime(
          `Queued for aggressive retry: ${eventId} (after ${
            backoffTime / 1000
          }s cooldown)`,
          "info"
        );
      }
    } else {
      this.logWithTime(
        `Stopping retries for ${eventId} after ${
          timeSinceUpdate / 1000
        }s without successful updates`,
        "error"
      );
    }
  }

  async processRetryQueue() {
    const now = moment();

    // Filter the retry queue to only include items ready for retry
    const readyForRetry = this.retryQueue.filter(
      (job) => !job.retryAfter || now.isAfter(job.retryAfter)
    );

    if (readyForRetry.length === 0) {
      return;
    }

    // Update retry queue to remove items we're processing
    this.retryQueue = this.retryQueue.filter(
      (job) => job.retryAfter && now.isBefore(job.retryAfter)
    );

    if (LOG_LEVEL >= 1) {
      this.logWithTime(
        `Processing ${readyForRetry.length} events from retry queue`,
        "info"
      );
    }

    // For larger retry queues, process them all at once rather than by retry count
    if (readyForRetry.length > 30) {
      // Extract all event IDs, regardless of retry count
      const allRetryEventIds = readyForRetry.map((job) => job.eventId);

      // Process all at once using the main batch processor
      await this.processBatch(allRetryEventIds);
      return;
    }

    // For smaller queues, continue with grouped processing
    // Group by retry count for better handling
    const retryGroups = {};
    readyForRetry.forEach((job) => {
      if (!retryGroups[job.retryCount]) {
        retryGroups[job.retryCount] = [];
      }
      retryGroups[job.retryCount].push(job.eventId);
    });

    // Process each retry group as a batch, prioritizing lower retry counts first
    const prioritizedGroups = Object.entries(retryGroups).sort(
      ([countA], [countB]) => parseInt(countA) - parseInt(countB)
    );

    for (const [retryCount, eventIds] of prioritizedGroups) {
      if (!this.isRunning) break;

      if (LOG_LEVEL >= 2) {
        this.logWithTime(
          `Processing batch of ${eventIds.length} retries (attempt ${
            parseInt(retryCount) + 1
          })`,
          "info"
        );
      }

      // Determine optimal batch size - use larger batches for better efficiency
      const batchSize = Math.min(
        eventIds.length,
        Math.max(30, Math.ceil(CONCURRENT_LIMIT / 2))
      );

      // Split into batches if needed
      const batches = [];
      for (let i = 0; i < eventIds.length; i += batchSize) {
        batches.push(eventIds.slice(i, i + batchSize));
      }

      // Process all batches of this retry count in parallel
      await Promise.all(
        batches.map(async (batch) => {
          try {
            // Use processBatch directly for maximum efficiency
            await this.processBatch(batch);
          } catch (error) {
            this.logWithTime(
              `Error processing retry batch: ${error.message}`,
              "error"
            );
          }
        })
      );

      // Very minimal delay between different retry count groups to maximize throughput
      await setTimeout(200);
    }
  }

  async getEventsToProcess() {
    try {
      const now = moment();

      // Find all active events due for update
      const urgentEvents = await Event.find({
        Skip_Scraping: { $ne: true },
        $or: [
          { Last_Updated: { $exists: false } },
          {
            Last_Updated: { $lt: now.clone().subtract(30, "seconds").toDate() },
          }, // Reduced from 45 seconds to 30 seconds
        ],
      })
        .select("Event_ID url Last_Updated")
        .sort({ Last_Updated: 1 }) // Oldest first
        .limit(BATCH_SIZE * 50) // Get a much larger sample for comprehensive batch processing
        .lean();

      // No events needing update
      if (!urgentEvents.length) {
        return [];
      }

      // Get current time as unix timestamp
      const currentTime = now.valueOf();

      // Calculate priority score for each event
      const prioritizedEvents = urgentEvents.map((event) => {
        const lastUpdated = event.Last_Updated
          ? moment(event.Last_Updated).valueOf()
          : 0;
        const timeSinceLastUpdate = currentTime - lastUpdated;

        // Higher score = higher priority
        let priorityScore = timeSinceLastUpdate / 1000; // Convert to seconds

        // CRITICAL - Events approaching 2-minute maximum threshold (absolute deadline)
        if (timeSinceLastUpdate > 110000) {
          // Within 10 seconds of 2-minute max
          priorityScore = priorityScore * 100; // 100x priority boost for events approaching max allowed time
        }
        // VERY HIGH - Events approaching 1.5 minutes
        else if (timeSinceLastUpdate > 80000) {
          // Within 10 seconds of 1.5-minute target
          priorityScore = priorityScore * 50; // 50x priority boost
        }
        // HIGH - Events over 1 minute
        else if (timeSinceLastUpdate > 60000) {
          // Over 1 minute
          priorityScore = priorityScore * 20; // 20x priority boost
        }
        // MEDIUM - Events over 45 seconds
        else if (timeSinceLastUpdate > 45000) {
          // Over 45 seconds
          priorityScore = priorityScore * 10; // 10x priority boost
        }

        return {
          eventId: event.Event_ID,
          url: event.url,
          lastUpdated: lastUpdated,
          priorityScore,
          timeSinceLastUpdate,
        };
      });

      // Sort by priority score (highest first)
      prioritizedEvents.sort((a, b) => b.priorityScore - a.priorityScore);

      // Smart batching: Group events by domain/URL patterns to maximize proxy efficiency
      const domains = new Map();
      prioritizedEvents.forEach((event) => {
        if (event.url) {
          try {
            const url = new URL(event.url);
            const domain = url.hostname;
            if (!domains.has(domain)) {
              domains.set(domain, []);
            }
            domains.get(domain).push(event);
          } catch (e) {
            // Invalid URL, just use the eventId as key
            if (!domains.has(event.eventId)) {
              domains.set(event.eventId, []);
            }
            domains.get(event.eventId).push(event);
          }
        } else {
          // No URL, use eventId as domain group
          if (!domains.has(event.eventId)) {
            domains.set(event.eventId, []);
          }
          domains.get(event.eventId).push(event);
        }
      });

      // Reassemble prioritized list, keeping domain groups together but prioritizing critical events
      const optimizedEventList = [];

      // First add all near-maximum events (approaching 2 min deadline)
      const nearMaxEvents = prioritizedEvents.filter(
        (event) => event.timeSinceLastUpdate > 110000
      );
      nearMaxEvents.forEach((event) => {
        optimizedEventList.push(event.eventId);
      });

      // Then add events approaching 1.5-minute target
      const approachingTargetEvents = prioritizedEvents.filter(
        (event) =>
          event.timeSinceLastUpdate > 80000 &&
          event.timeSinceLastUpdate <= 110000 &&
          !nearMaxEvents.some((maxEvent) => maxEvent.eventId === event.eventId)
      );
      approachingTargetEvents.forEach((event) => {
        optimizedEventList.push(event.eventId);
      });

      // Then add events over 1 minute
      const over60SecEvents = prioritizedEvents.filter(
        (event) =>
          event.timeSinceLastUpdate > 60000 &&
          event.timeSinceLastUpdate <= 80000 &&
          !nearMaxEvents.some(
            (maxEvent) => maxEvent.eventId === event.eventId
          ) &&
          !approachingTargetEvents.some(
            (targetEvent) => targetEvent.eventId === event.eventId
          )
      );
      over60SecEvents.forEach((event) => {
        optimizedEventList.push(event.eventId);
      });

      // Then add remaining events grouped by domain
      for (const domainEvents of domains.values()) {
        // Skip events already added
        const remainingEvents = domainEvents.filter(
          (event) =>
            !nearMaxEvents.some(
              (maxEvent) => maxEvent.eventId === event.eventId
            ) &&
            !approachingTargetEvents.some(
              (targetEvent) => targetEvent.eventId === event.eventId
            ) &&
            !over60SecEvents.some(
              (overEvent) => overEvent.eventId === event.eventId
            )
        );

        // Add all remaining events from this domain/group
        remainingEvents.forEach((event) => {
          optimizedEventList.push(event.eventId);
        });
      }

      // For large batches, use a larger batch size to ensure we keep up
      const dynamicBatchSize = Math.min(
        optimizedEventList.length,
        Math.max(BATCH_SIZE, Math.ceil(optimizedEventList.length / 1.5)) // Process at least 66% of events per cycle
      );

      // Limit to the maximum batch size but make sure we don't exceed our processing capacity
      const finalEventsList = optimizedEventList.slice(0, dynamicBatchSize);

      // Log stats about the events we're processing - only at appropriate log level
      if (LOG_LEVEL >= 2) {
        const nearMaxCount = nearMaxEvents.length;
        const approachingTargetCount = approachingTargetEvents.length;
        const over60SecCount = over60SecEvents.length;
        const oldestEvent = prioritizedEvents[0];
        if (oldestEvent) {
          const ageInSeconds = oldestEvent.timeSinceLastUpdate / 1000;
          this.logWithTime(
            `Processing ${
              finalEventsList.length
            } events. Oldest event is ${ageInSeconds.toFixed(1)}s old. ` +
              `${nearMaxCount} events near 2min max, ${approachingTargetCount} events near 1.5min, ${over60SecCount} over 1min.`,
            "info"
          );
        }
      }

      return finalEventsList;
    } catch (error) {
      console.error("Error getting events to process:", error);
      return [];
    }
  }

  async startContinuousScraping() {
    if (!this.isRunning) {
      this.isRunning = true;
      this.logWithTime("Starting standard sequential scraping...", "info");

      // Main scraping loop
      while (this.isRunning) {
        try {
          // Process events from retry queue first
          const retryEvents = this.getRetryEventsToProcess();

          if (retryEvents.length > 0) {
            this.logWithTime(
              `Processing ${retryEvents.length} events from retry queue sequentially`,
              "info"
            );

            // Process each event sequentially
            for (const job of retryEvents) {
              if (!this.isRunning) break;

              try {
                // Process this individual event with its retry count
                await this.scrapeEvent(job.eventId, job.retryCount);

                // Remove from retry queue if successful
                this.retryQueue = this.retryQueue.filter(
                  (item) => item.eventId !== job.eventId
                );

                // Small pause between events
                await setTimeout(100);
              } catch (error) {
                this.logWithTime(
                  `Error processing retry event ${job.eventId}: ${error.message}`,
                  "error"
                );
              }
            }
          }

          // Get regular events to process
          const events = await this.getEvents();

          if (events.length > 0) {
            this.logWithTime(
              `Processing ${events.length} events sequentially`,
              "info"
            );

            // Process each event sequentially
            for (const eventId of events) {
              if (!this.isRunning) break;

              try {
                // Skip if already being processed
                if (this.processingEvents.has(eventId)) {
                  continue;
                }

                // Process this individual event
                await this.scrapeEvent(eventId, 0);

                // Small pause between events
                await setTimeout(100);
              } catch (error) {
                this.logWithTime(
                  `Error processing event ${eventId}: ${error.message}`,
                  "error"
                );
              }
            }
          } else {
            this.logWithTime("No events to process at this time", "info");
          }

          // Process failed events
          const failedEvents = Array.from(this.failedEvents);

          if (failedEvents.length > 0) {
            this.logWithTime(
              `Processing ${failedEvents.length} failed events sequentially`,
              "info"
            );

            for (const eventId of failedEvents) {
              if (!this.isRunning) break;

              try {
                // Skip if in cooldown
                const now = moment();
                if (
                  this.cooldownEvents.has(eventId) &&
                  now.isBefore(this.cooldownEvents.get(eventId))
                ) {
                  continue;
                }

                // Skip if already being processed
                if (this.processingEvents.has(eventId)) {
                  continue;
                }

                // Process this failed event
                const failureCount = this.getRecentFailureCount(eventId);
                await this.scrapeEvent(eventId, failureCount);

                // Small pause between events
                await setTimeout(200);
              } catch (error) {
                this.logWithTime(
                  `Error processing failed event ${eventId}: ${error.message}`,
                  "error"
                );
              }
            }
          }

          // Add a dynamic pause to avoid overloading
          const pauseTime = this.getAdaptivePauseTime();
          await setTimeout(pauseTime);
        } catch (error) {
          console.error(`Error in scraping loop: ${error.message}`);
          // Brief pause on error to avoid spinning
          await setTimeout(1000);
        }
      }
    }
  }

  // Helper method to get retry events ready to process
  getRetryEventsToProcess() {
    const now = moment();

    // Filter the retry queue to only include items ready for retry
    const readyForRetry = this.retryQueue.filter(
      (job) => !job.retryAfter || now.isAfter(job.retryAfter)
    );

    // Sort by retry count and priority
    readyForRetry.sort((a, b) => {
      // First by retry count (lower first)
      const retryDiff = a.retryCount - b.retryCount;
      if (retryDiff !== 0) return retryDiff;

      // Then by priority if available
      const aPriority = a.priority || 0;
      const bPriority = b.priority || 0;
      return bPriority - aPriority;
    });

    return readyForRetry;
  }

  async captureCookies(page, fingerprint) {
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
            `Attempt ${
              retryCount + 1
            }: Challenge detected during cookie capture`
          );

          const challengeResolved = await this.handleTicketmasterChallenge(
            page
          );
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

        fs.writeFileSync(COOKIES_FILE, JSON.stringify(cookies, null, 2));
        console.log(
          `Successfully captured cookies on attempt ${retryCount + 1}`
        );
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
  }

  // Get events to process in priority order
  async getEvents() {
    try {
      const now = moment();

      // Find all active events due for update
      const events = await Event.find({
        Skip_Scraping: { $ne: true },
        $or: [
          { Last_Updated: { $exists: false } },
          {
            Last_Updated: { $lt: now.clone().subtract(30, "seconds").toDate() },
          },
        ],
      })
        .select("Event_ID url Last_Updated")
        .sort({ Last_Updated: 1 }) // Oldest first
        .limit(50) // Reasonable limit for sequential processing
        .lean();

      if (!events.length) {
        return [];
      }

      // Get current time
      const currentTime = now.valueOf();

      // Calculate priority for each event
      const prioritizedEvents = events.map((event) => {
        const lastUpdated = event.Last_Updated
          ? moment(event.Last_Updated).valueOf()
          : 0;
        const timeSinceLastUpdate = currentTime - lastUpdated;

        // Higher score = higher priority
        let priorityScore = timeSinceLastUpdate / 1000;

        // Critical events approaching 2-minute threshold
        if (timeSinceLastUpdate > 110000) {
          priorityScore = priorityScore * 100;
        }
        // Very high - approaching 1.5 minutes
        else if (timeSinceLastUpdate > 80000) {
          priorityScore = priorityScore * 50;
        }
        // High - over 1 minute
        else if (timeSinceLastUpdate > 60000) {
          priorityScore = priorityScore * 20;
        }
        // Medium - over 45 seconds
        else if (timeSinceLastUpdate > 45000) {
          priorityScore = priorityScore * 10;
        }

        return {
          eventId: event.Event_ID,
          priorityScore,
          timeSinceLastUpdate,
        };
      });

      // Sort by priority (highest first)
      prioritizedEvents.sort((a, b) => b.priorityScore - a.priorityScore);

      // Extract just the event IDs
      return prioritizedEvents.map((event) => event.eventId);
    } catch (error) {
      console.error("Error getting events to process:", error);
      return [];
    }
  }

  // Get events to process with optimized domain grouping
  async getEvents() {
    try {
      const now = moment();

      // Find all active events due for update - increased limit for large-scale processing
      const events = await Event.find({
        Skip_Scraping: { $ne: true },
        $or: [
          { Last_Updated: { $exists: false } },
          {
            Last_Updated: { $lt: now.clone().subtract(30, "seconds").toDate() },
          },
        ],
      })
        .select("Event_ID url Last_Updated")
        .sort({ Last_Updated: 1 }) // Oldest first
        .limit(DB_FETCH_LIMIT) // Fetch more events for efficient DB interaction
        .lean();

      if (!events.length) {
        return {
          prioritizedEvents: [],
          domainGroups: {},
        };
      }

      // Get current time
      const currentTime = now.valueOf();

      // Calculate priority for each event
      const prioritizedEvents = events.map((event) => {
        const lastUpdated = event.Last_Updated
          ? moment(event.Last_Updated).valueOf()
          : 0;
        const timeSinceLastUpdate = currentTime - lastUpdated;

        // Higher score = higher priority
        let priorityScore = timeSinceLastUpdate / 1000;

        // Critical events approaching 2-minute threshold
        if (timeSinceLastUpdate > 110000) {
          priorityScore = priorityScore * 100;
        }
        // Very high - approaching 1.5 minutes
        else if (timeSinceLastUpdate > 80000) {
          priorityScore = priorityScore * 50;
        }
        // High - over 1 minute
        else if (timeSinceLastUpdate > 60000) {
          priorityScore = priorityScore * 20;
        }
        // Medium - over 45 seconds
        else if (timeSinceLastUpdate > 45000) {
          priorityScore = priorityScore * 10;
        }

        return {
          eventId: event.Event_ID,
          url: event.url,
          priorityScore,
          timeSinceLastUpdate,
        };
      });

      // Sort by priority (highest first)
      prioritizedEvents.sort((a, b) => b.priorityScore - a.priorityScore);

      // Group events by domain for efficient proxy utilization
      const domainGroups = {};

      prioritizedEvents.forEach((event) => {
        if (!event.url) {
          // No URL, use eventId as domain group
          const key = `no-url-${event.eventId}`;
          if (!domainGroups[key]) domainGroups[key] = [];
          domainGroups[key].push(event.eventId);
          return;
        }

        try {
          const url = new URL(event.url);
          const domain = url.hostname;
          if (!domainGroups[domain]) domainGroups[domain] = [];
          domainGroups[domain].push(event.eventId);
        } catch (e) {
          // Invalid URL, use eventId as domain group
          const key = `invalid-url-${event.eventId}`;
          if (!domainGroups[key]) domainGroups[key] = [];
          domainGroups[key].push(event.eventId);
        }
      });

      return {
        prioritizedEvents: prioritizedEvents.map((e) => e.eventId),
        domainGroups,
      };
    } catch (error) {
      console.error("Error getting events to process:", error);
      return {
        prioritizedEvents: [],
        domainGroups: {},
      };
    }
  }

  // Optimized continuous scraping for handling 10,000+ events
  async startContinuousScraping() {
    if (!this.isRunning) {
      this.isRunning = true;
      this.logWithTime(
        "Starting optimized hybrid scraping for 10,000+ events...",
        "info"
      );

      // Start CSV upload cycle immediately (non-blocking)
      if (ENABLE_CSV_UPLOAD) {
        this.runCsvUploadCycle();

        // Set up recurring CSV upload cycle every 6 minutes
        this.csvUploadIntervalId = setInterval(() => {
          // Each interval call is non-blocking
          this.runCsvUploadCycle();
        }, CSV_UPLOAD_INTERVAL);
      }
      // Main scraping loop
      while (this.isRunning) {
        try {
          // Process retry events - use concurrent execution for efficiency
          const retryEvents = this.getRetryEventsToProcess();

          if (retryEvents.length > 0) {
            this.logWithTime(
              `Processing ${retryEvents.length} retry events with optimized concurrent execution`,
              "info"
            );

            // Group retry events by domain for better proxy utilization
            const retryDomainGroups = {};

            for (const job of retryEvents) {
              // Find the event URL
              const event = await Event.findOne({ Event_ID: job.eventId })
                .select("url")
                .lean();

              let domainKey = "unknown";

              if (event?.url) {
                try {
                  const url = new URL(event.url);
                  domainKey = url.hostname;
                } catch (e) {
                  domainKey = `unknown-${job.eventId}`;
                }
              } else {
                domainKey = `unknown-${job.eventId}`;
              }

              if (!retryDomainGroups[domainKey])
                retryDomainGroups[domainKey] = [];
              retryDomainGroups[domainKey].push({
                eventId: job.eventId,
                retryCount: job.retryCount,
              });
            }

            // Process each domain group concurrently with shared proxies
            await Promise.all(
              Object.values(retryDomainGroups).map(async (domainJobs) => {
                // Get a proxy for this domain group
                const { proxyAgent, proxy } =
                  this.proxyManager.getProxyForBatch(
                    domainJobs.map((j) => j.eventId)
                  );

                // Process domain jobs in concurrent batches
                for (
                  let i = 0;
                  i < domainJobs.length;
                  i += CONCURRENT_DOMAIN_LIMIT
                ) {
                  const batchJobs = domainJobs.slice(
                    i,
                    i + CONCURRENT_DOMAIN_LIMIT
                  );

                  // Process this batch concurrently
                  await Promise.all(
                    batchJobs.map(async (job) => {
                      try {
                        // Skip if already being processed
                        if (this.processingEvents.has(job.eventId)) return;

                        // Process this individual event with its retry count
                        await this.scrapeEvent(
                          job.eventId,
                          job.retryCount,
                          proxyAgent,
                          proxy
                        );

                        // Remove from retry queue if successful
                        this.retryQueue = this.retryQueue.filter(
                          (item) => item.eventId !== job.eventId
                        );
                      } catch (error) {
                        this.logWithTime(
                          `Error processing retry event ${job.eventId}: ${error.message}`,
                          "error"
                        );
                      }
                    })
                  );

                  // Small pause between batches within the same domain
                  await setTimeout(200);
                }
              })
            );
          }

          // Get regular events to process with domain grouping
          const { prioritizedEvents, domainGroups } = await this.getEvents();

          if (prioritizedEvents.length > 0) {
            this.logWithTime(
              `Processing ${prioritizedEvents.length} events with optimized concurrent execution`,
              "info"
            );

            // Extract critical events (approaching 2 min deadline) to process first
            const criticalEvents = prioritizedEvents.slice(0, 50);

            // Process critical events with max concurrency
            await Promise.all(
              criticalEvents.map(async (eventId) => {
                try {
                  // Skip if already being processed
                  if (this.processingEvents.has(eventId)) return;

                  // Process critical event
                  await this.scrapeEvent(eventId, 0);
                } catch (error) {
                  this.logWithTime(
                    `Error processing critical event ${eventId}: ${error.message}`,
                    "error"
                  );
                }
              })
            );

            // Process remaining events by domain groups
            await Promise.all(
              Object.entries(domainGroups).map(async ([domain, eventIds]) => {
                // Skip events that were processed as critical
                const remainingEvents = eventIds.filter(
                  (eventId) => !criticalEvents.includes(eventId)
                );

                if (remainingEvents.length === 0) return;

                // Get a proxy for this domain
                const { proxyAgent, proxy } =
                  this.proxyManager.getProxyForBatch(remainingEvents);

                // Process domain events in concurrent batches
                for (
                  let i = 0;
                  i < remainingEvents.length;
                  i += CONCURRENT_DOMAIN_LIMIT
                ) {
                  const batchEvents = remainingEvents.slice(
                    i,
                    i + CONCURRENT_DOMAIN_LIMIT
                  );

                  // Process this batch concurrently
                  await Promise.all(
                    batchEvents.map(async (eventId) => {
                      try {
                        // Skip if already being processed
                        if (this.processingEvents.has(eventId)) return;

                        // Process this individual event
                        await this.scrapeEvent(eventId, 0, proxyAgent, proxy);
                      } catch (error) {
                        this.logWithTime(
                          `Error processing event ${eventId}: ${error.message}`,
                          "error"
                        );
                      }
                    })
                  );

                  // Small pause between batches within the same domain
                  await setTimeout(200);
                }
              })
            );
          } else {
            this.logWithTime("No events to process at this time", "info");
          }

          // Process failed events - optimized for high concurrency
          const failedEvents = Array.from(this.failedEvents);

          if (failedEvents.length > 0) {
            this.logWithTime(
              `Processing ${failedEvents.length} failed events with optimized concurrency`,
              "info"
            );

            // Group failed events by failure count
            const failureGroups = {};

            for (const eventId of failedEvents) {
              // Skip if in cooldown
              const now = moment();
              if (
                this.cooldownEvents.has(eventId) &&
                now.isBefore(this.cooldownEvents.get(eventId))
              ) {
                continue;
              }

              // Skip if already being processed
              if (this.processingEvents.has(eventId)) {
                continue;
              }

              const failureCount = this.getRecentFailureCount(eventId);
              if (!failureGroups[failureCount])
                failureGroups[failureCount] = [];
              failureGroups[failureCount].push(eventId);
            }

            // Process failure groups in order (lower failure counts first)
            const sortedFailureCounts = Object.keys(failureGroups).sort(
              (a, b) => parseInt(a) - parseInt(b)
            );

            for (const failureCount of sortedFailureCounts) {
              const eventsInGroup = failureGroups[failureCount];

              // Group by domain for efficient proxy usage
              const failedDomainGroups = {};

              for (const eventId of eventsInGroup) {
                const event = await Event.findOne({ Event_ID: eventId })
                  .select("url")
                  .lean();

                let domainKey = "unknown";

                if (event?.url) {
                  try {
                    const url = new URL(event.url);
                    domainKey = url.hostname;
                  } catch (e) {
                    domainKey = `unknown-${eventId}`;
                  }
                } else {
                  domainKey = `unknown-${eventId}`;
                }

                if (!failedDomainGroups[domainKey])
                  failedDomainGroups[domainKey] = [];
                failedDomainGroups[domainKey].push(eventId);
              }

              // Process each domain group concurrently
              await Promise.all(
                Object.values(failedDomainGroups).map(async (domainEvents) => {
                  // Get a proxy for this domain group
                  const { proxyAgent, proxy } =
                    this.proxyManager.getProxyForBatch(domainEvents);

                  // Process in batches for controlled concurrency
                  for (
                    let i = 0;
                    i < domainEvents.length;
                    i += CONCURRENT_DOMAIN_LIMIT
                  ) {
                    const batchEvents = domainEvents.slice(
                      i,
                      i + CONCURRENT_DOMAIN_LIMIT
                    );

                    // Process this batch concurrently
                    await Promise.all(
                      batchEvents.map(async (eventId) => {
                        try {
                          // Process this failed event
                          await this.scrapeEvent(
                            eventId,
                            parseInt(failureCount),
                            proxyAgent,
                            proxy
                          );
                        } catch (error) {
                          this.logWithTime(
                            `Error processing failed event ${eventId}: ${error.message}`,
                            "error"
                          );
                        }
                      })
                    );

                    // Small pause between batches
                    await setTimeout(200);
                  }
                })
              );

              // Small pause between failure count groups
              await setTimeout(300);
            }
          }

          // Add a dynamic pause to avoid overloading, but keep it minimal for 10,000+ events
          const pauseTime = Math.min(this.getAdaptivePauseTime(), 200);
          await setTimeout(pauseTime);
        } catch (error) {
          console.error(`Error in scraping loop: ${error.message}`);
          // Brief pause on error to avoid spinning
          await setTimeout(1000);
        }
      }
    }
  }

  /**
   * Calculate adaptive pause time optimized for handling 10,000+ events
   * @returns {number} Pause time in milliseconds
   */
  getAdaptivePauseTime() {
    // Base pause time - keep it minimal for high throughput
    let pauseTime = 100; // Base of 100ms for ultra-aggressive scraping

    // Add time based on active jobs and failed events - use logarithmic scaling for large numbers
    const activeJobCount = this.activeJobs.size;
    const failedEventCount = this.failedEvents.size;

    // Logarithmic scaling to handle large numbers of events without excessive pauses
    if (activeJobCount > 0) {
      pauseTime += Math.min(25 * Math.log10(activeJobCount + 1), 75);
    }

    if (failedEventCount > 0) {
      pauseTime += Math.min(25 * Math.log10(failedEventCount + 1), 75);
    }

    // Additional time if circuit breaker tripped
    if (this.apiCircuitBreaker.tripped) {
      pauseTime += 150;
    }

    return pauseTime;
  }

  
  // Add handleApiError method to fix the reported error
  async handleApiError(eventId, error, headers) {
    // Check if this is a status code error that needs special handling
    const is403Error =
      error.message.includes("403") || error.message.includes("Forbidden");
    const is429Error =
      error.message.includes("429") ||
      error.message.includes("Too Many Requests");
    const is400Error =
      error.message.includes("400") || error.message.includes("Bad Request");

    // Handle rate limiting and access denied errors
    if (is403Error || is429Error || is400Error) {
      // Increment API circuit breaker counter
      this.apiCircuitBreaker.failures++;

      // If we've hit the threshold, trip the circuit breaker
      if (this.apiCircuitBreaker.failures >= this.apiCircuitBreaker.threshold) {
        if (!this.apiCircuitBreaker.tripped) {
          this.logWithTime(
            `Circuit breaker tripped after ${this.apiCircuitBreaker.failures} API errors`,
            "error"
          );
          this.apiCircuitBreaker.tripped = true;

          // Reset after timeout
          setTimeout(() => {
            this.logWithTime(
              `Circuit breaker reset after ${
                this.apiCircuitBreaker.resetTimeout / 1000
              }s timeout`,
              "info"
            );
            this.apiCircuitBreaker.tripped = false;
            this.apiCircuitBreaker.failures = 0;
          }, this.apiCircuitBreaker.resetTimeout);
        }
        return true; // Error was handled by circuit breaker
      }

      // Try rotating proxy and headers for this event
      if (this.proxyManager.getAvailableProxyCount() > 1) {
        if (LOG_LEVEL >= 2) {
          this.logWithTime(
            `Rotating proxy for event ${eventId} due to ${
              is403Error ? "403" : "429"
            } error`,
            "info"
          );
        }
        this.proxyManager.blacklistCurrentProxy(eventId);
        return true; // Error handled by proxy rotation
      }
    }

    return false; // Error not handled
  }

  /**
   * Asynchronously upload all_events_combined.csv file to Sync service
   * This runs every 6 minutes while continuous scraping is active
   * and doesn't block the main scraping process
   */
  async runCsvUploadCycle() {
    try {
      // Create the path to the all_events_combined.csv file
      const allEventsCsvPath = path.join(DATA_DIR, 'all_events_combined.csv');
      
      // Check if file exists
      if (!nodeFs.existsSync(allEventsCsvPath)) {
        if (LOG_LEVEL >= 1) {
          this.logWithTime(
            `CSV upload skipped: all_events_combined.csv not found at ${allEventsCsvPath}`,
            "warning"
          );
        }
        // Simple console log
        console.log(`[${new Date().toISOString()}] CSV UPLOAD: File not found - ${allEventsCsvPath}`);
        return;
      }
      
      // Get file stats to include file size in logs
      const fileStats = await fs.stat(allEventsCsvPath);
      const fileSizeMB = (fileStats.size / (1024 * 1024)).toFixed(2);
      
      // Check if the file contains required fields
      let hasMappingId = false;
      let hasEventId = false;
      
      try {
        const csvContent = await fs.readFile(allEventsCsvPath, 'utf8');
        const firstLine = csvContent.split('\n')[0];
        
        // Check if the header line contains mapping_id and event_id
        hasMappingId = firstLine.includes('mapping_id');
        hasEventId = firstLine.includes('event_id');
        
        console.log(`[${new Date().toISOString()}] CSV UPLOAD CHECK: mapping_id=${hasMappingId ? 'YES' : 'NO'}, event_id=${hasEventId ? 'YES' : 'NO'}`);
        
        if (!hasMappingId || !hasEventId) {
          this.logWithTime(
            `CSV WARNING: Combined CSV file is missing required fields: ${!hasMappingId ? 'mapping_id ' : ''}${!hasEventId ? 'event_id' : ''}`,
            "warning"
          );
          
          // If the combined CSV is missing required fields, try to regenerate it
          try {
            console.log(`[${new Date().toISOString()}] CSV UPLOAD: Attempting to regenerate combined CSV file with missing fields`);
            
            // Import the generateCombinedEventsCSV function dynamically
            const { generateCombinedEventsCSV } = await import('./controllers/inventoryController.js');
            
            // Generate a new combined CSV
            const result = generateCombinedEventsCSV(false);
            
            if (result.success) {
              console.log(`[${new Date().toISOString()}] CSV UPLOAD: Successfully regenerated combined CSV file`);
            } else {
              console.log(`[${new Date().toISOString()}] CSV UPLOAD: Failed to regenerate combined CSV file: ${result.message}`);
            }
          } catch (regenerateError) {
            console.error(`[${new Date().toISOString()}] CSV UPLOAD: Error regenerating combined CSV: ${regenerateError.message}`);
          }
        }
      } catch (checkError) {
        console.log(`[${new Date().toISOString()}] CSV UPLOAD CHECK ERROR: ${checkError.message}`);
      }
      
      // Log the start of upload process
      this.logWithTime(
        `Starting CSV upload cycle for all_events_combined.csv (${fileSizeMB} MB)`, 
        "info"
      );
      
      // Simple console log for upload start
      console.log(`[${new Date().toISOString()}] CSV UPLOAD: Starting upload of all_events_combined.csv (${fileSizeMB} MB)`);
      
      // Initialize the SyncService
      const syncService = new SyncService(COMPANY_ID, API_TOKEN);
      
      // Upload the file asynchronously - use Promise to handle this without waiting
      try {
        // Add timestamp to track upload timing
        const uploadStartTime = moment();
        
        // Execute the upload
        const result = await syncService.uploadCsvToSync(allEventsCsvPath);
        
        // Calculate upload time
        const uploadDuration = moment().diff(uploadStartTime, 'seconds');
        
        if (result.success) {
          this.logWithTime(
            `Successfully uploaded all_events_combined.csv (${fileSizeMB} MB in ${uploadDuration}s)`,
            "success"
          );
          
          // Simple console log for success
          console.log(`[${new Date().toISOString()}] CSV UPLOAD: SUCCESS - Uploaded ${fileSizeMB} MB in ${uploadDuration}s`);
          
          // Track last successful upload time
          this.lastCsvUploadTime = moment();
        } else {
          this.logWithTime(
            `CSV upload completed but reported failure: ${result.message || 'Unknown error'}`,
            "warning"
          );
          
          // Simple console log for failure
          console.log(`[${new Date().toISOString()}] CSV UPLOAD: FAILED - ${result.message || 'Unknown error'}`);
        }
      } catch (uploadError) {
        // More detailed error logging
        this.logWithTime(
          `Error uploading CSV file (${fileSizeMB} MB): ${uploadError.message}`,
          "error"
        );
        
        // Simple console log for error
        console.log(`[${new Date().toISOString()}] CSV UPLOAD: ERROR - ${uploadError.message}`);
        
        // Check for specific types of errors
        if (uploadError.message.includes('network') || uploadError.message.includes('timeout')) {
          this.logWithTime(
            "Network or timeout error during CSV upload - will retry on next cycle",
            "warning"
          );
        } else if (uploadError.message.includes('403') || uploadError.message.includes('401')) {
          this.logWithTime(
            "Authentication error during CSV upload - check API credentials",
            "error"
          );
        }
        
        // Don't rethrow - we want to keep scraping even if upload fails
        console.error(`CSV upload error details:`, uploadError);
      }
    } catch (error) {
      // Log error but don't throw - we don't want to interrupt scraping
      this.logWithTime(
        `Error in CSV upload cycle: ${error.message}`,
        "error"
      );
      
      // Simple console log for cycle error
      console.log(`[${new Date().toISOString()}] CSV UPLOAD: CYCLE ERROR - ${error.message}`);
      
      console.error(`CSV upload cycle error:`, error);
    }
  }

  /**
   * Stop the continuous scraping process and clean up resources
   * @returns {Promise<void>}
   */
  async stopContinuousScraping() {
    // Set the isRunning flag to false to stop the main scraping loop
    this.isRunning = false;
    
    this.logWithTime("Stopping continuous scraping process...", "info");
    
    // Clear the CSV upload interval if it exists
    if (this.csvUploadIntervalId) {
      clearInterval(this.csvUploadIntervalId);
      this.csvUploadIntervalId = null;
      this.logWithTime("CSV upload cycle stopped", "info");
    }
    
    // Run one final CSV upload to ensure latest data is uploaded
    try {
      this.logWithTime("Running final CSV upload before shutdown", "info");
      await this.runCsvUploadCycle();
    } catch (error) {
      this.logWithTime(`Error in final CSV upload: ${error.message}`, "error");
    }
    
    // Release any active proxies
    this.proxyManager.releaseAllProxies();
    
    this.logWithTime("Continuous scraping process stopped", "success");
    
    // Return runtime statistics
    const runTime = moment.duration(moment().diff(this.startTime));
    return {
      runtime: {
        hours: Math.floor(runTime.asHours()),
        minutes: runTime.minutes(),
        seconds: runTime.seconds()
      },
      successCount: this.successCount,
      failedCount: this.failedEvents.size
    };
  }
}

const scraperManager = new ScraperManager();
export default scraperManager;

// Add a function to export that allows checking logs
export function checkScraperLogs() {
  return scraperManager.checkLogs();
}
