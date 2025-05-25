import moment from "moment";
import { setTimeout } from "timers/promises";
import { Event, ErrorLog, ConsecutiveGroup } from "./models/index.js";
import { ScrapeEvent, refreshHeaders, generateEnhancedHeaders } from "./scraper.js";
import { promiseAllSettled, promiseWithConcurrency } from "./helpers/promiseHelpers.js";
import { cpus } from "os";
import fs from "fs/promises";
import path from "path";
import ProxyManager from "./helpers/ProxyManager.js";
import { v4 as uuidv4 } from 'uuid';
import SyncService from './services/syncService.js';
import nodeFs from 'fs';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import SessionManager from './helpers/SessionManager.js';

// Add this at the top of the file, after imports
export const ENABLE_CSV_UPLOAD = false; // Set to false to disable all_events_combined.csv upload

const MAX_UPDATE_INTERVAL = 180000; // Strict 3-minute update requirement
const CRITICAL_UPDATE_THRESHOLD = 150000; // Events approaching 2.5 minutes since last update are critical
const SUPER_CRITICAL_THRESHOLD = 165000; // Events approaching 2.75 minutes get highest priority
const CONCURRENT_LIMIT = Math.max(60, Math.floor(cpus().length * 5)); // Dramatically increased for maximum parallel processing
const MAX_RETRIES = 15; // Updated from 10 per request of user
const SCRAPE_TIMEOUT = 45000; // Reduced to 45 seconds to fail faster and retry with fresh proxies
const BATCH_SIZE = Math.max(CONCURRENT_LIMIT * 4, 120); // Significantly increased batch size for bulk processing
const RETRY_BACKOFF_MS = 1500; // Reduced base backoff time for faster retries
const ALWAYS_USE_PARALLEL = true; // ADDED: Always use parallel scraping
const MIN_TIME_BETWEEN_EVENT_SCRAPES = 60000; // Minimum 1 minute between scrapes (reduced to allow 3-minute updates)
const URGENT_THRESHOLD = 30000; // Events needing update within 30 seconds of deadline (tighter window)
const MAX_ALLOWED_UPDATE_INTERVAL = 180000; // Maximum 3 minutes allowed between updates (strict)
const PARALLEL_SCRAPE_THRESHOLD = 0; // 0 - ALWAYS use parallel scraping for all events
const EVENT_FAILURE_THRESHOLD = 600000; // 10 minutes without updates before marking an event as stopped/failed

// New cooldown settings - shorter cooldowns to allow for more frequent retries within 10 minute window
const SHORT_COOLDOWNS = [1000, 2000, 4000]; // Shortened cooldowns: 1s, 2s, 4s for more frequent retries
const LONG_COOLDOWN_MINUTES = 1; // Reduced to 1 minute for more frequent retries within the 10 minute window

// Logging levels: 0 = errors only, 1 = warnings + errors, 2 = info + warnings + errors, 3 = all (verbose)
const LOG_LEVEL = 1; // Default to warnings and errors only

// Cookie expiration threshold: refresh cookies every 15 minutes
const COOKIE_EXPIRATION_MS = 15 * 60 * 1000; // 15 minutes (reduced for more frequent rotation)
const SESSION_REFRESH_INTERVAL = 15 * 60 * 1000; // 15 minutes for session refresh

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
  COOKIE_REFRESH_INTERVAL: 15 * 60 * 1000, // 15 minutes (reduced for more frequent rotation)
  MAX_COOKIE_AGE:   30 * 60 * 60 * 1000,
  COOKIE_ROTATION: {
    ENABLED: true,
    MAX_STORED_COOKIES: 100,
    ROTATION_INTERVAL: 15 * 60 * 1000, // 15 minutes (reduced for more frequent rotation)
    LAST_ROTATION: Date.now(),
    ENFORCE_UNIQUE: true, // Ensure unique cookies are generated
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
 * Now includes robust session management for difficult-to-scrape sites
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

    // Initialize SessionManager for robust session handling
    this.sessionManager = new SessionManager(this);
    
    // Setup cookie rotation tracking
    this.cookieRotationIntervalId = null;
    this.lastCookieRotation = Date.now();

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

  async refreshEventHeaders(eventId, forceRefresh = false) {
    
    try {
      // ALWAYS use random event IDs for cookie refresh, never the original eventId
      let eventToUse;
      
      // Try multiple ways to get random event IDs to ensure we always get one
      try {
        // First attempt: Get multiple random active events from the database 
        const randomEvents = await Event.aggregate([
          {
            $match: {
              Skip_Scraping: { $ne: true },
              url: { $exists: true, $ne: "" },
            },
          },
          { $sample: { size: 20 } }, // Increased to 20 for even more diversity
          { $project: { Event_ID: 1, url: 1 } },
        ]);

        if (randomEvents && randomEvents.length > 0) {
          // Select a truly random event (not timestamp-based) for better cookie diversity
          const randomIndex = Math.floor(Math.random() * randomEvents.length);
          const selectedEvent = randomEvents[randomIndex];
          eventToUse = selectedEvent.Event_ID;
          
          if (LOG_LEVEL >= 1) { // Increased to level 1 for visibility
            this.logWithTime(
              `Using random event ${eventToUse} for cookie refresh (original event: ${eventId})`,
              "info" // Changed from "debug" to "info"
            );
          }
        } else {
          // Second attempt: Try to get any random event ID from other data sources
          try {
            // Try to find any event from the failedEvents set
            if (this.failedEvents.size > 0) {
              const failedEventsArray = Array.from(this.failedEvents);
              const randomFailedEvent = failedEventsArray[Math.floor(Math.random() * failedEventsArray.length)];
              
              // Make sure it's different from the original event ID
              if (randomFailedEvent && randomFailedEvent !== eventId) {
                eventToUse = randomFailedEvent;
                this.logWithTime(
                  `Using random failed event ${eventToUse} for cookie refresh`,
                  "info"
                );
              }
            }
            
            // If still no event, try from active jobs
            if (!eventToUse && this.activeJobs.size > 0) {
              const activeJobsArray = Array.from(this.activeJobs.keys());
              const randomActiveEvent = activeJobsArray[Math.floor(Math.random() * activeJobsArray.length)];
              
              // Make sure it's different from the original event ID
              if (randomActiveEvent && randomActiveEvent !== eventId) {
                eventToUse = randomActiveEvent;
                this.logWithTime(
                  `Using random active event ${eventToUse} for cookie refresh`,
                  "info"
                );
              }
            }
            
            // If we still don't have a random event, try to use any cached event ID
            if (!eventToUse) {
              if (this.headerRefreshTimestamps.size > 0) {
                // Use a random event ID from the cache that's different from the original
                const cachedEvents = Array.from(this.headerRefreshTimestamps.keys());
                const filteredEvents = cachedEvents.filter(id => id !== eventId);
                if (filteredEvents.length > 0) {
                  eventToUse = filteredEvents[Math.floor(Math.random() * filteredEvents.length)];
                  this.logWithTime(
                    `Using cached event ID ${eventToUse} for cookie refresh`,
                    "info"
                  );
                } else {
                  // If no suitable cached event, try a random one from the database again
                  try {
                    const broadQuery = await Event.findOne().sort({ Last_Updated: 1 }).limit(1).lean();
                    if (broadQuery) {
                      eventToUse = broadQuery.Event_ID;
                      this.logWithTime(
                        `Using fallback database event ${eventToUse} for cookie refresh`,
                        "info"
                      );
                    } else {
                      eventToUse = eventId; // Last resort: use original
                      this.logWithTime(
                        `No alternative events found, using original event ID ${eventToUse} for cookie refresh`,
                        "warning"
                      );
                    }
                  } catch (fallbackError) {
                    eventToUse = eventId; // Last resort: use original
                    this.logWithTime(
                      `Fallback database query failed, using original event ID ${eventToUse} for cookie refresh`,
                      "warning"
                    );
                  }
                }
              } else {
                // No cached events available, try one more database query
                try {
                  const anyEvent = await Event.findOne().sort({ Last_Updated: 1 }).limit(1).lean();
                  if (anyEvent) {
                    eventToUse = anyEvent.Event_ID;
                    this.logWithTime(
                      `Using any available event ID ${eventToUse} for cookie refresh`,
                      "info"
                    );
                  } else {
                    eventToUse = eventId; // Last resort: use original
                    this.logWithTime(
                      `No events found in database, using original event ID ${eventToUse} for cookie refresh`,
                      "warning"
                    );
                  }
                } catch (dbFallbackError) {
                  eventToUse = eventId; // Last resort: use original
                  this.logWithTime(
                    `Final database query failed, using original event ID ${eventToUse} for cookie refresh`,
                    "warning"
                  );
                }
              }
            }
          } catch (fallbackError) {
            this.logWithTime(
              `Error finding alternative random event: ${fallbackError.message}`,
              "warning"
            );
            // Try to use the original event ID as last resort
            eventToUse = eventId;
            this.logWithTime(
              `Falling back to original event ID ${eventToUse} for cookie refresh`,
              "warning"
            );
          }
        }
      } catch (dbError) {
        this.logWithTime(
          `Error getting random event: ${dbError.message}`,
          "warning"
        );
        
        // Try to find any existing event ID in the cache
        if (this.headerRefreshTimestamps.size > 0) {
          const cachedEvents = Array.from(this.headerRefreshTimestamps.keys());
          const filteredEvents = cachedEvents.filter(id => id !== eventId);
          if (filteredEvents.length > 0) {
            eventToUse = filteredEvents[Math.floor(Math.random() * filteredEvents.length)];
            this.logWithTime(
              `Using cached event ID ${eventToUse} for cookie refresh after database error`,
              "warning"
            );
          } else {
            // Last resort: use original eventId
            eventToUse = eventId;
            this.logWithTime(
              `No alternative event IDs available, using original event ID ${eventToUse} for cookie refresh`,
              "warning"
            );
          }
        } else {
          // Last resort: use original eventId
          eventToUse = eventId;
          this.logWithTime(
            `No cached event IDs available, using original event ID ${eventToUse} for cookie refresh`,
            "warning"
          );
        }
      }

      const eventDoc = await Event.findOne({ Event_ID: eventToUse })
        .select("url")
        .lean();
      const refererUrl = eventDoc?.url || "https://www.ticketmaster.com/";
      
      // More unique request ID format with microsecond precision
      const requestId = `req-${Date.now()}-${Math.random().toString(36).substring(2, 10)}`;
      
      const lastRefresh = this.headerRefreshTimestamps.get(eventToUse);

      // Generate a new X-Forwarded-For and X-Request-Id for each request
      const refreshedIpAndId = {
        "X-Forwarded-For": generateRandomIp(),
        "X-Request-Id": requestId,
        "X-Timestamp": Date.now().toString(),
        "X-Unique-ID": uuidv4()
      };

      // More aggressive refresh interval (15 minutes)
      const effectiveExpirationTime = COOKIE_MANAGEMENT.COOKIE_REFRESH_INTERVAL;

      // Force refresh if requested or refresh interval exceeded
      if (
        forceRefresh || 
        !lastRefresh ||
        moment().diff(lastRefresh) > effectiveExpirationTime
      ) {
        if (LOG_LEVEL >= 1) { // Increased logging level for better visibility
          this.logWithTime(`Refreshing headers for ${eventToUse} ${forceRefresh ? '(forced)' : ''}`, "info");
        }

        // Clear any stale headers from the cache
        this.headersCache.delete(eventToUse);

        // Skip rotation pool and always get fresh cookies
        // This ensures we always have fresh, unique cookies
        
        // First attempt: Try with session manager
        let sessionData = null;
        try {
          // Get a fresh session or create a new one
          sessionData = await this.sessionManager.getSessionForEvent(eventToUse, null, true);
          
          if (sessionData && sessionData.cookies) {
            this.logWithTime(
              `Got fresh session for ${eventToUse} from session manager`,
              "info"
            );
            
            // Convert session cookies to headers
            const cookieString = sessionData.cookies
              .map((c) => `${c.name}=${c.value}`)
              .join("; ");
              
            // Get random fingerprint for diversity
            const fingerprint = sessionData.fingerprint || randomFingerprint();
            
            // Generate enhanced headers
            const enhanced = generateEnhancedHeaders(fingerprint, cookieString);
            
            // Inject anti-bot fields
            enhanced["Referer"] = refererUrl;
            enhanced["X-Forwarded-For"] = refreshedIpAndId["X-Forwarded-For"];
            enhanced["X-Request-Id"] = refreshedIpAndId["X-Request-Id"];
            enhanced["X-Unique-ID"] = refreshedIpAndId["X-Unique-ID"];
            
            const standardizedHeaders = {
              headers: enhanced,
              cookies: sessionData.cookies,
              fingerprint: fingerprint,
              timestamp: Date.now(),
              sessionId: sessionData.sessionId,
              freshlyGenerated: true
            };
            
            // Cache the headers
            this.headersCache.set(eventToUse, standardizedHeaders);
            this.headerRefreshTimestamps.set(eventToUse, moment());
            
            return standardizedHeaders;
          }
        } catch (sessionError) {
          this.logWithTime(
            `Session manager error: ${sessionError.message}, trying direct refresh`,
            "warning"
          );
        }

        // Second attempt: Get a random proxy for refreshing headers
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
          enhanced["X-Unique-ID"] = refreshedIpAndId["X-Unique-ID"];
          
          const standardizedHeaders = {
            headers: enhanced,
            cookies: capturedState.cookies,
            fingerprint: mergedFp,
            timestamp: Date.now(),
            freshlyGenerated: true
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
            if (this.headerRotationPool.length > 20) // Increased pool size
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
          
          // Mark headers as reused (not freshly generated)
          refreshedHeaders.freshlyGenerated = false;

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
      refreshedHeaders.headers["X-Request-Id"] = `req-${Date.now()}-${Math.random().toString(36).substring(2, 10)}`;
      refreshedHeaders.headers["X-Unique-ID"] = uuidv4();
      refreshedHeaders.freshlyGenerated = false;
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
        poolHeaders.headers["X-Request-Id"] = `req-${Date.now()}-${Math.random().toString(36).substring(2, 10)}`;
        poolHeaders.headers["X-Unique-ID"] = uuidv4();
      }
      
      poolHeaders.freshlyGenerated = false;
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
        "X-Request-Id": `req-${Date.now()}-${Math.random().toString(36).substring(2, 10)}`,
        "X-Unique-ID": uuidv4(),
      },
      freshlyGenerated: false
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
              // Calculate the increased list price based on the percentage from database
              const basePrice = parseFloat(group.inventory.listPrice);
              // Apply price increase and round to 2 decimal places for consistency
              const increasedPrice = parseFloat(
                (basePrice * (1 + priceIncreasePercentage / 100)).toFixed(2)
              );

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
        // Apply price increase and round to 2 decimal places for consistency
        const increasedPrice = parseFloat(
          (basePrice * (1 + priceIncreasePercentage / 100)).toFixed(2)
        );

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

  /**
   * Get a random event ID for session/cookie management
   * This ensures we never use the actual event ID for cookies
   */
  async getRandomEventId(originalEventId) {
    try {
      // Try to get random events from database
      const randomEvents = await Event.aggregate([
        {
          $match: {
            Skip_Scraping: { $ne: true },
            url: { $exists: true, $ne: "" },
            Event_ID: { $ne: originalEventId } // Explicitly exclude the original event ID
          },
        },
        { $sample: { size: 3 } },
        { $project: { Event_ID: 1 } },
      ]);
      
      if (randomEvents && randomEvents.length > 0) {
        const randomIndex = Math.floor(Math.random() * randomEvents.length);
        return randomEvents[randomIndex].Event_ID;
      }
      
      // Fallback options if database query fails
      if (this.failedEvents.size > 0) {
        const failedEventsArray = Array.from(this.failedEvents);
        const filtered = failedEventsArray.filter(id => id !== originalEventId);
        if (filtered.length > 0) {
          return filtered[Math.floor(Math.random() * filtered.length)];
        }
      }
      
      if (this.activeJobs.size > 0) {
        const activeJobsArray = Array.from(this.activeJobs.keys());
        const filtered = activeJobsArray.filter(id => id !== originalEventId);
        if (filtered.length > 0) {
          return filtered[Math.floor(Math.random() * filtered.length)];
        }
      }
      
      // Last resort: use a previously successful event ID from the cache or retry database query
      if (this.headerRefreshTimestamps.size > 0) {
        // Use any existing event ID that has a valid timestamp
        const cachedEvents = Array.from(this.headerRefreshTimestamps.keys());
        const validCachedEvents = cachedEvents.filter(id => id !== originalEventId);
        if (validCachedEvents.length > 0) {
          return validCachedEvents[Math.floor(Math.random() * validCachedEvents.length)];
        }
      }

      // If all else fails, retry the database query with broader criteria
      try {
        const fallbackEvents = await Event.aggregate([
          { $match: { url: { $exists: true } } },
          { $sample: { size: 5 } },
          { $project: { Event_ID: 1 } }
        ]);
        
        if (fallbackEvents && fallbackEvents.length > 0) {
          return fallbackEvents[Math.floor(Math.random() * fallbackEvents.length)].Event_ID;
        }
      } catch (retryError) {
        this.logWithTime(`Retry database query failed: ${retryError.message}`, "warning");
      }
      
      // Absolute last resort: return the original event ID
      return originalEventId;
    } catch (error) {
      this.logWithTime(`Error getting random event ID: ${error.message}`, "warning");
      return originalEventId;
    }
  }

  /**
   * Scrape an event in parallel using multiple sessions and proxies
   * @param {string} eventId - The event ID to scrape
   * @param {number} retryCount - Current retry count
   * @param {Object} options - Additional options
   * @returns {Promise<Array>} - Array of scrape results
   */
  async scrapeEventParallel(eventId, retryCount = 0, options = {}) {
    // Number of parallel scrapes to perform
    const parallelCount = options.parallelCount || 3;
    // Timeout for parallel scraping (longer than regular scraping)
    const parallelTimeout = options.timeout || SCRAPE_TIMEOUT * 1.5; // 50% longer timeout for parallel scrapes
    
    this.logWithTime(
      `Starting parallel scrape for high-priority event ${eventId} with ${parallelCount} parallel sessions (timeout: ${Math.floor(parallelTimeout/1000)}s)`,
      "warning"
    );
    
    // Create array of parallel scrape promises
    const scrapePromises = [];
    const startTime = moment();
    
    try {
      // First scrape with existing session if available
      if (options.useExistingSession && this.headerRefreshTimestamps.get(eventId)) {
        this.logWithTime(`Using existing session for parallel scrape #1`, "info");
        scrapePromises.push(
          this.scrapeEvent(eventId, retryCount, null, null, null, { 
            isParallelScrape: true,
            parallelIndex: 0,
            parallelTimeout: parallelTimeout
          })
        );
      }
      
      // Add additional parallel scrapes with fresh sessions
      const remainingParallel = options.useExistingSession ? parallelCount - 1 : parallelCount;
      
      // Use shorter timeout for parallel scrapes to fail faster and retry
      const effectiveTimeout = options.timeout || Math.min(parallelTimeout, SCRAPE_TIMEOUT);
      this.logWithTime(`Using timeout of ${Math.floor(effectiveTimeout/1000)}s for parallel scrapes`, "info");
      
      for (let i = 0; i < remainingParallel; i++) {
  // Diagnostic logging for proxy/session/header setup
  this.logWithTime(
    `Preparing parallel scrape #${(options.useExistingSession ? i + 2 : i + 1)}: requesting fresh proxy/session...`,
    "debug"
  );
        const parallelIndex = options.useExistingSession ? i + 1 : i;
        this.logWithTime(`Setting up parallel scrape #${parallelIndex + 1} with fresh session`, "info");
        
        try {
          // Get a fresh proxy for each parallel scrape
          const { proxyAgent: freshProxyAgent, proxy: freshProxy } = 
            this.proxyManager.getProxyForBatch([eventId]);
          // Diagnostic logging for proxy and headers
          this.logWithTime(
            `Parallel scrape #${parallelIndex + 1} proxy: ${freshProxy ? JSON.stringify(freshProxy) : 'none'}`,
            "debug"
          );
          // Force fresh session for each parallel scrape
          const freshHeaders = await this.refreshEventHeaders(eventId, true);
          // Diagnostic logging for proxy and headers
          this.logWithTime(
            `Parallel scrape #${parallelIndex + 1} headers: ${freshHeaders ? JSON.stringify(freshHeaders) : 'none'}`,
            "debug"
          );
          // Add to scrape promises
          scrapePromises.push(
            this.scrapeEvent(
              eventId, 
              retryCount, 
              freshProxyAgent, 
              freshProxy, 
              freshHeaders, 
              { 
                isParallelScrape: true, 
                parallelIndex: parallelIndex,
                parallelTimeout: parallelTimeout
              }
            )
          );
        } catch (setupError) {
          this.logWithTime(
            `Error setting up parallel scrape #${parallelIndex + 1}: ${setupError.message}`,
            "error"
          );
          // Add a rejected promise to maintain the count
          scrapePromises.push(Promise.reject(setupError));
        }
      }
      
      // Add timeout to the entire parallel scraping operation
      const timeoutPromise = new Promise((_, reject) => {
        // Use global setTimeout, not the one from timers/promises
        global.setTimeout(() => {
          reject(new Error(`Parallel scrape timeout after ${Math.floor(parallelTimeout/1000)} seconds`));
        }, parallelTimeout);
      });
      
      // Use Promise.race with a shorter global timeout
      const racePromise = Promise.race([
        promiseAllSettled(scrapePromises),
        timeoutPromise
      ]);
      
      // Only log per-scrape progress, but don't enforce individual timeouts
      scrapePromises.forEach((promise, index) => {
        // Just log when each scrape completes
        promise.then(
          () => this.logWithTime(`Individual scrape #${index+1} completed successfully`, "debug"),
          (err) => this.logWithTime(`Individual scrape #${index+1} failed: ${err.message}`, "debug")
        );
      });
      
      // Wait for all parallel scrapes to complete or timeout
      const results = await racePromise;
      
      // Count successful scrapes
      const successfulScrapes = results.filter(r => r.status === 'fulfilled' && r.value).length;
      const duration = moment().diff(startTime);
      
      this.logWithTime(
        `Completed parallel scrape for event ${eventId}: ${successfulScrapes}/${parallelCount} successful in ${Math.floor(duration/1000)}s`,
        successfulScrapes > 0 ? "success" : "error"
      );
      
      // Return array of results (may contain errors)
      return results.map(r => r.status === 'fulfilled' ? r.value : null);
    } catch (error) {
      const duration = moment().diff(startTime);
      this.logWithTime(
        `Parallel scrape for event ${eventId} failed after ${Math.floor(duration/1000)}s: ${error.message}`,
        "error"
      );
      
      // Check if any scrapes were successful despite the overall failure
      const partialResults = await promiseAllSettled(scrapePromises);
      const successfulScrapes = partialResults.filter(r => r.status === 'fulfilled' && r.value).length;
      
      if (successfulScrapes > 0) {
        this.logWithTime(
          `Recovered ${successfulScrapes}/${scrapePromises.length} successful scrapes from failed parallel operation`,
          "warning"
        );
        return partialResults.map(r => r.status === 'fulfilled' ? r.value : null);
      }
      
      // If all failed, return array of nulls
      return Array(parallelCount).fill(null);
    }
  }
  
  /**
   * Scrape an event
   * @param {string} eventId - The event ID to scrape
   * @param {number} retryCount - Current retry count
   * @param {Object} proxyAgent - Proxy agent to use
   * @param {Object} proxy - Proxy configuration
   * @param {Object} passedHeaders - Headers to use
   * @param {Object} options - Additional options
   * @returns {Promise<boolean>} - Success status
   */
  async scrapeEvent(eventId, retryCount = 0, proxyAgent = null, proxy = null, passedHeaders = null, options = {}) {
    // Skip if the event should be skipped
    if (this.shouldSkipEvent(eventId)) {
      return false;
    }

    // If this is part of a parallel scrape, skip the circuit breaker check
    const isParallelScrape = options.isParallelScrape || false;
    const parallelIndex = options.parallelIndex || 0;
    const parallelTimeout = options.parallelTimeout || SCRAPE_TIMEOUT;
    
    // Get time since last update
    const lastUpdate = this.eventUpdateTimestamps.get(eventId);
    const timeSinceUpdate = lastUpdate ? moment().diff(lastUpdate) : Infinity;
    
    // Check if this event is high priority (approaching 3-minute interval)
    const isHighPriority = timeSinceUpdate > PARALLEL_SCRAPE_THRESHOLD;
    
    // ALWAYS use parallel scraping for all events unless this is already part of a parallel scrape
    if (ALWAYS_USE_PARALLEL && !isParallelScrape && !options.preventParallel) {
      this.logWithTime(
        `Using parallel scraping for event ${eventId} (${Math.floor(timeSinceUpdate/1000)}s since update)`,
        "info"
      );
      
      // Determine number of parallel sessions based on time since last update
      let parallelCount = 3; // Default
      
      // For events approaching the deadline, use more parallel sessions
      if (timeSinceUpdate > SUPER_CRITICAL_THRESHOLD) {
        parallelCount = 6; // Double the sessions for super critical events
        this.logWithTime(`SUPER CRITICAL event ${eventId} - using ${parallelCount} parallel sessions`, "warning");
      } else if (timeSinceUpdate > CRITICAL_UPDATE_THRESHOLD) {
        parallelCount = 4; // More sessions for critical events
        this.logWithTime(`CRITICAL event ${eventId} - using ${parallelCount} parallel sessions`, "warning");
      }
      
      // Use parallel scraping and return the result
      const parallelResults = await this.scrapeEventParallel(eventId, retryCount, {
        parallelCount,
        useExistingSession: true,
        timeout: Math.min(SCRAPE_TIMEOUT, 60000) // Use shorter timeout for faster failure
      });
      
      // Return true if any parallel scrape succeeded
      return parallelResults.some(result => result === true);
    }
    
    // If circuit breaker is tripped, delay non-critical events
    if (this.apiCircuitBreaker.tripped && !isParallelScrape) {
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

    // Declare headers variable outside try block so it's accessible in catch block
    let headers;
    // Track if we used fresh cookies for this event
    let usedFreshCookies = false;

    try {
      // More detailed logging for better troubleshooting
      if (LOG_LEVEL >= 1) {
        const parallelInfo = isParallelScrape ? ` (Parallel ${parallelIndex+1})` : '';
        this.logWithTime(
          `Scraping ${eventId} (Attempt ${retryCount + 1})${parallelInfo}`,
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

      // Determine if we need fresh cookies/session for this event
      // We always get fresh cookies on retries or if it's been 15+ minutes
      const needsFreshCookies = retryCount > 0 || 
                                !this.headerRefreshTimestamps.get(eventId) || 
                                moment().diff(this.headerRefreshTimestamps.get(eventId)) > SESSION_REFRESH_INTERVAL;

      // If headers were passed from batch processing, still check if they're fresh enough
      if (passedHeaders) {
        const hasPassedSessionId = !!passedHeaders.sessionId;
        const passedHeadersAge = passedHeaders.timestamp ? Date.now() - passedHeaders.timestamp : Infinity;
        
        // Use passed headers only if they're fresh enough (less than 15 minutes old)
        if (!needsFreshCookies && passedHeadersAge < SESSION_REFRESH_INTERVAL) {
          headers = passedHeaders;
          if (LOG_LEVEL >= 2) {
            this.logWithTime(
              `Using passed headers for event ${eventId} from batch processing (age: ${Math.floor(passedHeadersAge/1000)}s)`,
              "info"
            );
          }
        } else {
          if (LOG_LEVEL >= 2) {
            this.logWithTime(
              `Passed headers too old (${Math.floor(passedHeadersAge/1000)}s) or retry attempt (${retryCount}), getting fresh cookies`,
              "info"
            );
          }
          // Force refresh to get fresh cookies
          headers = await this.refreshEventHeaders(eventId, true);
          usedFreshCookies = true;
        }
      } else {
        // Prioritize session manager for cookie/session management
        try {
          // Always force new session on retries or after expiration
          const forceNewSession = needsFreshCookies;
          
          // Get a random event ID instead of using the actual event ID
          const randomEventId = await this.getRandomEventId(eventId);
          
          if (LOG_LEVEL >= 2) {
            this.logWithTime(
              `Using random event ID ${randomEventId} for session management (original: ${eventId})`,
              "info"
            );
          }
          
          const sessionData = await this.sessionManager.getSessionForEvent(randomEventId, proxyToUse, forceNewSession);
          
          if (sessionData) {
            const sessionHeaders = await this.sessionManager.getSessionHeaders(eventId);
            
            if (sessionHeaders) {
              headers = {
                headers: sessionHeaders,
                cookies: sessionData.cookies,
                fingerprint: sessionData.fingerprint,
                sessionId: sessionData.sessionId,
                timestamp: Date.now(),
                freshlyGenerated: forceNewSession
              };
              
              usedFreshCookies = forceNewSession;
              
              if (LOG_LEVEL >= 2) {
                this.logWithTime(
                  `Using ${forceNewSession ? 'fresh' : 'existing'} session headers for event ${eventId} (session: ${sessionData.sessionId})`,
                  "info"
                );
              }
            }
          }
        } catch (sessionError) {
          if (LOG_LEVEL >= 2) {
            this.logWithTime(
              `Session management failed for event ${eventId}: ${sessionError.message}, falling back to header refresh`,
              "warning"
            );
          }
        }

        // If session approach fails, use direct header management
        if (!headers) {
          // For retries or no recent headers, always force refresh
          if (needsFreshCookies) {
            if (LOG_LEVEL >= 2) {
              this.logWithTime(
                `Getting fresh headers for event ${eventId}${
                  retryCount > 0 ? " (retry attempt)" : ""
                }`,
                "info"
              );
            }
            
            // Get a random event ID for cookie refresh
            const randomEventId = await this.getRandomEventId(eventId);
            if (LOG_LEVEL >= 2) {
              this.logWithTime(
                `Using random event ID ${randomEventId} for header refresh (original: ${eventId})`,
                "info"
              );
            }
            
            headers = await this.refreshEventHeaders(randomEventId, true);
            usedFreshCookies = true;
          } else {
            // Check if we have recent headers
            const lastRefresh = this.headerRefreshTimestamps.get(eventId) || 0;
            const cookieAge = Date.now() - lastRefresh;
            
            // Use cached headers only if they're fresh enough
            if (cookieAge < SESSION_REFRESH_INTERVAL / 2) {
              headers = this.headersCache.get(eventId);
              if (LOG_LEVEL >= 2) {
                this.logWithTime(
                  `Using cached headers for event ${eventId} (age: ${Math.floor(
                    cookieAge / 1000
                  )}s)`,
                  "info"
                );
              }
            } else {
              // Otherwise refresh
              if (LOG_LEVEL >= 2) {
                              this.logWithTime(
                `Cached headers too old (${Math.floor(cookieAge/1000)}s), getting fresh headers for ${eventId}`,
                "info"
              );
            }
            
            // Get a random event ID for cookie refresh
            const randomEventId = await this.getRandomEventId(eventId);
            if (LOG_LEVEL >= 2) {
              this.logWithTime(
                `Using random event ID ${randomEventId} for header refresh (original: ${eventId})`,
                "info"
              );
            }
            
            headers = await this.refreshEventHeaders(randomEventId, true);
              usedFreshCookies = true;
            }
          }

          // If headers still aren't available, force one last refresh
          if (!headers) {
            if (LOG_LEVEL >= 1) {
              this.logWithTime(
                `No headers available for ${eventId}, forcing final refresh`,
                "warning"
              );
            }
            
            // Get a random event ID for cookie refresh
            const randomEventId = await this.getRandomEventId(eventId);
            if (LOG_LEVEL >= 1) {
              this.logWithTime(
                `Using random event ID ${randomEventId} for final header refresh (original: ${eventId})`,
                "warning"
              );
            }
            
            headers = await this.refreshEventHeaders(randomEventId, true);
            usedFreshCookies = true;
          }
        }
      }

      if (!headers) {
        throw new Error("Failed to obtain valid headers");
      }
      
      // Log whether we're using fresh cookies
      if (LOG_LEVEL >= 2) {
        this.logWithTime(
          `${usedFreshCookies ? 'Using fresh cookies' : 'Reusing existing cookies'} for event ${eventId}`,
          usedFreshCookies ? "info" : "debug"
        );
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
          headers.headers?.Cookie?.substring(0, 20) ||
          headers.headers?.["User-Agent"]?.substring(0, 20);
        if (headerKey) {
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
                h.headers?.Cookie?.substring(0, 20) === headerKey ||
                h.headers?.["User-Agent"]?.substring(0, 20) === headerKey
          )
        ) {
          this.headerRotationPool.push(headers);
          // Limit pool size
          if (this.headerRotationPool.length > 10) {
            this.headerRotationPool.shift();
            }
          }
        }
      }

      // Update session usage on success
      if (headers.sessionId) {
        this.sessionManager.updateSessionUsage(headers.sessionId, true);
        if (LOG_LEVEL >= 3) {
          this.logWithTime(
            `Updated session ${headers.sessionId} usage for successful scrape of event ${eventId}`,
            "debug"
          );
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

      // Update session usage on failure
      if (headers?.sessionId) {
        this.sessionManager.updateSessionUsage(headers.sessionId, false);
        if (LOG_LEVEL >= 3) {
          this.logWithTime(
            `Updated session ${headers.sessionId} usage for failed scrape of event ${eventId}`,
            "debug"
          );
        }
      }

      // New: Try to handle the API error with smart strategies first
      const errorHandled = await this.handleApiError(eventId, error, headers);

      // If the error was handled by the circuit breaker or proxy rotation, skip retry
      if (errorHandled) {
        return false;
      }

      // Track failure count
      this.incrementFailureCount(eventId);

      // Add to failed events
      this.failedEvents.add(eventId);

      // Mark headers as potentially problematic
      if (headers) {
        const headerKey =
          headers.headers?.Cookie?.substring(0, 20) ||
          headers.headers?.["User-Agent"]?.substring(0, 20);
        if (headerKey) {
          const currentSuccessRate = this.headerSuccessRates.get(headerKey) || {
            success: 0,
            failure: 0,
          };
          currentSuccessRate.failure++;
          this.headerSuccessRates.set(headerKey, currentSuccessRate);
        }
      }

      // Increment global error counter
      this.globalConsecutiveErrors++;

      // Track failures by type for batch processing
      const errorType = this.categorizeError(error);
      if (!this.failureTypeGroups.has(errorType)) {
        this.failureTypeGroups.set(errorType, new Set());
      }
      this.failureTypeGroups.get(errorType).add(eventId);

      // Calculate dynamic retry limit for this event
      const baseRetryLimit = MAX_RETRIES;
      const eventRetryLimit = this.eventMaxRetries.get(eventId) || baseRetryLimit;

      // Determine if we should retry this event
      if (retryCount >= eventRetryLimit) {
        const lastUpdate = this.eventUpdateTimestamps.get(eventId);
        const timeSinceUpdate = lastUpdate ? moment().diff(lastUpdate) : Infinity;

        // If approaching max allowed update interval, force one more retry
        if (timeSinceUpdate > MAX_ALLOWED_UPDATE_INTERVAL - 30000) {
        this.logWithTime(
            `Event ${eventId} exceeded retry limit but approaching max update deadline - forcing retry`,
          "warning"
        );
        } else {
          this.logWithTime(
            `Event ${eventId} exceeded retry limit (${retryCount}/${eventRetryLimit}) - stopping retries`,
            "error"
          );
          
          // Log to database for critical events that have stopped updating
          await this.logError(
            eventId,
            "MAX_RETRIES_EXCEEDED",
            new Error(`Event exceeded maximum retry limit of ${eventRetryLimit}`),
            { retryCount, eventRetryLimit, timeSinceUpdate }
          );

          // Don't add to retry queue - this event will be marked as failed
        return false;
        }
      }

      // Calculate backoff time based on retry count and error type
      let backoffTime;
      const isApiError = error.message.includes("403") || 
        error.message.includes("400") ||
        error.message.includes("429") ||
        error.message.includes("API");

      if (retryCount < SHORT_COOLDOWNS.length) {
        // Use progressive short cooldowns for initial retries
        backoffTime = SHORT_COOLDOWNS[retryCount];

        if (LOG_LEVEL >= 1) {
          this.logWithTime(
            `${isApiError ? "API error" : "Error"} for ${eventId}: ${error.message}. Retry ${retryCount + 1}/${eventRetryLimit} in ${backoffTime/1000}s`,
            "warning"
          );
        }
      } else {
        // Use longer cooldown for persistent failures
        backoffTime = LONG_COOLDOWN_MINUTES * 60 * 1000;
        
          this.logWithTime(
          `Persistent ${isApiError ? "API errors" : "errors"} for ${eventId}: ${error.message}. Long cooldown retry in ${LONG_COOLDOWN_MINUTES} minute`,
            "error"
          );

        // Log long cooldown to error logs
        await this.logError(
          eventId,
          "LONG_COOLDOWN",
          new Error(`Event put in ${LONG_COOLDOWN_MINUTES} minute cooldown after persistent failures`),
          {
            cooldownDuration: backoffTime,
            isApiError,
            originalError: error.message,
            retryCount
          }
        );
      }

      // Set cooldown
      const cooldownUntil = moment().add(backoffTime, "milliseconds");
      this.cooldownEvents.set(eventId, cooldownUntil);

      // Add to retry queue
        this.retryQueue.push({
          eventId,
          retryCount: retryCount + 1,
          retryAfter: cooldownUntil,
        priority: Math.max(1, 10 - retryCount), // Higher priority for fewer retries
      });

      // Update event schedule for next update
      this.eventUpdateSchedule.set(
        eventId,
        moment().add(Math.min(backoffTime, MIN_TIME_BETWEEN_EVENT_SCRAPES), "milliseconds")
      );

        if (LOG_LEVEL >= 2) {
          this.logWithTime(
          `Queued ${eventId} for retry ${retryCount + 1}/${eventRetryLimit} (cooldown: ${backoffTime/1000}s)`,
            "info"
          );
        }

      return false;
    } finally {
      this.processingEvents.delete(eventId);
      this.activeJobs.delete(eventId);
      this.releaseSemaphore();
    }
  }



 
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

  /**
   * Get events to process in priority order with critical events first
   * @returns {Array<string>} - Array of event IDs
   */
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

      // Calculate priority for each event
      const prioritizedEvents = events.map((event) => {
        const timeSinceLastUpdate = event.Last_Updated
          ? now.diff(moment(event.Last_Updated))
          : Infinity;
        
        // Mark super critical and critical events for special handling
        const isSuperCritical = timeSinceLastUpdate > SUPER_CRITICAL_THRESHOLD;
        const isCritical = timeSinceLastUpdate > CRITICAL_UPDATE_THRESHOLD;
        
        // Calculate priority score - highest priority first
        let priorityScore = 1;

        // Super Critical - approaching 2.75 minutes (165 seconds) - highest possible priority
        if (isSuperCritical) {
          priorityScore = 1000000; // Extremely high priority
        }
        // Critical - over 2.5 minutes (150 seconds) - very high priority
        else if (isCritical) {
          priorityScore = 100000;
        }
        // Urgent - over 2 minutes (120 seconds)
        else if (timeSinceLastUpdate > 120000) {
          priorityScore = 10000;
        }
        // High - over 90 seconds
        else if (timeSinceLastUpdate > 90000) {
          priorityScore = 1000;
        }
        // Medium-high - over 60 seconds
        else if (timeSinceLastUpdate > 60000) {
          priorityScore = 100;
        }
        // Medium - over 30 seconds
        else if (timeSinceLastUpdate > 30000) {
          priorityScore = 10;
        }

        return {
          eventId: event.Event_ID,
          priorityScore,
          timeSinceLastUpdate,
          isSuperCritical,
          isCritical
        };
      });

      // Sort by priority (highest first)
      prioritizedEvents.sort((a, b) => b.priorityScore - a.priorityScore);

      // Create separate list of super-critical events for immediate processing
      const superCriticalEvents = prioritizedEvents.filter(event => event.isSuperCritical);
      
      if (superCriticalEvents.length > 0) {
        this.logWithTime(`Found ${superCriticalEvents.length} SUPER CRITICAL events approaching 3-minute deadline`, "warning");
      }
      
      // Create list of critical events for high-priority processing
    const criticalEvents = prioritizedEvents.filter((event) => event.isCritical && !event.isSuperCritical);

    if (criticalEvents.length > 0) {
      this.logWithTime(`Found ${criticalEvents.length} CRITICAL events approaching deadline`, "warning");
    }

    // For super-critical events, immediately process them with highest priority
    if (superCriticalEvents.length > 0) {
      // Process super-critical events immediately with dedicated resources
      this.processSuperCriticalEvents(superCriticalEvents.map((e) => e.eventId));
    }

    // Extract just the event IDs (all events, including critical ones that will get special handling)
    return prioritizedEvents.map((event) => event.eventId);
  } catch (error) {
    console.error("Error getting events to process:", error);
    return [];
  }
}

/**
 * Process super-critical events immediately with highest priority
 * @param {Array<string>} eventIds - Array of super-critical event IDs
 */
async processSuperCriticalEvents(eventIds) {
  if (eventIds.length === 0) return;

  this.logWithTime(` URGENT: Processing ${eventIds.length} super-critical events`, "error");

  // Process each super-critical event with maximum parallel sessions
  const promises = eventIds.map((eventId) => async () => {
    try {
      this.logWithTime(` Immediate processing of SUPER CRITICAL event ${eventId}`, "warning");

      // Use maximum parallelism (6 sessions) and shorter timeout
      await this.scrapeEventParallel(eventId, 0, {
        parallelCount: 6,
        useExistingSession: true,
        timeout: SCRAPE_TIMEOUT / 2, // Half timeout for faster feedback
        preventParallel: false, // Ensure we use parallel scraping
      });

      return true;
    } catch (error) {
      this.logWithTime(`Failed immediate processing of super-critical event ${eventId}: ${error.message}`, "error");
      return false;
    }
  });

  // Process all super-critical events with maximum concurrency
  // and priority indices to ensure they run first
  await promiseWithConcurrency(promises, Math.min(eventIds.length, 10), {
    timeout: SCRAPE_TIMEOUT / 2, // Shorter timeout
    priorityIndices: eventIds.map((_, i) => i), // All are priority
  });
}

async startContinuousScraping() {
  if (!this.isRunning) {
    this.isRunning = true;
    this.logWithTime("Starting continuous parallel scraping with aggressive timeouts...", "info");

    // Start cookie rotation cycle immediately (non-blocking)
    this.forcePeriodicCookieRotation();
    this.logWithTime("Started 15-minute cookie rotation cycle", "info");
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
                // Skip if in cooldown or already being processed
                if (this.shouldSkipEvent(job.eventId)) {
                  continue;
                }

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

            // Create promise-returning functions for each event
            const eventPromiseFunctions = events
              .filter(eventId => !this.shouldSkipEvent(eventId))
              .map(eventId => {
                return async () => {
                  try {
                    // Process this individual event
                    return await this.scrapeEvent(eventId, 0);
                  } catch (error) {
                    this.logWithTime(
                      `Error processing event ${eventId}: ${error.message}`,
                      "error"
                    );
                    return false;
                  }
                };
              });

            // Find super-critical and critical events for prioritization
            const superCriticalIndices = events
              .map((eventId, index) => {
                const lastUpdate = this.eventUpdateTimestamps.get(eventId);
                const timeSinceUpdate = lastUpdate ? moment().diff(lastUpdate) : Infinity;
                return timeSinceUpdate > SUPER_CRITICAL_THRESHOLD ? index : -1;
              })
              .filter(index => index !== -1);

            // Process events in parallel with aggressive timeouts and priorities
            if (eventPromiseFunctions.length > 0) {
              this.logWithTime(`Processing ${eventPromiseFunctions.length} events in parallel with ${Math.min(eventPromiseFunctions.length, CONCURRENT_LIMIT)} concurrent limit`, "info");
              
              // Process in batches of CONCURRENT_LIMIT with priority for critical events
              await promiseWithConcurrency(
                eventPromiseFunctions,
                Math.min(eventPromiseFunctions.length, CONCURRENT_LIMIT),
                {
                  timeout: SCRAPE_TIMEOUT,
                  priorityIndices: superCriticalIndices
                }
              );
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
                // Skip if in cooldown or already being processed
                if (this.shouldSkipEvent(eventId)) {
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

  /**
   * Calculate adaptive pause time for sequential processing
   * @returns {number} Pause time in milliseconds
   */
  getAdaptivePauseTime() {
    // Base pause time - moderate for sequential processing
    let pauseTime = 200; // Base of 200ms for sequential processing

    // Add time based on active jobs and failed events
    const activeJobCount = this.activeJobs.size;
    const failedEventCount = this.failedEvents.size;

    // Linear scaling for sequential processing
    if (activeJobCount > 0) {
      pauseTime += Math.min(50 * activeJobCount, 500);
    }

    if (failedEventCount > 0) {
      pauseTime += Math.min(10 * failedEventCount, 300);
    }

    // Additional time if circuit breaker tripped
    if (this.apiCircuitBreaker.tripped) {
      pauseTime += 500;
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
    
    // Clear the cookie rotation interval if it exists
    if (this.cookieRotationIntervalId) {
      clearInterval(this.cookieRotationIntervalId);
      this.cookieRotationIntervalId = null;
      this.logWithTime("Cookie rotation cycle stopped", "info");
    }
    
    // Clear the CSV upload interval if it exists
    if (this.csvUploadIntervalId) {
      clearInterval(this.csvUploadIntervalId);
      this.csvUploadIntervalId = null;
      this.logWithTime("CSV upload cycle stopped", "info");
    }
    
    

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

  /**
   * Get recent failure count for an event
   * @param {string} eventId - The event ID
   * @returns {number} Number of recent failures
   */
  getRecentFailureCount(eventId) {
    const failures = this.eventFailureCounts.get(eventId) || [];
    const now = Date.now();
    const recentFailures = failures.filter(failure => 
      now - failure.timestamp < 10 * 60 * 1000 // Last 10 minutes
    );
    return recentFailures.length;
  }

  /**
   * Increment failure count for an event
   * @param {string} eventId - The event ID
   */
  incrementFailureCount(eventId) {
    if (!this.eventFailureCounts.has(eventId)) {
      this.eventFailureCounts.set(eventId, []);
    }
    
    const failures = this.eventFailureCounts.get(eventId);
    failures.push({
      timestamp: Date.now(),
      count: failures.length + 1
    });
    
    // Keep only recent failures (last 20)
    if (failures.length > 20) {
      failures.shift();
    }
  }

  /**
   * Clear failure count for an event
   * @param {string} eventId - The event ID
   */
  clearFailureCount(eventId) {
    this.eventFailureCounts.delete(eventId);
    this.eventFailureTimes.delete(eventId);
  }

  /**
   * Categorize error type for grouping
   * @param {Error} error - The error object
   * @returns {string} Error category
   */
  categorizeError(error) {
    const message = error.message.toLowerCase();
    
    if (message.includes('403') || message.includes('forbidden')) {
      return 'FORBIDDEN';
    } else if (message.includes('429') || message.includes('rate limit')) {
      return 'RATE_LIMIT';
    } else if (message.includes('400') || message.includes('bad request')) {
      return 'BAD_REQUEST';
    } else if (message.includes('timeout')) {
      return 'TIMEOUT';
    } else if (message.includes('network') || message.includes('connection')) {
      return 'NETWORK';
    } else if (message.includes('empty') || message.includes('null')) {
      return 'EMPTY_RESULT';
    } else {
      return 'OTHER';
    }
  }

  /**
   * Force periodic cookie and session rotation
   * Schedules automatic cookie and session refresh every 15 minutes
   */
  forcePeriodicCookieRotation() {
    // Clear any existing interval
    if (this.cookieRotationIntervalId) {
      clearInterval(this.cookieRotationIntervalId);
    }
    
    this.logWithTime("Starting 15-minute cookie and session rotation schedule", "info");
    
    // Immediately rotate on start
    this.rotateAllCookiesAndSessions();
    
    // Set up rotation interval (15 minutes)
    this.cookieRotationIntervalId = setInterval(() => {
      this.rotateAllCookiesAndSessions();
    }, SESSION_REFRESH_INTERVAL);
    
    return this.cookieRotationIntervalId;
  }
  
  /**
   * Rotates all cookies and sessions to ensure freshness
   */
  async rotateAllCookiesAndSessions() {
    try {
      this.logWithTime("Performing full cookie and session rotation", "info");
      
      // Clear header cache to force refresh
      this.headersCache.clear();
      this.headerRefreshTimestamps.clear();
      
      // Rotate sessions via session manager
      try {
        await this.sessionManager.forceSessionRotation();
        this.logWithTime("Successfully rotated all sessions", "success");
      } catch (sessionError) {
        this.logWithTime(`Error rotating sessions: ${sessionError.message}`, "error");
      }
      
      // Force cookie reset
      try {
        await this.resetCookiesAndHeaders();
        this.logWithTime("Successfully reset all cookies and headers", "success");
      } catch (cookieError) {
        this.logWithTime(`Error resetting cookies: ${cookieError.message}`, "error");
      }
      
      // Create multiple fresh test sessions to prime the cache
      try {
        // Get multiple random event IDs from the database
        const randomEvents = await Event.aggregate([
          {
            $match: {
              Skip_Scraping: { $ne: true },
              url: { $exists: true, $ne: "" },
            },
          },
          { $sample: { size: 5 } }, // Get 5 random events to create diversity
          { $project: { Event_ID: 1 } },
        ]);
        
        if (randomEvents && randomEvents.length > 0) {
          this.logWithTime(`Creating ${randomEvents.length} fresh test sessions with random events`, "info");
          
          // Process each random event to create multiple fresh sessions
          for (const randomEvent of randomEvents) {
            const eventId = randomEvent.Event_ID;
            // Force refresh headers to create new cookies
            await this.refreshEventHeaders(eventId, true);
            this.logWithTime(`Created fresh test session using random event ${eventId}`, "info");
            
            // Small delay between refreshes
            await setTimeout(500);
          }
        } else {
          // Fallback to using cached event IDs if available
          if (this.headerRefreshTimestamps.size > 0) {
            const cachedEvents = Array.from(this.headerRefreshTimestamps.keys());
            this.logWithTime(`No database events found, using ${Math.min(3, cachedEvents.length)} cached event IDs`, "warning");
            
            // Use up to 3 cached event IDs
            const eventsToUse = cachedEvents.slice(0, 3);
            for (const cachedId of eventsToUse) {
              await this.refreshEventHeaders(cachedId, true);
              this.logWithTime(`Created fresh test session using cached event ID ${cachedId}`, "info");
              await setTimeout(500);
            }
          } else {
            this.logWithTime(`No database events or cached events found, skipping test session creation`, "warning");
          }
        }
      } catch (testError) {
        this.logWithTime(`Error creating test sessions: ${testError.message}`, "warning");
        
                  // Even on error, try with any available event ID from cache
          try {
            if (this.headerRefreshTimestamps.size > 0) {
              // Get any event ID from the cache
              const cachedEvents = Array.from(this.headerRefreshTimestamps.keys());
              const fallbackId = cachedEvents[Math.floor(Math.random() * cachedEvents.length)];
              
              await this.refreshEventHeaders(fallbackId, true);
              this.logWithTime(`Created fallback session after error using cached event ID ${fallbackId}`, "info");
            } else {
              // Try a direct database query as last resort
              try {
                const fallbackEvent = await Event.findOne().sort({ Last_Updated: 1 }).limit(1).lean();
                if (fallbackEvent) {
                  await this.refreshEventHeaders(fallbackEvent.Event_ID, true);
                  this.logWithTime(`Created fallback session using database event ID ${fallbackEvent.Event_ID}`, "info");
                } else {
                  this.logWithTime(`No fallback events available, skipping session creation`, "error");
                }
              } catch (dbError) {
                this.logWithTime(`Final database fallback failed: ${dbError.message}`, "error");
              }
            }
          } catch (fallbackError) {
            this.logWithTime(`Failed to create fallback sessions: ${fallbackError.message}`, "error");
          }
      }
      
      this.logWithTime("Cookie and session rotation complete", "success");
    } catch (error) {
      this.logWithTime(`Error in cookie rotation: ${error.message}`, "error");
    }
  }
}

const scraperManager = new ScraperManager();
export default scraperManager;

// Add a function to export that allows checking logs
export function checkScraperLogs() {
  return scraperManager.checkLogs();
}
