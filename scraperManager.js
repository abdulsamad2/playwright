import moment from "moment";
import { setTimeout } from "timers/promises";
import { Event, ErrorLog, ConsecutiveGroup } from "./models/index.js";
import { ScrapeEvent, refreshHeaders, generateEnhancedHeaders } from "./scraper.js";
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
import { runCsvUploadCycle } from './helpers/csvUploadCycle.js';

// Add this at the top of the file, after imports
export const ENABLE_CSV_UPLOAD = false; // Set to false to disable all_events_combined.csv upload

const MAX_UPDATE_INTERVAL = 120000; // Strict 2-minute update requirement (reduced from 160000)
const CONCURRENT_LIMIT = 200; // Increased from 100 for 1000+ events
const MAX_RETRIES = 25; // Increased from 20 for better recovery
const SCRAPE_TIMEOUT = 45000; // Increased from 20 seconds to 45 seconds for better success rate
const MIN_TIME_BETWEEN_EVENT_SCRAPES = 30000; // Reduced to 30 seconds for faster cycles
const MAX_ALLOWED_UPDATE_INTERVAL = 180000; // Maximum 3 minutes allowed between updates
const EVENT_FAILURE_THRESHOLD = 120000; // Reduced to 2 minutes for faster recovery
const STALE_EVENT_THRESHOLD = 600000; // 10 minutes - events will be stopped after this

// Enhanced recovery settings for 1000+ events
const RECOVERY_BATCH_SIZE = 100; // Increased from 50 for better throughput
const MAX_RECOVERY_BATCHES = 20; // Increased from 10 for parallel processing
const PARALLEL_BATCH_SIZE = 150; // Increased from 100 for better batching
const MAX_PARALLEL_BATCHES = 25; // Increased from 20 for 1000+ events

// Multi-tier recovery intervals for aggressive processing
const CRITICAL_RECOVERY_INTERVAL = 10000; // Check critical events every 10 seconds
const AGGRESSIVE_RECOVERY_INTERVAL = 20000; // Check stale events every 20 seconds
const STANDARD_RECOVERY_INTERVAL = 30000; // Check standard events every 30 seconds
const AUTO_STOP_CHECK_INTERVAL = 60000; // Check auto-stop events every minute

// Logging levels: 0 = errors only, 1 = warnings + errors, 2 = info + warnings + errors, 3 = all (verbose)
const LOG_LEVEL = 3; // Default to warnings and errors only

// Cookie expiration threshold: refresh cookies every 15 minutes
const COOKIE_EXPIRATION_MS = 15 * 60 * 1000; // 15 minutes (reduced for more frequent rotation)
const SESSION_REFRESH_INTERVAL = 15 * 60 * 1000; // 15 minutes for session refresh

// Anti-bot helpers: rotate User-Agent and spoof IP
const generateRandomIp = () => Array.from({ length: 4 }, () => Math.floor(Math.random() * 256)).join('.');
// Number of proxy rotation attempts when fetching fresh cookies/headers

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

// CSV Upload Constants
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const DATA_DIR = path.join(__dirname, 'data');
const COMPANY_ID = '702';
const API_TOKEN = 'OaJwtlUQiriMSrnGd7cauDWtIyAMnS363icaz-7t1vJ7bjIBe9ZFjBwgPYY1Q9eKV_Jt';
const CSV_UPLOAD_INTERVAL = 6 * 60 * 1000; // 6 minutes in milliseconds


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
   this.runCsvUploadCycle = runCsvUploadCycle;
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
    
    // New: High-performance event processing
    this.eventProcessingQueue = []; // Queue of events to process
    this.processingBatches = new Map(); // Track active processing batches
    this.eventLastProcessedTime = new Map(); // Track when each event was last processed
    this.parallelWorkers = []; // Array of parallel worker promises
    this.maxParallelWorkers = MAX_PARALLEL_BATCHES;
    this.csvProcessingQueue = []; // Non-blocking CSV processing queue
    this.csvProcessingActive = false;
    this.eventPriorityScores = new Map(); // Cache priority scores
    this.processingLocks = new Map(); // Track event locks
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
        return true;
      } else {
        this.cooldownEvents.delete(eventId);
      }
    }

    // Check if event is already being processed
    if (this.processingEvents.has(eventId)) {
      return true;
    }

    // For high-performance mode, use last processed time instead of update timestamps
    const lastProcessed = this.eventLastProcessedTime.get(eventId);
    if (lastProcessed && Date.now() - lastProcessed < 30000) {
      // Skip if processed in last 30 seconds
      return true;
    }

    return false;
  }

  async refreshEventHeaders(eventId, forceRefresh = false) {
    // Anti-bot: Aggressively fetch fresh cookies/headers with 15-minute refresh cycle
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
                    `Using cached event ID ${eventToUse} for cookie refresh after database error`,
                    "warning"
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

    try {
      // Get event data upfront
      const event = await Event.findOne({ Event_ID: eventId })
        .select("priceIncreasePercentage inHandDate mapping_id Available_Seats metadata")
        .lean();
        
      if (!event) {
        throw new Error(`Event ${eventId} not found in database`);
      }

      const priceIncreasePercentage = event.priceIncreasePercentage || 25;
      const inHandDate = event.inHandDate || new Date();
      const mapping_id = event.mapping_id;

      if (!mapping_id) {
        throw new Error(`Event ${eventId} is missing required mapping_id`);
      }

      // Filter valid groups
      const validScrapeResult = scrapeResult.filter(
        (group) =>
          group.seats &&
          group.seats.length >= 2 &&
          group.inventory &&
          group.inventory.quantity >= 2
      );

      const currentTicketCount = validScrapeResult.length;

      // Quick update of basic info
      await Event.updateOne(
        { Event_ID: eventId },
        {
          $set: {
            Available_Seats: currentTicketCount,
            Last_Updated: new Date(), // Ensure Last_Updated is always set to now
            "metadata.lastUpdate": new Date(),
            "metadata.ticketCount": currentTicketCount
          },
        }
      );

      const LOG_LEVEL = (global.config && typeof global.config.LOG_LEVEL !== 'undefined') ? global.config.LOG_LEVEL : (process.env.LOG_LEVEL || 2);

      if (validScrapeResult?.length > 0) {
        // Fetch existing groups for comparison
        const existingGroups = await ConsecutiveGroup.find(
          { eventId }, 
          { section: 1, row: 1, seats: 1, "inventory.listPrice": 1 } // Assuming inventory.listPrice is comparable or seats have their own price
        ).lean();

        const existingSeats = new Set(
          existingGroups.flatMap((g) =>
            g.seats.map((s) => `${g.section}-${g.row}-${s.number}-${s.price || g.inventory?.listPrice}`) // Use seat price, fallback to group price
          )
        );

        const newSeats = new Set(
          validScrapeResult.flatMap((g) =>
            g.seats.map(
              (s) => `${g.section}-${g.row}-${s}-${g.inventory.listPrice}` // Assuming g.inventory.listPrice is the comparable price for new seats
            )
          )
        );

        if (LOG_LEVEL >= 3) {
          this.logWithTime(`[Debug SM ${eventId}] Existing Seats Count: ${existingSeats.size}`, "debug");
          if (existingSeats.size > 0) this.logWithTime(`[Debug SM ${eventId}] Sample Existing Seats: ${Array.from(existingSeats).slice(0, 3).join(', ')}`, "debug");
          this.logWithTime(`[Debug SM ${eventId}] New Seats Count: ${newSeats.size}`, "debug");
          if (newSeats.size > 0) this.logWithTime(`[Debug SM ${eventId}] Sample New Seats: ${Array.from(newSeats).slice(0, 3).join(', ')}`, "debug");
        }

        const hasChanges =
          existingSeats.size !== newSeats.size ||
          [...existingSeats].some((s) => !newSeats.has(s)) ||
          [...newSeats].some((s) => !existingSeats.has(s));

        if (LOG_LEVEL >= 3) {
          this.logWithTime(`[Debug SM ${eventId}] Change Detected for ConsecutiveGroup: ${hasChanges}`, "debug");
        }

        if (!hasChanges) {
          if (LOG_LEVEL >= 3) {
              this.logWithTime(`[Debug SM ${eventId}] No seat changes detected. Skipping ConsecutiveGroup update.`, "debug");
          }
        } else {
          // Use bulk operations for efficiency
          const bulkOps = [];
          
          // Delete existing groups
          bulkOps.push({
            deleteMany: {
              filter: { eventId }
            }
          });

          // Prepare groups to insert
          const groupsToInsert = validScrapeResult.map((group) => {
            const basePrice = parseFloat(group.inventory.listPrice) || 500.0;
            const increasedPrice = basePrice * (1 + priceIncreasePercentage / 100);
            const formattedInHandDate = inHandDate instanceof Date ? inHandDate : new Date();

            return {
              insertOne: {
                document: {
                  eventId,
                  mapping_id,
                  section: group.section,
                  row: group.row,
                  seatCount: group.inventory.quantity,
                  seatRange: `${Math.min(...group.seats)}-${Math.max(...group.seats)}`,
                  seats: group.seats.map((seatNumber) => ({
                    number: seatNumber.toString(),
                    inHandDate: formattedInHandDate,
                    price: increasedPrice,
                    mapping_id,
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
                    notes: group.inventory.notes || `These are internal notes. @sec[${group.section}]`,
                    tags: group.inventory.tags || "john drew don",
                    inventoryId: group.inventory.inventoryId,
                    offerId: group.inventory.offerId,
                    splitType: group.inventory.splitType || "CUSTOM",
                    publicNotes: group.inventory.publicNotes || `These are ${group.row} Row`,
                    listPrice: increasedPrice,
                    customSplit: group.inventory.customSplit || `${Math.ceil(group.inventory.quantity / 2)},${group.inventory.quantity}`,
                    mapping_id,
                    tickets: group.inventory.tickets.map((ticket) => ({
                      id: ticket.id,
                      seatNumber: ticket.seatNumber,
                      notes: ticket.notes,
                      cost: ticket.cost,
                      faceValue: ticket.faceValue,
                      taxedCost: ticket.taxedCost,
                      sellPrice: typeof ticket?.sellPrice === "number" && !isNaN(ticket?.sellPrice)
                        ? ticket.sellPrice
                        : parseFloat(ticket?.cost || ticket?.faceValue || 0),
                      stockType: ticket.stockType,
                      eventId: ticket.eventId,
                      accountId: ticket.accountId,
                      status: ticket.status,
                      auditNote: ticket.auditNote,
                      mapping_id: mapping_id,
                    })),
                  },
                }
              }
            };
          });

          // Add all insert operations
          bulkOps.push(...groupsToInsert);

          // Execute bulk operation
          if (bulkOps.length > 1) { // More than just the delete
            if (LOG_LEVEL >= 2) {
              this.logWithTime(`[Info SM ${eventId}] Updating ${groupsToInsert.length} consecutive groups.`, "info");
            }
            await ConsecutiveGroup.bulkWrite(bulkOps, { ordered: false });
          } else if (bulkOps.length === 1 && groupsToInsert.length === 0 && existingGroups.length > 0) {
            // This means only a delete operation was added (to clear all groups) and no new groups to insert, but there were existing groups.
            if (LOG_LEVEL >= 2) {
              this.logWithTime(`[Info SM ${eventId}] Deleting all ${existingGroups.length} existing consecutive groups as no new valid groups found.`, "info");
            }
            await ConsecutiveGroup.bulkWrite(bulkOps, { ordered: false }); // Execute the delete
          }
        } // This closes the 'else' from 'if (!hasChanges)'
      } else if (validScrapeResult?.length === 0) {
          // Handle case where scrape result is empty: delete all existing groups for this eventId
          const existingGroupCount = await ConsecutiveGroup.countDocuments({ eventId });
          if (existingGroupCount > 0) {
              if (LOG_LEVEL >= 2) {
                  this.logWithTime(`[Info SM ${eventId}] No valid groups in scrape result. Deleting ${existingGroupCount} existing consecutive groups.`, "info");
              }
              await ConsecutiveGroup.deleteMany({ eventId });
          } else {
              if (LOG_LEVEL >= 3) {
                  this.logWithTime(`[Debug SM ${eventId}] No valid groups in scrape result and no existing groups to delete.`, "debug");
              }
          }
      }  

      // Update schedule
      this.eventUpdateSchedule.set(
        eventId,
        moment().add(MIN_TIME_BETWEEN_EVENT_SCRAPES, "milliseconds")
      );
      
      // Make sure to properly update the timestamp in memory as well
      this.eventUpdateTimestamps.set(eventId, moment());
      
      // Also update the last processed time to prevent immediate reprocessing
      this.eventLastProcessedTime.set(eventId, Date.now());

      if (LOG_LEVEL >= 3) {
        this.logWithTime(
          `Updated event ${eventId} in ${(performance.now() - startTime).toFixed(2)}ms`,
          "debug"
        );
      }
    } catch (error) {
      await this.logError(eventId, "DATABASE_ERROR", error);
      throw error;
    }
  }

  /**
   * Generate CSV inventory for an event for each scrape (SUPER FAST - non-blocking)
   * @param {string} eventId - The event ID
   * @param {Array} scrapeResult - The scrape result data
   */
  async generateInventoryCsv(eventId, scrapeResult) {
    try {
      // Import the inventory controller
      const inventoryController = (
        await import("./controllers/inventoryController.js")
      ).default;

      // Get event data upfront with minimal fields for speed
      const event = await Event.findOne({ Event_ID: eventId })
        .select("Event_Name Venue Event_DateTime priceIncreasePercentage inHandDate mapping_id")
        .lean();

      if (!event) {
        return; // Fail silently for speed
      }

      // Quick validation - fail fast if missing critical data
      if (!event.mapping_id || !event.priceIncreasePercentage) {
        return; // Fail silently for speed
      }

      const mapping_id = event.mapping_id;
      const priceIncreasePercentage = event.priceIncreasePercentage;

      // Quick filter - only essential validation
      const validGroups = scrapeResult.filter(
        (group) => group.seats?.length >= 2 && group.inventory?.quantity >= 2
      );

      if (validGroups.length === 0) {
        return; // Fail silently for speed
      }

      // Fast record creation - minimal processing
      const inventoryRecords = validGroups.map((group) => {
        const basePrice = parseFloat(group.inventory.listPrice) || 0;
        const increasedPrice = (basePrice * (1 + priceIncreasePercentage / 100)).toFixed(2);
        const formattedInHandDate = event?.inHandDate?.toISOString().split("T")[0];

        // Minimal record structure for speed
        return {
          inventory_id: `${eventId}-${group.section}-${group.row}-${Date.now()}`, // Fast ID generation
          event_name: event?.Event_Name || `Event ${eventId}`,
          venue_name: event?.Venue || "Unknown Venue",
          event_date: event?.Event_DateTime?.toISOString(),
          event_id: mapping_id,
          quantity: group.inventory.quantity,
          section: group.section,
          row: group.row,
          seats: group.seats.map(seat => seat.toString()).join(","),
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
          custom_split: `${Math.ceil(group.inventory.quantity / 2)},${group.inventory.quantity}`,
          stock_type: "MOBILE_TRANSFER",
          zone: "N",
          shown_quantity: String(Math.ceil(group.inventory.quantity / 2)),
          passthrough: "",
          mapping_id: mapping_id,
          source_event_id: eventId,
          // Essential fields only - remove unnecessary data
          barcodes: "",
          internal_notes: `@sec[${group.section}]`,
          public_notes: `${group.row} Row`,
          tags: ""
        };
      });

      // Use super-fast bulk inventory processing (returns immediately)
      const result = inventoryController.addBulkInventory(inventoryRecords, eventId);

      // Log result but don't wait for background processing
      if (result.success) {
        if (LOG_LEVEL >= 3) {
          this.logWithTime(
            `Fast: Processed ${result.stats.processed} records for event ${eventId}`,
            "debug"
          );
        }
      }
    } catch (error) {
      // Log error but don't throw - keep processing fast
      if (LOG_LEVEL >= 1) {
        console.error(`Fast CSV processing error for ${eventId}: ${error.message}`);
      }
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

  async scrapeEvent(eventId, retryCount = 0, proxyAgent = null, proxy = null, passedHeaders = null) {
    // Skip if the event should be skipped
    if (this.shouldSkipEvent(eventId)) {
      return false;
    }

    // If circuit breaker is tripped, allow critical events to proceed
    if (this.apiCircuitBreaker.tripped) {
      const lastUpdate = this.eventUpdateTimestamps.get(eventId);
      const timeSinceUpdate = lastUpdate ? moment().diff(lastUpdate) : Infinity;

      // Allow processing of critical events approaching 3-minute maximum, or events approaching 2-minute target
      if (
        timeSinceUpdate < MAX_UPDATE_INTERVAL - 10000 && // Reduced threshold for more aggressive processing
        timeSinceUpdate < MAX_ALLOWED_UPDATE_INTERVAL - 20000 // Reduced threshold
      ) {
        if (LOG_LEVEL >= 2) {
          this.logWithTime(
            `Skipping ${eventId} temporarily: Circuit breaker tripped`,
            "info"
          );
        }
        return false;
      } else if (timeSinceUpdate > MAX_ALLOWED_UPDATE_INTERVAL - 20000) {
        // Log urgent processing despite circuit breaker
        if (LOG_LEVEL >= 1) {
          this.logWithTime(
            `URGENT: Processing ${eventId} despite circuit breaker: Approaching maximum allowed time (${
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
        this.logWithTime(
          `Scraping ${eventId} (Attempt ${retryCount + 1}/${MAX_RETRIES})`,
          "info"
        );
      }

      // Get a new proxy on retries - no blocking
      if (retryCount > 0) {
        // Release any existing proxy to get a new one
        this.proxyManager.releaseProxy(eventId, false);
        
        if (LOG_LEVEL >= 2) {
          this.logWithTime(
            `Getting new proxy for retry #${retryCount} of event ${eventId}`,
            "info"
          );
        }
        
        // Reset proxy variables to ensure we get fresh ones
        proxyAgent = null;
        proxy = null;
      }

      // Use passed proxy if available, otherwise get a new one
      let proxyToUse = proxy;
      let proxyAgentToUse = proxyAgent;

      if (!proxyAgentToUse || !proxyToUse) {
        const { proxyAgent: newProxyAgent, proxy: newProxy } =
          this.proxyManager.getProxyForBatch([eventId]);
        proxyAgentToUse = newProxyAgent;
        proxyToUse = newProxy;
        
        if (LOG_LEVEL >= 2) {
          this.logWithTime(
            `Using proxy ${newProxy?.proxy || 'default'} for ${retryCount > 0 ? 'retry #' + retryCount : 'initial attempt'} of event ${eventId}`,
            "info"
          );
        }
      }

      // Look up the event first before trying to scrape
      const event = await Event.findOne({ Event_ID: eventId })
        .select("Skip_Scraping inHandDate url")
        .lean();

      if (!event) {
        this.logWithTime(`Event ${eventId} not found in database`, "error");
        // Put event in cooldown to avoid immediate retries
        this.cooldownEvents.set(eventId, moment().add(30, "minutes")); // Reduced cooldown
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

      // ENHANCED: Always force fresh cookies/headers on retries
      const needsFreshCookies = retryCount > 0 || 
                               !this.headerRefreshTimestamps.get(eventId) || 
                               moment().diff(this.headerRefreshTimestamps.get(eventId)) > SESSION_REFRESH_INTERVAL;

      // If headers were passed from batch processing, still check if they're fresh enough
      if (passedHeaders && !retryCount) {
        const passedHeadersAge = passedHeaders.timestamp ? Date.now() - passedHeaders.timestamp : Infinity;
        
        // Use passed headers only if they're fresh enough (less than 15 minutes old) and not a retry
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
          const randomEventId = await this.getRandomEventId(eventId);
          headers = await this.refreshEventHeaders(randomEventId, true);
          usedFreshCookies = true;
        }
      } else {
        // On retries, always force fresh session
        const forceNewSession = retryCount > 0;
        
        if (forceNewSession && LOG_LEVEL >= 2) {
          this.logWithTime(
            `Forcing fresh session for retry #${retryCount} of event ${eventId}`,
            "info"
          );
        }
        
        // Prioritize session manager for cookie/session management
        try {
          // Always force new session on retries or after expiration
          const forceNewSession = needsFreshCookies || retryCount > 0;
          
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
                freshlyGenerated: forceNewSession,
                retryCount: retryCount // Add retry count for tracking
              };
              
              usedFreshCookies = forceNewSession;
              
              if (LOG_LEVEL >= 2) {
                this.logWithTime(
                  `Using ${forceNewSession ? 'fresh' : 'existing'} session headers for ${retryCount > 0 ? 'retry #' + retryCount : 'initial attempt'} of event ${eventId} (session: ${sessionData.sessionId})`,
                  "info"
                );
              }
              
              // Add retry information to headers
              if (headers.headers && retryCount > 0) {
                headers.headers["X-Retry-Count"] = retryCount.toString();
                headers.headers["X-Retry-ID"] = `retry-${eventId}-${retryCount}-${Date.now()}`;
              }
            }
          }
        } catch (sessionError) {
          if (LOG_LEVEL >= 2) {
            this.logWithTime(
              `Session manager error: ${sessionError.message}, falling back to header refresh`,
              "warning"
            );
          }
        }

        // If session approach fails, use direct header management
        if (!headers) {
          // For retries or no recent headers, always force refresh
          if (needsFreshCookies || retryCount > 0) {
            if (LOG_LEVEL >= 2) {
              this.logWithTime(
                `Getting fresh headers for ${retryCount > 0 ? 'retry #' + retryCount : 'initial attempt'} of event ${eventId}`,
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
            
            // Add retry information to headers
            if (headers && headers.headers && retryCount > 0) {
              headers.headers["X-Retry-Count"] = retryCount.toString();
              headers.headers["X-Retry-ID"] = `retry-${eventId}-${retryCount}-${Date.now()}`;
            }
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
          `${usedFreshCookies ? 'Using fresh cookies' : 'Reusing existing cookies'} for ${retryCount > 0 ? 'retry #' + retryCount : 'initial attempt'} of event ${eventId}`,
          usedFreshCookies ? "info" : "debug"
        );
      }

      // Set a longer timeout for the scrape with better error handling
      const result = await Promise.race([
        ScrapeEvent({ 
          eventId, 
          headers, 
          retryCount, // Add retry count to scrape data
          retryAttempt: retryCount > 0 // Flag that this is a retry
        }, proxyAgentToUse, proxyToUse),
        setTimeout(SCRAPE_TIMEOUT).then(() => {
          throw new Error(`Scrape timeout after ${SCRAPE_TIMEOUT}ms`);
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
        // Don't treat empty results as failures - they might be valid
        this.logWithTime(
          `Event ${eventId} returned empty result - may be sold out or no inventory`,
          "warning"
        );
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

      // Generate fresh CSV inventory for each scrape (non-blocking)
      this.queueCsvGeneration(eventId, result);

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

      // Increase retry limit by 3 after a successful scrape (reduced from 5)
      const prevMax = this.eventMaxRetries.get(eventId) ?? MAX_RETRIES;
      const newMax = Math.min(prevMax + 3, MAX_RETRIES * 2); // Cap at 2x MAX_RETRIES
      this.eventMaxRetries.set(eventId, newMax);
      if (LOG_LEVEL >= 2) {
        this.logWithTime(
          `Increased retry limit for ${eventId} to ${newMax}`,
          "info"
        );
      }

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

      // MODIFIED: Check the time since last update to determine if we should stop
      const lastUpdate = this.eventUpdateTimestamps.get(eventId);
      const timeSinceUpdate = lastUpdate ? moment().diff(lastUpdate) : Infinity;

      // If approaching max allowed update interval of 10 minutes, escalate retries
      if (timeSinceUpdate >= 600000) { // 10 minutes threshold
        this.logWithTime(
          `Event ${eventId} exceeded 10-minute threshold (${Math.floor(timeSinceUpdate/1000)}s) - marking for auto-stop`,
          "error"
        );
        
        // Use the unified auto-stop mechanism
        await this.setEventSkipScraping(eventId, "exceeded_10min_threshold", retryCount, MAX_RETRIES);
        return false;
      } 
      
      // Calculate backoff time based on retry count and error type
      let backoffTime;
      const isApiError = error.message.includes("403") || 
        error.message.includes("400") ||
        error.message.includes("429") ||
        error.message.includes("timeout") || // Include timeout errors
        error.message.includes("API");

      // For critical events approaching the threshold, use smaller backoff times
      if (timeSinceUpdate > 540000) { // Over 9 minutes
        // Very aggressive retry for events approaching threshold
        backoffTime = 3000; // Just 3 seconds for critical events
        this.logWithTime(
          `CRITICAL: Event ${eventId} at ${Math.floor(timeSinceUpdate/1000)}s since update - using minimal 3s backoff`,
          "warning"
        );
      } else if (timeSinceUpdate > 300000) { // Over 5 minutes
        // Aggressive retry for stale events
        backoffTime = Math.min(5000, SHORT_COOLDOWNS[retryCount % SHORT_COOLDOWNS.length]);
        this.logWithTime(
          `URGENT: Event ${eventId} at ${Math.floor(timeSinceUpdate/1000)}s since update - using reduced ${backoffTime}ms backoff`,
          "warning"
        );
      } else if (retryCount < SHORT_COOLDOWNS.length) {
        // Use progressive short cooldowns for initial retries
        backoffTime = SHORT_COOLDOWNS[retryCount];

        if (LOG_LEVEL >= 1) {
          this.logWithTime(
            `${isApiError ? "API/Timeout error" : "Error"} for ${eventId}: ${error.message}. Retry ${retryCount + 1}/${MAX_RETRIES} in ${backoffTime}ms`,
            "warning"
          );
        }
      } else {
        // Use shorter cooldown for persistent failures but ensure we can meet 10-minute target
        const remainingTime = Math.max(600000 - timeSinceUpdate, 10000);
        backoffTime = Math.min(30000, remainingTime / 3); // Allow at least 3 more retries within remaining time
        
        this.logWithTime(
          `Persistent ${isApiError ? "API/timeout errors" : "errors"} for ${eventId}: ${error.message}. ` +
          `Retry in ${backoffTime/1000}s (${Math.floor(timeSinceUpdate/1000)}s since last update, ${Math.floor(remainingTime/1000)}s until threshold)`,
          "warning"
        );
      }

      // Set cooldown
      const cooldownUntil = moment().add(backoffTime, "milliseconds");
      this.cooldownEvents.set(eventId, cooldownUntil);

      // Add to retry queue with flags for fresh proxy and session
      this.retryQueue.push({
        eventId,
        retryCount: retryCount + 1,
        retryAfter: cooldownUntil,
        priority: Math.max(1, 15 - retryCount), // Higher priority for fewer retries
        needsFreshProxy: true, // Flag to indicate we need a fresh proxy
        needsFreshSession: true // Flag to indicate we need a fresh session
      });

      // Update event schedule for next update
      this.eventUpdateSchedule.set(
        eventId,
        moment().add(Math.min(backoffTime, MIN_TIME_BETWEEN_EVENT_SCRAPES), "milliseconds")
      );

      if (LOG_LEVEL >= 2) {
        this.logWithTime(
          `Queued ${eventId} for retry ${retryCount + 1}/${MAX_RETRIES} (cooldown: ${backoffTime}ms) with fresh proxy and session`,
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

  /**
   * Calculate adaptive pause time for sequential processing
   * @returns {number} Pause time in milliseconds
   */
  getAdaptivePauseTime() {
    // Minimal pause for high-performance processing
    let pauseTime = 10; // Reduced from 200ms to 10ms

    // Only add significant pause if system is overloaded
    const activeJobCount = this.activeJobs.size;
    const failedEventCount = this.failedEvents.size;

    // Only throttle if we have many active jobs
    if (activeJobCount > CONCURRENT_LIMIT * 0.8) {
      pauseTime += 50;
    }

    // Add small pause for failed events
    if (failedEventCount > 100) {
      pauseTime += 20;
    }

    // Circuit breaker adds minimal pause
    if (this.apiCircuitBreaker.tripped) {
      pauseTime += 100;
    }

    return Math.min(pauseTime, 200); // Cap at 200ms max
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

      // Try rotating proxy (just get a new one, no blocking)
      if (this.proxyManager.getAvailableProxyCount() > 1) {
        if (LOG_LEVEL >= 2) {
          this.logWithTime(
            `Getting new proxy for event ${eventId} due to ${
              is403Error ? "403" : "429"
            } error`,
            "info"
          );
        }
        
        // Just release the proxy to get a new one next time
        this.proxyManager.releaseProxy(eventId, false);
        return true; // Error handled by proxy rotation
      }
    }

    return false; // Error not handled
  }

  /**
   * Asynchronously upload all_events_combined.csv file to Sync service (OPTIMIZED)
   * This runs every 6 minutes while continuous scraping is active
   * and doesn't block the main scraping process
 */


  /**
   * Stop the continuous scraping process and clean up resources
   * @returns {Promise<void>}
   */
  async stopContinuousScraping() {
    // Set the isRunning flag to false to stop the main scraping loop
    this.isRunning = false;
    
    this.logWithTime("Stopping continuous scraping process...", "info");
    
    // Wait for all parallel workers to finish
    if (this.parallelWorkers.length > 0) {
      this.logWithTime(`Waiting for ${this.parallelWorkers.length} parallel workers to finish...`, "info");
      await Promise.allSettled(this.parallelWorkers);
      this.parallelWorkers = [];
    }
    
    // Clear the cookie rotation interval if it exists
    if (this.cookieRotationIntervalId) {
      clearInterval(this.cookieRotationIntervalId);
      this.cookieRotationIntervalId = null;
      this.logWithTime("Cookie rotation cycle stopped", "info");
    }
    
    // // Clear the CSV upload interval if it exists
    // if (this.csvUploadIntervalId) {
    //   clearInterval(this.csvUploadIntervalId);
    //   this.csvUploadIntervalId = null;
    //   this.logWithTime("CSV upload cycle stopped", "info");
    // }
    
    // // Wait for CSV processing to complete
    // if (this.csvProcessingActive) {
    //   this.logWithTime("Waiting for CSV processing to complete...", "info");
    //   let waitTime = 0;
    //   while (this.csvProcessingActive && waitTime < 30000) {
    //     await setTimeout(100);
    //     waitTime += 100;
    //   }
    // }
    
    // // Run one final CSV upload to ensure latest data is uploaded
    // try {
    //   this.logWithTime("Running final CSV upload before shutdown", "info");
    //   await this.runCsvUploadCycle();
    // } catch (error) {
    //   this.logWithTime(`Error in final CSV upload: ${error.message}`, "error");
    // }
    
    // Release any active proxies
    this.proxyManager.releaseAllProxies();
    
    // Clear all processing queues
    this.eventProcessingQueue = [];
    this.csvProcessingQueue = [];
    this.processingEvents.clear();
    this.processingBatches.clear();
    
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
      failedCount: this.failedEvents.size,
      totalEventsProcessed: this.eventLastProcessedTime.size
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
    
    // Immediately rotate on start (non-blocking)
    this.rotateAllCookiesAndSessions().catch(err => {
      this.logWithTime(`Initial cookie rotation error: ${err.message}`, "error");
    });
    
    // Set up rotation interval (15 minutes)
    this.cookieRotationIntervalId = setInterval(() => {
      // Non-blocking rotation
      this.rotateAllCookiesAndSessions().catch(err => {
        this.logWithTime(`Periodic cookie rotation error: ${err.message}`, "error");
      });
    }, SESSION_REFRESH_INTERVAL);
    
    return this.cookieRotationIntervalId;
  }
  
  /**
   * Rotates all cookies and sessions to ensure freshness
   */
  async rotateAllCookiesAndSessions() {
    // Make the entire process non-blocking by wrapping in a Promise with timeout
    return new Promise(async (resolve) => {
      // Set a maximum timeout for the entire rotation process
      const rotationTimeout = setTimeout(() => {
        this.logWithTime("Cookie and session rotation timed out after 2 minutes - continuing operation", "warning");
        resolve(false);
      }, 120000); // 2 minute timeout

      try {
        this.logWithTime("Performing full cookie and session rotation", "info");
        
        // Clear header cache to force refresh
        this.headersCache.clear();
        this.headerRefreshTimestamps.clear();
        
        // Rotate sessions via session manager with timeout
        try {
          const sessionPromise = this.sessionManager.forceSessionRotation();
          const sessionResult = await Promise.race([
            sessionPromise,
            setTimeout(30000).then(() => { throw new Error("Session rotation timed out after 30 seconds"); })
          ]);
          this.logWithTime("Successfully rotated all sessions", "success");
        } catch (sessionError) {
          this.logWithTime(`Error rotating sessions: ${sessionError.message}`, "error");
          // Continue execution even after session error
        }
        
        // Force cookie reset with timeout
        try {
          const cookiePromise = this.resetCookiesAndHeaders();
          const cookieResult = await Promise.race([
            cookiePromise,
            setTimeout(30000).then(() => { throw new Error("Cookie reset timed out after 30 seconds"); })
          ]);
          this.logWithTime("Successfully reset all cookies and headers", "success");
        } catch (cookieError) {
          this.logWithTime(`Error resetting cookies: ${cookieError.message}`, "error");
          // Continue execution even after cookie error
        }
        
        // Create multiple fresh test sessions to prime the cache
        try {
          // Get multiple random event IDs from the database with timeout
          const randomEventsPromise = Event.aggregate([
            {
              $match: {
                Skip_Scraping: { $ne: true },
                url: { $exists: true, $ne: "" },
              },
            },
            { $sample: { size: 5 } }, // Get 5 random events to create diversity
            { $project: { Event_ID: 1 } },
          ]);
          
          const randomEvents = await Promise.race([
            randomEventsPromise,
            setTimeout(10000).then(() => { throw new Error("Database query timed out after 10 seconds"); })
          ]);
          
          if (randomEvents && randomEvents.length > 0) {
            this.logWithTime(`Creating ${randomEvents.length} fresh test sessions with random events`, "info");
            
            // Process each random event to create multiple fresh sessions
            const sessionPromises = [];
            for (const randomEvent of randomEvents) {
              const eventId = randomEvent.Event_ID;
              // Push each refresh promise to array but don't await
              sessionPromises.push(
                Promise.race([
                  this.refreshEventHeaders(eventId, true),
                  setTimeout(15000).then(() => { throw new Error(`Header refresh timed out for ${eventId}`); })
                ])
                .then(() => this.logWithTime(`Created fresh test session using random event ${eventId}`, "info"))
                .catch(err => this.logWithTime(`Error creating session for ${eventId}: ${err.message}`, "warning"))
              );
              
              // Small delay between starting refreshes
              await setTimeout(100);
            }
            
            // Wait for all session promises with overall timeout
            await Promise.race([
              Promise.allSettled(sessionPromises),
              setTimeout(60000).then(() => { throw new Error("Session creation timed out after 60 seconds"); })
            ]);
          } else {
            // Fallback to using cached event IDs if available
            if (this.headerRefreshTimestamps.size > 0) {
              const cachedEvents = Array.from(this.headerRefreshTimestamps.keys());
              this.logWithTime(`No database events found, using ${Math.min(3, cachedEvents.length)} cached event IDs`, "warning");
              
              // Use up to 3 cached event IDs with timeouts
              const fallbackPromises = [];
              const eventsToUse = cachedEvents.slice(0, 3);
              for (const cachedId of eventsToUse) {
                fallbackPromises.push(
                  Promise.race([
                    this.refreshEventHeaders(cachedId, true),
                    setTimeout(15000).then(() => { throw new Error(`Header refresh timed out for ${cachedId}`); })
                  ])
                  .then(() => this.logWithTime(`Created fresh test session using cached event ID ${cachedId}`, "info"))
                  .catch(err => this.logWithTime(`Error creating session for ${cachedId}: ${err.message}`, "warning"))
                );
                await setTimeout(100);
              }
              
              // Wait for all fallback promises with timeout
              await Promise.race([
                Promise.allSettled(fallbackPromises),
                setTimeout(45000).then(() => { throw new Error("Fallback session creation timed out after 45 seconds"); })
              ]);
            } else {
              this.logWithTime(`No database events or cached events found, skipping test session creation`, "warning");
            }
          }
        } catch (testError) {
          this.logWithTime(`Error creating test sessions: ${testError.message}`, "warning");
          
          // Even on error, try with any available event ID from cache (with timeout)
          try {
            if (this.headerRefreshTimestamps.size > 0) {
              // Get any event ID from the cache
              const cachedEvents = Array.from(this.headerRefreshTimestamps.keys());
              const fallbackId = cachedEvents[Math.floor(Math.random() * cachedEvents.length)];
              
              await Promise.race([
                this.refreshEventHeaders(fallbackId, true),
                setTimeout(15000).then(() => { throw new Error(`Final header refresh timed out for ${fallbackId}`); })
              ]);
              this.logWithTime(`Created fallback session after error using cached event ID ${fallbackId}`, "info");
            } else {
              // Try a direct database query as last resort (with timeout)
              try {
                const fallbackEventPromise = Event.findOne().sort({ Last_Updated: 1 }).limit(1).lean();
                const fallbackEvent = await Promise.race([
                  fallbackEventPromise,
                  setTimeout(10000).then(() => { throw new Error("Final database query timed out after 10 seconds"); })
                ]);
                
                if (fallbackEvent) {
                  await Promise.race([
                    this.refreshEventHeaders(fallbackEvent.Event_ID, true),
                    setTimeout(15000).then(() => { throw new Error(`Final header refresh timed out for ${fallbackEvent.Event_ID}`); })
                  ]);
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
        clearTimeout(rotationTimeout);
        resolve(true);
      } catch (error) {
        this.logWithTime(`Error in cookie rotation: ${error.message}`, "error");
        clearTimeout(rotationTimeout);
        resolve(false);
      }
    });
  }

  // Helper method to get retry events ready to process
  getRetryEventsToProcess() {
    const now = Date.now();
    return this.retryQueue.filter(event => 
      event.nextAttemptAt <= now && 
      (!this.processingLocks.has(event.id) || 
      (now - this.processingLocks.get(event.id)) > LOCK_TIMEOUT)
    );
  }

  /**
   * Start a parallel worker that processes events from the queue
   */
  async startParallelWorker(workerId) {
    while (this.isRunning) {
      try {
        // Get batch of events from queue
        const batchSize = PARALLEL_BATCH_SIZE;
        const batch = [];
        
        // Pull events from queue
        while (batch.length < batchSize && this.eventProcessingQueue.length > 0) {
          const event = this.eventProcessingQueue.shift();
          if (event && !this.processingEvents.has(event.eventId || event)) {
            batch.push(event);
          }
        }

        if (batch.length > 0) {
          // Process batch in parallel
          await this.processBatchParallel(batch, workerId);
        } else {
          // No events to process, wait a bit
          await setTimeout(50); // Reduced for faster cycling
        }
      } catch (error) {
        console.error(`Worker ${workerId} error: ${error.message}`);
        await setTimeout(500);
      }
    }
  }

  /**
   * Process a batch of events in parallel
   */
  async processBatchParallel(batch, workerId) {
    const batchId = `batch-${workerId}-${Date.now()}`;
    this.processingBatches.set(batchId, batch);

    try {
      // Process all events in parallel with unique sessions for each
      const promises = batch.map(async (eventItem) => {
        const eventId = eventItem.eventId || eventItem;
        const retryCount = eventItem.retryCount || 0;
        
        try {
          // Mark as processing
          this.processingEvents.add(eventId);
          
          // Process with unique session for each event (no shared headers)
          const result = await this.scrapeEventOptimized(eventId, retryCount, null);
          
          if (result) {
            // Update last processed time
            this.eventLastProcessedTime.set(eventId, Date.now());
          }
          
          return { eventId, success: result };
        } catch (error) {
          console.error(`Error processing ${eventId}: ${error.message}`);
          return { eventId, success: false, error };
        } finally {
          // Always remove from processing set
          this.processingEvents.delete(eventId);
        }
      });

      // Wait for all to complete
      const results = await Promise.all(promises);
      
      // Log batch results
      const successful = results.filter(r => r.success).length;
      if (LOG_LEVEL >= 2) {
        this.logWithTime(
          `Worker ${workerId} completed batch: ${successful}/${batch.length} successful (each with unique session)`,
          "info"
        );
      }
      
    } finally {
      this.processingBatches.delete(batchId);
    }
  }

  async processEventBatch(events) {
    const now = Date.now();
    const processable = events.filter(event => {
      const lockTime = this.processingLocks.get(event.id);
      return !lockTime || (now - lockTime) > LOCK_TIMEOUT;
    });

    processable.forEach(event => this.processingLocks.set(event.id, now));
    
    const results = await this._processBatch(processable);
    
    results.successful.forEach(event => this.processingLocks.delete(event.id));
    return results;
  }

  /**
   * Optimized scrape event method for parallel processing
   */
  async scrapeEventOptimized(eventId, retryCount = 0, sharedHeaders = null) {
    // Check if we should skip
    const lastProcessed = this.eventLastProcessedTime.get(eventId);
    if (lastProcessed && Date.now() - lastProcessed < 30000) {
      return true; // Skip if processed in last 30 seconds
    }

    let proxyAgent = null;
    let proxy = null;

    try {
      // Simply get a new proxy on retries - no blocking
      if (retryCount > 0) {
        // Just release the proxy and get a new one
        this.proxyManager.releaseProxy(eventId, false);
        this.logWithTime(`Getting fresh proxy for retry #${retryCount} of event ${eventId}`, "info");
      }

      // Get a unique proxy for this specific event
      try {
        const proxyData = this.proxyManager.getProxyForEvent(eventId);
        if (proxyData) {
          const proxyAgentData = this.proxyManager.createProxyAgent(proxyData);
          proxyAgent = proxyAgentData.proxyAgent;
          proxy = proxyAgentData.proxy;
          
          // Track proxy assignment
          this.proxyManager.assignProxyToEvent(eventId, proxy.proxy);
        }
      } catch (proxyError) {
        this.logWithTime(`Error getting proxy for ${eventId}: ${proxyError.message}`, "warning");
      }

      // Force fresh headers for each event to ensure unique sessions
      // ENHANCED: Always force refresh on retries
      const randomEventId = await this.getRandomEventId(eventId);
      const headers = await this.refreshEventHeaders(randomEventId, retryCount > 0); // Force refresh on retries
      
      if (retryCount > 0) {
        this.logWithTime(`Using fresh session/cookies for retry #${retryCount} of event ${eventId}`, "info");
      }
      
      if (!headers) {
        throw new Error("Failed to obtain unique headers");
      }

      // Add unique identifiers to headers to ensure no sharing
      if (headers.headers) {
        headers.headers["X-Event-ID"] = eventId;
        headers.headers["X-Session-ID"] = `session-${eventId}-${Date.now()}`;
        headers.headers["X-Unique-Request"] = `${eventId}-${Math.random().toString(36).substring(2)}`;
        headers.headers["X-Processing-Time"] = Date.now().toString();
        // Add retry count to headers for tracking
        if (retryCount > 0) {
          headers.headers["X-Retry-Count"] = retryCount.toString();
          headers.headers["X-Retry-ID"] = `retry-${eventId}-${retryCount}-${Date.now()}`;
        }
      }

      // Create event object with unique session data
      const eventWithUniqueSession = {
        eventId: eventId,
        headers: headers,
        sessionId: `unique-${eventId}-${Date.now()}-retry${retryCount}`,
        proxyId: proxy?.proxy || 'default',
        retryCount: retryCount
      };

      // Quick scrape with shorter timeout and unique session
      const result = await Promise.race([
        ScrapeEvent(eventWithUniqueSession, proxyAgent, proxy),
        setTimeout(SCRAPE_TIMEOUT).then(() => {
          throw new Error("Scrape timeout");
        }),
      ]);

      if (!result || !Array.isArray(result) || result.length === 0) {
        throw new Error("Invalid scrape result");
      }

      // Update metadata asynchronously
      this.updateEventMetadataAsync(eventId, result);
      
      // Queue CSV generation (non-blocking)
      this.queueCsvGeneration(eventId, result);

      // Success tracking
      this.successCount++;
      this.lastSuccessTime = moment();
      
      // FIX: Always update eventUpdateTimestamps with moment object (not Date)
      this.eventUpdateTimestamps.set(eventId, moment());
      
      // FIX: Also update Last_Updated in database to ensure getEvents() stops returning this event
      try {
        await Event.updateOne(
          { Event_ID: eventId },
          { $set: { Last_Updated: new Date() } }
        );
      } catch (dbError) {
        this.logWithTime(`Error updating Last_Updated for ${eventId}: ${dbError.message}`, "warning");
      }
      
      // Update processed time to prevent immediate reprocessing
      this.eventLastProcessedTime.set(eventId, Date.now());
      
      this.failedEvents.delete(eventId);
      this.clearFailureCount(eventId);

      // Release proxy after successful use
      if (proxy) {
        this.proxyManager.releaseProxy(eventId, true);
      }

      return true;
    } catch (error) {
      // Release proxy on error
      if (proxy) {
        this.proxyManager.releaseProxy(eventId, false, error);
      }
      
      // Quick failure handling
      this.failedEvents.add(eventId);
      this.incrementFailureCount(eventId);
      
      // Get last update time to check 10-minute threshold
      const lastUpdate = this.eventUpdateTimestamps.get(eventId);
      const timeSinceUpdate = lastUpdate ? moment().diff(lastUpdate) : Infinity;

      if (timeSinceUpdate >= STALE_EVENT_THRESHOLD) {
        this.logWithTime(
          `Event ${eventId} exceeded staleness threshold (${Math.floor(timeSinceUpdate/1000)}s) in optimized path. Retry count: ${retryCount}. Marking for auto-stop.`,
          "error"
        );
        await this.setEventSkipScraping(eventId, "exceeded_staleness_threshold_optimized", retryCount, STALE_EVENT_THRESHOLD);
      } else if (retryCount >= MAX_RETRIES) {
        this.logWithTime(
          `Event ${eventId} exceeded MAX_RETRIES (${MAX_RETRIES}) in optimized path. Time since last update: ${Math.floor(timeSinceUpdate/1000)}s. Marking for auto-stop.`,
          "error"
        );
        await this.setEventSkipScraping(eventId, "max_retries_exceeded_optimized", retryCount, MAX_RETRIES);
      } else {
        // Event is not stale and has retries remaining
        const newRetryCount = retryCount + 1;
        const baseBackoff = 15000; // 15 seconds
        const incrementFactor = 5000; // 5 seconds per retry count
        const maxBackoff = 300000; // 5 minutes
        const jitter = Math.floor(Math.random() * 5000); // 0-5 seconds jitter
        
        let backoffDuration = baseBackoff + (retryCount * incrementFactor) + jitter;
        backoffDuration = Math.min(backoffDuration, maxBackoff);

        const retryJob = {
          eventId,
          retryCount: newRetryCount,
          retryAfter: moment().add(backoffDuration, 'milliseconds'),
          priority: 10, // Default priority, consistent with scrapeEvent
          needsFreshProxy: true,
          needsFreshSession: true
        };

        this.retryQueue.push(retryJob); // Push to the central retryQueue
        
        this.logWithTime(
          `Queuing retry #${newRetryCount} for ${eventId} (optimized path) in ${backoffDuration}ms. Time since last update: ${Math.floor(timeSinceUpdate/1000)}s.`,
          "info"
        );
      }
      
      return false;
    }
  }

  /**
   * Async metadata update to avoid blocking
   */
  async updateEventMetadataAsync(eventId, scrapeResult) {
    try {
      // Immediately update tracking before processing in background
      this.eventUpdateTimestamps.set(eventId, moment());
      this.eventLastProcessedTime.set(eventId, Date.now());
      
      // Update Last_Updated directly to ensure events stop appearing in getEvents()
      await Event.updateOne(
        { Event_ID: eventId },
        { $set: { Last_Updated: new Date() } }
      );
      
      // Process full metadata update in background
      setImmediate(async () => {
        try {
          await this.updateEventMetadata(eventId, scrapeResult);
        } catch (error) {
          console.error(`Async metadata update error for ${eventId}: ${error.message}`);
        }
      });
    } catch (error) {
      console.error(`Error updating Last_Updated for ${eventId}: ${error.message}`);
    }
  }

  /**
   * Queue CSV generation for non-blocking processing (INSTANT)
   */
  queueCsvGeneration(eventId, scrapeResult) {
    // Don't queue - process immediately and return
    setImmediate(() => {
      this.generateInventoryCsv(eventId, scrapeResult).catch(err =>
        console.error(`Background CSV error for ${eventId}: ${err.message}`)
      );
    });
  }

  /**
   * Non-blocking CSV processing worker (LIGHTWEIGHT)
   */
  async startCsvProcessingWorker() {
    // This is now handled by the background CSV generation in inventoryController
    // Just mark as active for compatibility
    this.csvProcessingActive = true;
    
    // Lightweight monitoring - check every 60 seconds
    const monitorInterval = setInterval(async () => {
      if (!this.isRunning) {
        clearInterval(monitorInterval);
        this.csvProcessingActive = false;
        return;
      }
      
      try {
        // Force combined CSV generation every 60 seconds
        const { forceCombinedCSVGeneration } = await import('./controllers/inventoryController.js');
        forceCombinedCSVGeneration(); // This is now non-blocking
        
        if (LOG_LEVEL >= 2) {
          this.logWithTime("Background CSV generation triggered", "info");
        }
      } catch (error) {
        console.error(`CSV monitoring error: ${error.message}`);
      }
    }, 60000); // Every 60 seconds
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
   * Start continuous scraping process
   * @returns {Promise<void>}
   */
  async startContinuousScraping() {
    if (!this.isRunning) {
      this.isRunning = true;
      this.logWithTime("Starting high-performance parallel scraping for 1000+ events...", "info");

      // Log Skip_Scraping status summary at startup
      try {
        const totalEvents = await Event.countDocuments({});
        const skippedEvents = await Event.countDocuments({ Skip_Scraping: true });
        const activeEvents = totalEvents - skippedEvents;
        
        this.logWithTime(
          `📊 Event Status Summary: ${totalEvents} total events, ${activeEvents} active for scraping, ${skippedEvents} excluded (Skip_Scraping: true)`,
          "info"
        );
        
        if (skippedEvents > 0) {
          this.logWithTime(
            `⚠️ ${skippedEvents} events will be EXCLUDED from scraping due to Skip_Scraping: true`,
            "warning"
          );
        }
      } catch (error) {
        this.logWithTime(`Error getting event status summary: ${error.message}`, "warning");
      }

      // Ensure proxy manager is available globally for the scraper
      global.proxyManager = this.proxyManager;

      // Start performance monitoring
      this.startPerformanceMonitoring();

      // Start cookie rotation cycle immediately (non-blocking)
      this.forcePeriodicCookieRotation();
      this.logWithTime("Started 15-minute cookie rotation cycle", "info");

      // Start CSV processing worker (non-blocking)
      this.startCsvProcessingWorker();

      // Start CSV upload cycle immediately (non-blocking)
      if (ENABLE_CSV_UPLOAD) {
        this.runCsvUploadCycle();

        // Set up recurring CSV upload cycle every 6 minutes
        this.csvUploadIntervalId = setInterval(() => {
          // Each interval call is non-blocking
          this.runCsvUploadCycle();
        }, CSV_UPLOAD_INTERVAL);
      }

      // Start multiple parallel processing workers
      for (let i = 0; i < this.maxParallelWorkers; i++) {
        this.parallelWorkers.push(this.startParallelWorker(i));
      }

      // Track metrics to detect stuck loops
      let lastEventsCount = 0;
      let sameEventsCounter = 0;
      let lastEventIds = [];

      // Main event queue management loop
      while (this.isRunning) {
        try {
          // Get all events that need processing
          const events = await this.getEvents();
          
          // CRITICAL FIX: Track if we're getting the same events repeatedly
          const currentEventIds = JSON.stringify(events.slice(0, 10).sort());
          if (currentEventIds === lastEventIds) {
            sameEventsCounter++;
            
            // If we've seen the same events 5 times, something is wrong
            if (sameEventsCounter >= 5) {
              this.logWithTime(
                `WARNING: Same ${events.length} events returned ${sameEventsCounter} times in a row. Potential processing loop detected.`,
                "error"
              );
              
              // Force cleanup of tracking to break the loop
              await this.forceCleanupAllTracking();
              
              // Reset counter after cleanup
              sameEventsCounter = 0;
            }
          } else {
            // Different events, reset counter
            lastEventIds = currentEventIds;
            sameEventsCounter = 0;
          }
          
          if (events.length > 0) {
            this.logWithTime(
              `Found ${events.length} events needing updates, distributing to parallel workers`,
              "info"
            );

            // Add events to processing queue if not already there
            let addedCount = 0;
            for (const eventId of events) {
              // Only add if not already in queue and not being processed
              if (!this.eventProcessingQueue.some(e => (typeof e === 'string' ? e === eventId : e.eventId === eventId)) && 
                  !this.processingEvents.has(eventId)) {
                this.eventProcessingQueue.push(eventId);
                addedCount++;
              }
            }
            
            // Report if we're adding events but not processing them
            if (addedCount > 0) {
              this.logWithTime(
                `Added ${addedCount} new events to processing queue (total: ${this.eventProcessingQueue.length})`,
                "info"
              );
            } else if (events.length > 0 && addedCount === 0) {
              this.logWithTime(
                `Warning: Found ${events.length} events needing updates but added 0 to queue - they may be stuck in processing`,
                "warning"
              );
            }
          }

          // Process retry queue
          const now = moment();
          const retryEvents = this.retryQueue.filter(job => job.retryAfter.isBefore(now));
          let retryAddedCount = 0;
          
          for (const job of retryEvents) {
            if (!this.eventProcessingQueue.some(e => (typeof e === 'string' ? e === job.eventId : e.eventId === job.eventId)) && 
                !this.processingEvents.has(job.eventId)) {
              // Add to front of queue with retry info
              this.eventProcessingQueue.unshift({
                eventId: job.eventId,
                retryCount: job.retryCount,
                isRetry: true
              });
              retryAddedCount++;
            }
          }
          
          if (retryAddedCount > 0) {
            this.logWithTime(
              `Added ${retryAddedCount} retry events to processing queue`,
              "info"
            );
          }
          
          // Remove processed retry jobs
          this.retryQueue = this.retryQueue.filter(job => !retryEvents.includes(job));

          // Short pause before next cycle
          await setTimeout(100); // Check every 100ms for new events
        } catch (error) {
          console.error(`Error in main event loop: ${error.message}`);
          await setTimeout(1000);
        }
      }
    }
  }

  /**
   * Get statistics about stale events (events not updated in specific time periods)
   */
  async getStaleEventStats() {
    const now = Date.now();
    const TEN_MINUTES = 10 * 60 * 1000;
    const SIX_MINUTES = 6 * 60 * 1000;
    const THREE_MINUTES = 3 * 60 * 1000;

    try {
      // Get all active events from database
      const activeEvents = await Event.find({
        Skip_Scraping: { $ne: true },
      })
        .select("Event_ID Event_Name Last_Updated")
        .lean();

      const stats = {
        totalActiveEvents: activeEvents.length,
        staleOver3Min: [],
        staleOver6Min: [],
        staleOver10Min: [],
        recentlyUpdated: []
      };

      for (const event of activeEvents) {
        const lastUpdated = event.Last_Updated ? new Date(event.Last_Updated).getTime() : 0;
        const timeSinceUpdate = now - lastUpdated;
        const minutesSinceUpdate = Math.floor(timeSinceUpdate / 60000);

        const eventInfo = {
          eventId: event.Event_ID,
          eventName: event.Event_Name,
          lastUpdated: event.Last_Updated,
          minutesSinceUpdate
        };

        if (timeSinceUpdate > TEN_MINUTES) {
          stats.staleOver10Min.push(eventInfo);
        } else if (timeSinceUpdate > SIX_MINUTES) {
          stats.staleOver6Min.push(eventInfo);
        } else if (timeSinceUpdate > THREE_MINUTES) {
          stats.staleOver3Min.push(eventInfo);
        } else {
          stats.recentlyUpdated.push(eventInfo);
        }
      }

      // Sort by time since update (most stale first)
      const sortByStale = (a, b) => b.minutesSinceUpdate - a.minutesSinceUpdate;
      stats.staleOver10Min.sort(sortByStale);
      stats.staleOver6Min.sort(sortByStale);
      stats.staleOver3Min.sort(sortByStale);

      return stats;
    } catch (error) {
      this.logWithTime(`Error getting stale event stats: ${error.message}`, "error");
      return null;
    }
  }

  /**
   * Handle stale events - try recovery first, then auto-stop if needed
   */
  async handleStaleEvents(processedEventsSet) {
    try {
      const stats = await this.getStaleEventStats();
      if (!stats) return;
      
      // Log summary of stale events
      if (stats.staleOver10Min.length > 0 || stats.staleOver6Min.length > 0 || stats.staleOver3Min.length > 0) {
        this.logWithTime(
          `⚠️ Stale Event Summary: ${stats.staleOver10Min.length} events >10min, ${stats.staleOver6Min.length} events >6min, ${stats.staleOver3Min.length} events >3min`,
          "warning"
        );
      } else {
        this.logWithTime(`✅ No stale events: ${stats.recentlyUpdated.length} events updated recently`, "success");
      }
      
      // Handle CRITICAL events (>10 minutes) with AGGRESSIVE recovery
      if (stats.staleOver10Min.length > 0) {
        const criticalEventIds = stats.staleOver10Min.map(e => e.eventId);
        this.logWithTime(`🚨 CRITICAL: ${criticalEventIds.length} events not updated for >10 minutes`, "error");
        
        // Try AGGRESSIVE recovery first for ALL critical events
        const recoveryResult = await this.attemptAggressiveRecovery(criticalEventIds);
        
        // Log recovery results
        this.logWithTime(
          `🚨 CRITICAL Recovery Results: ${recoveryResult.recovered.length} recovered, ${recoveryResult.failed.length} failed out of ${recoveryResult.attempts} attempts`,
          recoveryResult.recovered.length > 0 ? "success" : "error"
        );
        
        // Add recovered events to processed set
        recoveryResult.recovered.forEach(eventId => processedEventsSet.add(eventId));
        
        // For failed recovery events, check if they should be auto-stopped
        if (recoveryResult.failed.length > 0) {
          await this.autoStopStaleEvents(recoveryResult.failed);
        }
      }
      
      // Handle STANDARD stale events (3-10 minutes) with intensive recovery
      if (stats.staleOver3Min.length > 0 || stats.staleOver6Min.length > 0) {
        const staleEvents = stats.staleOver3Min.concat(stats.staleOver6Min);
        // Filter to events not recently processed to avoid over-recovery
        const eventsToRecover = staleEvents.filter(event => 
          !processedEventsSet.has(event.eventId) && 
          event.minutesSinceUpdate >= 3
        );
        
        if (eventsToRecover.length > 0) {
          const eventIdsToRecover = eventsToRecover.map(e => e.eventId);
          this.logWithTime(`🔄 STANDARD: Attempting recovery of ${eventIdsToRecover.length} stale events (3-10 minutes old)`, "warning");
          
          // Try intensive recovery for batches of events
          const recoveryResult = await this.attemptIntensiveRecovery(eventIdsToRecover);
          
          // Log recovery results
          this.logWithTime(
            `🔄 STANDARD Recovery Results: ${recoveryResult.recovered.length} recovered, ${recoveryResult.failed.length} failed out of ${recoveryResult.attempts} attempts`,
            recoveryResult.recovered.length > 0 ? "success" : "warning"
          );
          
          // Add recovered events to processed set to prevent over-recovery
          recoveryResult.recovered.forEach(eventId => processedEventsSet.add(eventId));
        }
      }
    } catch (error) {
      this.logWithTime(`Error handling stale events: ${error.message}`, "error");
    }
  }

  /**
   * Handle CRITICAL stale events (>10 minutes) with AGGRESSIVE recovery and potential auto-stop
   */
  async handleCriticalStaleEvents(criticalEvents, processedEventsSet) {
    if (criticalEvents.length === 0) return;
    
    const criticalEventIds = criticalEvents.map(e => e.eventId);
    this.logWithTime(`🚨 CRITICAL: ${criticalEventIds.length} events not updated for >10 minutes`, "error");
    
    // Try AGGRESSIVE recovery first for ALL critical events
    const recoveryResult = await this.attemptAggressiveRecovery(criticalEventIds);
    
    // Log recovery results
    this.logWithTime(
      `🚨 CRITICAL Recovery Results: ${recoveryResult.recovered.length} recovered, ${recoveryResult.failed.length} failed out of ${recoveryResult.attempts} attempts`,
      recoveryResult.recovered.length > 0 ? "success" : "error"
    );
    
    // Add recovered events to processed set
    recoveryResult.recovered.forEach(eventId => processedEventsSet.add(eventId));
    
    // For failed recovery events, check if they should be auto-stopped
    if (recoveryResult.failed.length > 0) {
      await this.handleAutoStopEvents(recoveryResult.failed);
    }
  }

  /**
   * Handle STANDARD stale events (3-10 minutes) with intensive recovery
   */
  async handleStandardStaleEvents(staleEvents, processedEventsSet) {
    if (staleEvents.length === 0) return;
    
    // Filter to events not recently processed to avoid over-recovery
    const eventsToRecover = staleEvents.filter(event => 
      !processedEventsSet.has(event.eventId) && 
      event.minutesSinceUpdate >= 3
    );
    
    if (eventsToRecover.length === 0) {
      this.logWithTime(`Skipping recovery - ${staleEvents.length} stale events already recently processed`, "info");
      return;
    }
    
    const eventIdsToRecover = eventsToRecover.map(e => e.eventId);
    this.logWithTime(`🔄 STANDARD: Attempting recovery of ${eventIdsToRecover.length} stale events (3-10 minutes old)`, "warning");
    
    // Try intensive recovery for batches of events
    const recoveryResult = await this.attemptIntensiveRecovery(eventIdsToRecover);
    
    // Log recovery results
    this.logWithTime(
      `🔄 STANDARD Recovery Results: ${recoveryResult.recovered.length} recovered, ${recoveryResult.failed.length} failed out of ${recoveryResult.attempts} attempts`,
      recoveryResult.recovered.length > 0 ? "success" : "warning"
    );
    
    // Add recovered events to processed set to prevent over-recovery
    recoveryResult.recovered.forEach(eventId => processedEventsSet.add(eventId));
    
    // For failed recovery events over 10 minutes, consider auto-stop
    if (recoveryResult.failed.length > 0) {
      const failedEvents = staleEvents.filter(e => recoveryResult.failed.includes(e.eventId));
      const failedOver10Min = failedEvents.filter(e => e.minutesSinceUpdate >= 10).map(e => e.eventId);
      
      if (failedOver10Min.length > 0) {
        await this.handleAutoStopEvents(failedOver10Min);
      } else {
        this.logWithTime(
          `Not auto-stopping ${recoveryResult.failed.length} failed events - under 10 minute threshold`,
          "info"
        );
      }
    }
  }

  /**
   * Handle events that failed recovery - decide whether to auto-stop based on time
   */
  async handleAutoStopEvents(eventIds) {
    if (eventIds.length === 0) {
      return;
    }

    this.logWithTime(`🛑 Evaluating ${eventIds.length} events for auto-stop after failed recovery`, "warning");
    
    // Get full event stats to check exact staleness
    const stats = await this.getStaleEventStats();
    if (!stats) return;
    
    // Filter to events that are still over 10 minutes stale
    const eventsStillStale = stats.staleOver10Min
      .filter(event => eventIds.includes(event.eventId));
    
    const eventsToAutoStop = eventsStillStale.map(e => e.eventId);
    
    if (eventsToAutoStop.length > 0) {
      this.logWithTime(
        `🛑 AUTO-STOPPING ${eventsToAutoStop.length} events: Failed recovery and still >10 minutes stale - setting Skip_Scraping = true`,
        "error"
      );
      
      // Auto-stop events that remain critically stale
      await this.autoStopStaleEvents(eventsToAutoStop);
    } else {
      this.logWithTime(
        `Not auto-stopping ${eventIds.length} events - no longer critically stale`,
        "info"
      );
    }
  }

  /**
   * Attempt AGGRESSIVE recovery for critically stale events (>8 minutes)
   */
  async attemptAggressiveRecovery(eventIds) {
    const results = {
      recovered: [],
      failed: [],
      attempts: 0
    };

    if (eventIds.length === 0) {
      return results;
    }

    this.logWithTime(`🚨 Starting AGGRESSIVE recovery for ${eventIds.length} critically stale events`, "warning");

    for (const eventId of eventIds) {
      try {
        results.attempts++;
        
        this.logWithTime(`🔧 AGGRESSIVE recovery attempt for event ${eventId}`, "warning");
        
        // STEP 1: Complete reset of all failure states
        this.failedEvents.delete(eventId);
        this.cooldownEvents.delete(eventId);
        this.eventFailureCounts.delete(eventId);
        this.eventFailureTimes.delete(eventId);
        this.processingEvents.delete(eventId);
        
        // STEP 2: Just release proxy - no blocking
        try {
          this.proxyManager.releaseProxy(eventId, false);
          this.logWithTime(`Released proxy for event ${eventId} during recovery`, "info");
        } catch (proxyError) {
          this.logWithTime(`Error releasing proxy: ${proxyError.message}`, "warning");
        }
        
        // STEP 3: Force complete session reset
        try {
          await this.sessionManager.forceSessionRotation();
          this.headersCache.clear();
          this.headerRefreshTimestamps.clear();
        } catch (sessionError) {
          this.logWithTime(`Session reset error during aggressive recovery: ${sessionError.message}`, "warning");
        }
        
        // STEP 4: Get multiple random event IDs for fresh headers
        const randomEventIds = [];
        try {
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
          
          randomEventIds.push(...randomEvents.map(e => e.Event_ID));
        } catch (dbError) {
          this.logWithTime(`Database error during aggressive recovery: ${dbError.message}`, "warning");
        }
        
        // STEP 5: Multiple recovery attempts with different strategies
        let recovered = false;
        const strategies = ['fresh_headers', 'new_proxy', 'fallback_headers', 'direct_scrape'];
        
        for (const strategy of strategies) {
          if (recovered) break;
          
          try {
            this.logWithTime(`Trying strategy "${strategy}" for event ${eventId}`, "info");
            
            switch (strategy) {
              case 'fresh_headers':
                if (randomEventIds.length > 0) {
                  const randomId = randomEventIds[Math.floor(Math.random() * randomEventIds.length)];
                  await this.refreshEventHeaders(randomId, true);
                }
                break;
                
              case 'new_proxy':
                // Just get a new proxy - no blocking
                try {
                  this.proxyManager.releaseProxy(eventId, false);
                  const { proxyAgent, proxy } = this.proxyManager.getProxyForEvent(eventId);
                  this.logWithTime(`Using new proxy for ${eventId} during recovery`, "info");
                } catch (proxyError) {
                  this.logWithTime(`Error getting new proxy: ${proxyError.message}`, "warning");
                }
                break;
                
              case 'fallback_headers':
                // Try with any available headers from rotation pool
                if (this.headerRotationPool.length > 0) {
                  this.logWithTime(`Using rotation pool headers for ${eventId}`, "info");
                }
                break;
                
              case 'direct_scrape':
                // Last resort: direct scrape with minimal setup
                this.logWithTime(`Direct scrape attempt for ${eventId}`, "info");
                break;
            }
            
            // Attempt scraping with current strategy
            const scrapeResult = await this.scrapeEventOptimized(eventId, 0, null);
            
            if (scrapeResult) {
              recovered = true;
              results.recovered.push(eventId);
              this.logWithTime(`✅ AGGRESSIVE recovery SUCCESS for event ${eventId} using strategy "${strategy}"`, "success");
              
              // Clear all failure tracking
              this.clearFailureCount(eventId);
              this.eventFailureTimes.delete(eventId);
              break;
            }
            
          } catch (strategyError) {
            this.logWithTime(`Strategy "${strategy}" failed for ${eventId}: ${strategyError.message}`, "warning");
          }
          
          // Small delay between strategies
          await setTimeout(200);
        }
        
        if (!recovered) {
          results.failed.push(eventId);
          this.logWithTime(`❌ AGGRESSIVE recovery FAILED for event ${eventId} - tried all strategies`, "error");
          
          // Mark for potential auto-stop
          this.eventFailureTimes.set(eventId, Date.now());
        }
        
        // Delay between events to avoid overwhelming the system
        await setTimeout(500);
        
      } catch (error) {
        results.failed.push(eventId);
        this.logWithTime(`💥 AGGRESSIVE recovery ERROR for ${eventId}: ${error.message}`, "error");
      }
    }

    return results;
  }

  /**
   * Attempt intensive recovery for stale events
   */
  async attemptIntensiveRecovery(eventIds) {
    const results = {
      recovered: [],
      failed: [],
      attempts: 0
    };

    if (eventIds.length === 0) {
      return results;
    }

    this.logWithTime(`Starting INTENSIVE recovery for ${eventIds.length} stale events`, "info");

    for (const eventId of eventIds) {
      try {
        results.attempts++;
        
        this.logWithTime(`🔄 INTENSIVE recovery attempt for event ${eventId}`, "info");
        
        // Mark recovery attempt time
        this.eventFailureTimes.set(eventId, Date.now());
        
        // Reset failure states for recovery
        this.cooldownEvents.delete(eventId);
        this.failedEvents.delete(eventId);
        this.processingEvents.delete(eventId);
        
        // Reduce failure count to give more retry chances
        const currentFailures = this.eventFailureCounts.get(eventId) || [];
        if (currentFailures.length > 0) {
          // Remove half of the failures to give more chances
          this.eventFailureCounts.set(eventId, currentFailures.slice(Math.floor(currentFailures.length / 2)));
        }
        
        // Try multiple recovery approaches
        let recovered = false;
        const approaches = ['fresh_session', 'new_proxy_headers', 'fallback_approach'];
        
        for (const approach of approaches) {
          if (recovered) break;
          
          try {
            this.logWithTime(`Trying approach "${approach}" for event ${eventId}`, "info");
            
            switch (approach) {
              case 'fresh_session':
                // Force completely fresh session and headers
                const randomEventId = await this.getRandomEventId(eventId);
                await this.refreshEventHeaders(randomEventId, true);
                break;
                
              case 'new_proxy_headers':
                // Get new proxy and force header refresh
                this.proxyManager.releaseProxy(eventId, false);
                const anotherRandomId = await this.getRandomEventId(eventId);
                await this.refreshEventHeaders(anotherRandomId, true);
                break;
                
              case 'fallback_approach':
                // Try with minimal setup as fallback
                this.logWithTime(`Fallback recovery approach for ${eventId}`, "info");
                break;
            }
            
            // Attempt scraping with current approach
            const scrapeResult = await this.scrapeEventOptimized(eventId, 0, null);
            
            if (scrapeResult) {
              recovered = true;
              results.recovered.push(eventId);
              this.logWithTime(`✅ INTENSIVE recovery SUCCESS for event ${eventId} using approach "${approach}"`, "success");
              
              // Clear failure tracking
              this.clearFailureCount(eventId);
              this.eventFailureTimes.delete(eventId);
              break;
            }
            
          } catch (approachError) {
            this.logWithTime(`Approach "${approach}" failed for ${eventId}: ${approachError.message}`, "warning");
          }
          
          // Small delay between approaches
          await setTimeout(300);
        }
        
        if (!recovered) {
          results.failed.push(eventId);
          this.logWithTime(`❌ INTENSIVE recovery FAILED for event ${eventId} - tried all approaches`, "warning");
        }
        
        // Small delay between recovery attempts
        await setTimeout(500);
        
      } catch (error) {
        results.failed.push(eventId);
        this.logWithTime(`💥 INTENSIVE recovery ERROR for ${eventId}: ${error.message}`, "error");
      }
    }

    return results;
  }

  /**
   * Auto-stop events that are stale and couldn't be recovered
   */
  async autoStopStaleEvents(eventIds) {
    if (eventIds.length === 0) {
      return;
    }

    try {
      // Double-check that these events actually exceed the 10-minute threshold
      const now = Date.now();
      const confirmedStaleEvents = [];
      
      for (const eventId of eventIds) {
        const lastUpdate = this.eventUpdateTimestamps.get(eventId);
        const timeSinceUpdate = lastUpdate ? now - lastUpdate.valueOf() : Infinity;
        
        if (timeSinceUpdate >= 600000) { // Confirm 10-minute threshold
          confirmedStaleEvents.push(eventId);
        } else {
          this.logWithTime(
            `Event ${eventId} not yet at 10-minute threshold (${Math.floor(timeSinceUpdate/1000)}s) - continuing retries`,
            "info"
          );
        }
      }
      
      if (confirmedStaleEvents.length === 0) {
        return; // No events actually at threshold
      }

      // Update events in database to stop scraping
      const updateResult = await Event.updateMany(
        { Event_ID: { $in: confirmedStaleEvents } },
        { 
          $set: { 
            Skip_Scraping: true,
            status: "auto_stopped",
            auto_stopped_reason: "Failed recovery after 10+ minutes of inactivity",
            auto_stopped_at: new Date(),
            last_recovery_attempt: new Date(),
            recovery_attempts_failed: true
          } 
        }
      );

      this.logWithTime(
        `🛑 AUTO-STOPPED ${updateResult.modifiedCount}/${confirmedStaleEvents.length} stale events: Skip_Scraping = true (exceeded 10-minute threshold) - excluded from future scraping`,
        "error"
      );

      // Clean up tracking for auto-stopped events
      for (const eventId of confirmedStaleEvents) {
        // Complete cleanup of all tracking data
        this.cleanupEventTracking(eventId);
        
        // Remove from retry queue permanently
        this.retryQueue = this.retryQueue.filter(item => item.eventId !== eventId);
        
        // Clear from all processing queues
        this.eventProcessingQueue = this.eventProcessingQueue.filter(item => 
          (typeof item === 'string' ? item : item.eventId) !== eventId
        );
        
        // Log detailed audit trail
        await this.logError(
          eventId,
          "AUTO_STOPPED",
          new Error(`Event auto-stopped after exceeding 10-minute threshold - excluded from scraping`),
          {
            reason: "exceeded_10min_threshold",
            lastUpdate: this.eventUpdateTimestamps.get(eventId)?.toISOString(),
            autoStoppedAt: new Date().toISOString(),
            excludedFromScraping: true,
            skipScraping: true
          }
        );
        
        this.logWithTime(
          `🚫 STOPPED event ${eventId}: Skip_Scraping = true (Reason: exceeded_10min_threshold) - excluded from future scraping`,
          "warning"
        );
      }

      // Force a CSV regeneration to exclude stopped events
      try {
        const { forceCombinedCSVGeneration } = await import('./controllers/inventoryController.js');
        forceCombinedCSVGeneration();
        this.logWithTime(
          `📊 Forced CSV regeneration to exclude ${confirmedStaleEvents.length} auto-stopped events`,
          "info"
        );
      } catch (csvError) {
        this.logWithTime(`Error forcing CSV regeneration: ${csvError.message}`, "warning");
      }

    } catch (error) {
      this.logWithTime(`Error auto-stopping stale events: ${error.message}`, "error");
    }
  }

  /**
   * Force immediate cleanup of all tracking Maps (manual cleanup)
   */
  async forceCleanupAllTracking() {
    try {
      const beforeCount = this.eventUpdateTimestamps.size;
      
      // Get currently active events from database
      const activeEvents = await Event.find({
        Skip_Scraping: { $ne: true },
      })
        .select("Event_ID")
        .lean();
      
      const activeEventIds = new Set(activeEvents.map(event => event.Event_ID));
      
      // Clear all tracking Maps and rebuild only with active events
      const activeCounts = {
        eventUpdateTimestamps: 0,
        eventLastProcessedTime: 0,
        eventFailureCounts: 0,
        eventFailureTimes: 0,
        processingEvents: 0,
        failedEvents: 0,
        cooldownEvents: 0
      };
      
      // Clean eventUpdateTimestamps
      for (const eventId of [...this.eventUpdateTimestamps.keys()]) {
        if (activeEventIds.has(eventId)) {
          activeCounts.eventUpdateTimestamps++;
        } else {
          this.eventUpdateTimestamps.delete(eventId);
        }
      }
      
      // Clean other tracking Maps
      for (const eventId of [...this.eventLastProcessedTime.keys()]) {
        if (activeEventIds.has(eventId)) {
          activeCounts.eventLastProcessedTime++;
        } else {
          this.eventLastProcessedTime.delete(eventId);
        }
      }
      
      for (const eventId of [...this.eventFailureCounts.keys()]) {
        if (activeEventIds.has(eventId)) {
          activeCounts.eventFailureCounts++;
        } else {
          this.eventFailureCounts.delete(eventId);
        }
      }
      
      for (const eventId of [...this.eventFailureTimes.keys()]) {
        if (activeEventIds.has(eventId)) {
          activeCounts.eventFailureTimes++;
        } else {
          this.eventFailureTimes.delete(eventId);
        }
      }
      
      for (const eventId of [...this.processingEvents.keys()]) {
        if (activeEventIds.has(eventId)) {
          activeCounts.processingEvents++;
        } else {
          this.processingEvents.delete(eventId);
        }
      }
      
      for (const eventId of [...this.failedEvents.keys()]) {
        if (activeEventIds.has(eventId)) {
          activeCounts.failedEvents++;
        } else {
          this.failedEvents.delete(eventId);
        }
      }
      
      for (const eventId of [...this.cooldownEvents.keys()]) {
        if (activeEventIds.has(eventId)) {
          activeCounts.cooldownEvents++;
        } else {
          this.cooldownEvents.delete(eventId);
        }
      }
      
      const cleanedCount = beforeCount - this.eventUpdateTimestamps.size;
      
      this.logWithTime(
        `🧽 FORCE CLEANUP: Removed ${cleanedCount} stale events | ` +
        `Active events in DB: ${activeEventIds.size} | Now tracking: ${this.eventUpdateTimestamps.size} | ` +
        `Active tracking: ${JSON.stringify(activeCounts)}`,
        "info"
      );
      
      return {
        cleaned: cleanedCount,
        activeInDB: activeEventIds.size,
        nowTracking: this.eventUpdateTimestamps.size,
        activeCounts
      };
      
    } catch (error) {
      this.logWithTime(`Error in force cleanup: ${error.message}`, "error");
      return { error: error.message };
    }
  }

  /**
   * Clean up inactive events from tracking Maps
   */
  async cleanupInactiveEvents() {
    try {
      // Get currently active events from database
      const activeEvents = await Event.find({
        Skip_Scraping: { $ne: true },
      })
        .select("Event_ID")
        .lean();
      
      const activeEventIds = new Set(activeEvents.map(event => event.Event_ID));
      
      // Clean up eventUpdateTimestamps
      let cleanedEvents = 0;
      for (const eventId of this.eventUpdateTimestamps.keys()) {
        if (!activeEventIds.has(eventId)) {
          this.eventUpdateTimestamps.delete(eventId);
          cleanedEvents++;
        }
      }
      
      // Clean up other tracking Maps
      for (const eventId of this.eventLastProcessedTime.keys()) {
        if (!activeEventIds.has(eventId)) {
          this.eventLastProcessedTime.delete(eventId);
        }
      }
      
      for (const eventId of this.eventFailureCounts.keys()) {
        if (!activeEventIds.has(eventId)) {
          this.eventFailureCounts.delete(eventId);
        }
      }
      
      for (const eventId of this.eventFailureTimes.keys()) {
        if (!activeEventIds.has(eventId)) {
          this.eventFailureTimes.delete(eventId);
        }
      }
      
      for (const eventId of this.processingEvents.keys()) {
        if (!activeEventIds.has(eventId)) {
          this.processingEvents.delete(eventId);
        }
      }
      
      for (const eventId of this.failedEvents.keys()) {
        if (!activeEventIds.has(eventId)) {
          this.failedEvents.delete(eventId);
        }
      }
      
      for (const eventId of this.cooldownEvents.keys()) {
        if (!activeEventIds.has(eventId)) {
          this.cooldownEvents.delete(eventId);
        }
      }
      
      if (cleanedEvents > 0) {
        this.logWithTime(
          `🧹 Cleaned up ${cleanedEvents} inactive events from tracking Maps | ` +
          `Active in DB: ${activeEventIds.size} | Still tracking: ${this.eventUpdateTimestamps.size}`,
          "info"
        );
      } else if (this.eventUpdateTimestamps.size > activeEventIds.size * 2) {
        this.logWithTime(
          `⚠️ Tracking accumulation detected: tracking ${this.eventUpdateTimestamps.size} events but only ${activeEventIds.size} active in DB`,
          "warning"
        );
      }
      
      return cleanedEvents;
    } catch (error) {
      this.logWithTime(`Error cleaning up inactive events: ${error.message}`, "error");
      return 0;
    }
  }

  /**
   * Get performance statistics for monitoring (FAST - non-blocking)
   */
  async getPerformanceStats() {
    const now = Date.now();
    const stats = {
      totalEvents: 0,
      eventsUpdatedLast3Min: 0,
      eventsUpdatedLast2Min: 0,
      eventsUpdatedLast1Min: 0,
      criticalEvents: [],
      activeWorkers: this.parallelWorkers.length,
      queueLength: this.eventProcessingQueue.length,
      processingCount: this.processingEvents.size,
      failedCount: this.failedEvents.size,
      successRate: this.successCount / (this.successCount + this.failedEvents.size) * 100,
      csvProcessingActive: this.csvProcessingActive,
      memoryUsage: process.memoryUsage(),
      lastCsvUpload: this.lastCsvUploadTime ? moment().diff(this.lastCsvUploadTime, 'minutes') : null
    };

    // Get currently active events from database to filter tracking
    let activeEventIds = new Set();
    try {
      const activeEvents = await Event.find({
        Skip_Scraping: { $ne: true },
      })
        .select("Event_ID")
        .lean();
      
      activeEventIds = new Set(activeEvents.map(event => event.Event_ID));
    } catch (error) {
      // Fallback: if database query fails, use all tracked events
      console.warn(`Failed to get active events for performance stats: ${error.message}`);
      activeEventIds = new Set(this.eventUpdateTimestamps.keys());
    }

    // Analyze event update times (fast loop) - only for ACTIVE events
    for (const [eventId, lastUpdate] of this.eventUpdateTimestamps) {
      // Skip events that are no longer active for scraping
      if (!activeEventIds.has(eventId)) {
        continue;
      }
      
      stats.totalEvents++;
      const timeSinceUpdate = now - lastUpdate.valueOf();
      
      if (timeSinceUpdate < 60000) {
        stats.eventsUpdatedLast1Min++;
      }
      if (timeSinceUpdate < 120000) {
        stats.eventsUpdatedLast2Min++;
      }
      if (timeSinceUpdate < 180000) {
        stats.eventsUpdatedLast3Min++;
      } else {
        stats.criticalEvents.push({
          eventId,
          minutesSinceUpdate: Math.floor(timeSinceUpdate / 60000)
        });
      }
    }

    return stats;
  }

  /**
   * Start performance monitoring (logs stats every 30 seconds) - ENHANCED
   */
  startPerformanceMonitoring() {
    let monitoringCycleCount = 0;
    const processedEvents = new Set(); // Track processed events
    let lastSuccessfulScrapeTime = Date.now(); // Track last successful scrape time
    
    setInterval(async () => {
      monitoringCycleCount++;
      
      // Clear processed events every 6 cycles (3 minutes)
      if (monitoringCycleCount % 6 === 0) {
        processedEvents.clear();
      }

      // Run cleanup every 4 cycles (2 minutes)
      if (monitoringCycleCount % 4 === 0) {
        await this.cleanupInactiveEvents();
      }
      
      // Run stale event recovery every 6 cycles (3 minutes)
      if (monitoringCycleCount % 6 === 0) {
        await this.handleStaleEvents(processedEvents);
      }
      
      // Check if system is completely stuck (no successful scrapes in 5 minutes)
      const currentTime = Date.now();
      const timeSinceLastSuccess = this.lastSuccessTime ? currentTime - this.lastSuccessTime.valueOf() : Infinity;
      
      if (timeSinceLastSuccess > 300000) { // 5 minutes
        this.logWithTime(`⚠️ SYSTEM STALLED: No successful scrapes in ${Math.floor(timeSinceLastSuccess/60000)} minutes - initiating emergency recovery!`, "error");
        
        // Emergency recovery procedure
        await this.performEmergencyRecovery();
        
        // Reset tracking to avoid multiple recoveries
        if (this.lastSuccessTime) {
          this.lastSuccessTime = moment();
        }
      }
      
      const stats = await this.getPerformanceStats();
      const eventsPerMinute = stats.eventsUpdatedLast1Min; // Events actually updated in last minute
      const projectedCapacity = eventsPerMinute * 100; // Events that can be updated in 3 minutes
      const memoryMB = Math.round(stats.memoryUsage.heapUsed / 1024 / 1024);
      
      // Get CSV stats synchronously
      let csvInfo = '';
      try {
        const { getInventoryStats } = await import('./controllers/inventoryController.js');
        const csvStats = getInventoryStats();
        csvInfo = ` | CSV: ${csvStats.totalEvents} events, ${csvStats.totalRecords} records in memory`;
      } catch (error) {
        csvInfo = ' | CSV: Stats unavailable';
      }
      
      this.logWithTime(
        `Performance: ${stats.eventsUpdatedLast3Min}/${stats.totalEvents} events updated in 3min | ` +
        `Queue: ${stats.queueLength} | Processing: ${stats.processingCount} | ` +
        `Workers: ${stats.activeWorkers} | Success: ${stats.successRate.toFixed(1)}% | ` +
        `Rate: ${eventsPerMinute}/min | Capacity: ${projectedCapacity} events/3min | ` +
        `Tracked: ${this.eventUpdateTimestamps.size} events | ` +
        `Memory: ${memoryMB}MB${csvInfo}`,
        "info"
      );
      
      // CSV processing status
      if (stats.csvProcessingActive) {
        this.logWithTime(
          `CSV Processing: Active (${stats.csvQueueLength} in queue)`,
          "info"
        );
      } else if (stats.csvQueueLength > 0) {
        this.logWithTime(
          `CSV Processing: Idle with ${stats.csvQueueLength} pending`,
          "warning"
        );
      }
      
      // Memory warning
      if (memoryMB > 1024) { // Over 1GB
        this.logWithTime(
          `HIGH MEMORY USAGE: ${memoryMB}MB - consider cleanup`,
          "warning"
        );
        
        // Trigger cleanup of old events
        try {
          const { cleanupOldEvents } = await import('./controllers/inventoryController.js');
          const cleaned = cleanupOldEvents(.50); // Clean events older than 12 hours
          if (cleaned > 0) {
            this.logWithTime(
              `Cleaned up ${cleaned} old events from memory`,
              "info"
            );
          }
        } catch (cleanupError) {
          console.error(`Error during memory cleanup: ${cleanupError.message}`);
        }
      }
      
      if (stats.criticalEvents.length > 0) {
        this.logWithTime(
          `WARNING: ${stats.criticalEvents.length} events not updated for >3 minutes`,
          "warning"
        );
      }
      
      // Check if we can handle 1000+ events
      if (projectedCapacity >= 1000) {
        this.logWithTime(
          `✅ System capable of handling 1000+ events (current capacity: ${projectedCapacity} events/3min)`,
          "success"
        );
      } else if (projectedCapacity >= 500) {
        this.logWithTime(
          `⚠️ System handling ${projectedCapacity} events/3min - scaling needed for 1000+`,
          "warning"
        );
      } else {
        this.logWithTime(
          `❌ System only handling ${projectedCapacity} events/3min - major optimization needed`,
          "error"
        );
      }
    }, 30000);
  }

  /**
   * Emergency recovery procedure when system is completely stuck
   */
  async performEmergencyRecovery() {
    this.logWithTime("🚨 INITIATING EMERGENCY SYSTEM RECOVERY 🚨", "error");
    
    try {
      // 1. Reset all processing maps and sets
      this.logWithTime("Clearing all processing locks and states", "warning");
      this.processingEvents.clear();
      this.processingLocks.clear();
      this.activeJobs.clear();
      
      // 2. Force reset of all session and cookie data
      this.logWithTime("Force resetting all sessions and cookies", "warning");
      this.headersCache.clear();
      this.headerRefreshTimestamps.clear();
      this.headerRotationPool = [];
      
      // 3. Release all proxies
      this.logWithTime("Releasing all proxies", "warning");
      this.proxyManager.releaseAllProxies();
      
      // 4. Hard reset sessions
      try {
        await Promise.race([
          this.sessionManager.forceSessionRotation(),
          setTimeout(20000).then(() => { throw new Error("Session rotation timed out during emergency recovery"); })
        ]);
        this.logWithTime("Successfully reset session manager", "success");
      } catch (sessionError) {
        this.logWithTime(`Session manager reset failed: ${sessionError.message}`, "error");
      }
      
      // 5. Reset circuit breaker if tripped
      if (this.apiCircuitBreaker.tripped) {
        this.logWithTime("Resetting API circuit breaker", "warning");
        this.apiCircuitBreaker.tripped = false;
        this.apiCircuitBreaker.failures = 0;
        this.apiCircuitBreaker.lastTripped = null;
      }
      
      // 6. Reset global error counters
      this.globalConsecutiveErrors = 0;
      
      // 7. Reload a subset of critical events into processing queue
      try {
        const criticalEvents = await Event.find({
          Skip_Scraping: { $ne: true },
          $or: [
            { Last_Updated: { $lt: new Date(Date.now() - 120000) } }, // Not updated in 2 minutes
            { Last_Updated: { $exists: false } }
          ]
        })
        .sort({ Last_Updated: 1 })
        .limit(50)
        .select("Event_ID")
        .lean();
        
        if (criticalEvents.length > 0) {
          // Clear current processing queue and add critical events
          this.eventProcessingQueue = [];
          for (const event of criticalEvents) {
            this.eventProcessingQueue.push(event.Event_ID);
          }
          
          this.logWithTime(`Added ${criticalEvents.length} critical events to processing queue`, "success");
        } else {
          this.logWithTime("No critical events found for emergency recovery", "warning");
        }
      } catch (dbError) {
        this.logWithTime(`Error loading critical events: ${dbError.message}`, "error");
      }
      
      // 8. Ensure concurrency semaphore is reset
      this.concurrencySemaphore = CONCURRENT_LIMIT;
      
      this.logWithTime("🔄 EMERGENCY RECOVERY COMPLETE - resuming operation", "success");
      return true;
    } catch (error) {
      this.logWithTime(`Emergency recovery failed: ${error.message}`, "error");
      return false;
    }
  }

  /**
   * Add event completion marking system and update batch processing logic
   */
  async _markEventCompleted(eventId) {
    try {
      await Event.updateOne(
        { _id: eventId },
        { 
          $set: { 
            lastUpdated: new Date(),
            processingStatus: 'completed'
          },
          $unset: { needsUpdate: "" }
        }
      );
      this.processingLocks.delete(eventId);
    } catch (error) {
      this.logWithTime(`Failed to mark event ${eventId} as completed: ${error.message}`, "error");
    }
  }

  async processEventBatch(batch) {
    const now = Date.now();
    const processable = batch.filter(event => {
      const lockTime = this.processingLocks.get(event.id);
      return !lockTime || (now - lockTime) > LOCK_TIMEOUT;
    });

    if (processable.length === 0) return [];

    // Apply new locks
    processable.forEach(event => {
      this.processingLocks.set(event.id, now);
    });

    try {
      const results = await ScrapeEvent(processable, this);
      
      // Mark successfully processed events
      await Promise.all(
        results
          .filter(r => r.success)
          .map(r => this._markEventCompleted(r.event._id))
      );

      return results;
    } catch (error) {
      this.logWithTime(`Batch processing error: ${error.message}`, "error");
      return [];
    } finally {
      // Cleanup locks for failed events after timeout
      setTimeout(() => {
        processable.forEach(event => {
          if (this.processingLocks.get(event.id) === now) {
            this.processingLocks.delete(event.id);
          }
        });
      }, LOCK_TIMEOUT);
    }
  }

  async getEventsNeedingUpdate() {
    return Event.find({
      $or: [
        { lastUpdated: { $lt: new Date(Date.now() - MAX_UPDATE_INTERVAL) } },
        { needsUpdate: { $exists: true } }
      ],
      processingStatus: { $ne: 'completed' }
    }).limit(100);
  }

  /**
   * Clean up all tracking data for an event
   * @param {string} eventId - The event ID to clean up
   */
  cleanupEventTracking(eventId) {
    // Remove from all tracking maps and sets
    this.eventUpdateTimestamps.delete(eventId);
    this.eventLastProcessedTime.delete(eventId);
    this.eventFailureCounts.delete(eventId);
    this.eventFailureTimes.delete(eventId);
    this.processingEvents.delete(eventId);
    this.failedEvents.delete(eventId);
    this.cooldownEvents.delete(eventId);
    this.processingLocks.delete(eventId);
    this.eventPriorityScores.delete(eventId);
    this.eventUpdateSchedule.delete(eventId);
    this.headersCache.delete(eventId);
    this.headerRefreshTimestamps.delete(eventId);
    
    // Remove from retry queue
    this.retryQueue = this.retryQueue.filter(job => job.eventId !== eventId);
    
    // Remove from processing queue
    this.eventProcessingQueue = this.eventProcessingQueue.filter(item => 
      (typeof item === 'string' ? item : item.eventId) !== eventId
    );
    
    // Remove from active jobs
    this.activeJobs.delete(eventId);
    
    // Release any associated proxies
    this.proxyManager.releaseProxy(eventId, false);
    
    if (LOG_LEVEL >= 3) {
      this.logWithTime(`🧹 Cleaned up all tracking data for event ${eventId}`, "debug");
    }
  }

  /**
   * Set Skip_Scraping = true for an event and clean up tracking
   * @param {string} eventId - The event ID
   * @param {string} reason - Reason for skipping
   * @param {number} retryCount - Current retry count
   * @param {number} retryLimit - Retry limit
   */
  async setEventSkipScraping(eventId, reason, retryCount, retryLimit) {
    try {
      // Only stop if 10-minute threshold is exceeded
      const lastUpdate = this.eventUpdateTimestamps.get(eventId);
      const timeSinceUpdate = lastUpdate ? moment().diff(lastUpdate) : Infinity;
      
      if (timeSinceUpdate < 600000 && reason !== "exceeded_10min_threshold") {
        this.logWithTime(
          `Prevented premature stopping of event ${eventId}: only ${Math.floor(timeSinceUpdate/1000)}s since last update (10-minute threshold not reached)`,
          "warning"
        );
        return false; // Don't stop yet - continue retrying
      }
      
      // Update event in database to stop scraping
      await Event.updateOne(
        { Event_ID: eventId },
        { 
          $set: { 
            Skip_Scraping: true,
            skip_timestamp: new Date(),
            time_since_update: timeSinceUpdate
          } 
        }
      );
      
      this.logWithTime(
        `🚫 STOPPED event ${eventId}: Skip_Scraping = true (Reason: ${reason}, Retries: ${retryCount}/${retryLimit}, Time since update: ${Math.floor(timeSinceUpdate/1000)}s) - excluded from future scraping`,
        "warning"
      );
      
      // Clean up all tracking data for this event
      this.cleanupEventTracking(eventId);
      
      // Log to error log for audit trail
      await this.logError(
        eventId,
        "EVENT_STOPPED",
        new Error(`Event scraping stopped: ${reason}`),
        {
          reason,
          retryCount,
          retryLimit,
          timeSinceUpdate: Math.floor(timeSinceUpdate/1000),
          timestamp: new Date().toISOString(),
          skipScrapingSet: true
        }
      );
      
      return true; // Successfully stopped
    } catch (error) {
      this.logWithTime(`Error setting Skip_Scraping for event ${eventId}: ${error.message}`, "error");
      return false;
    }
  }

  /**
   * Get events to process in priority order
   * @returns {Promise<string[]>} Array of event IDs to process
   */
  async getEvents() {
    try {
      const now = moment();

      // Find ALL active events - no limit for 1000+ events
      // CRITICAL: Only get events where Skip_Scraping is explicitly NOT true
      const events = await Event.find({
        $or: [
          { Skip_Scraping: { $ne: true } },
          { Skip_Scraping: { $exists: false } }, // Include events without the field
          { Skip_Scraping: null },
          { Skip_Scraping: false }
        ]
      })
        .select("Event_ID url Last_Updated")
        .lean();

      if (!events.length) {
        return [];
      }

      // Get current time
      const currentTime = now.valueOf();

      // Calculate priority for each event and filter those needing updates
      const eventsNeedingUpdate = [];
      
      for (const event of events) {
        const eventId = event.Event_ID;
        
        // CRITICAL FIX: First check if this event was recently processed in memory
        // This prevents returning events that have been processed but DB hasn't been updated yet
        const lastProcessedTime = this.eventLastProcessedTime.get(eventId);
        if (lastProcessedTime && (Date.now() - lastProcessedTime) < 60000) {
          // Skip events processed in the last minute regardless of database state
          continue;
        }
        
        // Also check processing lock to avoid duplicate processing
        if (this.processingEvents.has(eventId)) {
          continue;
        }
        
        const lastUpdated = event.Last_Updated
          ? moment(event.Last_Updated).valueOf()
          : 0;
        const timeSinceLastUpdate = currentTime - lastUpdated;
        
        // Skip if updated too recently (unless critical)
        if (timeSinceLastUpdate < 30000 && lastUpdated > 0) {
          continue; // Skip events updated in last 30 seconds
        }

        // Calculate priority score
        let priorityScore = timeSinceLastUpdate / 1000;

        // Critical events approaching 3-minute threshold
        if (timeSinceLastUpdate > 150000) { // 2.5 minutes
          priorityScore = priorityScore * 1000;
        }
        // Very urgent - approaching 2 minutes
        else if (timeSinceLastUpdate > 110000) {
          priorityScore = priorityScore * 500;
        }
        // Urgent - over 1.5 minutes
        else if (timeSinceLastUpdate > 90000) {
          priorityScore = priorityScore * 100;
        }
        // High - over 1 minute
        else if (timeSinceLastUpdate > 60000) {
          priorityScore = priorityScore * 50;
        }
        // Medium - over 45 seconds
        else if (timeSinceLastUpdate > 45000) {
          priorityScore = priorityScore * 10;
        }

        eventsNeedingUpdate.push({
          eventId: event.Event_ID,
          priorityScore,
          timeSinceLastUpdate,
          isCritical: timeSinceLastUpdate > 150000
        });
        
        // Cache the priority score
        this.eventPriorityScores.set(event.Event_ID, priorityScore);
      }

      // Sort by priority (highest first)
      eventsNeedingUpdate.sort((a, b) => b.priorityScore - a.priorityScore);

      // Return all events that need updating
      return eventsNeedingUpdate.map((event) => event.eventId);
    } catch (error) {
      console.error("Error getting events to process:", error);
      return [];
    }
  }
}

// Initialize recovery methods after class is fully defined
ScraperManager.prototype.initializeRecoverySystem = function() {
  this.handleStaleEvents = this.handleStaleEvents?.bind(this) || function() {};
  this.handleCriticalStaleEvents = this.handleCriticalStaleEvents?.bind(this) || this.handleStaleEvents;
  this.handleStandardStaleEvents = this.handleStandardStaleEvents?.bind(this) || this.handleStaleEvents;
  this.handleAutoStopEvents = this.handleAutoStopEvents?.bind(this) || function() {};
  this.recoverEventBatch = this.recoverEventBatch?.bind(this) || function() {};
  this.recoverSingleEvent = this.recoverSingleEvent?.bind(this) || function() {};
};

const scraperManager = new ScraperManager();
scraperManager.initializeRecoverySystem();
export default scraperManager;

// Add a function to export that allows checking logs
export function checkScraperLogs() {
  return scraperManager.checkLogs();
}

// Export cleanup function for manual cleanup
export function forceCleanupTracking() {
  return scraperManager.forceCleanupAllTracking();
}
