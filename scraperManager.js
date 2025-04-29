import moment from "moment";
import { setTimeout } from "timers/promises";
import { Event, ErrorLog, ConsecutiveGroup } from "./models/index.js";
import { ScrapeEvent, refreshHeaders, generateEnhancedHeaders } from "./scraper.js";
import { cpus } from "os";
import fs from "fs/promises";
import path from "path";
import ProxyManager from "./helpers/ProxyManager.js";
import { BrowserFingerprint } from "./browserFingerprint.js";

// Updated constants for stricter update intervals and high performance on 32GB system
const MAX_UPDATE_INTERVAL = 160000; // Strict 2-minute update requirement
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

// Cookie expiration threshold: only refresh cookies when expired (default 1 hour)
const COOKIE_EXPIRATION_MS = 30 * 60 * 1000;

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
    this.responseCache = new Map(); // Cache successful responses
    this.headerRotationPool = []; // Pool of working headers
    this.proxySuccessRates = new Map(); // Track success rates per proxy
    this.apiCircuitBreaker = {
      failures: 0,
      threshold: 10,
      resetTimeout: 30000,
      lastTripped: null,
      tripped: false
    };
    this.headerSuccessRates = new Map(); // Track which headers work better
    
    // New: Failed events batching
    this.failureTypeGroups = new Map(); // Group failed events by error type
    this.lastFailedBatchProcess = null; // Last time we processed a batch of failed events
    this.failedEventsProcessingInterval = 5000; // Process failed events every 5 seconds
    this.eventMaxRetries = new Map(); // Track dynamic retry limits per event
    
    // Add cache configuration
    this.cacheConfig = {
      responseCacheDuration: 5 * 60 * 1000, // 5 minutes default cache duration
      maxCacheSize: 1000, // Maximum number of cached responses
      cacheCleanupInterval: 60 * 1000, // Cleanup cache every minute
    };
    
    // Start cache cleanup interval
    setInterval(() => this.cleanupCache(), this.cacheConfig.cacheCleanupInterval);
  }

  // Add cache cleanup method
  cleanupCache() {
    const now = Date.now();
    for (const [eventId, { timestamp, data }] of this.responseCache.entries()) {
      if (now - timestamp > this.cacheConfig.responseCacheDuration) {
        this.responseCache.delete(eventId);
      }
    }
    
    // Limit cache size
    if (this.responseCache.size > this.cacheConfig.maxCacheSize) {
      const entries = Array.from(this.responseCache.entries());
      entries.sort((a, b) => a[1].timestamp - b[1].timestamp);
      const toRemove = entries.slice(0, entries.length - this.cacheConfig.maxCacheSize);
      toRemove.forEach(([eventId]) => this.responseCache.delete(eventId));
    }
  }

  logWithTime(message, type = "info") {
    // Map log levels to numeric values
    const logLevels = {
      error: 0,
      warning: 1,
      info: 2,
      success: 2,
      debug: 3
    };

    // Skip logging if level is higher than configured LOG_LEVEL
    if (logLevels[type] > LOG_LEVEL) {
      return;
    }

    const now = moment();
    const formattedTime = now.format("YYYY-MM-DD HH:mm:ss");

    const statusEmoji = {
      success: "✅",
      error: "❌",
      warning: "⚠️",
      info: "ℹ️",
      debug: "🔍"
    }[type] || "📝";

    // Only include detailed stats for errors or if we're at the highest verbosity
    if (type === "error" || LOG_LEVEL >= 3) {
      const runningTime = moment.duration(now.diff(this.startTime));
      console.log(
        `${statusEmoji} [${formattedTime}] ${message}\n` +
        `   Runtime: ${Math.floor(runningTime.asHours())}h ${runningTime.minutes()}m ${runningTime.seconds()}s\n` +
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
      console.error(`Error for event ${eventId} (${errorType}): ${error.message}`);
      
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
        console.error(`Cannot log to ErrorLog - event ${eventId} not found in database`);
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
            `Processing ${eventId} despite minimum time restriction: Approaching max allowed time (${timeSinceLastUpdate / 1000}s since last update)`,
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
    const eventDoc = await Event.findOne({ Event_ID: eventId }).select("url").lean();
    const refererUrl = eventDoc?.url || "https://www.fans.com/";
    const requestId = Math.random().toString(36).substring(2,10) + "-" + Date.now().toString(36);
    const lastRefresh = this.headerRefreshTimestamps.get(eventId);
    // Only refresh cookies/headers if they have expired
    if (!lastRefresh || moment().diff(lastRefresh) > COOKIE_EXPIRATION_MS) {
      try {
        // Only log at higher verbosity levels
        if (LOG_LEVEL >= 2) {
          this.logWithTime(`Refreshing headers for ${eventId}`, "debug");
        }
        
        // New: Check if we have successful headers in the rotation pool first
        if (this.headerRotationPool.length > 0) {
          // Use headers that have been successful recently
          const headerIndex = Math.floor(Math.random() * this.headerRotationPool.length);
          const cachedHeaders = this.headerRotationPool[headerIndex];
          
          // Update timestamp but don't overwrite the original headers
          this.headerRefreshTimestamps.set(eventId, moment());
          return cachedHeaders;
        }
        
        let capturedState;
        // Rotate proxies to fetch fresh cookies; skip proxies that result in no cookies (e.g. challenge pages)
        for (let attempt = 0; attempt < PROXY_ROTATION_ATTEMPTS; attempt++) {
          const { proxyAgent: cookieProxyAgent, proxy: cookieProxy } = this.proxyManager.getProxyForBatch([eventId]);
          try {
            capturedState = await refreshHeaders(eventId, cookieProxy);
          } catch (e) {
            capturedState = null;
          }
          // If we got a valid cookie set (not a challenge), stop rotating
          if (capturedState && capturedState.cookies && capturedState.cookies.length >= MIN_VALID_COOKIES) {
            break;
          }
          // Otherwise blacklist proxy and retry
          this.proxyManager.blacklistCurrentProxy(eventId);
          this.logWithTime(`Proxy rotation (attempt ${attempt + 1}) failed to fetch cookies for ${eventId}, rotating proxy`, "warning");
        }
        // If still no capturedState, try without proxy as last resort
        if (!capturedState) {
          capturedState = await refreshHeaders(eventId);
        }
        
        if (capturedState) {
          // Only proceed if we have a valid cookies set (avoid challenge/bot pages)
          if (!capturedState.cookies || capturedState.cookies.length < MIN_VALID_COOKIES) {
            this.logWithTime(
              `Skipping header refresh for ${eventId}: invalid or insufficient cookies`,
              "warning"
            );
            return null;
          }
          // Build enhanced headers using the shared generator
          const cookieString = capturedState.cookies
            .map(c => `${c.name}=${c.value}`)
            .join("; ");
          const poolFp = randomFingerprint();
          const mergedFp = { ...capturedState.fingerprint, ...poolFp };
          // Generate varied headers: UA, Accept, Connection, etc.
          const enhanced = generateEnhancedHeaders(mergedFp, cookieString);
          // Inject anti-bot fields
          enhanced['Referer'] = refererUrl;
          enhanced['X-Forwarded-For'] = generateRandomIp();
          enhanced['X-Request-Id'] = requestId;
          const standardizedHeaders = {
            headers: enhanced,
            cookies: capturedState.cookies,
            fingerprint: mergedFp,
          };
          // Cache and rotate
          this.headersCache.set(eventId, standardizedHeaders);
          this.headerRefreshTimestamps.set(eventId, moment());
          if (!this.headerRotationPool.some(h => h.headers.Cookie === cookieString)) {
            this.headerRotationPool.push(standardizedHeaders);
            if (this.headerRotationPool.length > 10) this.headerRotationPool.shift();
          }
          return standardizedHeaders;
        }
      } catch (error) {
        this.logWithTime(
          `Failed to refresh headers for ${eventId}: ${error.message}`,
          "error"
        );
      }
    }

    return this.headersCache.get(eventId);
  }

  async resetCookiesAndHeaders() {
    // Avoid resetting cookies too frequently (at most once per hour)
    const now = moment();
    if (this.lastCookieReset && now.diff(this.lastCookieReset) < COOKIE_EXPIRATION_MS) {
      if (LOG_LEVEL >= 1) {
        this.logWithTime(
          `Skipping cookie reset - last reset was ${moment.duration(now.diff(this.lastCookieReset)).humanize()} ago`,
          "warning"
        );
      }
      return;
    }
    
    try {
      this.logWithTime("Detected multiple API failures - resetting cookies and headers", "warning");
      
      // Check if cookies.json exists before trying to delete it
      try {
        await fs.access(this.cookiesPath);
        // Only delete if expired
        const stats = await fs.stat(this.cookiesPath);
        const cookieAge = now.diff(moment(stats.mtime));
        if (cookieAge < COOKIE_EXPIRATION_MS) {
          if (LOG_LEVEL >= 2) {
            this.logWithTime(`Cookies not expired yet (age ${cookieAge}ms), skipping fresh retrieval`, "info");
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
      this.logWithTime("Applying 30-second cooldown to allow for cookie regeneration", "info");
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
 
        // Filter scrape results to include only groups with at least 2 seats
        const validScrapeResult = scrapeResult.filter(group => 
          group.seats && group.seats.length >= 2 && group.inventory && group.inventory.quantity >= 2
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
          ).lean().session(session);

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
              this.logWithTime(`Seat changes detected for event ${eventId}, updating database`, "info");
            }
            
            // Bulk delete and insert for better performance
            await ConsecutiveGroup.deleteMany({ eventId }).session(session);

            const groupsToInsert = validScrapeResult.map((group) => {
              // Calculate the increased list price based on the percentage
              const basePrice = parseFloat(group.inventory.listPrice) || 500.00;
              const increasedPrice = basePrice * (1 + priceIncreasePercentage / 100);
              
              // Format in-hand date as YYYY-MM-DD
              const formattedInHandDate = inHandDate instanceof Date ? 
                inHandDate : new Date();
              
              return {
                eventId,
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
                })),
                inventory: {
                  quantity: group.inventory.quantity,
                  section: group.section,
                  hideSeatNumbers: group.inventory.hideSeatNumbers || true,
                  row: group.row,
                  cost: group.inventory.cost,
                  stockType: group.inventory.stockType || 'MOBILE_TRANSFER',
                  lineType: group.inventory.lineType,
                  seatType: group.inventory.seatType,
                  inHandDate: formattedInHandDate,
                  notes: group.inventory.notes || `These are internal notes. @sec[${group.section}]`,
                  tags: group.inventory.tags || "john drew don",
                  inventoryId: group.inventory.inventoryId,
                  offerId: group.inventory.offerId,
                  splitType: group.inventory.splitType || 'CUSTOM',
                  publicNotes: group.inventory.publicNotes || `These are ${group.row} Row`,
                  listPrice: increasedPrice,
                  customSplit: group.inventory.customSplit || `${Math.ceil(group.inventory.quantity/2)},${group.inventory.quantity}`,
                  mapping_id: group.mapping_id || group.skybox || '',
                  tickets: group.inventory.tickets.map((ticket) => ({
                    id: ticket.id,
                    seatNumber: ticket.seatNumber,
                    notes: ticket.notes,
                    cost: ticket.cost,
                    faceValue: ticket.faceValue,
                    taxedCost: ticket.taxedCost,
                    sellPrice: ticket.sellPrice * 1.25, // Using multiplication instead of exponentiation 
                    stockType: ticket.stockType,
                    eventId: ticket.eventId,
                    accountId: ticket.accountId,
                    status: ticket.status,
                    auditNote: ticket.auditNote,
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
              this.logWithTime(`Updated ${groupsToInsert.length} consecutive groups for event ${eventId}`, "success");
            }
          } else {
            if (LOG_LEVEL >= 3) {
              this.logWithTime(`No seat changes detected for event ${eventId}`, "debug");
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
      this.eventUpdateSchedule.set(eventId, moment().add(MIN_TIME_BETWEEN_EVENT_SCRAPES, 'milliseconds'));
      
      if (LOG_LEVEL >= 2) {
        this.logWithTime(
          `Updated event ${eventId} in ${(performance.now() - startTime).toFixed(
            2
          )}ms, next update by ${this.eventUpdateSchedule.get(eventId).format('HH:mm:ss')}`,
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
   * Generate CSV inventory for an event if this is the first scrape
   * @param {string} eventId - The event ID
   * @param {Array} scrapeResult - The scrape result data
   */
  async generateInventoryCsv(eventId, scrapeResult) {
    try {
      // Import the inventory controller
      const inventoryController = (await import('./controllers/inventoryController.js')).default;
      
      // Check if this is the first scrape for this event
      if (!inventoryController.isFirstScrape(eventId)) {
        if (LOG_LEVEL >= 2) {
          this.logWithTime(`Skipping CSV generation for event ${eventId} - not the first scrape`, "info");
        }
        return;
      }
      
      if (LOG_LEVEL >= 1) {
        this.logWithTime(`Generating CSV for event ${eventId} (first scrape)`, "info");
      }
      
      // Get event data upfront
      const event = await Event.findOne({ Event_ID: eventId }).lean();
      if (!event) {
        this.logWithTime(`Event ${eventId} not found for CSV generation`, "error");
        return;
      }
      
      // Get price increase percentage and in-hand date from event
      const priceIncreasePercentage = event.priceIncreasePercentage || 25; // Default 25% if not set
      const inHandDate = event.inHandDate || new Date();
      
      // Filter out groups with fewer than 2 seats
      const validGroups = scrapeResult.filter(group => 
        group.seats && group.seats.length >= 2 && group.inventory && group.inventory.quantity >= 2
      );
      
      if (validGroups.length === 0) {
        this.logWithTime(`No valid groups with at least 2 seats found for event ${eventId}`, "warning");
        return;
      }
      
      // Format the scrape results as inventory records
      const inventoryRecords = [];
      for (const group of validGroups) {
        // Calculate the increased list price based on the percentage
        const basePrice = parseFloat(group.inventory.listPrice) || 500.00;
        const increasedPrice = (basePrice * (1 + priceIncreasePercentage / 100)).toFixed(2);
        
        // Format in-hand date as YYYY-MM-DD
        const formattedInHandDate = inHandDate instanceof Date ? 
          inHandDate.toISOString().split('T')[0] : 
          new Date().toISOString().split('T')[0];
        
        // Create a record with the exact required format
        inventoryRecords.push({
          inventory_id: `${eventId}-${group.section}-${group.row}-${Math.min(...group.seats)}`,
          event_name: event?.Event_Name || `Event ${eventId}`,
          venue_name: event?.Venue || 'Unknown Venue',
          event_date: event?.Event_DateTime?.toISOString() || new Date().toISOString(),
          event_id: eventId,
          quantity: group.inventory.quantity,
          section: group.section,
          row: group.row,
          seats: group.seats.join(','),
          barcodes: group.seats.map(() => `${Math.floor(Math.random() * 1000000000000)}${Math.floor(Math.random() * 1000000000000)}`).join('|'),
          internal_notes: `These are internal notes. @sec[${group.section}]`,
          public_notes: `These are ${group.row} Row`,
          tags: "john drew don",
          list_price: increasedPrice,
          face_price: group.inventory?.tickets?.[0]?.faceValue || '',
          taxed_cost: group.inventory?.tickets?.[0]?.taxedCost || '',
          cost: group.inventory?.tickets?.[0]?.cost || '',
          hide_seats: 'Y',
          in_hand: 'N',
          in_hand_date: formattedInHandDate,
          instant_transfer: 'N',
          files_available: 'Y',
          split_type: 'CUSTOM',
          custom_split: `${Math.ceil(group.inventory.quantity/2)},${group.inventory.quantity}`,
          stock_type: 'MOBILE_TRANSFER',
          zone: 'N',
          shown_quantity: String(Math.ceil(group.inventory.quantity/2)),
          passthrough: '128shd8923kjej47',
          mapping_id: group.mapping_id || group.skybox || ''
        });
      }
      
      // Add the inventory records in bulk
      const result = await inventoryController.addBulkInventory(inventoryRecords, eventId);
      
      if (result.success) {
        this.logWithTime(`Successfully generated CSV for event ${eventId}: ${result.message}`, "success");
      } else {
        this.logWithTime(`Failed to generate CSV for event ${eventId}: ${result.message}`, "error");
      }
    } catch (error) {
      this.logWithTime(`Error generating CSV for event ${eventId}: ${error.message}`, "error");
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
      if (timeSinceUpdate < MAX_UPDATE_INTERVAL - 20000 && timeSinceUpdate < MAX_ALLOWED_UPDATE_INTERVAL - 30000) {
        if (LOG_LEVEL >= 2) {
          this.logWithTime(`Skipping ${eventId} temporarily: Circuit breaker tripped`, "info");
        }
        return false;
      } else if (timeSinceUpdate > MAX_ALLOWED_UPDATE_INTERVAL - 30000) {
        // Log urgent processing despite circuit breaker
        if (LOG_LEVEL >= 1) {
          this.logWithTime(
            `Processing ${eventId} despite circuit breaker: Approaching maximum allowed time (${timeSinceUpdate / 1000}s since last update)`,
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
        this.logWithTime(`Scraping ${eventId} (Attempt ${retryCount + 1})`, "info");
      }

      // Use passed proxy if available, otherwise get a new one
      let proxyToUse = proxy;
      let proxyAgentToUse = proxyAgent;
      
      if (!proxyAgentToUse || !proxyToUse) {
        const { proxyAgent: newProxyAgent, proxy: newProxy } = this.proxyManager.getProxyForBatch([eventId]);
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
        this.eventUpdateSchedule.set(eventId, moment().add(MIN_TIME_BETWEEN_EVENT_SCRAPES, 'milliseconds'));
        return true;
      }

      // Check cache first
      const cachedResponse = this.responseCache.get(eventId);
      if (cachedResponse && Date.now() - cachedResponse.timestamp < this.cacheConfig.responseCacheDuration) {
        if (LOG_LEVEL >= 2) {
          this.logWithTime(`Using cached data for ${eventId} (cache age: ${Math.round((Date.now() - cachedResponse.timestamp) / 1000)}s)`, "success");
        }
        return cachedResponse.data;
      }

      // Refresh headers with backoff/caching strategy
      const headers = await this.refreshEventHeaders(eventId);
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
        throw new Error(`Invalid scrape result: expected array but got ${typeof result}`);
      }
      
      if (result.length === 0) {
        throw new Error("Empty scrape result: array is empty");
      }
      
      // We're still storing in the cache for metrics/monitoring purposes
      // but we'll never use the cached results for returning to the client
      this.responseCache.set(eventId, {
        timestamp: Date.now(),
        data: result
      });
      
      // Mark this header as successful
      if (headers) {
        const headerKey = headers.headers.Cookie?.substring(0, 20) || headers.headers["User-Agent"]?.substring(0, 20);
        const currentSuccessRate = this.headerSuccessRates.get(headerKey) || { success: 0, failure: 0 };
        currentSuccessRate.success++;
        this.headerSuccessRates.set(headerKey, currentSuccessRate);
        
        // Add to rotation pool if not already there
        if (!this.headerRotationPool.some(h => 
          h.headers.Cookie?.substring(0, 20) === headerKey ||
          h.headers["User-Agent"]?.substring(0, 20) === headerKey
        )) {
          this.headerRotationPool.push(headers);
          // Limit pool size
          if (this.headerRotationPool.length > 10) {
            this.headerRotationPool.shift();
          }
        }
      }

      await this.updateEventMetadata(eventId, result);
      
      // Generate CSV inventory if this is the first scrape
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
          this.logWithTime(`Reactivated previously failing event: ${eventId}`, "success");
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
      this.apiCircuitBreaker.failures = Math.max(0, this.apiCircuitBreaker.failures - 1);
      
      // Reset global consecutive error counter on success
      this.globalConsecutiveErrors = 0;
      
      // Increase retry limit by 5 after a successful scrape
      const prevMax = this.eventMaxRetries.get(eventId) ?? MAX_RETRIES;
      const newMax = prevMax + 3;
      this.eventMaxRetries.set(eventId, newMax);
      this.logWithTime(`Increased retry limit for ${eventId} to ${newMax}`, "info");
      
      return true;
    } catch (error) {
      this.failedEvents.add(eventId);
      
      // New: Try to handle the API error with smart strategies first
      const errorHandled = await this.handleApiError(eventId, error, this.headersCache.get(eventId));
      
      if (errorHandled) {
        // If we handled the error with our special strategies, use a very short cooldown
        // and don't increment the failure count (we're trying a different approach)
        const shortCooldown = 1000; // 1 second
        this.cooldownEvents.set(eventId, moment().add(shortCooldown, "milliseconds"));
        
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
      const isApiError = error.message.includes("403") || 
                        error.message.includes("400") || 
                        error.message.includes("429") ||
                        error.message.includes("API");
      
      if (retryCount < SHORT_COOLDOWNS.length) {
        // Use the short, progressive cooldowns for initial retries
        backoffTime = SHORT_COOLDOWNS[retryCount];
        
        // Log only at appropriate levels
        if (LOG_LEVEL >= 1) {
          this.logWithTime(
            `${isApiError ? "API error" : "Error"} for ${eventId}: ${error.message}. Short cooldown for ${backoffTime/1000}s`,
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
            `Persistent ${isApiError ? "API errors" : "errors"} for ${eventId}: ${error.message}. Event hasn't been successfully updated for ${timeSinceUpdate/1000}s. Marking as stopped.`,
          "error"
        );
        } else {
          this.logWithTime(
            `Persistent ${isApiError ? "API errors" : "errors"} for ${eventId}: ${error.message}. ${LONG_COOLDOWN_MINUTES} minute cooldown before retry.`,
            "warning"
          );
        }
        
        // Log long cooldown to error logs
        await this.logError(eventId, "LONG_COOLDOWN", new Error(`Event put in ${LONG_COOLDOWN_MINUTES} minute cooldown after persistent failures`), {
          cooldownDuration: LONG_COOLDOWN_MINUTES * 60 * 1000,
          isApiError,
          originalError: error.message,
          failureCount: recentFailures,
          retryCount
        });
      }
      
      // If we've had 3 consecutive API errors, trigger a cookie reset
      if (isApiError) {
        this.globalConsecutiveErrors++;
        if (this.globalConsecutiveErrors >= 3) {
          // Don't await here to prevent blocking the current event processing
          this.resetCookiesAndHeaders().catch(e => 
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
                lastErrorTime: new Date()
              } 
            }
          );
          if (LOG_LEVEL >= 1) {
            this.logWithTime(`Marked event ${eventId} as stopped in database`, "warning");
          }
        } catch (err) {
          console.error(`Failed to update status for event ${eventId}:`, err);
        }
      }

      // Still update the event schedule for 2-minute compliance
      this.eventUpdateSchedule.set(eventId, moment().add(MIN_TIME_BETWEEN_EVENT_SCRAPES, 'milliseconds'));

      const maxRetriesForEvent = this.eventMaxRetries.get(eventId) ?? MAX_RETRIES;
      
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
        this.logWithTime(`Stopping retries for ${eventId} after ${timeSinceUpdate/1000}s without successful updates`, "error");
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
    // Determine optimal batch size based on available proxies and system resources
    const maxEventsPerProxy = this.proxyManager.MAX_EVENTS_PER_PROXY * 2; // Doubled from original to process more events per proxy
    
    // Dynamic chunk sizing - even larger chunks for maximum throughput
    const chunkSize = Math.min(
      maxEventsPerProxy * 2, 
      Math.max(25, Math.min(Math.ceil(eventIds.length / Math.max(1, availableProxies/2)), 50))
    );
    
    if (LOG_LEVEL >= 1) {
      this.logWithTime(
        `Processing ${eventIds.length} events using ${availableProxies} proxies with chunk size ${chunkSize}`,
        "info"
      );
    }
    
    // Split into chunks for better control
    const chunks = [];
    
    for (let i = 0; i < eventIds.length; i += chunkSize) {
      chunks.push(eventIds.slice(i, i + chunkSize));
    }
    
    // Process chunks with controlled parallelism and better error handling
    let successCount = 0;
    let failureCount = 0;
    
    // Process in batches with maximum parallelism - increased from 8 to 16 chunks in parallel
    const batchCount = Math.ceil(chunks.length / 16);
    
    for (let batchIndex = 0; batchIndex < batchCount; batchIndex++) {
      const batchStart = batchIndex * 16;
      const batchEnd = Math.min(batchStart + 16, chunks.length);
      const batchChunks = chunks.slice(batchStart, batchEnd);
      
      // Generate unique batch IDs for each chunk
      const batchProcesses = batchChunks.map(async (chunk) => {
        const batchId = Date.now().toString(36) + Math.random().toString(36).substr(2, 5);
        
        try {
          // Get proxies for this specific batch with improved proxy assignment
          const proxyData = this.proxyManager.getProxyForBatch(chunk);
          
          // Handle case where no healthy proxies are available
          if (proxyData.noHealthyProxies || proxyData.assignmentFailed) {
            // Log detailed error based on the specific issue
            if (proxyData.noHealthyProxies) {
              this.logWithTime(`No healthy proxies available for batch ${batchId}`, "error");
            } else if (proxyData.assignmentFailed) {
              this.logWithTime(`Failed to assign proxies for batch ${batchId}`, "error");
            }
            
            // Check if we should retry later or fail now based on proxy availability
            const shouldRetryBatch = this.proxyManager.getAvailableProxyCount() > 0 || 
                                     this.proxyManager.getTotalProxies() > 0;
            
            if (shouldRetryBatch) {
              // Add to retry queue rather than failing immediately
              chunk.forEach(eventId => {
                // Check if the event is already in the retry queue
                const existingRetry = this.retryQueue.find(item => item.eventId === eventId);
                if (!existingRetry) {
                  this.retryQueue.push({
                    eventId,
                    retryCount: 0,
                    retryAfter: moment().add(5000 + Math.random() * 5000, 'milliseconds'),
                    lastError: new Error("Proxy availability issue, scheduled for retry")
                  });
                }
              });
              
              this.logWithTime(`Scheduled ${chunk.length} events from batch ${batchId} for retry due to proxy issues`, "info");
              return { results: [], failed: [] };
            }
            
            // If we shouldn't retry, mark all as failed
            this.logWithTime(`Failed all ${chunk.length} events in batch ${batchId} due to proxy unavailability`, "error");
            return { 
              results: [], 
              failed: chunk.map(eventId => ({ 
                eventId, 
                error: new Error(proxyData.noHealthyProxies ? 
                  "No healthy proxies available" : 
                  "Failed to assign proxies")
              }))
            };
          }
          
          // Store the proxy assignment for this batch
          this.batchProxies.set(batchId, { proxy: proxyData.proxy, chunk });
          
          if (LOG_LEVEL >= 3) {
            const proxyMapSize = proxyData.eventProxyMap ? proxyData.eventProxyMap.size : 0;
            this.logWithTime(`Using ${proxyMapSize} proxies for batch ${batchId} (${chunk.length} events)`, "debug");
          }

          // Process the entire chunk efficiently as a batch using unique proxies per event
          const result = await this.processBatchEfficiently(chunk, batchId, proxyData.proxyAgent, proxyData.proxy, proxyData.eventProxyMap);
          successCount += result.results.length;
          failureCount += result.failed.length;
          results.push(...result.results);
          failed.push(...result.failed);
          
          // Mark this proxy as successful if we have results
          if (result.results.length > 0 && proxyData.proxy) {
            this.proxyManager.updateProxyHealth(proxyData.proxy.proxy, true);
          }
          
          // If we had failures, and they outnumber successes, mark proxy as having issues
          if (result.failed.length > result.results.length && result.failed.length > 0 && proxyData.proxy) {
            this.proxyManager.updateProxyHealth(proxyData.proxy.proxy, false);
          }
        } catch (error) {
          this.logWithTime(`Error processing batch ${batchId}: ${error.message}`, "error");
          
          // If there's a proxy error, mark the proxy as unhealthy
          if (this.batchProxies.has(batchId)) {
            const { proxy, chunk } = this.batchProxies.get(batchId);
            if (proxy && proxy.proxy) {
              this.proxyManager.updateProxyHealth(proxy.proxy, false);
              this.proxyManager.releaseProxyBatch(chunk);
            }
          }
          
          // Add these events to failed list
          failed.push(...chunk.map(eventId => ({ 
            eventId, 
            error: new Error(`Batch processing error: ${error.message}`) 
          })));
          failureCount += chunk.length;
        } finally {
          // Clean up batch tracking
          this.batchProxies.delete(batchId);
        }
      });
      
      // Wait for this batch of chunks to complete before moving to next batch
      await Promise.all(batchProcesses);
      
      // Add a small delay between batches to prevent rate limiting
      if (batchIndex < batchCount - 1) {
        await setTimeout(200);
      }
    }
    
    if (LOG_LEVEL >= 1 && eventIds.length > 10) {
      this.logWithTime(`Completed processing ${eventIds.length} events: ${successCount} successful, ${failureCount} failed`, "info");
    }
    
    return { results, failed };
  }

  // New method to process a batch of events efficiently by handling API requests together
  async processBatchEfficiently(eventIds, batchId, proxyAgent, proxy, eventProxyMap) {
    const results = [];
    const failed = [];
    
    // Define sharedHeaders at the top level of the function
    let sharedHeaders = null;

    // Skip excessive logging for small batches to reduce log noise
    const shouldLogDetails = LOG_LEVEL >= 2 && eventIds.length > 5;

    // Pre-filter events that should be skipped to avoid unnecessary DB lookups
    const filteredIds = eventIds.filter(eventId => !this.shouldSkipEvent(eventId));
    
    if (filteredIds.length === 0) {
      if (LOG_LEVEL >= 1) {
        this.logWithTime(`All events in batch ${batchId} should be skipped, skipping validation`, "warning");
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
          this.eventUpdateSchedule.set(eventId, moment().add(MIN_TIME_BETWEEN_EVENT_SCRAPES, 'milliseconds'));
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
            if (!eventsByDomain['default']) {
              eventsByDomain['default'] = [];
            }
            eventsByDomain['default'].push(eventId);
          }
        } else {
          // No URL, add to default group
          if (!eventsByDomain['default']) {
            eventsByDomain['default'] = [];
          }
          eventsByDomain['default'].push(eventId);
        }

        return { eventId, valid: true, url: event.url };
      } catch (error) {
        return { eventId, valid: false, reason: "error", error };
      }
    });

    const validations = await Promise.all(validationPromises);
    
    // Filter to valid events
    const validEventDetails = validations.filter(v => v.valid);
    
    if (validEventDetails.length === 0) {
      return { results, failed };
    }
    
    // Get a shared set of headers for this batch to reduce header generation overhead
    try {
      sharedHeaders = await this.refreshEventHeaders(validEventDetails[0].eventId);
      if (!sharedHeaders) {
        throw new Error("Failed to obtain shared headers for batch");
      }
    } catch (error) {
      this.logWithTime(`Failed to obtain shared headers for batch: ${error.message}`, "error");
    }
    
    // Process domains in parallel for better efficiency
    await Promise.all(Object.entries(eventsByDomain).map(async ([domain, domainEvents]) => {
      // Get domain-specific headers if possible
      let domainHeaders = sharedHeaders;
      if (domain !== 'default' && domainEvents.length > 0) {
        try {
          domainHeaders = await this.refreshEventHeaders(domainEvents[0]);
        } catch (error) {
          // Fall back to shared headers
          domainHeaders = sharedHeaders;
        }
      }
      
      // Process up to 20 events at a time per domain for maximum throughput
      const domainConcurrentLimit = 20; // Doubled from 10 to 20
      
      // Split domain events into smaller groups
      for (let i = 0; i < domainEvents.length; i += domainConcurrentLimit) {
        const concurrentBatch = domainEvents.slice(i, i + domainConcurrentLimit);
        
        // Process events in this domain batch concurrently
        await Promise.all(concurrentBatch.map(async (eventId) => {
          // Skip if invalidated
          if (!validations.find(v => v.eventId === eventId && v.valid)) {
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
                  this.logWithTime(`Using dedicated proxy ${eventProxy.proxy.substring(0, 15)}... for event ${eventId}`, "debug");
                }
              }
            }
            
            if (!usingEventSpecificProxy && LOG_LEVEL >= 3) {
              this.logWithTime(`No dedicated proxy for event ${eventId}, using shared proxy`, "debug");
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
                this.proxyManager.updateProxyHealth(eventProxy.proxy, false);
              }
            }
          } catch (error) {
            failed.push({ eventId, error });
            
            // Mark this proxy as having issues if using a dedicated one
            if (eventProxyMap && eventProxyMap.has(eventId)) {
              const eventProxyData = eventProxyMap.get(eventId);
              if (eventProxyData && eventProxyData.proxy) {
                this.proxyManager.updateProxyHealth(eventProxyData.proxy.proxy, false);
              }
            }
          } finally {
            // Release the proxy for this event
            if (eventProxyMap && eventProxyMap.has(eventId)) {
              this.proxyManager.releaseProxy(eventId);
            }
          }
        }));
        
        // Add a small delay between batches within the same domain
        if (i + domainConcurrentLimit < domainEvents.length) {
          await setTimeout(500);
        }
      }
    }));
    
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
    const isApiError = error.message.includes("403") || 
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
          `${isApiError ? "API error" : "Error"} for ${eventId}: ${error.message}. Short cooldown for ${backoffTime/1000}s`,
          "warning"
        );
      }
    } else {
      // For persistent failures, use a longer cooldown
      backoffTime = LONG_COOLDOWN_MINUTES * 60 * 1000; // Convert minutes to ms
      
      if (shouldMarkStopped) {
      this.logWithTime(
          `Persistent ${isApiError ? "API errors" : "errors"} for ${eventId}: ${error.message}. Event hasn't been successfully updated for ${timeSinceUpdate/1000}s. Marking as stopped.`,
        "error"
      );
      } else {
        this.logWithTime(
          `Persistent ${isApiError ? "API errors" : "errors"} for ${eventId}: ${error.message}. ${LONG_COOLDOWN_MINUTES} minute cooldown before retry.`,
          "warning"
        );
      }
      
      // Log long cooldown to error logs
      await this.logError(eventId, "LONG_COOLDOWN", new Error(`Event put in ${LONG_COOLDOWN_MINUTES} minute cooldown after persistent failures`), {
        cooldownDuration: LONG_COOLDOWN_MINUTES * 60 * 1000,
        isApiError,
        originalError: error.message,
        failureCount: recentFailures,
        retryCount
      });
    }
    
    // If we've had 3 consecutive API errors, trigger a cookie reset
    if (isApiError) {
      this.globalConsecutiveErrors++;
      if (this.globalConsecutiveErrors >= 3) {
        // Don't await here to prevent blocking the current event processing
        this.resetCookiesAndHeaders().catch(e => 
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
              lastErrorTime: new Date()
            } 
          }
        );
        if (LOG_LEVEL >= 1) {
          this.logWithTime(`Marked event ${eventId} as stopped in database`, "warning");
        }
      } catch (err) {
        console.error(`Failed to update status for event ${eventId}:`, err);
      }
    }
    
    // Still update the event schedule for 2-minute compliance
    this.eventUpdateSchedule.set(eventId, moment().add(MIN_TIME_BETWEEN_EVENT_SCRAPES, 'milliseconds'));

    // Only add to retry queue if we haven't hit the failure threshold
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
      this.logWithTime(`Stopping retries for ${eventId} after ${timeSinceUpdate/1000}s without successful updates`, "error");
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
      this.logWithTime(`Processing ${readyForRetry.length} events from retry queue`, "info");
    }

    // For larger retry queues, process them all at once rather than by retry count
    if (readyForRetry.length > 30) {
      // Extract all event IDs, regardless of retry count
      const allRetryEventIds = readyForRetry.map(job => job.eventId);
      
      // Process all at once using the main batch processor
      await this.processBatch(allRetryEventIds);
      return;
    }

    // For smaller queues, continue with grouped processing
    // Group by retry count for better handling
    const retryGroups = {};
    readyForRetry.forEach(job => {
      if (!retryGroups[job.retryCount]) {
        retryGroups[job.retryCount] = [];
      }
      retryGroups[job.retryCount].push(job.eventId);
    });

    // Process each retry group as a batch, prioritizing lower retry counts first
    const prioritizedGroups = Object.entries(retryGroups)
      .sort(([countA], [countB]) => parseInt(countA) - parseInt(countB));

    for (const [retryCount, eventIds] of prioritizedGroups) {
      if (!this.isRunning) break;
      
      if (LOG_LEVEL >= 2) {
        this.logWithTime(`Processing batch of ${eventIds.length} retries (attempt ${parseInt(retryCount) + 1})`, "info");
      }
      
      // Determine optimal batch size - use larger batches for better efficiency
      const batchSize = Math.min(eventIds.length, Math.max(30, Math.ceil(CONCURRENT_LIMIT / 2)));
      
      // Split into batches if needed
      const batches = [];
      for (let i = 0; i < eventIds.length; i += batchSize) {
        batches.push(eventIds.slice(i, i + batchSize));
      }
      
      // Process all batches of this retry count in parallel
      await Promise.all(batches.map(async (batch) => {
        try {
          // Use processBatch directly for maximum efficiency
          await this.processBatch(batch);
        } catch (error) {
          this.logWithTime(`Error processing retry batch: ${error.message}`, "error");
        }
      }));
      
      // Very minimal delay between different retry count groups to maximize throughput
      await setTimeout(200);
    }
  }

  async getEventsToProcess() {
    try {
      const now = moment();
      const twoMinutesFromNow = now.clone().add(2, 'minutes').toDate();
      
      // Prioritize events needing update soon
      // Find all active events due for update - massively increased limit for batch processing
      const urgentEvents = await Event.find({
        Skip_Scraping: { $ne: true },
        $or: [
          { Last_Updated: { $exists: false } },
          { Last_Updated: { $lt: now.clone().subtract(1, 'minute').toDate() } }
        ]
      })
      .select('Event_ID url Last_Updated')
      .sort({ Last_Updated: 1 }) // Oldest first
      .limit(BATCH_SIZE * 40) // Get a much larger sample for comprehensive batch processing
      .lean();
    
      // No events needing update
      if (!urgentEvents.length) {
        return [];
      }
    
      // Get current time as unix timestamp
      const currentTime = now.valueOf();
      
      // Calculate priority score for each event
      const prioritizedEvents = urgentEvents.map(event => {
        const lastUpdated = event.Last_Updated ? moment(event.Last_Updated).valueOf() : 0;
        const timeSinceLastUpdate = currentTime - lastUpdated;
        
        // Higher score = higher priority
        // Give more weight to events that haven't been updated in a long time
        let priorityScore = timeSinceLastUpdate / 1000; // Convert to seconds
        
        // CRITICAL - Events approaching 3-minute maximum threshold (absolute deadline)
        if (timeSinceLastUpdate > MAX_ALLOWED_UPDATE_INTERVAL - 10000) { // Within 10 seconds of 3-minute max
          priorityScore = priorityScore * 20; // 20x priority boost for events approaching max allowed time
        }
        // Critical priority for events approaching 2-minute deadline
        else if (timeSinceLastUpdate > MAX_UPDATE_INTERVAL - 10000) { // Within 10 seconds of 2-minute deadline
          priorityScore = priorityScore * 10; // 10x priority boost for critical events
        }
        // High priority for events approaching deadline
        else if (timeSinceLastUpdate > MAX_UPDATE_INTERVAL - 30000) { // Within 30 seconds of deadline
          priorityScore = priorityScore * 5; // 5x priority boost for urgent events
        }
        // Medium priority for events getting close to deadline
        else if (timeSinceLastUpdate > MAX_UPDATE_INTERVAL - 60000) { // Within 60 seconds of deadline
          priorityScore = priorityScore * 2; // 2x priority boost for upcoming events
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
      prioritizedEvents.forEach(event => {
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
          // No URL, just use the eventId as key
          if (!domains.has(event.eventId)) {
            domains.set(event.eventId, []);
          }
          domains.get(event.eventId).push(event);
        }
      });
      
      // Reassemble prioritized list, keeping domain groups together but prioritizing critical events
      const optimizedEventList = [];
      
      // First add all near-maximum events (approaching 3-minute deadline)
      const nearMaxEvents = prioritizedEvents.filter(event => event.timeSinceLastUpdate > MAX_ALLOWED_UPDATE_INTERVAL - 10000);
      nearMaxEvents.forEach(event => {
        optimizedEventList.push(event.eventId);
      });
      
      // Then add critical events (approaching 2-minute deadline)
      const criticalEvents = prioritizedEvents.filter(event => 
        event.timeSinceLastUpdate > MAX_UPDATE_INTERVAL - 10000 && 
        event.timeSinceLastUpdate <= MAX_ALLOWED_UPDATE_INTERVAL - 10000 &&
        !nearMaxEvents.some(maxEvent => maxEvent.eventId === event.eventId)
      );
      criticalEvents.forEach(event => {
        optimizedEventList.push(event.eventId);
      });
      
      // Then add remaining events grouped by domain
      for (const domainEvents of domains.values()) {
        // Skip events already added
        const remainingEvents = domainEvents.filter(event => 
          !nearMaxEvents.some(maxEvent => maxEvent.eventId === event.eventId) &&
          !criticalEvents.some(criticalEvent => criticalEvent.eventId === event.eventId)
        );
        
        // Add all remaining events from this domain/group
        remainingEvents.forEach(event => {
          optimizedEventList.push(event.eventId);
        });
      }
      
      // For 1000+ events, use a larger batch size to ensure we keep up
      const dynamicBatchSize = Math.min(
        optimizedEventList.length,
        Math.max(BATCH_SIZE, Math.ceil(optimizedEventList.length / 3)) // Process at least 33% of events per cycle (increased from 20%)
      );
      
      // Limit to the maximum batch size but make sure we don't exceed our processing capacity
      const finalEventsList = optimizedEventList.slice(0, dynamicBatchSize);
      
      // Log stats about the events we're processing - only at appropriate log level
      if (LOG_LEVEL >= 2) {
        const nearMaxCount = nearMaxEvents.length;
        const criticalCount = criticalEvents.length;
        const oldestEvent = prioritizedEvents[0];
        if (oldestEvent) {
          const ageInSeconds = oldestEvent.timeSinceLastUpdate / 1000;
          this.logWithTime(
            `Processing ${finalEventsList.length} events. Oldest event is ${ageInSeconds.toFixed(1)}s old. ` +
            `${nearMaxCount} events near 3min max, ${criticalCount} events near 2min deadline.`,
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
      this.logWithTime("Starting continuous scraping...", "info");
      
      // Main scraping loop
      while (this.isRunning) {
        try {
          // First process retry queue to catch up on failed events
          await this.processRetryQueue();
          
          // Get next batch of events to process
          const eventIds = await this.getEventsToProcess();
          
          if (eventIds.length > 0) {
            // Use larger batchSize for more efficient processing
            const { failed } = await this.processBatch(eventIds);
            
            // If we have many failed events, process them immediately instead of waiting
            if (failed.length > 10) {
              this.logWithTime(`Processing ${failed.length} failed events from previous batch immediately`, "info");
              await this.processFailedEventsBatch();
            }
          } else {
            this.logWithTime("No events to process at this time", "info");
          }
          
          // Process any accumulated failed events
          await this.processFailedEventsBatch();
          
          // Add a dynamic pause to avoid overloading - shorter pause for faster processing
          const pauseTime = Math.min(this.getAdaptivePauseTime(), 1000); // Cap at 1 second max
          await setTimeout(pauseTime);
        } catch (error) {
          console.error(`Error in scraping loop: ${error.message}`);
          // Brief pause on error to avoid spinning
          await setTimeout(2000);
        }
      }
    }
  }
  
  /**
   * Calculate adaptive pause time based on system load
   * @returns {number} Pause time in milliseconds
   */
  getAdaptivePauseTime() {
    // Base pause time - significantly reduced for faster cycles
    let pauseTime = 500; // Base of 500ms
    
    // Add time based on active jobs and failed events
    const activeJobCount = this.activeJobs.size;
    const failedEventCount = this.failedEvents.size;
    
    // Scale pause with system load - keep minimal for maximum throughput
    pauseTime += Math.min(activeJobCount * 10, 200); // Add at most 200ms for active jobs
    pauseTime += Math.min(failedEventCount * 5, 200); // Add at most 200ms for failed events
    
    // Additional time if circuit breaker tripped
    if (this.apiCircuitBreaker.tripped) {
      pauseTime += 500;
    }
    
    return pauseTime;
  }

  async cleanupStaleTasks() {
    // Handle jobs that might be stuck
    const staleTimeLimit = 5 * 60 * 1000; // 5 minutes
    const now = moment();

    for (const [eventId, startTime] of this.activeJobs.entries()) {
      if (now.diff(startTime) > staleTimeLimit) {
        if (LOG_LEVEL >= 1) {
          this.logWithTime(
            `Cleaning up stale job for ${eventId} (started ${
              now.diff(startTime) / 1000
            }s ago)`,
            "warning"
          );
        }
        this.activeJobs.delete(eventId);
        this.processingEvents.delete(eventId);
        // Release a semaphore for this stale job
        this.concurrencySemaphore++;
      }
    }
  }

  async monitoringTask() {
    while (this.isRunning) {
      try {
        await this.cleanupStaleTasks();
        
        // Check for events that missed their deadlines
        const now = moment();
        let missedDeadlines = 0;
        let criticalMissedDeadlines = 0;
        let failedEvents = 0;
        
        for (const [eventId, lastUpdateTime] of this.eventUpdateTimestamps.entries()) {
          // Check if event has exceeded the 2-minute target
          const timeSinceUpdate = now.diff(lastUpdateTime);
          
          // First check for events that haven't been updated in 10 minutes - mark as failed/stopped
          if (timeSinceUpdate >= EVENT_FAILURE_THRESHOLD && !this.processingEvents.has(eventId)) {
            failedEvents++;
            
            // Mark these events as stopped/failed in the database
            await this.stopEventScraping(eventId, "exceeded 10-minute inactivity threshold");
            
            if (LOG_LEVEL >= 1) {
              this.logWithTime(
                `FAILED: Event ${eventId} has not been updated for 10+ minutes! (${timeSinceUpdate / 1000}s since last update) - marked as stopped`,
                "error"
              );
            }
          }
          // Next check for events exceeding maximum allowed time (3 minutes)
          else if (timeSinceUpdate > MAX_ALLOWED_UPDATE_INTERVAL && !this.processingEvents.has(eventId)) {
            criticalMissedDeadlines++;
            
            // Force immediate processing of events exceeding max time
            this.priorityQueue.add(eventId);
            
            if (LOG_LEVEL >= 1) {
              this.logWithTime(
                `CRITICAL: Event ${eventId} has exceeded maximum allowed update time! (${timeSinceUpdate / 1000}s since last update)`,
                "error"
              );
            }
          }
          // Then check for events missing 2-minute target
          else if (timeSinceUpdate > MAX_UPDATE_INTERVAL && !this.processingEvents.has(eventId)) {
            missedDeadlines++;
            
            // Force processing of missed events
            this.priorityQueue.add(eventId);
          }
        }
        
        if (failedEvents > 0) {
          this.logWithTime(`FAILURE: ${failedEvents} events exceeded the 10-minute inactivity threshold and were marked as stopped!`, "error");
        }
        
        if (criticalMissedDeadlines > 0) {
          this.logWithTime(`CRITICAL WARNING: ${criticalMissedDeadlines} events exceeded the 3-minute maximum update deadline!`, "error");
        }
        
        if (missedDeadlines > 0) {
          this.logWithTime(`WARNING: ${missedDeadlines} events missed their 2-minute update target`, "warning");
        }
        
        // Check if all recent attempts failed, which might indicate an API issue
        const recentAttempts = this.successCount + this.failedEvents.size;
        if (recentAttempts > 5 && this.failedEvents.size / recentAttempts > 0.8) {
          this.logWithTime(`High failure rate detected: ${this.failedEvents.size}/${recentAttempts} events failing`, "error");
          // If we're not already resetting, do so now
          if (this.globalConsecutiveErrors < 3) {
            this.globalConsecutiveErrors = 3; // Force a reset on next cycle
          }
        }
        
        // Log system health status - only log at appropriate levels
        if (LOG_LEVEL >= 1) {
          this.logWithTime(
            `System status: ${this.successCount} successful scrapes, ` +
            `${this.activeJobs.size}/${CONCURRENT_LIMIT} active, ` +
            `${this.failedEvents.size} failed, ${this.retryQueue.length} in retry queue, ` +
            `${this.eventUpdateSchedule.size} total tracked events` +
            (this.globalConsecutiveErrors > 0 ? `, consecutive errors: ${this.globalConsecutiveErrors}` : ""),
            "info"
          );
        }
        
        // Clear old cooldowns and failure records
        const expiredCooldowns = [];
        for (const [eventId, cooldownTime] of this.cooldownEvents.entries()) {
          if (now.isAfter(cooldownTime)) {
            expiredCooldowns.push(eventId);
          }
        }
        
        for (const eventId of expiredCooldowns) {
          this.cooldownEvents.delete(eventId);
        }
        
        // Clear failure counts older than 1 hour to prevent permanent penalties
        const oldFailureThreshold = moment().subtract(1, 'hour');
        for (const [eventId, failureTime] of this.eventFailureTimes.entries()) {
          if (failureTime.isBefore(oldFailureThreshold)) {
            this.clearFailureCount(eventId);
          }
        }
      } catch (error) {
        console.error("Error in monitoring task:", error);
      }
      
      // Run monitoring every 15 seconds for more responsive monitoring
      await setTimeout(15 * 1000);
    }
  }

  async start() {
    // Start monitoring task
    this.monitoringTask();

    // Start main scraping loop
    return this.startContinuousScraping();
  }

  stop() {
    this.isRunning = false;
    this.logWithTime("Stopping scraper");
  }

  // Add a function to export that allows checking logs
  checkLogs() {
    // Implementation of checkLogs method
  }

  // Add new method for handling 403 errors smartly
  async handleApiError(eventId, error, headers) {
    const is403Error = error.message.includes("403") || error.message.includes("Forbidden");
    const is429Error = error.message.includes("429") || error.message.includes("Too Many Requests");
    
    if (is403Error || is429Error) {
      this.apiCircuitBreaker.failures++;
      
      // Mark this header as problematic
      if (headers) {
        const headerKey = headers.headers.Cookie?.substring(0, 20) || headers.headers["User-Agent"]?.substring(0, 20);
        const currentSuccessRate = this.headerSuccessRates.get(headerKey) || { success: 0, failure: 0 };
        currentSuccessRate.failure++;
        this.headerSuccessRates.set(headerKey, currentSuccessRate);
        
        // Remove from rotation pool if success rate is too low
        if (currentSuccessRate.failure > (currentSuccessRate.success * 2) && currentSuccessRate.failure > 5) {
          this.headerRotationPool = this.headerRotationPool.filter(h => 
            h.headers.Cookie?.substring(0, 20) !== headerKey &&
            h.headers["User-Agent"]?.substring(0, 20) !== headerKey
          );
        }
      }
      
      // Check if circuit breaker should trip
      if (this.apiCircuitBreaker.failures >= this.apiCircuitBreaker.threshold) {
        if (!this.apiCircuitBreaker.tripped) {
          this.logWithTime(`Circuit breaker tripped: Too many API errors (${this.apiCircuitBreaker.failures})`, "error");
          this.apiCircuitBreaker.tripped = true;
          this.apiCircuitBreaker.lastTripped = moment();
          
          // Clear all current headers and reset cookies
          await this.resetCookiesAndHeaders();
          
          // Reset after timeout
          setTimeout(() => {
            this.apiCircuitBreaker.tripped = false;
            this.apiCircuitBreaker.failures = 0;
            this.logWithTime("Circuit breaker reset", "info");
          }, this.apiCircuitBreaker.resetTimeout);
        }
        return true; // Error was handled by circuit breaker
      }
      
      // Try rotating proxy and headers for this event
      if (this.proxyManager.getAvailableProxyCount() > 1) {
        if (LOG_LEVEL >= 2) {
          this.logWithTime(`Rotating proxy for event ${eventId} due to ${is403Error ? '403' : '429'} error`, "info");
        }
        this.proxyManager.blacklistCurrentProxy(eventId);
        return true; // Error handled by proxy rotation
      }
    }
    
    return false; // Error not handled
  }

  // Add new method to process batches of failed events by error type
  async processFailedEventsBatch() {
    const now = moment();
    
    // Don't process too frequently
    if (this.lastFailedBatchProcess && moment().diff(this.lastFailedBatchProcess) < this.failedEventsProcessingInterval) {
      return;
    }
    
    this.lastFailedBatchProcess = moment();
    
    // Skip if no failed events
    if (this.failedEvents.size === 0) {
      return;
    }
    
    if (LOG_LEVEL >= 2) {
      this.logWithTime(`Processing ${this.failedEvents.size} failed events in batches`, "info");
    }
    
    // Group failed events by error types and failure count
    const failureGroups = new Map();
    
    // Group similar events together
    for (const eventId of this.failedEvents) {
      // Skip events in cooldown
      if (this.cooldownEvents.has(eventId) && now.isBefore(this.cooldownEvents.get(eventId))) {
        continue;
      }
      
      // Skip events already being processed
      if (this.processingEvents.has(eventId)) {
        continue;
      }
      
      const failureCount = this.getRecentFailureCount(eventId);
      const key = `count-${failureCount}`;
      
      if (!failureGroups.has(key)) {
        failureGroups.set(key, []);
      }
      
      failureGroups.get(key).push(eventId);
    }
    
    // Process each group separately, starting with fewer failures first
    const sortedGroups = Array.from(failureGroups.entries())
      .sort(([keyA], [keyB]) => {
        const countA = parseInt(keyA.split('-')[1]);
        const countB = parseInt(keyB.split('-')[1]);
        return countA - countB; // Lower failure count first
      });
    
    for (const [key, eventIds] of sortedGroups) {
      if (eventIds.length === 0) continue;
      
      // Determine optimal batch size based on failure count
      // More failures = smaller batch size for better error isolation
      const failureCount = parseInt(key.split('-')[1]);
      const batchSize = Math.max(
        1, 
        Math.min(
          eventIds.length,
          Math.ceil(CONCURRENT_LIMIT / (1 + failureCount)) // Decrease batch size as failure count increases
        )
      );
      
      // Split into batches
      const batches = [];
      for (let i = 0; i < eventIds.length; i += batchSize) {
        batches.push(eventIds.slice(i, i + batchSize));
      }
      
      if (LOG_LEVEL >= 2) {
        this.logWithTime(`Processing ${eventIds.length} events with ${failureCount} failure(s) in ${batches.length} batches`, "info");
      }
      
      // Process each batch
      for (const batch of batches) {
        if (!this.isRunning) break;
        
        // Skip events that are now in cooldown or being processed
        const validEvents = batch.filter(
          eventId => !this.cooldownEvents.has(eventId) || 
                    now.isAfter(this.cooldownEvents.get(eventId))
        ).filter(
          eventId => !this.processingEvents.has(eventId)
        );
        
        if (validEvents.length === 0) continue;
        
        // Group by domain for better proxy utilization
        const domains = {};
        for (const eventId of validEvents) {
          const event = await Event.findOne({ Event_ID: eventId })
            .select("url")
            .lean();
          
          if (!event || !event.url) {
            // No URL, use eventId as domain key
            if (!domains[eventId]) domains[eventId] = [];
            domains[eventId].push(eventId);
            continue;
          }
          
          try {
            const url = new URL(event.url);
            const domain = url.hostname;
            if (!domains[domain]) domains[domain] = [];
            domains[domain].push(eventId);
          } catch (e) {
            // Invalid URL, use eventId as domain key
            if (!domains[eventId]) domains[eventId] = [];
            domains[eventId].push(eventId);
          }
        }
        
        // Process each domain group with a shared proxy
        const domainGroups = Object.values(domains);
        
        // Process domain groups in parallel
        await Promise.all(domainGroups.map(async (domainEvents) => {
          try {
            // Get a proxy for this batch
            const { proxyAgent, proxy } = this.proxyManager.getProxyForBatch(domainEvents);
            
            // Get headers from the rotation pool or generate new ones if needed
            let headers = null;
            if (this.headerRotationPool.length > 0) {
              const headerIndex = Math.floor(Math.random() * this.headerRotationPool.length);
              headers = this.headerRotationPool[headerIndex];
            } else {
              // Try to get headers for first event
              headers = await this.refreshEventHeaders(domainEvents[0]);
            }
            
            if (!headers) {
              throw new Error("Failed to obtain valid headers for batch");
            }
            
            // Process events in this domain group with shared proxy and headers
            const promises = domainEvents.map(eventId => 
              this.scrapeEvent(eventId, this.getRecentFailureCount(eventId), proxyAgent, proxy)
            );
            
            await Promise.all(promises);
          } catch (error) {
            this.logWithTime(`Error processing domain group: ${error.message}`, "error");
          }
        }));
        
        // Brief pause between batches to avoid overwhelming system
        await setTimeout(500);
      }
      
      // Pause between different failure count groups
      await setTimeout(1000);
    }
  }
}

const scraperManager = new ScraperManager();
export default scraperManager;

// Add a function to export that allows checking logs
export function checkScraperLogs() {
  return scraperManager.checkLogs();
}
