import moment from "moment";
import { setTimeout } from "timers/promises";
import { Event, ErrorLog, ConsecutiveGroup } from "./models/index.js";
import { ScrapeEvent, refreshHeaders } from "./scraper.js";
import { cpus } from "os";
import fs from "fs/promises";
import path from "path";
import ProxyManager from "./helpers/ProxyManager.js";
import { BrowserFingerprint } from "./browserFingerprint.js";

// Updated constants for stricter update intervals and high performance on 32GB system
const MAX_UPDATE_INTERVAL = 160000; // Strict 2-minute update requirement
const CONCURRENT_LIMIT = Math.max(30, Math.floor(cpus().length * 3)); // Reduced from 4x to 3x CPU cores to avoid proxy exhaustion
const MAX_RETRIES = 3; // Reduced to 3 for faster failure detection
const SCRAPE_TIMEOUT = 160000; // Reduced timeout to 35 seconds for faster failure detection
const BATCH_SIZE = Math.max(CONCURRENT_LIMIT * 2, 45); // Smaller batches for better control with limited proxies
const RETRY_BACKOFF_MS = 1500; // Reduced base backoff time for faster retries
const MIN_TIME_BETWEEN_EVENT_SCRAPES = 120000; // Minimum 1 minute between scrapes (allowing for 2-minute updates)
const URGENT_THRESHOLD = 30000; // Events needing update within 30 seconds of deadline (tighter window)
const MAX_ALLOWED_UPDATE_INTERVAL = 180000; // Maximum 3 minutes allowed between updates

// New cooldown settings - shorter cooldowns
const SHORT_COOLDOWNS = [1500, 3000, 6000]; // Even shorter cooldowns: 1.5s, 3s, 6s for faster recovery
const LONG_COOLDOWN_MINUTES = 3; // Reduced to 3 minutes for persistently failing events

// Logging levels: 0 = errors only, 1 = warnings + errors, 2 = info + warnings + errors, 3 = all (verbose)
const LOG_LEVEL = 2; // Default to warnings and errors only

/**
 * ScraperManager class that maintains the original API while using the modular architecture internally
 */
class ScraperManager {
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
    
    // Initialize the ProxyManager
    if (!global.proxyManager) {
      global.proxyManager = new ProxyManager(this.logger);
      this.logWithTime("Global ProxyManager initialized");
    }
    this.proxyManager = global.proxyManager;
    
    // Add failure tracking by error type
    this.errorsByType = {
      '403': new Set(),
      '429': new Set(),
      'network': new Set(),
      'other': new Set()
    };
    
    // Add maximum retries counter
    this.MAX_RETRIES = 4;
    
    // Add proxy success tracking
    this.proxySuccessEvents = new Map();
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
        .select("url _id")
        .lean();

      // Fix for missing eventUrl - provide a fallback
      const eventUrl = event?.url || `unknown-url-for-event-${eventId}`;
      const eventObjectId = event?._id || null;

      // Log to console first in case DB logging fails
      console.error(`Error for event ${eventId} (${errorType}): ${error.message}`);
      
      // Only try to log to database if we have minimal required data
      if (eventObjectId) {
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
      } else {
        console.error(`Cannot log to ErrorLog - event ${eventId} not found in database`);
      }
    } catch (err) {
      console.error("Error logging to database:", err);
      console.error("Original error:", error);
    }
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

  async refreshEventHeaders(eventId, proxy = null) {
    const lastRefresh = this.headerRefreshTimestamps.get(eventId);
    // Only refresh headers if they haven't been refreshed in last 5 minutes
    if (!lastRefresh || moment().diff(lastRefresh) > 300000) {
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
        
        const capturedState = await refreshHeaders(eventId, proxy);
        
        if (capturedState) {
          // Ensure we have a standardized header object format
          let standardizedHeaders;
          
          if (capturedState.cookies && capturedState.cookies.length > 0) {
            // Create headers from cookies
            const cookieString = capturedState.cookies
              .map((cookie) => `${cookie.name}=${cookie.value}`)
              .join("; ");
              
            const userAgent = capturedState.fingerprint 
              ? BrowserFingerprint.generateUserAgent(capturedState.fingerprint)
              : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
              
            standardizedHeaders = {
              headers: {
                "User-Agent": userAgent,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": capturedState.fingerprint?.language || "en-US",
                "Cookie": cookieString,
                "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe"
              },
              // Also include the original cookies and fingerprint for backwards compatibility
              cookies: capturedState.cookies,
              fingerprint: capturedState.fingerprint
            };
          } else if (capturedState.headers) {
            // Use fallback headers directly if available
            standardizedHeaders = {
              headers: capturedState.headers,
              fingerprint: capturedState.fingerprint
            };
          } else {
            if (LOG_LEVEL >= 1) {
              this.logWithTime(`No valid headers found for ${eventId}`, "warning");
            }
            return null;
          }
          
          this.headersCache.set(eventId, standardizedHeaders);
          this.headerRefreshTimestamps.set(eventId, moment());
          
          // New: Add to rotation pool if not already there
          if (!this.headerRotationPool.some(h => 
            h.headers.Cookie === standardizedHeaders.headers.Cookie
          )) {
            this.headerRotationPool.push(standardizedHeaders);
            // Limit pool size
            if (this.headerRotationPool.length > 10) {
              this.headerRotationPool.shift();
            }
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
    if (this.lastCookieReset && now.diff(this.lastCookieReset) < 60 * 60 * 1000) {
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
        
        // Delete cookies.json
        await fs.unlink(this.cookiesPath);
        if (LOG_LEVEL >= 1) {
          this.logWithTime("Deleted cookies.json", "info");
        }
      } catch (e) {
        // File doesn't exist, that's fine
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

        const previousTicketCount = event.Available_Seats || 0;
        const currentTicketCount = scrapeResult.length;

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

        if (scrapeResult?.length > 0) {
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
            scrapeResult.flatMap((g) =>
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

            const groupsToInsert = scrapeResult.map((group) => ({
              eventId,
              section: group.section,
              row: group.row,
              seatCount: group.inventory.quantity,
              seatRange: `${Math.min(...group.seats)}-${Math.max(
                ...group.seats
              )}`,
              seats: group.seats.map((seatNumber) => ({
                number: seatNumber.toString(),
                inHandDate: event.inHandDate,
                price: group.inventory.listPrice,
              })),
              inventory: {
                quantity: group.inventory.quantity,
                section: group.section,
                hideSeatNumbers: group.inventory.hideSeatNumbers,
                row: group.row,
                cost: group.inventory.cost,
                stockType: group.inventory.stockType,
                lineType: group.inventory.lineType,
                seatType: group.inventory.seatType,
                inHandDate: event.inHandDate,
                notes: group.inventory.notes,
                tags: group.inventory.tags,
                inventoryId: group.inventory.inventoryId,
                offerId: group.inventory.offerId,
                splitType: group.inventory.splitType,
                publicNotes: group.inventory.publicNotes,
                listPrice: group.inventory.listPrice,
                customSplit: group.inventory.customSplit,
                tickets: group.inventory.tickets.map((ticket) => ({
                  id: ticket.id,
                  seatNumber: ticket.seatNumber,
                  notes: ticket.notes,
                  cost: ticket.cost,
                  faceValue: ticket.faceValue,
                  taxedCost: ticket.taxedCost,
                  sellPrice: ticket.sellPrice ** 1.25, // Using exponentiation as in old code
                  stockType: ticket.stockType,
                  eventId: ticket.eventId,
                  accountId: ticket.accountId,
                  status: ticket.status,
                  auditNote: ticket.auditNote,
                })),
              },
            }));

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

  async scrapeEvent(eventId, retryCount = 0, proxyAgent = null, proxy = null) {
    // Skip if the event should be skipped
    if (this.shouldSkipEvent(eventId)) {
      this.logWithTime(`Skipping event ${eventId} due to skip configuration`);
      return false;
    }
    
    // Skip if in cooldown
    if (this.cooldownEvents.has(eventId)) {
      const cooldownUntil = this.cooldownEvents.get(eventId);
      if (moment().isBefore(cooldownUntil)) {
        const remainingSeconds = cooldownUntil.diff(moment(), "seconds");
        this.logWithTime(
          `Event ${eventId} is in cooldown for ${remainingSeconds} more seconds`,
          "info"
        );
        return false;
      }
      this.cooldownEvents.delete(eventId);
    }
    
    // Log the start of scraping
    this.logWithTime(`Starting to scrape event ${eventId} (retry #${retryCount})`);
    
    try {
      // If no proxy is provided, get one specifically for this event
      if (!proxyAgent || !proxy) {
        try {
          // Use the proxy manager to get an event-specific proxy
          const proxyData = this.proxyManager.getProxyForEvent(eventId);
          
          if (!proxyData) {
            throw new Error("No healthy proxy available for this event");
          }
          
          const proxyAgentData = this.proxyManager.createProxyAgent(proxyData);
          proxyAgent = proxyAgentData.proxyAgent;
          proxy = proxyData;
          
          // Track that this proxy is being used by this event
          this.proxyManager.assignProxyToEvent(eventId, proxyData.proxy);
          
          this.logWithTime(`Using dedicated proxy ${proxy.proxy} for event ${eventId}`);
        } catch (proxyError) {
          this.logWithTime(
            `Error getting proxy for event ${eventId}: ${proxyError.message}`,
            "error"
          );
          
          // Fall back to a random proxy from the manager
          try {
            const randomProxy = this.proxyManager.proxies[
              Math.floor(Math.random() * this.proxyManager.proxies.length)
            ];
            const proxyAgentData = this.proxyManager.createProxyAgent(randomProxy);
            proxyAgent = proxyAgentData.proxyAgent;
            proxy = randomProxy;
            
            this.logWithTime(
              `Using fallback proxy ${proxy.proxy} for event ${eventId}`
            );
          } catch (fallbackError) {
            throw new Error(
              `Failed to get any proxy for event ${eventId}: ${fallbackError.message}`
            );
          }
        }
      }
      
      // Get headers - first try from cache, then try to get fresh ones
      let headers = this.headersCache.get(eventId);
      
      // If this is a retry with count >= 2, always get fresh headers
      if (retryCount >= 2 || !headers) {
        try {
          headers = await this.refreshEventHeaders(eventId, proxy);
        } catch (headerError) {
          this.logWithTime(
            `Error refreshing headers for event ${eventId}: ${headerError.message}`,
            "error"
          );
          
          // Try to use a header from the rotation pool
          if (this.headerRotationPool.length > 0) {
            const randomIndex = Math.floor(
              Math.random() * this.headerRotationPool.length
            );
            headers = this.headerRotationPool[randomIndex];
            
            this.logWithTime(
              `Using fallback headers from rotation pool for event ${eventId}`
            );
          } else {
            // Last resort: generate basic headers
            headers = this.generateBasicHeaders();
            
            this.logWithTime(
              `Using basic generated headers for event ${eventId} as last resort`
            );
          }
        }
      }
      
      if (!headers) {
        throw new Error("Failed to obtain valid headers");
      }
      
      // Set a longer timeout for the scrape
      const result = await Promise.race([
        ScrapeEvent({ eventId, headers }, proxyAgent, proxy),
        setTimeout(SCRAPE_TIMEOUT).then(() => {
          throw new Error("Scrape timed out");
        }),
      ]);
      
      if (!result || !Array.isArray(result) || result.length === 0) {
        throw new Error("Empty or invalid scrape result");
      }
      
      // Success! Record proxy success
      if (this.proxyManager && proxy && proxy.proxy) {
        this.proxyManager.releaseProxy(eventId, true);
        this.proxySuccessEvents.set(proxy.proxy, (this.proxySuccessEvents.get(proxy.proxy) || 0) + 1);
      }
      
      // We're still storing in the cache for metrics/monitoring purposes
      // but we'll never use the cached results for returning to the client
      this.responseCache.set(eventId, result);
      
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
      
      // Success! If this event was previously failing, update its status in DB
      const recentFailures = this.getRecentFailureCount(eventId);
      
      if (recentFailures > 0) {
        this.logWithTime(
          `Event ${eventId} succeeded after ${recentFailures} failures`
        );
        this.resetFailureCount(eventId);
      }
      
      this.failedEvents.delete(eventId);
      
      this.logWithTime(
        `Successfully scraped event ${eventId} (${result.length} tickets)`
      );
      
      return result;
    } catch (error) {
      // Check if this is a retryable temporary error before marking as failed
      const isTemporaryError = this.isTemporaryError(error);
      
      // Record error by type
      const is403 = error.message && (error.message.includes("403") || (error.response && error.response.status === 403));
      const is429 = error.message && (error.message.includes("429") || (error.response && error.response.status === 429));
      const isNetwork = error.message && (
        error.message.includes("ECONNRESET") || 
        error.message.includes("ETIMEDOUT") ||
        error.message.includes("timeout") ||
        error.message.includes("network")
      );
      
      let errorType = 'other';
      if (is403) errorType = '403';
      else if (is429) errorType = '429';
      else if (isNetwork) errorType = 'network';
      
      // Track the error type
      this.errorsByType[errorType].add(eventId);
      
      // Only add to failedEvents if this is NOT a temporary error
      if (!isTemporaryError) {
        this.failedEvents.add(eventId);
      }
      
      // Release proxy with failure
      if (this.proxyManager && proxy && proxy.proxy) {
        this.proxyManager.releaseProxy(eventId, false, error);
      }
      
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
      
      await this.logError(eventId, "SCRAPE_ERROR", error, { retryCount, errorType });
      
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
      
      if (is429Error) {
        // Rate limit hit - use a longer cooldown
        backoffTime = Math.max(10, 5 * Math.pow(2, recentFailures)) * 60 * 1000; // 5-10 minutes minimum, then exponential backoff
        this.logWithTime(
          `Rate limit hit for event ${eventId}. Cooling down for ${Math.round(backoffTime/60000)} minutes`,
          "error"
        );
      } else if (is403Error) {
        // 403 error - likely IP blocked, need to wait longer
        backoffTime = Math.max(5, 3 * Math.pow(2, recentFailures)) * 60 * 1000; // 3-5 minutes minimum, then exponential backoff
        this.logWithTime(
          `403 Forbidden for event ${eventId}. Cooling down for ${Math.round(backoffTime/60000)} minutes`,
          "error"
        );
      } else if (isNetwork) {
        // Network error - shorter cooldown
        backoffTime = Math.pow(2, recentFailures) * 10 * 1000; // Start at 10s, then exponential backoff
        this.logWithTime(
          `Network error for event ${eventId}. Retrying in ${Math.round(backoffTime/1000)} seconds`,
          "warning"
        );
      } else {
        // Other errors - use moderate cooldown
        backoffTime = Math.pow(2, recentFailures) * 30 * 1000; // Start at 30s, then exponential backoff
        this.logWithTime(
          `Error scraping event ${eventId}: ${error.message}. Retrying in ${Math.round(backoffTime/1000)} seconds`,
          "error"
        );
      }
      
      // If we've reached MAX_RETRIES, mark event as stopped
      if (recentFailures >= this.MAX_RETRIES) {
        shouldMarkStopped = true;
        this.logWithTime(
          `Event ${eventId} has failed ${recentFailures} times. Maximum retries exceeded.`,
          "error"
        );
      }
      
      // Set cooldown and add to retry queue if not stopped
      if (!shouldMarkStopped) {
        const cooldownUntil = moment().add(backoffTime, "milliseconds");
        this.cooldownEvents.set(eventId, cooldownUntil);
        
        this.retryQueue.push({
          eventId,
          retryCount: recentFailures,
          retryAfter: cooldownUntil,
        });
        
        this.logWithTime(
          `Scheduled event ${eventId} for retry at ${cooldownUntil.format("HH:mm:ss")} (${Math.round(backoffTime/1000)}s cooldown)`
        );
      }
      
      return false;
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
    // Determine optimal batch size based on available proxies
    // We want to use smaller batches when we have fewer proxies to avoid overloading
    const maxEventsPerProxy = this.proxyManager.MAX_EVENTS_PER_PROXY;
    // Dynamic chunk sizing based on available proxies
    const chunkSize = Math.min(
      maxEventsPerProxy, 
      Math.max(3, Math.min(Math.ceil(eventIds.length / Math.max(1, availableProxies)), 10))
    );
    
    if (LOG_LEVEL >= 1) {
      this.logWithTime(
        `Processing ${eventIds.length} events using ${availableProxies} proxies with chunk size ${chunkSize}`,
        "info"
      );
    }
    
    // Split into smaller groups for better control
    const chunks = [];
    
    for (let i = 0; i < eventIds.length; i += chunkSize) {
      chunks.push(eventIds.slice(i, i + chunkSize));
    }
    
    // Process chunks with controlled parallelism and better error handling
    let successCount = 0;
    let failureCount = 0;
    
    // Process in smaller batches for better control
    const batchCount = Math.ceil(chunks.length / 4); // Process up to 4 chunks in parallel
    
    for (let batchIndex = 0; batchIndex < batchCount; batchIndex++) {
      const batchStart = batchIndex * 4;
      const batchEnd = Math.min(batchStart + 4, chunks.length);
      const batchChunks = chunks.slice(batchStart, batchEnd);
      
      // Generate unique batch IDs for each chunk
      const batchProcesses = batchChunks.map(async (chunk) => {
        const batchId = Date.now().toString(36) + Math.random().toString(36).substr(2, 5);
        
        try {
          // Get a proxy for this specific batch
          const { proxyAgent, proxy } = this.proxyManager.getProxyForBatch(chunk);
          
          // Store the proxy assignment for this batch
          this.batchProxies.set(batchId, { proxy, chunk });
          
          if (LOG_LEVEL >= 3) {
            this.logWithTime(`Using proxy ${proxy.proxy} for batch ${batchId} (${chunk.length} events)`, "debug");
          }

          // Process the entire chunk efficiently as a batch
          const result = await this.processBatchEfficiently(chunk, batchId, proxyAgent, proxy);
          successCount += result.results.length;
          failureCount += result.failed.length;
          results.push(...result.results);
          failed.push(...result.failed);
          
          // Update proxy success 
          if (this.proxyManager && proxy && proxy.proxy) {
            try {
              this.proxyManager.recordProxySuccess(proxy.proxy);
            } catch (error) {
              console.error(`Error updating proxy success: ${error.message}`);
            }
          }
          
          // If we had failures, and they outnumber successes, mark proxy as having issues
          if (result.failed.length > result.results.length && result.failed.length > 0) {
            this.proxyManager.recordProxyFailure(proxy.proxy, { message: "Batch failures exceeded successes" });
          }
        } catch (error) {
          this.logWithTime(`Error processing batch ${batchId}: ${error.message}`, "error");
          
          // If there's a proxy error, mark the proxy as unhealthy
          if (this.batchProxies.has(batchId)) {
            const { proxy, chunk } = this.batchProxies.get(batchId);
            this.proxyManager.updateProxyHealth(proxy.proxy, false);
            this.proxyManager.releaseProxyBatch(chunk);
          }
          
          // Add these events to failed list
          failed.push(...chunk.map(eventId => ({ 
            eventId, 
            error: new Error(`Batch processing error: ${error.message}`) 
          })));
          failureCount += chunk.length;
          
          // Update proxy failure
          if (this.proxyManager && proxy && proxy.proxy) {
            try {
              this.proxyManager.recordProxyFailure(proxy.proxy, error);
            } catch (proxyError) {
              console.error(`Error updating proxy failure: ${proxyError.message}`);
            }
          }
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
  async processBatchEfficiently(eventIds, batchId, proxyAgent, proxy) {
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

        return { eventId, valid: true, event };
      } catch (error) {
        return { eventId, valid: false, reason: "error", error };
      }
    });

    // Wait for all validations in parallel
    const validationResults = await Promise.all(validationPromises);
    const validEvents = validationResults
      .filter(result => result.valid)
      .map(result => ({ eventId: result.eventId, event: result.event }));

    // Log invalid events (only log summary for large batches)
    const invalidEvents = validationResults.filter(result => !result.valid);
    if (invalidEvents.length > 0 && shouldLogDetails) {
      // For large batches, just log a summary
      const skipCount = invalidEvents.filter(r => r.reason === "skip").length;
      const notFoundCount = invalidEvents.filter(r => r.reason === "not_found").length;
      const flaggedCount = invalidEvents.filter(r => r.reason === "flagged").length;
      const errorCount = invalidEvents.filter(r => r.reason === "error").length;
      
      this.logWithTime(`Batch ${batchId} invalid events: ${skipCount} skipped, ${notFoundCount} not found, ${flaggedCount} flagged, ${errorCount} errors`, "warning");
      
      // Log specific errors for debugging
      const errorEvents = invalidEvents.filter(r => r.reason === "error");
      if (errorEvents.length > 0 && LOG_LEVEL >= 2) {
        errorEvents.forEach(result => {
          this.logWithTime(`Error validating event ${result.eventId}: ${result.error.message}`, "error");
        });
      }
    }

    if (validEvents.length === 0) {
      if (LOG_LEVEL >= 1) {
        this.logWithTime(`No valid events to process in batch ${batchId}`, "warning");
      }
      return { results, failed };
    }

    // Set all valid events as processing
    validEvents.forEach(({ eventId }) => {
      this.processingEvents.add(eventId);
      this.activeJobs.set(eventId, moment());
    });

    // Helper function to validate headers are complete
    const validateHeaders = (headers) => {
      if (!headers) return false;
      
      // Check for required fields
      const hasHeaders = headers.headers && typeof headers.headers === 'object';
      if (!hasHeaders) return false;
      
      // Check for required header fields
      const hasCookie = headers.headers.Cookie || headers.headers.cookie;
      const hasUserAgent = headers.headers["User-Agent"] || headers.headers["user-agent"];
      
      return hasCookie && hasUserAgent;
    };
    
    // Implement header caching strategy for better efficiency
    let headerAttempts = 0;
    const maxHeaderAttempts = Math.min(validEvents.length, 3);
    
    try {
      await this.acquireSemaphore();
      
      // First check if we have recent valid headers for any event in the batch
      for (const { eventId } of validEvents) {
        const cachedHeaders = this.headersCache.get(eventId);
        const lastRefresh = this.headerRefreshTimestamps.get(eventId);
        
        if (cachedHeaders && lastRefresh && moment().diff(lastRefresh) < 300000) { // 5 minutes
          // Validate the headers are complete before using them
          if (validateHeaders(cachedHeaders)) {
            sharedHeaders = cachedHeaders;
            if (LOG_LEVEL >= 3) {
              this.logWithTime(`Using recent cached headers from event ${eventId} for batch ${batchId}`, "debug");
            }
            break;
          }
        }
      }
      
      // If no valid cached headers, try to refresh headers until we get valid ones
      while (!sharedHeaders && headerAttempts < maxHeaderAttempts) {
        headerAttempts++;
        const eventToTry = validEvents[headerAttempts - 1].eventId;
        
        try {
          // Force refresh headers for this attempt
          const headers = await this.refreshEventHeaders(eventToTry);
          
          // Validate the headers
          if (validateHeaders(headers)) {
            sharedHeaders = headers;
            break;
          }
        } catch (headerError) {
          // Continue to next attempt
        }
        
        // Short delay before next attempt
        if (!sharedHeaders && headerAttempts < maxHeaderAttempts) {
          await setTimeout(500); // Reduced delay for higher throughput
        }
      }
      
      // If we still don't have valid headers after all attempts, try to create fallback headers
      if (!sharedHeaders) {
        // Create fallback headers with minimal required fields
        const userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
        const fallbackHeaders = {
          headers: {
            "User-Agent": userAgent,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "x-tm-api-key": "b462oi7fic6pehcdkzony5bxhe"
          },
          fingerprint: {
            language: "en-US",
            timezone: "America/New_York",
            screen: { width: 1920, height: 1080 }
          }
        };
        
        sharedHeaders = fallbackHeaders;
      }
      
      // Cache headers for all events in this batch
      for (const { eventId } of validEvents) {
        this.headersCache.set(eventId, sharedHeaders);
        this.headerRefreshTimestamps.set(eventId, moment());
      }
    } catch (error) {
      if (LOG_LEVEL >= 1) {
        this.logWithTime(`Failed to refresh headers for batch ${batchId}: ${error.message}`, "error");
      }
      
      // Release all events in batch
      validEvents.forEach(({ eventId }) => {
        this.activeJobs.delete(eventId);
        this.processingEvents.delete(eventId);
        failed.push({ eventId, error });
      });
      
      this.releaseSemaphore();
      return { results, failed };
    }

    // Process all valid events with increased parallelism for high-memory system
    // Use smaller chunk size for better throughput and error isolation
    const chunkSize = Math.min(validEvents.length, 5);
    const eventChunks = [];
    
    for (let i = 0; i < validEvents.length; i += chunkSize) {
      eventChunks.push(validEvents.slice(i, i + chunkSize));
    }
    
    // Process chunks one by one for better error handling
    for (const chunk of eventChunks) {
      try {
        const chunkPromises = chunk.map(async ({ eventId }) => {
          try {
            const result = await Promise.race([
              ScrapeEvent({ eventId, headers: sharedHeaders }),
              setTimeout(SCRAPE_TIMEOUT).then(() => {
                throw new Error("Scrape timed out");
              }),
            ]);

            if (!result || !Array.isArray(result) || result.length === 0) {
              throw new Error("Empty or invalid scrape result");
            }

            // We're still storing in the cache for metrics/monitoring purposes
            // but we'll never use the cached results for returning to the client
            this.responseCache.set(eventId, result);
            
            // Mark this header as successful
            if (sharedHeaders) {
              const headerKey = sharedHeaders.headers.Cookie?.substring(0, 20) || sharedHeaders.headers["User-Agent"]?.substring(0, 20);
              const currentSuccessRate = this.headerSuccessRates.get(headerKey) || { success: 0, failure: 0 };
              currentSuccessRate.success++;
              this.headerSuccessRates.set(headerKey, currentSuccessRate);
              
              // Add to rotation pool if not already there
              if (!this.headerRotationPool.some(h => 
                h.headers.Cookie?.substring(0, 20) === headerKey ||
                h.headers["User-Agent"]?.substring(0, 20) === headerKey
              )) {
                this.headerRotationPool.push(sharedHeaders);
                // Limit pool size
                if (this.headerRotationPool.length > 10) {
                  this.headerRotationPool.shift();
                }
              }
            }

            await this.updateEventMetadata(eventId, result);
            
            // Success handling
            const recentFailures = this.getRecentFailureCount(eventId);
            if (recentFailures > 0) {
              try {
                await Event.updateOne(
                  { Event_ID: eventId },
                  { $set: { Skip_Scraping: false, status: "active" } }
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
            this.globalConsecutiveErrors = 0;
            
            results.push({ eventId, success: true });
            return { eventId, success: true };
          } catch (error) {
            // Check if this is a temporary error that should be retried without marking as failed
            const isTemporaryError = this.isTemporaryError(error);
            
            await this.handleEventError(eventId, error, 0, isTemporaryError ? [] : failed);
            return { eventId, success: false, error, isTemporaryError };
          } finally {
            this.activeJobs.delete(eventId);
            this.processingEvents.delete(eventId);
          }
        });

        await Promise.all(chunkPromises);
        
        // Short delay between processing chunks to reduce rate limiting
        if (eventChunks.length > 1) {
          await setTimeout(100);
        }
      } catch (error) {
        if (LOG_LEVEL >= 1) {
          this.logWithTime(`Error processing chunk in batch ${batchId}: ${error.message}`, "error");
        }
      }
    }
    
    this.releaseSemaphore();
    
    return { results, failed };
  }

  // Helper method to handle event errors consistently
  async handleEventError(eventId, error, retryCount, failedList) {
    // Check if this is a retryable temporary error before marking as failed
    const isTemporaryError = this.isTemporaryError(error);
      
    // Only add to failedEvents if this is NOT a temporary error
    if (!isTemporaryError) {
      this.failedEvents.add(eventId);
    }
    
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
      // For persistent failures, use a longer cooldown and mark as stopped
      backoffTime = LONG_COOLDOWN_MINUTES * 60 * 1000; // Convert minutes to ms
      shouldMarkStopped = true;
      
      this.logWithTime(
        `Persistent ${isApiError ? "API errors" : "errors"} for ${eventId}: ${error.message}. Marking as stopped with ${LONG_COOLDOWN_MINUTES} minute cooldown`,
        "error"
      );
      
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

    if (retryCount < MAX_RETRIES) {
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
      this.logWithTime(`Max retries exceeded for ${eventId}`, "error");
    }
    
    // Only add to failed list if it's not a temporary error and we were passed a failedList
    if (failedList && !isTemporaryError) {
      failedList.push({ eventId, error });
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
      
      // Determine optimal batch size based on system resources
      const batchSize = Math.min(eventIds.length, Math.max(10, Math.ceil(CONCURRENT_LIMIT / 3)));
      
      // Split into smaller batches if needed
      const batches = [];
      for (let i = 0; i < eventIds.length; i += batchSize) {
        batches.push(eventIds.slice(i, i + batchSize));
      }
      
      // Process all batches of this retry count in parallel
      await Promise.all(batches.map(async (batch) => {
        try {
          // Get a proxy for this batch
          const { proxyAgent, proxy } = this.proxyManager.getProxyForBatch(batch);
          
          // Process batch with shared proxy
          const batchPromises = batch.map(eventId => 
            this.scrapeEvent(eventId, parseInt(retryCount), proxyAgent, proxy)
          );
          
          await Promise.all(batchPromises);
        } catch (error) {
          this.logWithTime(`Error processing retry batch: ${error.message}`, "error");
        }
      }));
      
      // Short delay between different retry count groups
      await setTimeout(1000);
    }
  }

  async getEventsToProcess() {
    try {
      const now = moment();
      const twoMinutesFromNow = now.clone().add(2, 'minutes').toDate();
      
      // Prioritize events needing update soon
      // Find all active events due for update
      const urgentEvents = await Event.find({
        Skip_Scraping: { $ne: true },
        $or: [
          { Last_Updated: { $exists: false } },
          { Last_Updated: { $lt: now.clone().subtract(1, 'minute').toDate() } }
        ]
      })
      .select('Event_ID url Last_Updated')
      .sort({ Last_Updated: 1 }) // Oldest first
      .limit(BATCH_SIZE * 10) // Get a much larger sample to select from for 1000+ events
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
        Math.max(BATCH_SIZE, Math.ceil(optimizedEventList.length / 10)) // Process at least 10% of events per cycle
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
    if (this.isRunning) return;
    this.isRunning = true;
    this.logWithTime("Starting continuous scraping...");
    
    while (this.isRunning) {
      try {
        // First, check for any events approaching the 3-minute maximum deadline
        const now = moment();
        const criticalEvents = [];
        
        for (const [eventId, lastUpdateTime] of this.eventUpdateTimestamps.entries()) {
          const timeSinceUpdate = now.diff(lastUpdateTime);
          
          // If event is approaching 3-minute maximum, add to critical list
          if (timeSinceUpdate > MAX_ALLOWED_UPDATE_INTERVAL - 20000 && !this.processingEvents.has(eventId)) {
            criticalEvents.push(eventId);
            
            // Remove from failed list if present
            this.failedEvents.delete(eventId);
          }
        }
        
        // Process critical events immediately if any exist
        if (criticalEvents.length > 0) {
          if (LOG_LEVEL >= 1) {
            this.logWithTime(`Processing ${criticalEvents.length} critical events approaching 3-minute maximum`, "warning");
          }
          
          // Process critical events with higher concurrency
          await this.processBatch(criticalEvents);
        }
        
        // Process retry queue next (process more at once for high volume)
        await this.processRetryQueue();
        
        // Clean up failed events before processing failed batches
        this.cleanupFailedEvents();
        
        // New: Process batches of failed events by error type
        await this.processFailedEventsBatch();
        
        // Get events to process
        const eventsToProcess = await this.getEventsToProcess();
        
        if (eventsToProcess.length === 0) {
          // No events to process, wait for a short time
          await setTimeout(500); // Shorter wait time for high-volume systems
          continue;
        }
        
        // Optimize batch size dynamically based on system load and event count
        const totalEvents = eventsToProcess.length;
        const optimalBatchSize = Math.min(
          totalEvents,
          Math.max(20, Math.ceil(CONCURRENT_LIMIT / 2))
        );
        
        // For 1000+ events, use more aggressive batching
        const adjustedBatchSize = totalEvents > 500 ? 
          Math.min(totalEvents, Math.max(40, CONCURRENT_LIMIT)) : 
          optimalBatchSize;
        
        // Break events into right-sized batches for optimal processing
        const batches = [];
        for (let i = 0; i < totalEvents; i += adjustedBatchSize) {
          batches.push(eventsToProcess.slice(i, i + adjustedBatchSize));
        }
        
        // Process batches in parallel with maximum concurrency for 32GB system
        // Use Promise.all directly for maximum throughput
        await Promise.all(batches.map(async (batch, index) => {
          const batchId = `batch-${Date.now()}-${index}`;
          try {
            await this.processBatch(batch);
          } catch (error) {
            this.logWithTime(
              `Error processing batch ${batchId}: ${error.message}`,
              "error"
            );
          }
        }));
        
        // Minimal pause to prevent CPU overuse while maintaining high throughput
        const pauseTime = Math.max(50, this.getAdaptivePauseTime() / 2); // Reduced pause for 32GB systems
        if (pauseTime > 0) {
          await setTimeout(pauseTime);
        }
      } catch (error) {
        this.logWithTime(`Error in continuous scraping: ${error.message}`, "error");
        console.error(error);
        // Brief pause on error to avoid tight loop
        await setTimeout(2000); // Shorter pause for faster recovery
      }
    }
  }

  // Dynamic pause time calculation based on system load - optimized for 32GB
  getAdaptivePauseTime() {
    const activeJobPercentage = (this.activeJobs.size / CONCURRENT_LIMIT) * 100;
    
    if (activeJobPercentage > 90) {
      // System is extremely loaded, add pause
      return 1000;
    } else if (activeJobPercentage > 70) {
      // Heavy load
      return 500;
    } else if (activeJobPercentage > 50) {
      // Medium load
      return 200;
    } else {
      // Light to moderate load, minimal pause
      return 50;
    }
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
        
        for (const [eventId, lastUpdateTime] of this.eventUpdateTimestamps.entries()) {
          // Check if event has exceeded the 2-minute target
          const timeSinceUpdate = now.diff(lastUpdateTime);
          
          // First check for events exceeding maximum allowed time (3 minutes)
          if (timeSinceUpdate > MAX_ALLOWED_UPDATE_INTERVAL && !this.processingEvents.has(eventId)) {
            criticalMissedDeadlines++;
            
            // Force immediate processing of events exceeding max time
            this.priorityQueue.add(eventId);
            
            // Remove from failed list if it's there
            this.failedEvents.delete(eventId);
            
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
        
        if (criticalMissedDeadlines > 0) {
          this.logWithTime(`CRITICAL WARNING: ${criticalMissedDeadlines} events exceeded the 3-minute maximum update deadline!`, "error");
        }
        
        if (missedDeadlines > 0) {
          this.logWithTime(`WARNING: ${missedDeadlines} events missed their 2-minute update target`, "warning");
        }
        
        // Clean up the failed events list
        this.cleanupFailedEvents();
        
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
    
    // Log the start of batch processing
    this.logWithTime(
      `Processing failed events batch. ${this.failedEvents.size} failed events, ${this.retryQueue.length} in retry queue`
    );
    
    // Group events by failure count for progressive retry strategy
    const failureGroups = {
      low: [], // 1 failure
      medium: [], // 2-3 failures
      high: [] // 4+ failures
    };
    
    // Process retry queue first
    const readyRetries = this.retryQueue.filter(
      (item) => now.isAfter(item.retryAfter)
    );
    
    for (const retryItem of readyRetries) {
      // Skip events that are in cooldown
      if (this.cooldownEvents.has(retryItem.eventId)) {
        const cooldownUntil = this.cooldownEvents.get(retryItem.eventId);
        if (now.isBefore(cooldownUntil)) {
          continue;
        }
        this.cooldownEvents.delete(retryItem.eventId);
      }
      
      const failures = this.getRecentFailureCount(retryItem.eventId);
      
      if (failures <= 1) {
        failureGroups.low.push(retryItem.eventId);
      } else if (failures <= 3) {
        failureGroups.medium.push(retryItem.eventId);
      } else if (failures < this.MAX_RETRIES) {
        failureGroups.high.push(retryItem.eventId);
      } else {
        // Log and mark permanently failed if we've exceeded maximum retries
        const errorType = this.getEventErrorType(retryItem.eventId);
        this.logWithTime(
          `Event ${retryItem.eventId} has failed ${failures} times (${errorType}). Marking as permanently failed.`,
          "error"
        );
        
        // Remove from retry queue
        this.retryQueue = this.retryQueue.filter(
          (item) => item.eventId !== retryItem.eventId
        );
        
        // Add to permanently failed tracking by error type
        if (errorType) {
          this.errorsByType[errorType].add(retryItem.eventId);
        }
        
        continue;
      }
      
      // Remove from retry queue since we're processing it now
      this.retryQueue = this.retryQueue.filter(
        (item) => item.eventId !== retryItem.eventId
      );
    }
    
    // Add other failed events not in retry queue
    for (const eventId of this.failedEvents) {
      // Skip events in cooldown
      if (this.cooldownEvents.has(eventId)) {
        const cooldownUntil = this.cooldownEvents.get(eventId);
        if (now.isBefore(cooldownUntil)) {
          continue;
        }
        this.cooldownEvents.delete(eventId);
      }
      
      // Skip events already in retry groups
      if (
        failureGroups.low.includes(eventId) ||
        failureGroups.medium.includes(eventId) ||
        failureGroups.high.includes(eventId)
      ) {
        continue;
      }
      
      const failures = this.getRecentFailureCount(eventId);
      
      if (failures <= 1) {
        failureGroups.low.push(eventId);
      } else if (failures <= 3) {
        failureGroups.medium.push(eventId);
      } else if (failures < this.MAX_RETRIES) {
        failureGroups.high.push(eventId);
      } else {
        // Log and skip if we've exceeded maximum retries
        this.logWithTime(
          `Event ${eventId} has failed ${failures} times. Skipping.`,
          "warning"
        );
        continue;
      }
    }
    
    // Log failure group counts
    this.logWithTime(
      `Failure groups: low=${failureGroups.low.length}, medium=${failureGroups.medium.length}, high=${failureGroups.high.length}`
    );
    
    // Process groups in order of increasing retry count (try easy ones first)
    const groups = [
      { name: "low", events: failureGroups.low, batchSize: 5 },
      { name: "medium", events: failureGroups.medium, batchSize: 3 },
      { name: "high", events: failureGroups.high, batchSize: 1 } // Process high-failure events one at a time
    ];
    
    for (const group of groups) {
      // Skip empty groups
      if (group.events.length === 0) {
        continue;
      }
      
      this.logWithTime(
        `Processing ${group.name} failure group with ${group.events.length} events`
      );
      
      // Get available proxy count to determine parallelism
      const availableProxies = this.proxyManager.getAvailableProxyCount();
      const parallelLimit = Math.min(
        availableProxies, 
        10, // Don't use more than 10 concurrent proxies
        Math.ceil(group.events.length / 2) // Don't use more proxies than half the event count
      );
      
      // If we're out of proxies, wait and try again later
      if (availableProxies < 1) {
        this.logWithTime(
          "No healthy proxies available, deferring batch processing",
          "warning"
        );
        
        // Put all events back in retry queue with exponential backoff
        const backoffMinutes = Math.min(30, Math.pow(2, group.name === "high" ? 4 : (group.name === "medium" ? 2 : 1)));
        
        for (const eventId of group.events) {
          const retryCount = this.getRecentFailureCount(eventId);
          this.retryQueue.push({
            eventId,
            retryCount,
            retryAfter: moment().add(backoffMinutes, "minutes"),
          });
          
          this.logWithTime(
            `Scheduled event ${eventId} (${group.name} failure group) for retry in ${backoffMinutes} minutes`,
            "info"
          );
        }
        
        continue; // Skip to next group
      }
      
      // Split into smaller batches based on available proxies
      const batches = [];
      for (let i = 0; i < group.events.length; i += parallelLimit) {
        batches.push(group.events.slice(i, i + parallelLimit));
      }
      
      this.logWithTime(
        `Created ${batches.length} batches (${parallelLimit} events per batch) for ${group.name} failure group`
      );
      
      // Process batches sequentially to avoid overwhelming the system
      for (const batch of batches) {
        try {
          // For high failure group, process one event at a time with longer delays
          if (group.name === "high") {
            for (const eventId of batch) {
              const failures = this.getRecentFailureCount(eventId);
              
              this.logWithTime(
                `Processing high-failure event ${eventId} with ${failures} previous failures`
              );
              
              try {
                // Get a dedicated proxy for this event
                const proxyData = this.proxyManager.getProxyForEvent(eventId);
                
                if (!proxyData) {
                  throw new Error("No healthy proxy available for high-failure event");
                }
                
                // Create proxy agent
                const { proxyAgent, proxy } = this.proxyManager.createProxyAgent(proxyData);
                
                // Track that this proxy is being used by this event
                this.proxyManager.assignProxyToEvent(eventId, proxyData.proxy);
                
                // Process the event with a longer timeout for high-failure events
                const result = await Promise.race([
                  ScrapeEvent({ eventId }, proxyAgent, proxy),
                  setTimeout(SCRAPE_TIMEOUT * 2).then(() => {
                    throw new Error("Scrape timed out (extended timeout)");
                  }),
                ]);
                
                if (!result || !Array.isArray(result) || result.length === 0) {
                  throw new Error("Empty or invalid scrape result");
                }
                
                // Success! Record proxy success
                this.proxyManager.releaseProxy(eventId, true);
                this.proxySuccessEvents.set(proxyData.proxy, (this.proxySuccessEvents.get(proxyData.proxy) || 0) + 1);
                
                // Update event status
                await this.updateEventMetadata(eventId, result);
                this.failedEvents.delete(eventId);
                
                this.logWithTime(`Successfully processed high-failure event ${eventId}`);
                
                // Add delay between high-failure events to avoid triggering rate limits
                await setTimeout(3000 + Math.random() * 2000);
              } catch (error) {
                // Record error by type
                const is403 = error.message && (error.message.includes("403") || (error.response && error.response.status === 403));
                const is429 = error.message && (error.message.includes("429") || (error.response && error.response.status === 429));
                const isNetwork = error.message && (
                  error.message.includes("ECONNRESET") || 
                  error.message.includes("ETIMEDOUT") ||
                  error.message.includes("timeout") ||
                  error.message.includes("network")
                );
                
                let errorType = 'other';
                if (is403) errorType = '403';
                else if (is429) errorType = '429';
                else if (isNetwork) errorType = 'network';
                
                // Release proxy with failure
                if (proxyData) {
                  this.proxyManager.releaseProxy(eventId, false, error);
                }
                
                // Increment failure count
                this.incrementFailureCount(eventId);
                
                // Apply longer cooldown for high-failure events
                const backoff = Math.pow(2, failures) * 60000; // Minutes in ms
                const cooldownUntil = moment().add(backoff, "milliseconds");
                this.cooldownEvents.set(eventId, cooldownUntil);
                
                // Add to retry queue
                this.retryQueue.push({
                  eventId,
                  retryCount: failures + 1,
                  retryAfter: cooldownUntil,
                });
                
                this.logWithTime(
                  `Error processing high-failure event ${eventId} (${errorType}): ${error.message}. Scheduled for retry in ${Math.round(backoff/60000)} minutes.`,
                  "error"
                );
                
                // Log error with detailed info
                await this.logError(eventId, "SCRAPE_ERROR", error, { retryCount: failures, errorType });
                
                // Add delay between high-failure events
                await setTimeout(5000);
              }
            }
          } else {
            // For low and medium failure groups, process events individually but in parallel
            const eventPromises = batch.map(async (eventId) => {
              const failures = this.getRecentFailureCount(eventId);
              
              try {
                // Get a dedicated proxy for this event
                const proxyData = this.proxyManager.getProxyForEvent(eventId);
                
                if (!proxyData) {
                  throw new Error(`No healthy proxy available for event ${eventId}`);
                }
                
                // Create proxy agent
                const { proxyAgent, proxy } = this.proxyManager.createProxyAgent(proxyData);
                
                // Track that this proxy is being used by this event
                this.proxyManager.assignProxyToEvent(eventId, proxyData.proxy);
                
                // Process the event
                const result = await Promise.race([
                  ScrapeEvent({ eventId }, proxyAgent, proxy),
                  setTimeout(SCRAPE_TIMEOUT).then(() => {
                    throw new Error("Scrape timed out");
                  }),
                ]);
                
                if (!result || !Array.isArray(result) || result.length === 0) {
                  throw new Error("Empty or invalid scrape result");
                }
                
                // Success! Record proxy success
                this.proxyManager.releaseProxy(eventId, true);
                this.proxySuccessEvents.set(proxyData.proxy, (this.proxySuccessEvents.get(proxyData.proxy) || 0) + 1);
                
                // Update event status
                await this.updateEventMetadata(eventId, result);
                this.failedEvents.delete(eventId);
                
                this.logWithTime(`Successfully processed event ${eventId} (${group.name} failure group)`);
                
                return { eventId, success: true };
              } catch (error) {
                // Record error by type
                const is403 = error.message && (error.message.includes("403") || (error.response && error.response.status === 403));
                const is429 = error.message && (error.message.includes("429") || (error.response && error.response.status === 429));
                const isNetwork = error.message && (
                  error.message.includes("ECONNRESET") || 
                  error.message.includes("ETIMEDOUT") ||
                  error.message.includes("timeout") ||
                  error.message.includes("network")
                );
                
                let errorType = 'other';
                if (is403) errorType = '403';
                else if (is429) errorType = '429';
                else if (isNetwork) errorType = 'network';
                
                // Release proxy with failure
                if (proxyData) {
                  this.proxyManager.releaseProxy(eventId, false, error);
                }
                
                // Increment failure count
                this.incrementFailureCount(eventId);
                
                // Apply cooldown based on failure count
                const backoff = Math.pow(2, failures) * (group.name === "medium" ? 30000 : 10000); // shorter for low failure count
                const cooldownUntil = moment().add(backoff, "milliseconds");
                this.cooldownEvents.set(eventId, cooldownUntil);
                
                // Add to retry queue
                this.retryQueue.push({
                  eventId,
                  retryCount: failures + 1,
                  retryAfter: cooldownUntil,
                });
                
                this.logWithTime(
                  `Error processing event ${eventId} (${group.name} failure group, ${errorType}): ${error.message}. Scheduled for retry in ${Math.round(backoff/60000)} minutes.`,
                  "error"
                );
                
                // Log error with detailed info
                await this.logError(eventId, "SCRAPE_ERROR", error, { retryCount: failures, errorType });
                
                return { eventId, success: false, error };
              }
            });
            
            // Wait for all events in batch to complete
            const results = await Promise.all(eventPromises);
            
            this.logWithTime(
              `Batch processing complete: ${results.filter(r => r.success).length} succeeded, ${results.filter(r => !r.success).length} failed`
            );
            
            // Add short delay between batches
            await setTimeout(2000);
          }
        } catch (batchError) {
          this.logWithTime(
            `Error processing batch: ${batchError.message}`,
            "error"
          );
          
          // Add delay before continuing to next batch
          await setTimeout(5000);
        }
      }
      
      // Add pause between different failure groups
      await setTimeout(3000);
    }
    
    // Log proxy usage stats
    const proxyStats = this.proxyManager.getUsageStats();
    this.logWithTime(
      `Proxy usage: ${proxyStats.usedProxies}/${proxyStats.totalProxies} proxies used, ${proxyStats.healthyProxies} healthy, ${proxyStats.bannedProxies} banned`
    );
    
    // Log completion of batch processing
    this.logWithTime(
      `Failed events batch processing complete. ${this.failedEvents.size} events still failing, ${this.retryQueue.length} in retry queue`
    );
  }
  
  /**
   * Get the error type for an event based on recent errors
   * @param {string} eventId - The event ID
   * @returns {string} The error type
   */
  getEventErrorType(eventId) {
    for (const [type, events] of Object.entries(this.errorsByType)) {
      if (events.has(eventId)) {
        return type;
      }
    }
    return 'other';
  }

  // Helper method to check if an error is temporary and should be retried without marking as failed
  isTemporaryError(error) {
    // Error message patterns that indicate temporary issues
    const temporaryErrorPatterns = [
      // Network errors
      'timeout', 'timed out', 'ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EHOSTUNREACH',
      'socket hang up', 'network error', 'Network Error', 'connection error',
      // API rate limiting & temporary issues
      '429', 'Too Many Requests', 'rate limit', 'ratelimit', 
      // Temporary API errors
      '500', '502', '503', '504', 'Internal Server Error', 'Bad Gateway', 'Service Unavailable', 'Gateway Timeout',
      // Transient errors
      'temporarily unavailable', 'please try again', 'try again later',
      // Proxy errors
      'proxy error', 'proxy connection', 'tunneling socket'
    ];

    if (!error || !error.message) return false;
    
    const errorMessage = error.message.toLowerCase();
    
    // Check if the error message contains any of the temporary error patterns
    for (const pattern of temporaryErrorPatterns) {
      if (errorMessage.includes(pattern.toLowerCase())) {
        if (LOG_LEVEL >= 3) {
          this.logWithTime(`Detected temporary error: "${error.message}" matches pattern "${pattern}"`, "debug");
        }
        return true;
      }
    }
    
    // No match found, not a temporary error
    if (LOG_LEVEL >= 3) {
      this.logWithTime(`Non-temporary error detected: "${error.message}"`, "debug");
    }
    return false;
  }

  // Helper method to clean up the failed events list
  cleanupFailedEvents() {
    const now = moment();
    const eventsToRemove = [];
    
    // Check each failed event to see if it's eligible for retry
    for (const eventId of this.failedEvents) {
      // If event is no longer in cooldown, remove from failed list
      if (this.cooldownEvents.has(eventId)) {
        const cooldownUntil = this.cooldownEvents.get(eventId);
        if (now.isAfter(cooldownUntil)) {
          eventsToRemove.push(eventId);
        }
      } else {
        // Event has no active cooldown, remove from failed list
        eventsToRemove.push(eventId);
      }
      
      // Check if event has exceeded maximum allowed time since update
      // If so, we should keep retrying it regardless
      const lastUpdate = this.eventUpdateTimestamps.get(eventId);
      if (lastUpdate && now.diff(lastUpdate) > MAX_ALLOWED_UPDATE_INTERVAL - 30000) {
        // Event is approaching 3-minute deadline, remove from failed list to force retry
        eventsToRemove.push(eventId);
        
        if (LOG_LEVEL >= 1) {
          this.logWithTime(
            `Removing ${eventId} from failed list: approaching max allowed time (${now.diff(lastUpdate) / 1000}s since last update)`,
            "warning"
          );
        }
      }
    }
    
    // Remove events from the failed list
    for (const eventId of eventsToRemove) {
      this.failedEvents.delete(eventId);
    }
    
    if (eventsToRemove.length > 0 && LOG_LEVEL >= 2) {
      this.logWithTime(`Removed ${eventsToRemove.length} events from failed list, ${this.failedEvents.size} remaining`, "info");
    }
    
    return eventsToRemove.length;
  }
}

const scraperManager = new ScraperManager();
export default scraperManager;

// Add a function to export that allows checking logs
export function checkScraperLogs() {
  return scraperManager.checkLogs();
}
