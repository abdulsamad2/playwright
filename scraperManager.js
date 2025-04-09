import moment from "moment";
import { setTimeout } from "timers/promises";
import { Event, ErrorLog, ConsecutiveGroup } from "./models/index.js";
import { ScrapeEvent, refreshHeaders } from "./scraper.js";
import { cpus } from "os";
import fs from "fs/promises";
import path from "path";
import ProxyManager from "./helpers/ProxyManager.js";
import { BrowserFingerprint } from "./browserFingerprint.js";

// Updated constants for stricter update intervals
const MAX_UPDATE_INTERVAL = 80000; // Strict 2-minute update requirement
const CONCURRENT_LIMIT = Math.max(8, Math.floor(cpus().length * 1.2)); // Increase to 120% of CPU cores
const MAX_RETRIES = 3; // Reduced to 3 for faster failure detection
const SCRAPE_TIMEOUT = 25000; // Reduced timeout to 25 seconds
const BATCH_SIZE = Math.max(CONCURRENT_LIMIT * 3, 15); // Larger batches for efficiency
const RETRY_BACKOFF_MS = 3000; // Reduced base backoff time for faster retries
const MIN_TIME_BETWEEN_EVENT_SCRAPES = 60000; // Minimum 1 minute between scrapes
const URGENT_THRESHOLD = 110000; // Events needing update within 10 seconds of deadline

// New cooldown settings - shorter cooldowns
const SHORT_COOLDOWNS = [3000, 6000, 10000]; // 3s, 6s, 10s
const LONG_COOLDOWN_MINUTES = 5; // Reduced to 5 minutes for persistently failing events

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
  }

  logWithTime(message, type = "info") {
    const now = moment();
    const runningTime = moment.duration(now.diff(this.startTime));
    const formattedTime = now.format("YYYY-MM-DD HH:mm:ss");

    const statusEmoji =
      {
        success: "✅",
        error: "❌",
        warning: "⚠️",
        info: "ℹ️",
      }[type] || "📝";

    // Only log detailed stats in info, success, or error messages to reduce noise
    if (["info", "success", "error"].includes(type)) {
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
        const timeLeft = moment
          .duration(cooldownUntil.diff(moment()))
          .asSeconds();
        this.logWithTime(
          `Skipping ${eventId}: In cooldown for ${timeLeft.toFixed(1)}s more`,
          "warning"
        );
        return true;
      } else {
        this.cooldownEvents.delete(eventId);
      }
    }

    // Check if event is already being processed
    if (this.processingEvents.has(eventId)) {
      this.logWithTime(`Skipping ${eventId}: Already processing`, "warning");
      return true;
    }

    // Check if minimum time between scrapes has elapsed
    const lastUpdate = this.eventUpdateTimestamps.get(eventId);
    if (
      lastUpdate &&
      moment().diff(lastUpdate) < MIN_TIME_BETWEEN_EVENT_SCRAPES
    ) {
      this.logWithTime(
        `Skipping ${eventId}: Too soon since last scrape (${moment().diff(
          lastUpdate
        )}ms)`,
        "warning"
      );
      return true;
    }

    return false;
  }

  async refreshEventHeaders(eventId) {
    const lastRefresh = this.headerRefreshTimestamps.get(eventId);
    // Only refresh headers if they haven't been refreshed in last 5 minutes
    if (!lastRefresh || moment().diff(lastRefresh) > 300000) {
      try {
        this.logWithTime(`Refreshing headers for ${eventId}`);
        const capturedState = await refreshHeaders(eventId);
        
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
            this.logWithTime(`No valid headers found for ${eventId}`, "warning");
            return null;
          }
          
          this.headersCache.set(eventId, standardizedHeaders);
          this.headerRefreshTimestamps.set(eventId, moment());
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
      this.logWithTime(
        `Skipping cookie reset - last reset was ${moment.duration(now.diff(this.lastCookieReset)).humanize()} ago`,
        "warning"
      );
      return;
    }
    
    try {
      this.logWithTime("Detected multiple API failures - resetting cookies and headers", "warning");
      
      // Check if cookies.json exists before trying to delete it
      try {
        await fs.access(this.cookiesPath);
        
        // Delete cookies.json
        await fs.unlink(this.cookiesPath);
        this.logWithTime("Deleted cookies.json", "info");
      } catch (e) {
        // File doesn't exist, that's fine
        this.logWithTime("No cookies.json file found to delete", "info");
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
          // Optimized seat comparison
          const existingGroups = await ConsecutiveGroup.find(
            { eventId },
            { section: 1, row: 1, seats: 1, "inventory.listPrice": 1 }
          ).lean();

          const existingSeats = new Set(
            existingGroups.flatMap((g) =>
              g.seats.map((s) => `${g.section}-${g.row}-${s.number}-${s.price}`)
            )
          );

          const newSeats = new Set(
            scrapeResult.flatMap((g) =>
              g.seats.map(
                (s) => `${g.section}-${g.row}-${s}-${g.inventory.listPrice}`
              )
            )
          );

          if (
            existingSeats.size !== newSeats.size ||
            [...existingSeats].some((s) => !newSeats.has(s))
          ) {
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
                ...group.inventory,
                tickets: group.inventory.tickets.map((ticket) => ({
                  ...ticket,
                  sellPrice: ticket.sellPrice * 1.25, // Apply markup
                })),
              },
            }));

            // Use fewer documents in a single batch insert
            const CHUNK_SIZE = 100;
            for (let i = 0; i < groupsToInsert.length; i += CHUNK_SIZE) {
              const chunk = groupsToInsert.slice(i, i + CHUNK_SIZE);
              await ConsecutiveGroup.insertMany(chunk, { session });
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
      this.logWithTime(
        `Updated event ${eventId} in ${(performance.now() - startTime).toFixed(
          2
        )}ms, next update by ${this.eventUpdateSchedule.get(eventId).format('HH:mm:ss')}`,
        "success"
      );
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
      return false;
    }

    await this.acquireSemaphore();
    this.processingEvents.add(eventId);
    this.activeJobs.set(eventId, moment());

    try {
      this.logWithTime(`Scraping ${eventId} (Attempt ${retryCount + 1})`);

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
        this.logWithTime(`Skipping ${eventId} (flagged)`, "info");
        // Still update the schedule for next time
        this.eventUpdateSchedule.set(eventId, moment().add(MIN_TIME_BETWEEN_EVENT_SCRAPES, 'milliseconds'));
        return true;
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

      if (!result || !Array.isArray(result) || result.length === 0) {
        throw new Error("Empty or invalid scrape result");
      }

      await this.updateEventMetadata(eventId, result);
      
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
      
      // Reset global consecutive error counter on success
      this.globalConsecutiveErrors = 0;
      
      return true;
    } catch (error) {
      this.failedEvents.add(eventId);
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
        
        this.logWithTime(
          `${isApiError ? "API error" : "Error"} for ${eventId}: ${error.message}. Short cooldown for ${backoffTime/1000}s`,
          "warning"
        );
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
          this.logWithTime(`Marked event ${eventId} as stopped in database`, "warning");
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
        this.logWithTime(
          `Queued for retry: ${eventId} (after ${
            backoffTime / 1000
          }s cooldown)`,
          "warning"
        );
      } else {
        this.logWithTime(`Max retries exceeded for ${eventId}`, "error");
      }
      return false;
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
    
    // Split into smaller groups for better control
    const chunkSize = Math.min(5, Math.ceil(CONCURRENT_LIMIT / 2));
    const chunks = [];
    
    for (let i = 0; i < eventIds.length; i += chunkSize) {
      chunks.push(eventIds.slice(i, i + chunkSize));
    }
    
    // Generate a unique batch ID for tracking
    const batchId = Date.now().toString(36) + Math.random().toString(36).substr(2, 5);
    
    // Process chunks with controlled parallelism
    for (const chunk of chunks) {
      try {
        // Get a proxy for this specific batch
        const { proxyAgent, proxy } = this.proxyManager.getProxyForBatch(chunk);
        
        // Store the proxy assignment for this batch
        this.batchProxies.set(batchId, { proxy, chunk });
        
        this.logWithTime(`Using proxy ${proxy.proxy} for batch ${batchId} (${chunk.length} events)`);

        // Process the entire chunk efficiently as a batch
        await this.processBatchEfficiently(chunk, batchId, proxyAgent, proxy);
        
        // Small delay between chunks to prevent rate limiting
        if (chunks.length > 1) {
          await setTimeout(500); // Increased delay between chunks
        }
      } catch (error) {
        this.logWithTime(`Error processing batch ${batchId}: ${error.message}`, "error");
        
        // If there's a proxy error, release all events from this batch
        if (this.batchProxies.has(batchId)) {
          const { chunk } = this.batchProxies.get(batchId);
          this.proxyManager.releaseProxyBatch(chunk);
        }
      } finally {
        // Clean up batch tracking
        this.batchProxies.delete(batchId);
      }
    }
    
    return { results, failed };
  }

  // New method to process a batch of events efficiently by handling API requests together
  async processBatchEfficiently(eventIds, batchId, proxyAgent, proxy) {
    const results = [];
    const failed = [];

    // Preload all event validations in parallel to save time
    const validationPromises = eventIds.map(async (eventId) => {
      if (this.shouldSkipEvent(eventId)) {
        return { eventId, valid: false, reason: "skip" };
      }

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

    // Log invalid events
    validationResults
      .filter(result => !result.valid)
      .forEach(result => {
        switch (result.reason) {
          case "skip":
            // Already logged in shouldSkipEvent
            break;
          case "not_found":
            this.logWithTime(`Event ${result.eventId} not found in database`, "error");
            break;
          case "flagged":
            this.logWithTime(`Skipping ${result.eventId} (flagged)`, "info");
            break;
          case "error":
            this.logWithTime(`Error validating event ${result.eventId}: ${result.error.message}`, "error");
            break;
        }
      });

    if (validEvents.length === 0) {
      this.logWithTime(`No valid events to process in batch ${batchId}`, "warning");
      return { results, failed };
    }

    // Set all valid events as processing
    validEvents.forEach(({ eventId }) => {
      this.processingEvents.add(eventId);
      this.activeJobs.set(eventId, moment());
    });

    await this.acquireSemaphore();
    
    // Implement header caching strategy for better efficiency
    let sharedHeaders = null;
    try {
      // First check if we have recent valid headers for any event in the batch
      for (const { eventId } of validEvents) {
        const cachedHeaders = this.headersCache.get(eventId);
        const lastRefresh = this.headerRefreshTimestamps.get(eventId);
        
        if (cachedHeaders && lastRefresh && moment().diff(lastRefresh) < 300000) { // 5 minutes
          // Validate the headers are complete before using them
          if (cachedHeaders.headers && 
              (cachedHeaders.headers.Cookie || cachedHeaders.headers.cookie) && 
              (cachedHeaders.headers["User-Agent"] || cachedHeaders.headers["user-agent"])) {
            sharedHeaders = cachedHeaders;
            this.logWithTime(`Using recent cached headers from event ${eventId} for batch ${batchId}`, "info");
            break;
          }
        }
      }
      
      // If no recent cached headers, refresh using first event
      if (!sharedHeaders) {
        const firstEventId = validEvents[0].eventId;
        this.logWithTime(`Starting event scraping for ${firstEventId} with shared cookies...`, "info");
        
        sharedHeaders = await this.refreshEventHeaders(firstEventId);
        if (!sharedHeaders) {
          throw new Error("Failed to obtain valid headers for batch");
        }
        
        // Verify the headers are complete
        if (!sharedHeaders.headers || 
            !(sharedHeaders.headers.Cookie || sharedHeaders.headers.cookie) || 
            !(sharedHeaders.headers["User-Agent"] || sharedHeaders.headers["user-agent"])) {
          
          // Try one more time with a different event
          if (validEvents.length > 1) {
            const secondEventId = validEvents[1].eventId;
            this.logWithTime(`First event headers incomplete, trying with ${secondEventId}`, "warning");
            sharedHeaders = await this.refreshEventHeaders(secondEventId);
            
            if (!sharedHeaders || 
                !sharedHeaders.headers || 
                !(sharedHeaders.headers.Cookie || sharedHeaders.headers.cookie) || 
                !(sharedHeaders.headers["User-Agent"] || sharedHeaders.headers["user-agent"])) {
              throw new Error("Failed to obtain complete headers after multiple attempts");
            }
          } else {
            throw new Error("Failed to obtain complete headers for batch");
          }
        }
        
        // Cache headers for all events in this batch
        for (const { eventId } of validEvents) {
          this.headersCache.set(eventId, sharedHeaders);
          this.headerRefreshTimestamps.set(eventId, moment());
        }
      }
    } catch (error) {
      this.logWithTime(`Failed to refresh headers for batch ${batchId}: ${error.message}`, "error");
      // Release all events in batch
      validEvents.forEach(({ eventId }) => {
        this.activeJobs.delete(eventId);
        this.processingEvents.delete(eventId);
        failed.push({ eventId, error });
      });
      this.releaseSemaphore();
      return { results, failed };
    }

    // Group events by common sections to minimize duplicate API calls
    // Process all valid events with increased parallelism
    const chunkSize = Math.min(validEvents.length, Math.max(3, Math.ceil(CONCURRENT_LIMIT / 2)));
    const chunks = [];
    
    for (let i = 0; i < validEvents.length; i += chunkSize) {
      chunks.push(validEvents.slice(i, i + chunkSize));
    }
    
    // Process chunks in sequence but events within chunks in parallel
    for (const chunk of chunks) {
      const chunkPromises = chunk.map(async ({ eventId }) => {
        try {
          this.logWithTime(`Processing ${eventId} in batch ${batchId}`);
          
          const result = await Promise.race([
            ScrapeEvent({ eventId, headers: sharedHeaders }),
            setTimeout(SCRAPE_TIMEOUT).then(() => {
              throw new Error("Scrape timed out");
            }),
          ]);

          if (!result || !Array.isArray(result) || result.length === 0) {
            throw new Error("Empty or invalid scrape result");
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
          this.globalConsecutiveErrors = 0;
          results.push({ eventId, success: true });
          return { eventId, success: true };
        } catch (error) {
          await this.handleEventError(eventId, error, 0, failed);
          return { eventId, success: false, error };
        } finally {
          this.activeJobs.delete(eventId);
          this.processingEvents.delete(eventId);
        }
      });
      
      await Promise.all(chunkPromises);
      
      // Small delay between chunks to prevent rate limiting
      if (chunks.length > 1) {
        await setTimeout(200);
      }
    }
    
    this.releaseSemaphore();
    this.logWithTime(`Completed batch ${batchId}: ${results.length} successes, ${failed.length} failures`, "info");
    return { results, failed };
  }

  // Helper method to handle event errors consistently
  async handleEventError(eventId, error, retryCount, failedList) {
    this.failedEvents.add(eventId);
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
      
      this.logWithTime(
        `${isApiError ? "API error" : "Error"} for ${eventId}: ${error.message}. Short cooldown for ${backoffTime/1000}s`,
        "warning"
      );
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
        this.logWithTime(`Marked event ${eventId} as stopped in database`, "warning");
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
      this.logWithTime(
        `Queued for retry: ${eventId} (after ${
          backoffTime / 1000
        }s cooldown)`,
        "warning"
      );
    } else {
      this.logWithTime(`Max retries exceeded for ${eventId}`, "error");
    }
    
    failedList.push({ eventId, error });
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

    // Process retries one at a time with delay between to avoid overwhelming the server
    for (const job of readyForRetry) {
      if (!this.isRunning) break;

      // Attempt the retry
      await this.scrapeEvent(job.eventId, job.retryCount);

      // Add delay between retries
      await setTimeout(2000);
    }
  }

  async getEventsToProcess() {
    try {
      const now = moment();
      const twoMinutesFromNow = now.clone().add(2, 'minutes').toDate();
      
      // Prioritize events needing update soon
      // Find all active events due for update within the next 2 minutes
      const urgentEvents = await Event.find({
        Skip_Scraping: { $ne: true },
        $or: [
          { Last_Updated: { $exists: false } },
          { Last_Updated: { $lt: now.clone().subtract(1, 'minute').toDate() } }
        ]
      })
      .select('Event_ID url Last_Updated')
      .sort({ Last_Updated: 1 }) // Oldest first
      .limit(BATCH_SIZE * 4) // Get a larger sample to select from
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
        
        // Adjust score for events in retry queue
        if (this.retryQueue.find(retry => retry.eventId === event.Event_ID)) {
          const retryCount = this.getRecentFailureCount(event.Event_ID);
          priorityScore = priorityScore * (1 - (retryCount * 0.1)); // Reduce priority slightly for retry attempts
        }
        
        // Boost score for events that haven't been updated in over 100 seconds (nearly at 2-minute deadline)
        if (timeSinceLastUpdate > MAX_UPDATE_INTERVAL - 20000) {
          priorityScore = priorityScore * 1.5;
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
      
      // Reassemble prioritized list, keeping domain groups together
      const optimizedEventList = [];
      for (const domainEvents of domains.values()) {
        // Add all events from this domain/group
        domainEvents.forEach(event => {
          optimizedEventList.push(event.eventId);
        });
      }
      
      // Limit to the maximum batch size but make sure we don't exceed our processing capacity
      const finalEventsList = optimizedEventList.slice(0, BATCH_SIZE);
      
      // Log stats about the events we're processing
      const oldestEvent = prioritizedEvents[0];
      if (oldestEvent) {
        const ageInSeconds = oldestEvent.timeSinceLastUpdate / 1000;
        this.logWithTime(`Processing ${finalEventsList.length} events. Oldest event is ${ageInSeconds.toFixed(1)}s old.`);
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
        // Process retry queue first
        await this.processRetryQueue();
        
        // Get events to process
        const eventsToProcess = await this.getEventsToProcess();
        
        if (eventsToProcess.length === 0) {
          // No events to process, wait for a short time
          await setTimeout(1000);
          continue;
        }
        
        // Optimize batch size dynamically based on system load
        const totalEvents = eventsToProcess.length;
        const optimalBatchSize = Math.min(
          totalEvents,
          Math.max(5, Math.ceil(CONCURRENT_LIMIT / 2))
        );
        
        // Break events into right-sized batches for optimal processing
        const batches = [];
        for (let i = 0; i < totalEvents; i += optimalBatchSize) {
          batches.push(eventsToProcess.slice(i, i + optimalBatchSize));
        }
        
        this.logWithTime(`Processing ${totalEvents} events in ${batches.length} batches`);
        
        // Process batches in parallel but with controlled concurrency
        const batchPromises = batches.map(async (batch, index) => {
          const batchStart = performance.now();
          const batchId = `batch-${Date.now()}-${index}`;
          try {
            await this.processBatch(batch);
            const batchDuration = performance.now() - batchStart;
            this.logWithTime(
              `Completed batch ${batchId} in ${batchDuration.toFixed(2)}ms (${batch.length} events)`,
              "info"
            );
          } catch (error) {
            this.logWithTime(
              `Error processing batch ${batchId}: ${error.message}`,
              "error"
            );
          }
        });
        
        // Wait for all batches to complete
        await Promise.all(batchPromises);
        
        // Check if we need to perform maintenance tasks (cleanup stale jobs, etc.)
        if (this.activeJobs.size === 0) {
          await this.cleanupStaleTasks();
        }
        
        // Pause briefly to prevent CPU overuse, adjust dynamically based on system load
        const pauseTime = this.getAdaptivePauseTime();
        if (pauseTime > 0) {
          await setTimeout(pauseTime);
        }
      } catch (error) {
        this.logWithTime(`Error in continuous scraping: ${error.message}`, "error");
        console.error(error);
        // Brief pause on error to avoid tight loop
        await setTimeout(5000);
      }
    }
  }

  // Dynamic pause time calculation based on system load
  getAdaptivePauseTime() {
    const activeJobPercentage = (this.activeJobs.size / CONCURRENT_LIMIT) * 100;
    
    if (activeJobPercentage > 80) {
      // System is highly loaded, longer pause
      return 2000;
    } else if (activeJobPercentage > 50) {
      // Medium load
      return 1000;
    } else if (activeJobPercentage > 20) {
      // Light load
      return 500;
    } else {
      // Very light load, minimal pause
      return 100;
    }
  }

  async cleanupStaleTasks() {
    // Handle jobs that might be stuck
    const staleTimeLimit = 5 * 60 * 1000; // 5 minutes
    const now = moment();

    for (const [eventId, startTime] of this.activeJobs.entries()) {
      if (now.diff(startTime) > staleTimeLimit) {
        this.logWithTime(
          `Cleaning up stale job for ${eventId} (started ${
            now.diff(startTime) / 1000
          }s ago)`,
          "warning"
        );
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
        
        for (const [eventId, deadline] of this.eventUpdateSchedule.entries()) {
          if (now.isAfter(deadline) && !this.processingEvents.has(eventId)) {
            missedDeadlines++;
            // Force immediate processing of missed events
            this.priorityQueue.add(eventId);
          }
        }
        
        if (missedDeadlines > 0) {
          this.logWithTime(`WARNING: ${missedDeadlines} events missed their 2-minute update deadline`, "error");
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
        
        // Log system health status
        this.logWithTime(
          `System status: ${this.successCount} successful scrapes, ` +
          `${this.activeJobs.size}/${CONCURRENT_LIMIT} active, ` +
          `${this.failedEvents.size} failed, ${this.retryQueue.length} in retry queue, ` +
          `${this.eventUpdateSchedule.size} total tracked events` +
          (this.globalConsecutiveErrors > 0 ? `, consecutive errors: ${this.globalConsecutiveErrors}` : ""),
          "info"
        );
        
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
      
      // Run monitoring every 30 seconds
      await setTimeout(30 * 1000);
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
}

const scraperManager = new ScraperManager();
export default scraperManager;

// Add a function to export that allows checking logs
export function checkScraperLogs() {
  return scraperManager.checkLogs();
}
