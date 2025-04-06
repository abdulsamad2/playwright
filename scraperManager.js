import moment from "moment";
import { Event, ErrorLog, ConsecutiveGroup } from "./models/index.js";
import { ScrapeEvent, refreshHeaders } from "./scraper.js";
import { setTimeout } from "timers/promises";
import { cpus } from "os";

// Updated constants for stricter update intervals
const MAX_UPDATE_INTERVAL = 120000; // Strict 2-minute update requirement
const CONCURRENT_LIMIT = Math.max(4, Math.floor(cpus().length * 0.9)); // Increased concurrency to 90% of CPU cores
const MAX_RETRIES = 3;
const SCRAPE_TIMEOUT = 30000; // Reduced timeout to 30 seconds
const BATCH_SIZE = Math.max(CONCURRENT_LIMIT * 2, 10); // Larger batches for efficiency
const RETRY_BACKOFF_MS = 5000; // Base backoff time for retries
const MIN_TIME_BETWEEN_EVENT_SCRAPES = 10000; // Reduced to 10 seconds minimum between scrapes
const URGENT_THRESHOLD = 110000; // Events needing update within 10 seconds of deadline

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

      if (!event) {
        console.error(`No event found for ID: ${eventId} when logging error`);
        return;
      }

      await ErrorLog.create({
        eventUrl: event.url,
        eventId: event._id,
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
    } catch (err) {
      console.error("Error logging to database:", err);
      console.error("Original error:", error);
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
      this.eventUpdateSchedule.set(eventId, moment().add(MAX_UPDATE_INTERVAL, 'milliseconds'));
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
        const headers = await refreshHeaders(eventId);
        if (headers) {
          this.headersCache.set(eventId, headers);
          this.headerRefreshTimestamps.set(eventId, moment());
          return headers;
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

  async scrapeEvent(eventId, retryCount = 0) {
    // Skip if the event should be skipped
    if (this.shouldSkipEvent(eventId)) {
      return false;
    }

    await this.acquireSemaphore();
    this.processingEvents.add(eventId);
    this.activeJobs.set(eventId, moment());

    try {
      this.logWithTime(`Scraping ${eventId} (Attempt ${retryCount + 1})`);

      // Look up the event first before trying to scrape
      const event = await Event.findOne({ Event_ID: eventId })
        .select("Skip_Scraping inHandDate")
        .lean();

      if (!event) {
        this.logWithTime(`Event ${eventId} not found in database`, "error");
        // Put event in cooldown to avoid immediate retries
        this.cooldownEvents.set(eventId, moment().add(60, "minutes"));
        return false;
      }

      if (event.Skip_Scraping) {
        this.logWithTime(`Skipping ${eventId} (flagged)`, "info");
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
      this.successCount++;
      this.lastSuccessTime = moment();
      this.eventUpdateTimestamps.set(eventId, moment());
      this.failedEvents.delete(eventId);
      return true;
    } catch (error) {
      this.failedEvents.add(eventId);
      await this.logError(eventId, "SCRAPE_ERROR", error, { retryCount });

      // Apply exponential backoff for retries
      const backoffTime = RETRY_BACKOFF_MS * Math.pow(2, retryCount);
      const cooldownUntil = moment().add(backoffTime, "milliseconds");
      this.cooldownEvents.set(eventId, cooldownUntil);

      this.logWithTime(
        `Error scraping ${eventId}: ${error.message}. Cooldown for ${
          backoffTime / 1000
        }s`,
        "error"
      );

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
    }
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
    
    // Process chunks with controlled parallelism
    for (const chunk of chunks) {
      // Process each chunk in parallel
      const promises = chunk.map(async (eventId) => {
        try {
          const result = await this.scrapeEvent(eventId);
          results.push({ eventId, success: result });
        } catch (error) {
          failed.push({ eventId, error });
        }
      });
      
      await Promise.all(promises);
      
      // Small delay between chunks to prevent rate limiting
      if (chunks.length > 1) {
        await setTimeout(200);
      }
    }
    
    return { failed };
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
    const now = moment();
    
    // First, identify events approaching their 2-minute deadline
    const urgentEvents = [];
    const nearDeadlineEvents = [];
    const regularEvents = [];
    
    // Get all events that need updating
    const allEvents = await Event.find({
      Skip_Scraping: { $ne: true },
      Event_ID: { $nin: [...this.processingEvents] },
    })
      .sort({ Last_Updated: 1 })
      .select("Event_ID Last_Updated")
      .lean();
    
    // Process based on deadline proximity
    for (const event of allEvents) {
      // If we don't have a scheduled update time, set one based on last update
      if (!this.eventUpdateSchedule.has(event.Event_ID)) {
        const lastUpdate = moment(event.Last_Updated);
        this.eventUpdateSchedule.set(
          event.Event_ID, 
          lastUpdate.add(MAX_UPDATE_INTERVAL, 'milliseconds')
        );
      }
      
      const deadline = this.eventUpdateSchedule.get(event.Event_ID);
      const timeToDeadline = deadline.diff(now);
      
      // Past deadline or within 10 seconds of deadline
      if (timeToDeadline <= 0) {
        urgentEvents.push(event.Event_ID);
      } 
      // Within 30 seconds of deadline
      else if (timeToDeadline <= URGENT_THRESHOLD) {
        nearDeadlineEvents.push(event.Event_ID);
      }
      // All other events
      else {
        regularEvents.push(event.Event_ID);
      }
    }
    
    // Log urgency metrics
    if (urgentEvents.length > 0) {
      this.logWithTime(`URGENT: ${urgentEvents.length} events past deadline`, "warning");
    }
    
    // Prioritize urgent events first, then near deadline, then some regular events
    const urgentBatchSize = Math.min(urgentEvents.length, BATCH_SIZE);
    const remainingCapacity = BATCH_SIZE - urgentBatchSize;
    
    let result = urgentEvents.slice(0, urgentBatchSize);
    
    if (remainingCapacity > 0) {
      const nearDeadlineBatchSize = Math.min(nearDeadlineEvents.length, Math.floor(remainingCapacity * 0.7));
      result = [...result, ...nearDeadlineEvents.slice(0, nearDeadlineBatchSize)];
      
      const regularBatchSize = remainingCapacity - nearDeadlineBatchSize;
      if (regularBatchSize > 0) {
        result = [...result, ...regularEvents.slice(0, regularBatchSize)];
      }
    }
    
    return result;
  }

  async startContinuousScraping() {
    this.isRunning = true;
    this.logWithTime(
      `Starting strict 2-minute scraper with ${CONCURRENT_LIMIT} concurrent jobs`
    );
    
    // Schedule checking every second for urgent events
    const checkInterval = 1000; // Check every second
    
    while (this.isRunning) {
      const cycleStart = performance.now();
      
      try {
        // Process retries first
        await this.processRetryQueue();
        
        // Get events to process with strict prioritization
        const eventIds = await this.getEventsToProcess();
        
        if (eventIds.length > 0) {
          this.logWithTime(`Processing batch of ${eventIds.length} events (${this.processingEvents.size} already in progress)`);
          await this.processBatch(eventIds);
        }
        
        // Short delay before next check
        const cycleTime = performance.now() - cycleStart;
        const delay = Math.max(0, checkInterval - cycleTime);
        
        if (delay > 0) {
          await setTimeout(delay);
        }
      } catch (error) {
        this.logWithTime(`Cycle error: ${error.message}`, "error");
        await setTimeout(5000); // Shorter recovery time
      }
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
        
        // Log system health status
        this.logWithTime(
          `System status: ${this.successCount} successful scrapes, ` +
          `${this.activeJobs.size}/${CONCURRENT_LIMIT} active, ` +
          `${this.failedEvents.size} failed, ${this.retryQueue.length} in retry queue, ` +
          `${this.eventUpdateSchedule.size} total tracked events`,
          "info"
        );
        
        // Clear old cooldowns
        const expiredCooldowns = [];
        for (const [eventId, cooldownTime] of this.cooldownEvents.entries()) {
          if (now.isAfter(cooldownTime)) {
            expiredCooldowns.push(eventId);
          }
        }
        
        for (const eventId of expiredCooldowns) {
          this.cooldownEvents.delete(eventId);
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
}

const scraperManager = new ScraperManager();
export default scraperManager;
