import moment from "moment";
import { Event, ErrorLog, ConsecutiveGroup } from "./models/index.js";
import { ScrapeEvent, refreshHeaders } from "./scraper.js";
import { setTimeout } from "timers/promises";
import { cpus } from "os";

const CONCURRENT_LIMIT = Math.max(1, Math.floor(cpus().length * 0.75)); // Reduced concurrency to 75% of CPU cores
const EVENT_REFRESH_INTERVAL = 120000; // Increased to 2 minutes
const MAX_RETRIES = 3;
const SCRAPE_TIMEOUT = 45000; // Increased timeout to 45 seconds
const BATCH_SIZE = Math.max(1, Math.floor(CONCURRENT_LIMIT * 1.5)); // Smaller batches
const PRIORITY_UPDATE_WINDOW = 20000; // 20-second window for priority updates
const RETRY_BACKOFF_MS = 5000; // Base backoff time for retries
const MIN_TIME_BETWEEN_EVENT_SCRAPES = 30000; // Minimum 30 seconds between scrapes of the same event

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
    this.processingEvents = new Set(); // Track events currently being processed to prevent duplicates
    this.headerRefreshTimestamps = new Map(); // Track when headers were last refreshed
    this.cooldownEvents = new Map(); // Events that need to cool down before retry
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

      this.logWithTime(
        `Updated event ${eventId} in ${(performance.now() - startTime).toFixed(
          2
        )}ms`,
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
    // Filter out any events that should be skipped
    const filteredEventIds = eventIds.filter(
      (eventId) => !this.shouldSkipEvent(eventId)
    );

    if (filteredEventIds.length === 0) {
      return { failed: [] };
    }

    // Process events sequentially instead of all at once to reduce strain
    const results = [];
    const failed = [];

    for (const eventId of filteredEventIds) {
      try {
        const result = await this.scrapeEvent(eventId);
        results.push({ eventId, success: result });

        // Add a small delay between each event to prevent rate limiting
        await setTimeout(1000);
      } catch (error) {
        failed.push({ eventId, error });
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
    const priorityCutoff = now.clone().subtract(PRIORITY_UPDATE_WINDOW, "ms");

    // Get priority events first (recently updated or failed)
    const priorityEvents = await Event.find({
      $or: [
        { Last_Updated: { $lt: priorityCutoff.toDate() } },
        { Event_ID: { $in: [...this.failedEvents].slice(0, 5) } }, // Limit failed events to 5
      ],
      Skip_Scraping: { $ne: true },
      // Don't process events we're already handling
      Event_ID: { $nin: [...this.processingEvents] },
    })
      .sort({ Last_Updated: 1 })
      .limit(Math.ceil(BATCH_SIZE / 2)) // Half the batch size
      .select("Event_ID")
      .lean();

    // If we still have capacity, get regular events
    const remainingCapacity = BATCH_SIZE - priorityEvents.length;
    let regularEvents = [];

    if (remainingCapacity > 0) {
      regularEvents = await Event.find({
        Skip_Scraping: { $ne: true },
        Event_ID: {
          $nin: [
            ...priorityEvents.map((e) => e.Event_ID),
            ...this.processingEvents,
          ],
        },
      })
        .sort({ Last_Updated: 1 })
        .limit(remainingCapacity)
        .select("Event_ID")
        .lean();
    }

    return [...priorityEvents, ...regularEvents].map((e) => e.Event_ID);
  }

  async startContinuousScraping() {
    this.isRunning = true;
    this.logWithTime(
      "Starting optimized scraper with rate limiting and cooldowns"
    );
    this.logWithTime(
      `Concurrency: ${CONCURRENT_LIMIT}, Batch size: ${BATCH_SIZE}, Retry backoff: ${RETRY_BACKOFF_MS}ms base`
    );

    while (this.isRunning) {
      const cycleStart = performance.now();

      try {
        // Process retries first
        await this.processRetryQueue();

        // Get events to process
        const eventIds = await this.getEventsToProcess();
        if (eventIds.length === 0) {
          this.logWithTime(
            "No events to process, waiting for next cycle",
            "info"
          );
          await setTimeout(EVENT_REFRESH_INTERVAL);
          continue;
        }

        this.logWithTime(`Processing batch of ${eventIds.length} events`);
        await this.processBatch(eventIds);

        const cycleTime = performance.now() - cycleStart;
        const delay = Math.max(0, EVENT_REFRESH_INTERVAL - cycleTime);

        if (delay > 0) {
          this.logWithTime(
            `Cycle completed in ${Math.round(
              cycleTime
            )}ms, waiting ${Math.round(delay)}ms until next cycle`
          );
          await setTimeout(delay);
        }
      } catch (error) {
        this.logWithTime(`Cycle error: ${error.message}`, "error");
        await setTimeout(30000); // Wait 30 seconds after error to recover
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

        // Log system status every 10 minutes
        this.logWithTime(
          `System status: ${this.successCount} successful scrapes, ${this.failedEvents.size} failed events, ${this.retryQueue.length} in retry queue`,
          "info"
        );

        // Clear old cooldowns
        const now = moment();
        for (const [eventId, cooldownTime] of this.cooldownEvents.entries()) {
          if (now.isAfter(cooldownTime)) {
            this.cooldownEvents.delete(eventId);
          }
        }
      } catch (error) {
        console.error("Error in monitoring task:", error);
      }

      // Run monitoring every 10 minutes
      await setTimeout(10 * 60 * 1000);
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
