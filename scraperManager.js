import moment from "moment";
import { Event, ErrorLog, ConsecutiveGroup } from "./models/index.js";
import { ScrapeEvent, refreshHeaders } from "./scraper.js";
import { setTimeout } from "timers/promises";
import { cpus } from "os";

const CONCURRENT_LIMIT = Math.max(1, Math.floor(cpus().length * 1.5)); // Dynamic based on CPU cores
const EVENT_REFRESH_INTERVAL = 60000; // 1 minute
const MAX_RETRIES = 3;
const SCRAPE_TIMEOUT = 30000; // Reduced timeout to 30 seconds
const BATCH_SIZE = CONCURRENT_LIMIT * 2; // Process events in batches
const PRIORITY_UPDATE_WINDOW = 5000; // 5-second window for priority updates

class ScraperManager {
  constructor() {
    this.startTime = moment();
    this.lastSuccessTime = null;
    this.successCount = 0;
    this.activeJobs = new Map();
    this.failedEvents = new Set();
    this.retryQueue = [];
    this.isRunning = false;
    this.headers = null;
    this.eventUpdateTimestamps = new Map();
    this.priorityQueue = new Set(); // For events that need immediate attention
    this.concurrencySemaphore = CONCURRENT_LIMIT; // Simple concurrency control
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

    console.log(
      `${statusEmoji} [${formattedTime}] ${message}\n` +
        `   Runtime: ${Math.floor(
          runningTime.asHours()
        )}h ${runningTime.minutes()}m ${runningTime.seconds()}s\n` +
        `   Active: ${this.activeJobs.size}/${CONCURRENT_LIMIT}, Success: ${this.successCount}, Failed: ${this.failedEvents.size}, Retry Queue: ${this.retryQueue.length}`
    );
  }

  async logError(eventId, errorType, error, metadata = {}) {
    try {
      const event = await Event.findOne({ eventId }).select("url _id").lean();
      if (!event) {
        console.error(`No event found for ID: ${eventId}`);
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
          throw new Error(`Event ${eventId} not found in database`);
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

            await ConsecutiveGroup.insertMany(groupsToInsert, { session });
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

  async scrapeEvent(eventId, retryCount = 0) {
    await this.acquireSemaphore();
    this.activeJobs.set(eventId, moment());

    try {
      this.logWithTime(`Scraping ${eventId} (Attempt ${retryCount + 1})`);

      const event = await Event.findOne({ Event_ID: eventId })
        .select("Skip_Scraping inHandDate")
        .lean();
      if (!event) {
        throw new Error(`Event ${eventId} not found`);
      }

      if (event.Skip_Scraping) {
        this.logWithTime(`Skipping ${eventId} (flagged)`, "info");
        return true;
      }

      const headers = await refreshHeaders(eventId);
      if (!headers) {
        throw new Error("Header refresh failed");
      }

      const result = await Promise.race([
        ScrapeEvent({ eventId, headers }),
        setTimeout(SCRAPE_TIMEOUT).then(() => {
          throw new Error("Scrape timed out");
        }),
      ]);

      if (!result) throw new Error("Empty scrape result");

      await this.updateEventMetadata(eventId, result);
      this.successCount++;
      this.lastSuccessTime = moment();
      this.eventUpdateTimestamps.set(eventId, moment());
      return true;
    } catch (error) {
      this.failedEvents.add(eventId);
      await this.logError(eventId, "SCRAPE_ERROR", error, { retryCount });

      if (retryCount < MAX_RETRIES) {
        this.retryQueue.push({ eventId, retryCount: retryCount + 1 });
        this.logWithTime(`Queued for retry: ${eventId}`, "warning");
      } else {
        this.logWithTime(`Max retries exceeded for ${eventId}`, "error");
      }
      return false;
    } finally {
      this.activeJobs.delete(eventId);
      this.releaseSemaphore();
    }
  }

  async processBatch(eventIds) {
    const results = await Promise.allSettled(
      eventIds.map((eventId) => this.scrapeEvent(eventId))
    );

    const failed = results
      .filter((r) => r.status === "rejected")
      .map((r, i) => ({ eventId: eventIds[i], error: r.reason }));

    return { failed };
  }

  async processRetryQueue() {
    while (this.retryQueue.length > 0 && this.isRunning) {
      const batch = this.retryQueue.splice(0, BATCH_SIZE);
      await this.processBatch(batch.map((job) => job.eventId));
      await setTimeout(1000); // Brief pause between batches
    }
  }

  async getEventsToProcess() {
    const now = moment();
    const priorityCutoff = now.clone().subtract(PRIORITY_UPDATE_WINDOW, "ms");

    // Get priority events first (recently updated or failed)
    const priorityEvents = await Event.find({
      $or: [
        { Last_Updated: { $lt: priorityCutoff.toDate() } },
        { Event_ID: { $in: [...this.failedEvents] } },
      ],
      Skip_Scraping: { $ne: true },
    })
      .sort({ Last_Updated: 1 })
      .limit(BATCH_SIZE)
      .select("Event_ID")
      .lean();

    // If we still have capacity, get regular events
    const remainingCapacity = BATCH_SIZE - priorityEvents.length;
    let regularEvents = [];

    if (remainingCapacity > 0) {
      regularEvents = await Event.find({
        Skip_Scraping: { $ne: true },
        Event_ID: { $nin: priorityEvents.map((e) => e.Event_ID) },
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
    this.logWithTime("Starting optimized scraper");
    this.logWithTime(
      `Concurrency: ${CONCURRENT_LIMIT}, Batch size: ${BATCH_SIZE}`
    );

    while (this.isRunning) {
      const cycleStart = performance.now();

      try {
        const eventIds = await this.getEventsToProcess();
        if (eventIds.length === 0) {
          await setTimeout(EVENT_REFRESH_INTERVAL);
          continue;
        }

        this.logWithTime(`Processing batch of ${eventIds.length} events`);
        await this.processBatch(eventIds);
        await this.processRetryQueue();

        const cycleTime = performance.now() - cycleStart;
        const delay = Math.max(0, EVENT_REFRESH_INTERVAL - cycleTime);

        if (delay > 0) {
          await setTimeout(delay);
        }
      } catch (error) {
        this.logWithTime(`Cycle error: ${error.message}`, "error");
        await setTimeout(5000); // Wait 5 seconds after error
      }
    }
  }

  stop() {
    this.isRunning = false;
    this.logWithTime("Stopping scraper");
  }
}

const scraperManager = new ScraperManager();
export default scraperManager;
