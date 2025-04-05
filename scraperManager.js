import moment from "moment";
import { Event, ErrorLog, ConsecutiveGroup } from "./models/index.js";
import { ScrapeEvent, refreshHeaders } from "./scraper.js";
import { setTimeout } from "timers/promises";
import { cpus } from "os";
import chalk from "chalk";

// Configuration constants
const CONCURRENT_LIMIT = Math.max(1, Math.floor(cpus().length * 1.5)); // Dynamic based on CPU cores
const EVENT_REFRESH_INTERVAL = 60000; // 1 minute
const MAX_RETRIES = 3;
const SCRAPE_TIMEOUT = 30000; // 30 seconds
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
    this.eventUpdates = new Map(); // Track last update time for each event
  }

  logWithTime(message, type = "info", eventId = null) {
    const now = moment();
    const runningTime = moment.duration(now.diff(this.startTime));
    const formattedTime = now.format("YYYY-MM-DD HH:mm:ss");
    
    let coloredMessage = message;
    let prefix = "";
    
    switch (type) {
      case "success":
        prefix = chalk.green("✅");
        coloredMessage = chalk.green(message);
        break;
      case "error":
        prefix = chalk.red("❌");
        coloredMessage = chalk.red(message);
        break;
      case "warning":
        prefix = chalk.yellow("⚠️");
        coloredMessage = chalk.yellow(message);
        break;
      case "info":
        prefix = chalk.blue("ℹ️");
        coloredMessage = chalk.blue(message);
        break;
      default:
        prefix = "📝";
    }

    const eventPrefix = eventId ? `[Event: ${eventId}] ` : "";
    const runTimeStr = `${Math.floor(runningTime.asHours())}h ${runningTime.minutes()}m ${runningTime.seconds()}s`;
    
    console.log(
      `${prefix} [${formattedTime}] ${eventPrefix}${coloredMessage}\n` +
      `   Runtime: ${runTimeStr} | Active: ${this.activeJobs.size}/${CONCURRENT_LIMIT} | Success: ${this.successCount} | Failed: ${this.failedEvents.size} | Retry: ${this.retryQueue.length}`
    );
  }

  async logError(eventId, errorType, error, metadata = {}) {
    try {
      // Log to console first for immediate visibility
      const errorMsg = `ERROR [${errorType}]: ${error.message}`;
      this.logWithTime(errorMsg, "error", eventId);
      
      // Add stack trace for debugging
      console.error(chalk.red(`Stack trace for ${eventId}:`));
      console.error(chalk.red(error.stack));
      
      // Then log to database
      const event = await Event.findOne({ eventId }).select("url _id").lean();
      if (!event) {
        console.error(chalk.red(`No event found for ID: ${eventId}`));
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
      console.error(chalk.red("Error logging to database:"), err);
      console.error(chalk.red("Original error:"), error);
    }
  }

  async updateEventMetadata(eventId, scrapeResult) {
    const startTime = performance.now();
    const session = await Event.startSession();
    
    try {
      await session.withTransaction(async () => {
        const event = await Event.findOne({ Event_ID: eventId }).session(session);
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
          // Optimized seat comparison using hashing for better performance
          const existingGroups = await ConsecutiveGroup.find(
            { eventId },
            { section: 1, row: 1, seats: 1, "inventory.listPrice": 1 }
          ).lean();

          // Use string hashing for faster comparison
          const existingSeatsHash = new Set();
          existingGroups.forEach(g => {
            g.seats.forEach(s => {
              existingSeatsHash.add(`${g.section}|${g.row}|${s.number}|${s.price}`);
            });
          });

          const newSeatsHash = new Set();
          scrapeResult.forEach(g => {
            g.seats.forEach(s => {
              newSeatsHash.add(`${g.section}|${g.row}|${s}|${g.inventory.listPrice}`);
            });
          });

          // Fast hash comparison
          const needsUpdate = existingSeatsHash.size !== newSeatsHash.size || 
            [...existingSeatsHash].some(hash => !newSeatsHash.has(hash));

          if (needsUpdate) {
            this.logWithTime(`Updating seats for ${eventId} (changed: ${Math.abs(existingSeatsHash.size - newSeatsHash.size)})`, "info", eventId);
            
            // Bulk operations for better performance
            await ConsecutiveGroup.deleteMany({ eventId }).session(session);
            
            if (scrapeResult.length > 0) {
              const groupsToInsert = scrapeResult.map(group => ({
                eventId,
                section: group.section,
                row: group.row,
                seatCount: group.inventory.quantity,
                seatRange: `${Math.min(...group.seats)}-${Math.max(...group.seats)}`,
                seats: group.seats.map(seatNumber => ({
                  number: seatNumber.toString(),
                  inHandDate: event.inHandDate,
                  price: group.inventory.listPrice,
                })),
                inventory: {
                  ...group.inventory,
                  tickets: group.inventory.tickets.map(ticket => ({
                    ...ticket,
                    sellPrice: ticket.sellPrice * 1.25, // Apply markup
                  })),
                },
              }));

              // Use ordered: false for faster bulk inserts
              await ConsecutiveGroup.insertMany(groupsToInsert, { 
                session,
                ordered: false
              });
            }
          } else {
            this.logWithTime(`No seat changes for ${eventId}`, "info", eventId);
          }
        }

        // Final metadata update
        await Event.updateOne(
          { Event_ID: eventId },
          { $set: { "metadata.full": metadata } }
        ).session(session);
      });

      const duration = (performance.now() - startTime).toFixed(2);
      this.logWithTime(`Updated event ${eventId} in ${duration}ms`, "success", eventId);
      
      // Track the last update time for this event
      this.eventUpdates.set(eventId, Date.now());
      
    } catch (error) {
      await this.logError(eventId, "DATABASE_ERROR", error);
      throw error;
    } finally {
      session.endSession();
    }
  }

  async acquireSemaphore() {
    const startWait = Date.now();
    while (this.concurrencySemaphore <= 0) {
      await setTimeout(100);
      // Log if waiting too long
      if (Date.now() - startWait > 5000) {
        this.logWithTime(`Waiting for concurrency semaphore (${this.concurrencySemaphore}/${CONCURRENT_LIMIT})`, "warning");
        break;
      }
    }
    this.concurrencySemaphore--;
  }

  releaseSemaphore() {
    this.concurrencySemaphore++;
  }

  async scrapeEvent(eventId, retryCount = 0) {
    // Check if we need to enforce the 1-minute gap
    const lastUpdate = this.eventUpdates.get(eventId);
    if (lastUpdate) {
      const elapsed = Date.now() - lastUpdate;
      if (elapsed < EVENT_REFRESH_INTERVAL) {
        const waitTime = EVENT_REFRESH_INTERVAL - elapsed;
        this.logWithTime(`Waiting ${waitTime}ms to maintain 1-minute gap for ${eventId}`, "info", eventId);
        await setTimeout(waitTime);
      }
    }
    
    await this.acquireSemaphore();
    this.activeJobs.set(eventId, moment());
    
    try {
      this.logWithTime(`Scraping ${eventId} (Attempt ${retryCount + 1})`, "info", eventId);

      const event = await Event.findOne({ Event_ID: eventId }).select("Skip_Scraping inHandDate").lean();
      if (!event) {
        throw new Error(`Event ${eventId} not found`);
      }

      if (event.Skip_Scraping) {
        this.logWithTime(`Skipping ${eventId} (flagged in database)`, "info", eventId);
        return true;
      }

      // Get fresh headers for this scrape
      const headers = await refreshHeaders(eventId);
      if (!headers) {
        throw new Error("Header refresh failed");
      }

      // Use Promise.race for timeout handling
      const result = await Promise.race([
        ScrapeEvent({ eventId, headers }),
        setTimeout(SCRAPE_TIMEOUT).then(() => {
          throw new Error(`Scrape timed out after ${SCRAPE_TIMEOUT}ms`);
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
        this.logWithTime(`Queued for retry: ${eventId}`, "warning", eventId);
      } else {
        this.logWithTime(`Max retries exceeded for ${eventId}`, "error", eventId);
      }
      return false;
    } finally {
      this.activeJobs.delete(eventId);
      this.releaseSemaphore();
    }
  }

  async processBatch(eventIds) {
    const startTime = performance.now();
    this.logWithTime(`Starting batch of ${eventIds.length} events`, "info");
    
    const results = await Promise.allSettled(
      eventIds.map(eventId => this.scrapeEvent(eventId))
    );
    
    const succeeded = results.filter(r => r.status === 'fulfilled' && r.value === true).length;
    const failed = results
      .filter(r => r.status === 'rejected' || r.value === false)
      .map((r, i) => ({ 
        eventId: eventIds[i], 
        error: r.status === 'rejected' ? r.reason : 'Scrape failed' 
      }));
    
    const duration = (performance.now() - startTime).toFixed(2);
    this.logWithTime(
      `Batch completed in ${duration}ms - Success: ${succeeded}, Failed: ${failed.length}`,
      failed.length ? "warning" : "success"
    );
    
    return { succeeded, failed };
  }

  async processRetryQueue() {
    if (this.retryQueue.length === 0) return;
    
    this.logWithTime(`Processing retry queue (${this.retryQueue.length} events)`, "info");
    
    while (this.retryQueue.length > 0 && this.isRunning) {
      const batch = this.retryQueue.splice(0, Math.min(BATCH_SIZE, this.retryQueue.length));
      const { succeeded, failed } = await this.processBatch(batch.map(job => job.eventId));
      
      this.logWithTime(`Retry batch - Success: ${succeeded}, Still failed: ${failed.length}`, 
        failed.length ? "warning" : "success");
      
      if (this.retryQueue.length > 0) {
        await setTimeout(1000); // Brief pause between batches
      }
    }
  }

  async getEventsToProcess() {
    const now = moment();
    const priorityCutoff = now.clone().subtract(PRIORITY_UPDATE_WINDOW, 'ms');
    
    // Get failed events first
    const failedEventIds = [...this.failedEvents];
    
    // Get priority events (need immediate attention)
    const priorityEvents = await Event.find({
      $or: [
        { Last_Updated: { $lt: priorityCutoff.toDate() } },
        { Event_ID: { $in: failedEventIds } }
      ],
      Skip_Scraping: { $ne: true }
    })
    .sort({ Last_Updated: 1 })
    .limit(BATCH_SIZE)
    .select("Event_ID")
    .lean();

    // Clear failed events that we're now processing
    priorityEvents.forEach(e => this.failedEvents.delete(e.Event_ID));

    // If we still have capacity, get regular events
    const remainingCapacity = BATCH_SIZE - priorityEvents.length;
    let regularEvents = [];
    
    if (remainingCapacity > 0) {
      regularEvents = await Event.find({
        Skip_Scraping: { $ne: true },
        Event_ID: { $nin: priorityEvents.map(e => e.Event_ID) }
      })
      .sort({ Last_Updated: 1 })
      .limit(remainingCapacity)
      .select("Event_ID")
      .lean();
    }

    const allEvents = [...priorityEvents, ...regularEvents].map(e => e.Event_ID);
    
    // Filter out events that have been updated within the last minute
    const now2 = Date.now();
    return allEvents.filter(eventId => {
      const lastUpdate = this.eventUpdates.get(eventId);
      return !lastUpdate || (now2 - lastUpdate) >= EVENT_REFRESH_INTERVAL;
    });
  }

  async startContinuousScraping() {
    this.isRunning = true;
    this.logWithTime(`Starting optimized scraper (v2.0)`, "info");
    this.logWithTime(`Concurrency: ${CONCURRENT_LIMIT}, Batch size: ${BATCH_SIZE}, Event interval: ${EVENT_REFRESH_INTERVAL}ms`, "info");

    while (this.isRunning) {
      const cycleStart = performance.now();
      
      try {
        const eventIds = await this.getEventsToProcess();
        
        if (eventIds.length === 0) {
          this.logWithTime("No events to process right now, waiting...", "info");
          await setTimeout(5000); // Short wait when no events to process
          continue;
        }

        this.logWithTime(`Processing batch of ${eventIds.length} events`, "info");
        await this.processBatch(eventIds);
        
        // Process retry queue after each main batch
        if (this.retryQueue.length > 0) {
          await this.processRetryQueue();
        }

        const cycleTime = performance.now() - cycleStart;
        const delay = Math.max(0, 5000 - cycleTime); // At least 5 seconds between cycles
        
        if (delay > 0) {
          await setTimeout(delay);
        }
      } catch (error) {
        this.logWithTime(`Cycle error: ${error.message}`, "error");
        console.error(chalk.red(error.stack));
        await setTimeout(5000); // Wait 5 seconds after error
      }
    }
  }

  stop() {
    this.isRunning = false;
    this.logWithTime("Stopping scraper", "warning");
  }
  
  getStatus() {
    return {
      running: this.isRunning,
      activeJobs: [...this.activeJobs.entries()].map(([id, time]) => ({
        id,
        runningFor: moment().diff(time, 'seconds')
      })),
      successCount: this.successCount,
      failedCount: this.failedEvents.size,
      retryQueueSize: this.retryQueue.length,
      uptime: moment.duration(moment().diff(this.startTime)).humanize()
    };
  }
}

const scraperManager = new ScraperManager();
export default scraperManager;