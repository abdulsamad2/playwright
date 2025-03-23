import moment from "moment";
import { Event, ErrorLog, ConsecutiveGroup } from "./models/index.js";
import { ScrapeEvent, refreshHeaders } from "./scraper.js";

const CONCURRENT_LIMIT = 500; // Adjust based on system capacity
const EVENT_REFRESH_INTERVAL = 60000; // 1 minute
const MAX_RETRIES = 3;
const SCRAPE_TIMEOUT = 60000; // 1 minute timeout per event

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
    this.eventUpdateTimestamps = new Map(); // Track last update time for each event
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

    console.log(`${statusEmoji} [${formattedTime}] ${message}`);
    console.log(
      `   Runtime: ${Math.floor(
        runningTime.asHours()
      )}h ${runningTime.minutes()}m ${runningTime.seconds()}s`
    );
    console.log(
      `   Active Jobs: ${this.activeJobs.size}, Success: ${this.successCount}, Failed: ${this.failedEvents.size}`
    );
  }

  async logError(eventId, errorType, error, metadata = {}) {
    try {
      const event = await Event.findOne({ eventId: eventId });
      if (!event) {
        console.error(`No event found for ID: ${eventId}`);
        return;
      }

      await ErrorLog.create({
        eventUrl: event.url,
        eventId: event._id,
        externalEventId: event.eventId,
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
    try {
      const event = await Event.findOne({ Event_ID: eventId });
      if (!event) {
        throw new Error(`Event ${eventId} not found in database`);
      }

      const previousTicketCount = event.Available_Seats || 0;
      const currentTicketCount = scrapeResult.length;

      const metadata = {
        lastUpdate: moment().format("YYYY-MM-DD HH:mm:ss"),
        iterationNumber: (event.metadata?.iterationNumber || 0) + 1,
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

      await Event.findOneAndUpdate(
        { Event_ID: eventId },
        {
          $set: {
            Available_Seats: currentTicketCount,
            Last_Updated: new Date(),
            metadata,
          },
        },
        { new: true }
      );

      if (scrapeResult && scrapeResult.length > 0) {
        const currentGroups = await ConsecutiveGroup.find({
          eventId: event.Event_ID,
        });

        const currentSeatsHash = new Set(
          currentGroups.flatMap((group) =>
            group.seats.map(
              (seat) =>
                `${group.section}-${group.row}-${seat.number}-${seat.price}`
            )
          )
        );

        const newSeatsHash = new Set(
          scrapeResult.flatMap((group) =>
            group.seats.map(
              (seatNumber) =>
                `${group.section}-${group.row}-${seatNumber}-${group.inventory.listPrice}`
            )
          )
        );

        const hasChanges =
          [...currentSeatsHash].some((seat) => !newSeatsHash.has(seat)) ||
          [...newSeatsHash].some((seat) => !currentSeatsHash.has(seat));

        if (hasChanges) {
          await ConsecutiveGroup.deleteMany({ eventId: event.Event_ID });

          const consecutiveGroups = scrapeResult.map((group) => {
            const seats = group.seats.map((seatNumber) => ({
              number: seatNumber.toString(),
              inHandDate: event.inHandDate,
              price: group.inventory.listPrice,
            }));

            const increasedCost = group.inventory.cost;

            return {
              eventId: event.Event_ID,
              section: group.section,
              row: group.row,
              seatCount: group.inventory.quantity,
              seatRange: `${Math.min(...group.seats)}-${Math.max(
                ...group.seats
              )}`,
              seats: seats,
              inventory: {
                quantity: group.inventory.quantity,
                section: group.section,
                hideSeatNumbers: group.inventory.hideSeatNumbers,
                row: group.row,
                cost: increasedCost,
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
                  sellPrice: ticket.sellPrice ** 1.25,
                  stockType: ticket.stockType,
                  eventId: ticket.eventId,
                  accountId: ticket.accountId,
                  status: ticket.status,
                  auditNote: ticket.auditNote,
                })),
              },
            };
          });

          await ConsecutiveGroup.insertMany(consecutiveGroups);
          this.logWithTime(
            `Updated consecutive groups for event ${eventId}`,
            "success"
          );
        } else {
          this.logWithTime(
            `No seat changes detected for event ${eventId}`,
            "info"
          );
        }
      }

      this.logWithTime(`Successfully updated event ${eventId}`, "success");
    } catch (error) {
      console.error("Error updating event metadata:", error);
      await this.logError(eventId, "DATABASE_ERROR", error);
      throw error;
    }
  }

  async scrapeEvent(eventId, retryCount = 0) {
    this.activeJobs.set(eventId, moment());
    try {
      this.logWithTime(
        `Starting scrape for event ${eventId} (Attempt ${
          retryCount + 1
        }/${MAX_RETRIES})`
      );

      const event = await Event.findOne({ Event_ID: eventId });
      if (!event) {
        throw new Error(`Event ${eventId} not found in database`);
      }

      if (event.Skip_Scraping) {
        this.logWithTime(
          `Skipping event ${eventId} due to Skip_Scraping flag`,
          "info"
        );
        return true;
      }

      this.headers = await refreshHeaders(eventId);
      if (!this.headers) {
        throw new Error("Failed to refresh headers");
      }

      const result = await Promise.race([
        ScrapeEvent({
          eventId: event.Event_ID,
          headers: this.headers,
        }),
        new Promise((_, reject) =>
          setTimeout(
            () => reject(new Error("Scrape timed out")),
            SCRAPE_TIMEOUT
          )
        ),
      ]);

      if (!result) {
        throw new Error("No data returned from scraper");
      }

      await this.updateEventMetadata(eventId, result);
      this.successCount++;
      this.lastSuccessTime = moment();
      this.eventUpdateTimestamps.set(eventId, moment()); // Update last update time
      this.logWithTime(`Successfully scraped event ${eventId}`, "success");
      return true;
    } catch (error) {
      this.failedEvents.add(eventId);
      await this.logError(eventId, "SCRAPE_ERROR", error, {
        retryCount,
        iterationNumber: retryCount + 1,
      });

      if (retryCount < MAX_RETRIES) {
        this.retryQueue.push({ eventId, retryCount: retryCount + 1 });
        this.logWithTime(
          `Added event ${eventId} to retry queue (attempt ${retryCount + 1})`,
          "warning"
        );
      } else {
        this.logWithTime(
          `Failed to scrape event ${eventId} after ${MAX_RETRIES} attempts`,
          "error"
        );
      }
      return false;
    } finally {
      this.activeJobs.delete(eventId);
    }
  }

  async processRetryQueue() {
    while (this.retryQueue.length > 0 && this.isRunning) {
      const { eventId, retryCount } = this.retryQueue.shift();
      this.logWithTime(`Processing retry for event ${eventId}`, "info");
      await this.scrapeEvent(eventId, retryCount);
      await new Promise((resolve) =>
        setTimeout(resolve, EVENT_REFRESH_INTERVAL)
      );
    }
  }

  async startContinuousScraping() {
    this.isRunning = true;
    console.log(`Scraper started. isRunning: ${this.isRunning}`); // Debug log    console.log("\n" + "=".repeat(50));
    this.logWithTime("Continuous Scraper Manager Starting");
    this.logWithTime(`Event Refresh Interval: ${EVENT_REFRESH_INTERVAL}ms`);
    this.logWithTime(`Retry Attempts: ${MAX_RETRIES}`);
    console.log("=".repeat(50) + "\n");

    while (this.isRunning) {
      try {
        const events = await Event.find({})
          .sort({ lastUpdated: 1 })
          .select("Event_ID");
        this.logWithTime(`Total Events to Process: ${events.length}`);

        const scrapePromises = [];
        for (const event of events) {
          if (!this.isRunning) break;

          const lastUpdateTime = this.eventUpdateTimestamps.get(event.Event_ID);
          const timeSinceLastUpdate = lastUpdateTime
            ? moment().diff(lastUpdateTime, "milliseconds")
            : EVENT_REFRESH_INTERVAL;

          if (timeSinceLastUpdate >= EVENT_REFRESH_INTERVAL) {
            scrapePromises.push(this.scrapeEvent(event.Event_ID));
          }
        }

        await Promise.all(scrapePromises);
        await this.processRetryQueue();

        if (this.failedEvents.size > 0) {
          this.logWithTime(
            `Failed events in this cycle: ${Array.from(this.failedEvents).join(
              ", "
            )}`,
            "warning"
          );
          this.failedEvents.clear();
        }

        await new Promise((resolve) =>
          setTimeout(resolve, EVENT_REFRESH_INTERVAL)
        );
      } catch (error) {
        this.logWithTime(
          `Fatal error in scraper manager: ${error.message}`,
          "error"
        );
        console.error(error);
        await new Promise((resolve) =>
          setTimeout(resolve, EVENT_REFRESH_INTERVAL)
        );
      }
    }
  }

  stop() {
    this.isRunning = false;
    this.logWithTime("Stopping scraper manager");
  }
}
const scraperManager = new ScraperManager();

export default scraperManager;
