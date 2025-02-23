import moment from "moment";
import { Event, ErrorLog, ConsecutiveGroup } from "./models/index.js";
import { ScrapeEvent, refreshHeaders } from "./scraper.js";

const CONCURRENT_LIMIT = 1;
const EVENT_REFRESH_INTERVAL = 60000; // 1 minute
const MAX_RETRIES = 3;

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
        eventId: event._id, // Store both _id and eventId
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
      const event = await Event.findOne({ eventId: eventId });
      if (!event) {
        throw new Error(`Event ${eventId} not found in database`);
      }

      const previousTicketCount = event.availableSeats || 0;
      const currentTicketCount = scrapeResult.length;

      // Update metadata regardless of seat changes
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

      // Always update event metadata
      await Event.findOneAndUpdate(
        { eventId: eventId },
        {
          $set: {
            availableSeats: currentTicketCount,
            lastUpdated: new Date(),
            metadata,
          },
        },
        { new: true }
      );

      // Only update consecutive groups if there are changes in seats
      if (scrapeResult && scrapeResult.length > 0) {
        // Get current consecutive groups
        const currentGroups = await ConsecutiveGroup.find({
          eventId: event.eventId,
        });

        // Create a hash of current seats for comparison
        const currentSeatsHash = new Set(
          currentGroups.flatMap((group) =>
            group.seats.map(
              (seat) =>
                `${group.section}-${group.row}-${seat.number}-${seat.price}`
            )
          )
        );

        // Create hash of new seats
        const newSeatsHash = new Set(
          scrapeResult.flatMap((group) =>
            group.seats.map(
              (seatNumber) =>
                `${group.section}-${group.row}-${seatNumber}-${group.inventory.listPrice}`
            )
          )
        );

        // Check if there are any differences in seats
        const hasChanges =
          [...currentSeatsHash].some((seat) => !newSeatsHash.has(seat)) ||
          [...newSeatsHash].some((seat) => !currentSeatsHash.has(seat));

        if (hasChanges) {
          // Delete existing groups only if there are changes
          await ConsecutiveGroup.deleteMany({ eventId: event.eventId });

          // Create new consecutive groups
          const consecutiveGroups = scrapeResult.map((group) => {
            const seats = group.seats.map((seatNumber) => ({
              number: seatNumber.toString(),
              price: group.inventory.listPrice,
            }));

            return {
              eventId: event.eventId,
              section: group.section,
              row: group.row,
              seatCount: group.inventory.quantity,
              seatRange: `${Math.min(...group.seats)}-${Math.max(
                ...group.seats
              )}`,
              seats: seats,
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
      throw error; // Propagate error to trigger retry
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

      const event = await Event.findOne({ eventId: eventId });
      if (!event) {
        throw new Error(`Event ${eventId} not found in database`);
      }

      if (event.skip_scraping) {
        this.logWithTime(
          `Skipping event ${eventId} due to skip_scraping flag`,
          "info"
        );
        return true;
      }

      this.headers = await refreshHeaders(eventId);
      if (!this.headers) {
        throw new Error("Failed to refresh headers");
      }

      const result = await ScrapeEvent({
        eventId: event.eventId,
        headers: this.headers,
      });

      if (!result) {
        throw new Error("No data returned from scraper");
      }

      await this.updateEventMetadata(eventId, result);
      this.successCount++;
      this.lastSuccessTime = moment();
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
    console.log("\n" + "=".repeat(50));
    this.logWithTime("Continuous Scraper Manager Starting");
    this.logWithTime(`Event Refresh Interval: ${EVENT_REFRESH_INTERVAL}ms`);
    this.logWithTime(`Retry Attempts: ${MAX_RETRIES}`);
    console.log("=".repeat(50) + "\n");

    while (this.isRunning) {
      try {
        const events = await Event.find({})
          .sort({ lastUpdated: 1 })
          .select("eventId");

        this.logWithTime(`Total Events to Process: ${events.length}`);

        for (const event of events) {
          if (!this.isRunning) break;

          await this.scrapeEvent(event.eventId);
          await new Promise((resolve) =>
            setTimeout(resolve, EVENT_REFRESH_INTERVAL)
          );
        }

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

export default ScraperManager;
