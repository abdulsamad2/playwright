import { ScrapeEvent, refreshHeaders } from "./scraper.js";
import moment from "moment";
import { writeFile } from "fs/promises";
import pLimit from "p-limit";

// Configuration
const CONCURRENT_LIMIT = 500;
const RETRY_DELAY = 1000;
const MAX_RETRIES = 3;
const REFRESH_INTERVAL = 6;

class ScraperManager {
  constructor(events) {
    this.events = events;
    this.startTime = moment();
    this.lastSuccessTime = null;
    this.successCount = 0;
    this.totalAttempts = 0;
    this.activeJobs = new Map();
    this.limit = pLimit(CONCURRENT_LIMIT);
    this.results = new Map();
    this.failedEvents = new Set();
    this.retryQueue = [];
  }

  logWithTime(message, type = "info") {
    const now = moment();
    const runningTime = moment.duration(now.diff(this.startTime));
    const formattedTime = now.format("YYYY-MM-DD HH:mm:ss");

    let statusEmoji = "📝";
    switch (type) {
      case "success":
        statusEmoji = "✅";
        break;
      case "error":
        statusEmoji = "❌";
        break;
      case "warning":
        statusEmoji = "⚠️";
        break;
      case "info":
        statusEmoji = "ℹ️";
        break;
    }

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

  async saveScrapeResult(eventId, result) {
    const timestamp = moment().format("YYYY-MM-DD_HH-mm-ss");
    const filename = `result_${eventId}_${timestamp}.json`;
    await writeFile(filename, JSON.stringify(result, null, 2), "utf-8");
    this.logWithTime(
      `Saved result for event ${eventId} to ${filename}`,
      "success"
    );
    return filename;
  }

  async scrapeEvent(eventId, retryCount = 0) {
    this.activeJobs.set(eventId, moment());

    try {
      this.logWithTime(
        `Starting scrape for event ${eventId} (Attempt ${
          retryCount + 1
        }/${MAX_RETRIES})`
      );
      const result = await ScrapeEvent({ eventId });

      if (result) {
        await this.saveScrapeResult(eventId, result);
        this.results.set(eventId, result);
        this.successCount++;
        this.lastSuccessTime = moment();
        this.activeJobs.delete(eventId);
        return true;
      }

      throw new Error("No data returned");
    } catch (error) {
      this.logWithTime(
        `Failed to scrape event ${eventId}: ${error.message}`,
        "error"
      );

      if (retryCount < MAX_RETRIES - 1) {
        this.logWithTime(`Queuing retry for event ${eventId}`, "warning");
        this.retryQueue.push({ eventId, retryCount: retryCount + 1 });
      } else {
        this.failedEvents.add(eventId);
        this.logWithTime(`Max retries reached for event ${eventId}`, "error");
      }

      this.activeJobs.delete(eventId);
      return false;
    }
  }

  async processRetryQueue() {
    while (this.retryQueue.length > 0) {
      const { eventId, retryCount } = this.retryQueue.shift();
      await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY));
      await this.limit(() => this.scrapeEvent(eventId, retryCount));
    }
  }

  async refreshAllHeaders() {
    this.logWithTime("Refreshing headers for all active scrapers...");
    try {
      const refreshPromises = Array.from(this.activeJobs.keys()).map(
        (eventId) =>
          refreshHeaders(eventId).catch((error) => {
            this.logWithTime(
              `Failed to refresh headers for ${eventId}: ${error.message}`,
              "error"
            );
            return null;
          })
      );
      await Promise.all(refreshPromises);
      this.logWithTime("Headers refresh completed", "success");
    } catch (error) {
      this.logWithTime(
        `Error during headers refresh: ${error.message}`,
        "error"
      );
    }
  }

  async start() {
    console.log("\n" + "=".repeat(50));
    this.logWithTime("Concurrent Scraper Manager Starting");
    this.logWithTime(`Total Events: ${this.events.length}`);
    this.logWithTime(`Concurrent Limit: ${CONCURRENT_LIMIT}`);
    this.logWithTime(`Retry Attempts: ${MAX_RETRIES}`);
    console.log("=".repeat(50) + "\n");

    // Setup periodic header refresh
    const refreshInterval = setInterval(() => {
      if (this.activeJobs.size > 0) {
        this.refreshAllHeaders();
      }
    }, REFRESH_INTERVAL * 60 * 1000);

    // Setup retry queue processor
    const retryProcessor = setInterval(() => {
      if (this.retryQueue.length > 0) {
        this.processRetryQueue();
      }
    }, RETRY_DELAY);

    try {
      // Start initial scraping
      const scrapePromises = this.events.map((eventId) =>
        this.limit(() => this.scrapeEvent(eventId))
      );

      await Promise.all(scrapePromises);

      // Process any remaining retries
      while (this.retryQueue.length > 0) {
        await this.processRetryQueue();
      }

      clearInterval(refreshInterval);
      clearInterval(retryProcessor);

      // Final summary
      console.log("\n" + "=".repeat(50));
      this.logWithTime("Scraping Completed");
      this.logWithTime(`Total Events: ${this.events.length}`);
      this.logWithTime(`Successful: ${this.successCount}`);
      this.logWithTime(`Failed: ${this.failedEvents.size}`);
      this.logWithTime(
        `Success Rate: ${(
          (this.successCount / this.events.length) *
          100
        ).toFixed(2)}%`
      );
      console.log("=".repeat(50) + "\n");

      // Save failed events list if any
      if (this.failedEvents.size > 0) {
        const failedEventsFile = `failed_events_${moment().format(
          "YYYY-MM-DD_HH-mm-ss"
        )}.json`;
        await writeFile(
          failedEventsFile,
          JSON.stringify(Array.from(this.failedEvents), null, 2)
        );
        this.logWithTime(
          `Failed events list saved to ${failedEventsFile}`,
          "warning"
        );
      }
    } catch (error) {
      this.logWithTime(
        `Fatal error in scraper manager: ${error.message}`,
        "error"
      );
      console.error(error);
    }
  }
}

// Example usage:
const events = [
  "0A00614FC8B22987",
  "0B00618E56F70A56",
  "0B00618E4B940977",
  "D006228B3121C00",
];

const manager = new ScraperManager(events);
manager.start();
