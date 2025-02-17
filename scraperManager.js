import { ScrapeEvent, refreshHeaders } from "./scraper.js";
import moment from "moment";
import { writeFile } from "fs/promises";
import pLimit from "p-limit";

// Configuration
const CONCURRENT_LIMIT = 500;
const RETRY_DELAY = 1000;
const MAX_RETRIES = 3;

// Controller Configuration
const CONTROLLER = {
  FORCE_HEADER_REFRESH_ERROR_COUNT: 10, // Force header refresh after this many consecutive errors
  REGULAR_HEADER_REFRESH_CYCLES: 50, // Regular header refresh after these many cycles
  CONTINUOUS_MODE: true, // Enable continuous operation
  CYCLE_DELAY_MS: 5000, // Delay between cycles (5 seconds)
};

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

    // New tracking variables
    this.currentCycle = 0;
    this.consecutiveErrors = 0;
    this.lastHeaderRefresh = moment();
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
      `   Cycle: ${this.currentCycle}, Active Jobs: ${this.activeJobs.size}, Success: ${this.successCount}, Failed: ${this.failedEvents.size}`
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
        this.consecutiveErrors = 0; // Reset error counter on success
        this.activeJobs.delete(eventId);
        return true;
      }

      throw new Error("No data returned");
    } catch (error) {
      this.logWithTime(
        `Failed to scrape event ${eventId}: ${error.message}`,
        "error"
      );

      this.consecutiveErrors++;

      // Check if we need to force header refresh due to errors
      if (
        this.consecutiveErrors >= CONTROLLER.FORCE_HEADER_REFRESH_ERROR_COUNT
      ) {
        this.logWithTime(
          "Forcing header refresh due to consecutive errors",
          "warning"
        );
        await this.refreshAllHeaders();
        this.consecutiveErrors = 0;
      }

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
      this.lastHeaderRefresh = moment();
      this.logWithTime("Headers refresh completed", "success");
    } catch (error) {
      this.logWithTime(
        `Error during headers refresh: ${error.message}`,
        "error"
      );
    }
  }

  async runCycle() {
    this.currentCycle++;
    this.logWithTime(`Starting cycle ${this.currentCycle}`);

    // Check if regular header refresh is needed
    if (this.currentCycle % CONTROLLER.REGULAR_HEADER_REFRESH_CYCLES === 0) {
      this.logWithTime("Performing regular header refresh");
      await this.refreshAllHeaders();
    }

    // Start scraping
    const scrapePromises = this.events.map((eventId) =>
      this.limit(() => this.scrapeEvent(eventId))
    );

    await Promise.all(scrapePromises);

    // Process any retries
    while (this.retryQueue.length > 0) {
      await this.processRetryQueue();
    }

    // Cycle summary
    this.logWithTime(`Cycle ${this.currentCycle} completed`);
    this.logWithTime(`Successful in this cycle: ${this.successCount}`);
    this.logWithTime(`Failed in this cycle: ${this.failedEvents.size}`);
  }

  async start() {
    console.log("\n" + "=".repeat(50));
    this.logWithTime("Continuous Scraper Manager Starting");
    this.logWithTime(`Total Events: ${this.events.length}`);
    this.logWithTime(`Concurrent Limit: ${CONCURRENT_LIMIT}`);
    this.logWithTime(`Retry Attempts: ${MAX_RETRIES}`);
    this.logWithTime("Controller Settings:");
    this.logWithTime(
      `- Force Header Refresh After: ${CONTROLLER.FORCE_HEADER_REFRESH_ERROR_COUNT} errors`
    );
    this.logWithTime(
      `- Regular Header Refresh Every: ${CONTROLLER.REGULAR_HEADER_REFRESH_CYCLES} cycles`
    );
    this.logWithTime(`- Continuous Mode: ${CONTROLLER.CONTINUOUS_MODE}`);
    console.log("=".repeat(50) + "\n");

    try {
      while (CONTROLLER.CONTINUOUS_MODE) {
        await this.runCycle();

        // Reset cycle statistics
        this.successCount = 0;
        this.failedEvents.clear();

        // Delay between cycles
        await new Promise((resolve) =>
          setTimeout(resolve, CONTROLLER.CYCLE_DELAY_MS)
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

];
const manager = new ScraperManager(events);
manager.start();
