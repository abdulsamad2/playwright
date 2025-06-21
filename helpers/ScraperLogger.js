import moment from "moment";
import { ErrorLog } from "../models/index.js";
import config from "../config/scraperConfig.js";

/**
 * ScraperLogger handles all logging for the scraper system
 */
class ScraperLogger {
  constructor(scraperState) {
    this.state = scraperState;
    this.startTime = moment();
  }

  /**
   * Log a message with timestamp and status
   */
  logWithTime(message, type = "info") {
    const now = moment();
    const runningTime = moment.duration(now.diff(this.startTime));
    const formattedTime = now.format("YYYY-MM-DD HH:mm:ss");

    const statusEmoji = {
      success: "✅",
      error: "❌",
      warning: "⚠️",
      info: "ℹ️",
    }[type] || "📝";

    // Only log detailed stats in info, success, or error messages to reduce noise
    if (["info", "success", "error"].includes(type)) {
      console.log(
        `${statusEmoji} [${formattedTime}] ${message}\n` +
        `   Runtime: ${Math.floor(runningTime.asHours())}h ${runningTime.minutes()}m ${runningTime.seconds()}s\n` +
        `   Active: ${this.state.activeJobs.size}/${config.CONCURRENT_LIMIT}, ` +
        `Success: ${this.state.successCount}, ` +
        `Failed: ${this.state.failedEvents.size}, ` +
        `Retry Queue: ${this.state.retryQueue.length}`
      );
    } else {
      console.log(`${statusEmoji} [${formattedTime}] ${message}`);
    }
  }

  /**
   * Log an error to both console and database
   */
  async logError(eventId, errorType, error, metadata = {}) {
    try {
      const event = await this.state.getEventById(eventId, ["url", "_id"]);

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
}

export default ScraperLogger; 