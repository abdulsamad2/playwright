import { setTimeout } from "timers/promises";
import { ScrapeEvent } from "../scraper.js";
import config from "../config/scraperConfig.js";

/**
 * Handles the main event processing logic
 */
class EventProcessor {
  constructor(
    logger,
    cookieManager,
    databaseManager,
    errorTracker,
    scheduler
  ) {
    this.logger = logger;
    this.cookieManager = cookieManager;
    this.databaseManager = databaseManager;
    this.errorTracker = errorTracker;
    this.scheduler = scheduler;
  }

  /**
   * Process a single event
   */
  async scrapeEvent(eventId, retryCount = 0) {
    const eventUpdateTimestamps = this.scheduler.getUpdateTimestamps();
    


    try {
      this.logger.logWithTime(`Scraping ${eventId} (Attempt ${retryCount + 1})`);

      // Look up the event first before trying to scrape
      const event = await this.databaseManager.getEventById(eventId);

      if (!event) {
        this.logger.logWithTime(`Event ${eventId} not found in database`, "error");

        return false;
      }

      if (event.Skip_Scraping) {
        this.logger.logWithTime(`Skipping ${eventId} (flagged)`, "info");
        // Still update the schedule for next time
        this.scheduler.scheduleNextUpdate(eventId);
        return true;
      }

      // Optimized failure handling - shorter cooldowns to keep system moving
      const recentFailures = this.errorTracker.getRecentFailureCount(eventId);
      if (recentFailures >= 5 && retryCount > 2) { // Increased threshold from 3 to 5
        const backoffMinutes = Math.min(10, 2 + recentFailures); // Reduced max from 30 to 10 minutes

        this.logger.logWithTime(
          `Event ${eventId} has failed ${recentFailures} times, short cooldown of ${backoffMinutes} minutes`,
          "warning"
        );

        
        // Clear some failure history to prevent permanent penalties
        if (recentFailures >= 8) {
          setTimeout(() => {
            this.errorTracker.clearFailureCount(eventId);
            this.logger.logWithTime(`Reset failure count for ${eventId} to prevent permanent blocking`, "info");
          }, 2 * 60 * 1000); // Reset after 2 minutes
        }
        
        // Still update the schedule to prevent urgent flags
        this.scheduler.scheduleNextUpdate(eventId);
        return false;
      }

      // Refresh headers with backoff/caching strategy
      const headers = await this.cookieManager.getHeadersForEvent(eventId);
      if (!headers) {
        throw new Error("Failed to obtain valid headers");
      }

      // Set a timeout for the scrape
      const result = await Promise.race([
        ScrapeEvent({ eventId, headers }),
        setTimeout(config.SCRAPE_TIMEOUT).then(() => {
          throw new Error("Scrape timed out");
        }),
      ]);

      if (!result || !Array.isArray(result) || result.length === 0) {
        throw new Error("Empty or invalid scrape result");
      }

      // Update metadata and record success
      await this.databaseManager.updateEventMetadata(eventId, result, this.scheduler);
      this.errorTracker.recordSuccess(eventId);
      
      return true;
    } catch (error) {
      // Record failure and apply policy
      this.errorTracker.recordFailure(
        eventId, 
        error, 
        retryCount, 
        this.scheduler
      );
      
      await this.logger.logError(eventId, "SCRAPE_ERROR", error, { retryCount });
      
      return false;
    }
  }

  /**
   * Process a batch of events
   */
  async processBatch(eventIds) {
    const results = [];
    const failed = [];
    
    if (eventIds.length <= 0) {
      return { failed };
    }
    
    // Split into smaller groups for better control
    const chunkSize = Math.min(5, Math.ceil(config.CONCURRENT_LIMIT / 2));
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
    
    return { results, failed };
  }

  /**
   * Process the retry queue (non-blocking)
   */
  async processRetryQueue() {
    const readyForRetry = this.errorTracker.getEventsReadyForRetry();

    if (readyForRetry.length === 0) {
      return 0;
    }

    // Process retries asynchronously without blocking the main loop
    // Limit concurrent retries to prevent overwhelming the server
    const maxConcurrentRetries = Math.min(3, readyForRetry.length);
    const retryBatches = [];
    
    for (let i = 0; i < readyForRetry.length; i += maxConcurrentRetries) {
      retryBatches.push(readyForRetry.slice(i, i + maxConcurrentRetries));
    }

    let totalRetryCount = 0;
    
    // Process batches with controlled concurrency
    for (const batch of retryBatches) {
      const retryPromises = batch.map(async (job) => {
        try {
          await this.scrapeEvent(job.eventId, job.retryCount);
          return true;
        } catch (error) {
          this.logger.logWithTime(
            `Retry failed for event ${job.eventId}: ${error.message}`,
            "warning"
          );
          return false;
        }
      });
      
      // Wait for current batch to complete before starting next batch
      const results = await Promise.allSettled(retryPromises);
      totalRetryCount += results.filter(r => r.status === 'fulfilled' && r.value).length;
      
      // Small delay between batches to prevent rate limiting
      if (retryBatches.length > 1) {
        await setTimeout(500); // Reduced from 2000ms to 500ms
      }
    }
    
    return totalRetryCount;
  }
}

export default EventProcessor;