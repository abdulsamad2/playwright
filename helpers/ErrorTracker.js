import moment from "moment";
import config from "../config/scraperConfig.js";

/**
 * Tracks failures and manages error retry policies
 */
class ErrorTracker {
  constructor(logger) {
    this.logger = logger;
    this.failedEvents = new Set(); // Set of event IDs that have failed
    this.eventFailureCounts = new Map(); // Track consecutive failures per event
    this.eventFailureTimes = new Map(); // Track when failures happened
    this.retryQueue = []; // Queue of events to retry
    this.globalConsecutiveErrors = 0; // Track consecutive errors across all events
  }

  /**
   * Get count of recent failures for an event
   */
  getRecentFailureCount(eventId) {
    return this.eventFailureCounts.get(eventId) || 0;
  }

  /**
   * Increment failure count for an event
   */
  incrementFailureCount(eventId) {
    const currentCount = this.eventFailureCounts.get(eventId) || 0;
    this.eventFailureCounts.set(eventId, currentCount + 1);
    this.eventFailureTimes.set(eventId, moment());
    this.failedEvents.add(eventId);
  }

  /**
   * Clear failure count for an event
   */
  clearFailureCount(eventId) {
    this.eventFailureCounts.delete(eventId);
    this.eventFailureTimes.delete(eventId);
    this.failedEvents.delete(eventId);
  }

  /**
   * Record a successful event
   */
  recordSuccess(eventId) {
    this.clearFailureCount(eventId);
    this.globalConsecutiveErrors = 0; // Reset global error counter on success
  }

  /**
   * Record a failure and apply appropriate policy
   */
  recordFailure(eventId, error, retryCount, concurrencyManager, scheduler) {
    this.failedEvents.add(eventId);
    this.incrementFailureCount(eventId);
    
    let backoffTime = config.RETRY_BACKOFF_MS * Math.pow(2, retryCount);
    let isApiError = false;
    
    // Detect API-specific errors for special handling
    if (error.message.includes("403") || 
        error.message.includes("400") || 
        error.message.includes("429") ||
        error.message.includes("API")) {
      isApiError = true;
      
      // For API errors, apply longer cooldown
      const apiBackoffMinutes = Math.min(15, 2 + retryCount * 3);
      backoffTime = apiBackoffMinutes * 60 * 1000;
      
      // Schedule next update to prevent urgent flags
      scheduler.scheduleNextUpdate(eventId);
      
      this.logger.logWithTime(
        `API error for ${eventId}: ${error.message}. Extended cooldown for ${apiBackoffMinutes} minutes`,
        "error"
      );
      
      // Increment global consecutive error counter for API errors
      this.globalConsecutiveErrors++;
    } else {
      this.logger.logWithTime(
        `Error scraping ${eventId}: ${error.message}. Cooldown for ${
          backoffTime / 1000
        }s`,
        "error"
      );
    }
    
    // Set cooldown for this event
    concurrencyManager.setCooldown(eventId, backoffTime);
    const cooldownUntil = moment().add(backoffTime, "milliseconds");

    // Add to retry queue if under max retries
    if (retryCount < config.MAX_RETRIES) {
      this.addToRetryQueue(eventId, retryCount + 1, cooldownUntil);
      
      this.logger.logWithTime(
        `Queued for retry: ${eventId} (after ${
          backoffTime / 1000
        }s cooldown)`,
        "warning"
      );
    } else {
      this.logger.logWithTime(`Max retries exceeded for ${eventId}`, "error");
      
      // For max retries, set a longer cooldown
      concurrencyManager.setCooldown(eventId, 30 * 60 * 1000); // 30 minutes
      
      // Still schedule next update to prevent urgent flags
      scheduler.scheduleNextUpdate(eventId);
    }
    
    return {
      backoffTime,
      isApiError,
      cooldownUntil
    };
  }

  /**
   * Add an event to the retry queue
   */
  addToRetryQueue(eventId, retryCount, retryAfter) {
    this.retryQueue.push({
      eventId,
      retryCount,
      retryAfter,
    });
  }

  /**
   * Get events ready for retry
   */
  getEventsReadyForRetry() {
    const now = moment();

    // Filter the retry queue to only include items ready for retry
    const readyForRetry = this.retryQueue.filter(
      (job) => !job.retryAfter || now.isAfter(job.retryAfter)
    );

    // Update retry queue to remove items we're processing
    this.retryQueue = this.retryQueue.filter(
      (job) => job.retryAfter && now.isBefore(job.retryAfter)
    );

    return readyForRetry;
  }

  /**
   * Clear old failure data to prevent permanent penalties
   */
  clearOldFailureData() {
    const oldFailureThreshold = moment().subtract(
      config.FAILURE_HISTORY_EXPIRY, 
      'milliseconds'
    );
    const clearedCount = 0;
    
    for (const [eventId, failureTime] of this.eventFailureTimes.entries()) {
      if (failureTime.isBefore(oldFailureThreshold)) {
        this.clearFailureCount(eventId);
        clearedCount++;
      }
    }
    
    return clearedCount;
  }

  /**
   * Check if global error threshold has been reached
   */
  shouldResetCookies() {
    return this.globalConsecutiveErrors >= 3;
  }

  /**
   * Reset global error counter
   */
  resetGlobalErrorCounter() {
    this.globalConsecutiveErrors = 0;
  }

  /**
   * Get failure statistics
   */
  getFailureStats() {
    return {
      failedEventCount: this.failedEvents.size,
      retryQueueLength: this.retryQueue.length,
      globalConsecutiveErrors: this.globalConsecutiveErrors
    };
  }
}

export default ErrorTracker; 