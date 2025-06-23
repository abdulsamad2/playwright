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
  recordFailure(eventId, error, retryCount, scheduler) {
    this.failedEvents.add(eventId);
    this.incrementFailureCount(eventId);

    // Optimized backoff: shorter initial delays, cap at reasonable maximum
    let backoffTime = Math.min(
      config.RETRY_BACKOFF_MS * Math.pow(1.5, retryCount),
      60000
    ); // Cap at 1 minute
    let isApiError = false;

    // Detect API-specific errors for special handling
    if (
      error.message.includes("403") ||
      error.message.includes("400") ||
      error.message.includes("429") ||
      error.message.includes("API")
    ) {
      isApiError = true;

      // For API errors, apply shorter cooldown to keep system moving
      const apiBackoffMinutes = Math.min(5, 1 + retryCount); // Reduced from 15 to 5 minutes max
      backoffTime = apiBackoffMinutes * 60 * 1000;

      // Schedule next update to prevent urgent flags
      scheduler.scheduleNextUpdate(eventId);

      this.logger.logWithTime(
        `API error for ${eventId}: ${error.message}. Cooldown for ${apiBackoffMinutes} minutes`,
        "warning" // Changed from error to warning to reduce noise
      );

      // Only increment global counter for severe API errors
      if (error.message.includes("429") || error.message.includes("403")) {
        this.globalConsecutiveErrors++;
      }
    } else {
      this.logger.logWithTime(
        `Error scraping ${eventId}: ${error.message}. Cooldown for ${
          backoffTime / 1000
        }s`,
        "warning" // Changed from error to warning
      );
    }


    const cooldownUntil = moment().add(backoffTime, "milliseconds");

    // Always add to retry queue with optimized retry count
    const maxRetries = isApiError ? config.MAX_RETRIES + 2 : config.MAX_RETRIES; // Extra retries for API errors
    if (retryCount < maxRetries) {
      this.addToRetryQueue(eventId, retryCount + 1, cooldownUntil);

      this.logger.logWithTime(
        `Queued for retry: ${eventId} (after ${backoffTime / 1000}s cooldown)`,
        "warning"
      );
    } else {
      this.logger.logWithTime(
        `Max retries exceeded for ${eventId} - will retry with fresh session later`,
        "warning"
      );



      // Clear failure count to give event a fresh start after cooldown
      setTimeout(() => {
        this.clearFailureCount(eventId);
        this.logger.logWithTime(
          `Cleared failure history for ${eventId} - ready for fresh attempts`,
          "info"
        );
      }, 5 * 60 * 1000); // Clear after 5 minutes

      // Still schedule next update to prevent urgent flags
      scheduler.scheduleNextUpdate(eventId);
    }

    return {
      backoffTime,
      isApiError,
      cooldownUntil,
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
      "milliseconds"
    );
    let clearedCount = 0; // Fixed: changed from const to let

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
      globalConsecutiveErrors: this.globalConsecutiveErrors,
    };
  }
}

export default ErrorTracker;
