import { setTimeout } from "timers/promises";
import moment from "moment";
import config from "../config/scraperConfig.js";

/**
 * Manages concurrency limits and processing job tracking
 */
class ConcurrencyManager {
  constructor(logger) {
    this.logger = logger;
    this.concurrencySemaphore = config.CONCURRENT_LIMIT;
    this.activeJobs = new Map(); // eventId -> start time
    this.processingEvents = new Set(); // Currently processing event IDs
    this.cooldownEvents = new Map(); // eventId -> cooldown until timestamp
    this.getEventById = null; // Will be set after DatabaseManager is initialized
  }

  /**
   * Wait until a concurrency slot is available
   */
  async acquireSemaphore() {
    while (this.concurrencySemaphore <= 0) {
      await setTimeout(100);
    }
    this.concurrencySemaphore--;
  }

  /**
   * Release a concurrency slot
   */
  releaseSemaphore() {
    this.concurrencySemaphore++;
  }

  /**
   * Start tracking a job
   */
  startJob(eventId) {
    this.activeJobs.set(eventId, moment());
    this.processingEvents.add(eventId);
  }

  /**
   * End tracking a job
   */
  endJob(eventId) {
    this.activeJobs.delete(eventId);
    this.processingEvents.delete(eventId);
  }

  /**
   * Check if an event should be skipped due to concurrency or cooldown
   */
  shouldSkipEvent(eventId, eventUpdateTimestamps) {
    // Check if event is in cooldown period
    if (this.cooldownEvents.has(eventId)) {
      const cooldownUntil = this.cooldownEvents.get(eventId);
      if (moment().isBefore(cooldownUntil)) {
        const timeLeft = moment
          .duration(cooldownUntil.diff(moment()))
          .asSeconds();
        this.logger.logWithTime(
          `Skipping ${eventId}: In cooldown for ${timeLeft.toFixed(1)}s more`,
          "warning"
        );
        return true;
      } else {
        this.cooldownEvents.delete(eventId);
      }
    }

    // Check if event is already being processed
    if (this.processingEvents.has(eventId)) {
      this.logger.logWithTime(`Skipping ${eventId}: Already processing`, "warning");
      return true;
    }

    // Check if minimum time between scrapes has elapsed
    const lastUpdate = eventUpdateTimestamps.get(eventId);
    if (
      lastUpdate &&
      moment().diff(lastUpdate) < config.MIN_TIME_BETWEEN_EVENT_SCRAPES
    ) {
      this.logger.logWithTime(
        `Skipping ${eventId}: Too soon since last scrape (${moment().diff(
          lastUpdate
        )}ms)`,
        "warning"
      );
      return true;
    }

    return false;
  }

  /**
   * Set a cooldown for an event
   */
  setCooldown(eventId, timeMs) {
    this.cooldownEvents.set(eventId, moment().add(timeMs, 'milliseconds'));
  }

  /**
   * Clean up stale jobs (stuck jobs)
   */
  cleanupStaleTasks() {
    const staleTimeLimit = config.STALE_TASK_TIMEOUT;
    const now = moment();
    const staleJobs = [];

    for (const [eventId, startTime] of this.activeJobs.entries()) {
      if (now.diff(startTime) > staleTimeLimit) {
        this.logger.logWithTime(
          `Cleaning up stale job for ${eventId} (started ${
            now.diff(startTime) / 1000
          }s ago)`,
          "warning"
        );
        staleJobs.push(eventId);
      }
    }
    
    // Cleanup stale jobs
    for (const eventId of staleJobs) {
      this.activeJobs.delete(eventId);
      this.processingEvents.delete(eventId);
      this.concurrencySemaphore++;
    }
    
    return staleJobs.length;
  }
  
  /**
   * Get count of active jobs
   */
  getActiveJobCount() {
    return this.activeJobs.size;
  }
  
  /**
   * Check if an event is in cooldown
   */
  isEventInCooldown(eventId) {
    if (!this.cooldownEvents.has(eventId)) return false;
    return moment().isBefore(this.cooldownEvents.get(eventId));
  }
  
  /**
   * Clear expired cooldowns
   */
  clearExpiredCooldowns() {
    const now = moment();
    const expiredCooldowns = [];
    
    for (const [eventId, cooldownTime] of this.cooldownEvents.entries()) {
      if (now.isAfter(cooldownTime)) {
        expiredCooldowns.push(eventId);
      }
    }
    
    for (const eventId of expiredCooldowns) {
      this.cooldownEvents.delete(eventId);
    }
    
    return expiredCooldowns.length;
  }
}

export default ConcurrencyManager; 