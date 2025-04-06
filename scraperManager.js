import moment from "moment";
import { setTimeout } from "timers/promises";

// Import modular components
import config from "./config/scraperConfig.js";
import ScraperLogger from "./helpers/ScraperLogger.js";
import CookieManager from "./helpers/CookieManager.js";
import ConcurrencyManager from "./helpers/ConcurrencyManager.js";
import EventScheduler from "./helpers/EventScheduler.js";
import ErrorTracker from "./helpers/ErrorTracker.js";
import DatabaseManager from "./helpers/DatabaseManager.js";
import EventProcessor from "./helpers/EventProcessor.js";

/**
 * Main ScraperManager class that orchestrates the event scraping process
 */
class ScraperManager {
  constructor() {
    // Initialize state
    this.isRunning = false;
    this.successCount = 0;
    this.lastSuccessTime = null;
    this.startTime = moment();
    
    // Create state object for components that need access to shared state
    this.state = {
      activeJobs: new Map(),
      successCount: 0,
      failedEvents: new Set(),
      retryQueue: [],
      getEventById: this.getEventById.bind(this),
    };
    
    // Initialize components
    this.logger = new ScraperLogger(this.state);
    this.cookieManager = new CookieManager(this.logger);
    this.concurrencyManager = new ConcurrencyManager(this.logger);
    this.scheduler = new EventScheduler(this.logger);
    this.errorTracker = new ErrorTracker(this.logger);
    this.databaseManager = new DatabaseManager(this.logger);
    this.eventProcessor = new EventProcessor(
      this.logger,
      this.concurrencyManager,
      this.cookieManager,
      this.databaseManager,
      this.errorTracker,
      this.scheduler
    );
    
    // Connect state handlers
    this.state.getEventById = this.databaseManager.getEventById.bind(this.databaseManager);
  }

  /**
   * Start continuous scraping process
   */
  async startContinuousScraping() {
    this.isRunning = true;
    this.logger.logWithTime(
      `Starting strict ${config.MAX_UPDATE_INTERVAL/1000}-second scraper with ${config.CONCURRENT_LIMIT} concurrent jobs`
    );
    
    // Schedule checking for urgent events
    const checkInterval = 1000; // Check every second
    
    while (this.isRunning) {
      const cycleStart = performance.now();
      
      try {
        // Check if we need to reset cookies due to global API failures
        if (this.errorTracker.shouldResetCookies()) {
          await this.cookieManager.resetCookiesAndHeaders();
          this.errorTracker.resetGlobalErrorCounter();
        }
        
        // Process retries first
        const retriesProcessed = await this.eventProcessor.processRetryQueue();
        if (retriesProcessed > 0) {
          this.logger.logWithTime(`Processed ${retriesProcessed} retries`, "info");
        }
        
        // Get events to process with strict prioritization
        const eventIds = await this.scheduler.getEventsToProcess(this.concurrencyManager.processingEvents);
        
        if (eventIds.length > 0) {
          this.logger.logWithTime(`Processing batch of ${eventIds.length} events (${this.concurrencyManager.processingEvents.size} already in progress)`);
          const result = await this.eventProcessor.processBatch(eventIds);
          
          // Update stats
          if (result.results) {
            const successCount = result.results.filter(r => r.success).length;
            this.successCount += successCount;
            this.lastSuccessTime = moment();
          }
        }
        
        // Short delay before next check
        const cycleTime = performance.now() - cycleStart;
        const delay = Math.max(0, checkInterval - cycleTime);
        
        if (delay > 0) {
          await setTimeout(delay);
        }
      } catch (error) {
        this.logger.logWithTime(`Cycle error: ${error.message}`, "error");
        await setTimeout(5000); // Shorter recovery time
      }
    }
  }

  /**
   * Start system monitoring task
   */
  async monitoringTask() {
    while (this.isRunning) {
      try {
        // Cleanup stale tasks
        this.concurrencyManager.cleanupStaleTasks();
        
        // Check for events that missed their deadlines
        const missedDeadlines = this.scheduler.checkForMissedDeadlines(
          this.concurrencyManager.processingEvents
        );
        
        if (missedDeadlines > 0) {
          this.logger.logWithTime(
            `WARNING: ${missedDeadlines} events missed their 2-minute update deadline`, 
            "error"
          );
        }
        
        // Check for high failure rates
        const failureStats = this.errorTracker.getFailureStats();
        const totalAttempts = this.successCount + failureStats.failedEventCount;
        
        if (totalAttempts > 5 && failureStats.failedEventCount / totalAttempts > 0.8) {
          this.logger.logWithTime(
            `High failure rate detected: ${failureStats.failedEventCount}/${totalAttempts} events failing`, 
            "error"
          );
          
          // If not already resetting, trigger cookie reset
          if (failureStats.globalConsecutiveErrors < 3) {
            this.errorTracker.globalConsecutiveErrors = 3; // Force a reset on next cycle
          }
        }
        
        // Clear old error data
        this.errorTracker.clearOldFailureData();
        this.concurrencyManager.clearExpiredCooldowns();
        
        // Update state
        this.state.successCount = this.successCount;
        this.state.failedEvents = this.errorTracker.failedEvents;
        this.state.retryQueue = this.errorTracker.retryQueue;
        this.state.activeJobs = this.concurrencyManager.activeJobs;
      } catch (error) {
        console.error("Error in monitoring task:", error);
      }
      
      // Run monitoring every 30 seconds
      await setTimeout(30 * 1000);
    }
  }

  /**
   * Start the scraper system
   */
  async start() {
    // Start monitoring task
    this.monitoringTask();

    // Start main scraping loop
    return this.startContinuousScraping();
  }

  /**
   * Stop the scraper system
   */
  stop() {
    this.isRunning = false;
    this.logger.logWithTime("Stopping scraper");
  }
}

// Export singleton instance
const scraperManager = new ScraperManager();
export default scraperManager;
