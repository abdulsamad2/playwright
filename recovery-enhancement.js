// Enhanced Recovery System for ScraperManager
// This file contains the optimized recovery methods to be integrated into scraperManager.js

// Recovery Configuration Constants - Optimized for 1000+ events
const CRITICAL_RECOVERY_INTERVAL = 15000; // 10 seconds - faster for 1000+ events
const AGGRESSIVE_RECOVERY_INTERVAL = 30000; // 20 seconds - more frequent
const STANDARD_RECOVERY_INTERVAL = 50000; // 30 seconds for standard recovery
const AUTO_STOP_CHECK_INTERVAL = 50000; // 1 minute for auto-stop checks

// Enhanced batch processing for 1000+ events
const RECOVERY_BATCH_SIZE = 10; // Increased from 50 for better throughput
const MAX_RECOVERY_BATCHES = 5; // Increased from 10 for parallel processing
const CONCURRENT_RECOVERY_LIMIT = 500; // Max concurrent recovery operations

// Aggressive thresholds for 3-minute guarantee
const CRITICAL_THRESHOLD = 120000; // 2 minutes - faster than 3 minutes
const STALE_THRESHOLD = 180000; // 3 minutes - maximum allowed
const AUTO_STOP_THRESHOLD = 600000; // 10 minutes - auto-stop threshold

// Enhanced cooldown settings
const SHORT_RECOVERY_COOLDOWN = 5000; // 5 seconds between recovery attempts
const LONG_RECOVERY_COOLDOWN = 15000; // 15 seconds for persistent failures

/**
 * Start recovery monitoring intervals for aggressive stale event handling
 */
function startRecoveryMonitoring() {
  this.logWithTime("Starting enhanced recovery monitoring for 3-minute update guarantee", "info");
  
  // Critical recovery check every 10 seconds
  this.criticalRecoveryIntervalId = setInterval(async () => {
    try {
      await this.handleCriticalStaleEvents();
    } catch (error) {
      this.logWithTime(`Critical recovery error: ${error.message}`, "error");
    }
  }, CRITICAL_RECOVERY_INTERVAL);

  // Standard recovery check every 30 seconds
  this.standardRecoveryIntervalId = setInterval(async () => {
    try {
      await this.handleStandardStaleEvents();
    } catch (error) {
      this.logWithTime(`Standard recovery error: ${error.message}`, "error");
    }
  }, STANDARD_RECOVERY_INTERVAL);

  // AUTO-STOP FUNCTIONALITY REMOVED - No longer automatically stopping stale events
}

/**
 * Handle critical stale events (>2 minutes) with immediate recovery
 */
async function handleCriticalStaleEvents() {
  const now = Date.now();
  const criticalThreshold = CRITICAL_THRESHOLD;
  
  try {
    // Verify database connection first
    if (!this.db || !this.db.readyState) {
      throw new Error('Database connection not ready');
    }

    const criticalEvents = await Event.find({
      Skip_Scraping: { $ne: true },
      Last_Updated: { $lt: new Date(now - criticalThreshold) }
    }).select("Event_ID").lean().catch(err => {
      throw new Error(`Database query failed: ${err.message}`);
    });

    if (criticalEvents.length > 0) {
      const eventIds = criticalEvents.map(e => e.Event_ID);
      this.logWithTime(`🚨 CRITICAL: ${eventIds.length} events >2min stale - immediate recovery`, "error");
      
      // Process in parallel batches for speed
      const batches = [];
      for (let i = 0; i < eventIds.length; i += RECOVERY_BATCH_SIZE) {
        batches.push(eventIds.slice(i, i + RECOVERY_BATCH_SIZE));
      }

      // Process up to MAX_RECOVERY_BATCHES in parallel
      const batchPromises = batches.slice(0, MAX_RECOVERY_BATCHES).map(batch => 
        this.recoverEventBatch(batch, 'critical')
      );
      
      await Promise.allSettled(batchPromises);
    }
  } catch (error) {
    this.logWithTime(`Error in critical recovery: ${error.message}`, "error");
    // Clear intervals if database connection failed
    if (error.message.includes('Database')) {
      this.stopRecoveryMonitoring();
    }
  }
}

/**
 * Handle standard stale events (>3 minutes) with standard recovery
 */
async function handleStandardStaleEvents(processedEvents = new Set()) {
  const CRITICAL_THRESHOLD = STALE_THRESHOLD;
  const STOP_THRESHOLD = AUTO_STOP_THRESHOLD;
  
  try {
    if (!this.db || !this.db.readyState) throw new Error('Database connection not ready');

    // Initialize trackers
    this.eventAttempts = this.eventAttempts || new Map();
    this.cooldownEvents = this.cooldownEvents || new Set();

    const now = Date.now();
    const staleEvents = await Event.find({
      Skip_Scraping: { $ne: true },
      Last_Updated: { $lt: new Date(now - CRITICAL_THRESHOLD) }
    }).select("Event_ID Last_Updated").lean();

    for (const event of staleEvents) {
      const eventId = event.Event_ID;
      const lastUpdated = new Date(event.Last_Updated).getTime();
      const timeStale = now - lastUpdated;
      
      // AUTO-STOP REMOVED: Events will continue to be retried regardless of staleness duration
      
      // Skip if in cooldown (5 seconds)
      if (this.cooldownEvents.has(eventId)) continue;
      
      // Skip if already processed
      if (processedEvents.has(eventId)) continue;

      try {
        // Track attempt (max 3 attempts in 3 minutes)
        const attempts = (this.eventAttempts.get(eventId) || 0) + 1;
        this.eventAttempts.set(eventId, attempts);
        
        // Short cooldown (5 seconds) after 3 attempts
        if (attempts > 3) {
          this.cooldownEvents.add(eventId);
          setTimeout(() => this.cooldownEvents.delete(eventId), SHORT_RECOVERY_COOLDOWN);
          continue;
        }
        
        // Immediately update timestamp and process
        await Event.updateOne(
          { Event_ID: eventId },
          { $set: { Last_Updated: new Date() }}
        );
        
        processedEvents.add(eventId);
        this.logWithTime(`♻️ Attempt ${attempts} on stale event ${eventId} (${Math.round(timeStale/1000)}s stale)`, 'debug');
        
        // Highest priority recovery
        await this.addToRecoveryQueue([eventId], 'critical');
      } catch (error) {
        this.logWithTime(`❗ Failed ${eventId}: ${error.message}`, 'error');
      }
    }
  } catch (error) {
    this.logWithTime(`‼️ Recovery system error: ${error.message}`, 'error');
  }
}

// AUTO-STOP FUNCTION REMOVED - Events will no longer be automatically stopped

/**
 * Recover a batch of events with specified recovery level
 */
async function recoverEventBatch(eventIds, recoveryLevel = 'standard') {
  const results = { recovered: [], failed: [] };

  try {
    const recoveryPromises = eventIds.map(eventId => 
      this.recoverSingleEvent(eventId, recoveryLevel)
        .then(success => {
          if (success) {
            results.recovered.push(eventId);
          } else {
            results.failed.push(eventId);
          }
        })
        .catch(err => {
          this.logWithTime(`Recovery error for ${eventId}: ${err.message}`, "error");
          results.failed.push(eventId);
        })
    );

    await Promise.allSettled(recoveryPromises);

    if (results.recovered.length > 0 || results.failed.length > 0) {
      this.logWithTime(
        `Batch recovery (${recoveryLevel}): ${results.recovered.length} recovered, ${results.failed.length} failed`,
        results.recovered.length > 0 ? "success" : "warning"
      );
    }

  } catch (error) {
    this.logWithTime(`Batch recovery error: ${error.message}`, "error");
  }

  return results;
}

/**
 * Recover a single event with specified strategy
 */
async function recoverSingleEvent(eventId, recoveryLevel = 'standard') {
  try {
    // Reset all failure tracking
    this.failedEvents.delete(eventId);
    this.cooldownEvents.delete(eventId);
    this.processingEvents.delete(eventId);
    this.eventFailureCounts.delete(eventId);
    this.eventFailureTimes.delete(eventId);

    // Get fresh proxy and session for critical recovery
    if (recoveryLevel === 'critical') {
      this.proxyManager.releaseProxy(eventId, false);
      await this.refreshEventHeaders(eventId, true);
    }

    // Attempt scrape with higher timeout for recovery
    const scrapeResult = await this.scrapeEventOptimized(
      eventId, 
      0, 
      null, 
      recoveryLevel === 'critical' ? 60000 : 45000
    );

    if (scrapeResult) {
      this.logWithTime(`✅ ${recoveryLevel.toUpperCase()} recovery SUCCESS for event ${eventId}`, "success");
      this.clearFailureCount(eventId);
      this.eventFailureTimes.delete(eventId);
      return true;
    }

    return false;

  } catch (error) {
    this.logWithTime(`Recovery failed for ${eventId}: ${error.message}`, "error");
    return false;
  }
}

/**
 * Stop recovery monitoring intervals
 */
function stopRecoveryMonitoring() {
  if (this.criticalRecoveryIntervalId) {
    clearInterval(this.criticalRecoveryIntervalId);
    this.criticalRecoveryIntervalId = null;
  }
  
  if (this.standardRecoveryIntervalId) {
    clearInterval(this.standardRecoveryIntervalId);
    this.standardRecoveryIntervalId = null;
  }
  
  if (this.autoStopIntervalId) {
    clearInterval(this.autoStopIntervalId);
    this.autoStopIntervalId = null;
  }
  
  this.logWithTime("Stopped recovery monitoring intervals", "info");
}

// Add this call to the startContinuousScraping method after starting parallel workers:
// this.startRecoveryMonitoring();

// Add this call to the stopContinuousScraping method:
// this.stopRecoveryMonitoring();
