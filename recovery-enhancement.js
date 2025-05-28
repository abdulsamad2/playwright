// Enhanced Recovery System for ScraperManager
// This file contains the optimized recovery methods to be integrated into scraperManager.js

// Recovery Configuration Constants
const CRITICAL_RECOVERY_INTERVAL = 15000; // 15 seconds
const AGGRESSIVE_RECOVERY_INTERVAL = 30000; // 30 seconds
const RECOVERY_BATCH_SIZE = 50; // events per batch
const MAX_RECOVERY_BATCHES = 10; // max concurrent batches
const LONG_COOLDOWN_MINUTES = 1; // 30 minute cooldown

/**
 * Start recovery monitoring intervals for aggressive stale event handling
 */
function startRecoveryMonitoring() {
  this.logWithTime("Starting enhanced recovery monitoring for 3-minute update guarantee", "info");
  
  // Critical recovery check every 15 seconds
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
  }, AGGRESSIVE_RECOVERY_INTERVAL);

  // Auto-stop check every 2 minutes
  this.autoStopIntervalId = setInterval(async () => {
    try {
      await this.handleAutoStopEvents();
    } catch (error) {
      this.logWithTime(`Auto-stop check error: ${error.message}`, "error");
    }
  }, 120000);
}

/**
 * Handle critical stale events (>2.5 minutes) with immediate recovery
 */
async function handleCriticalStaleEvents() {
  const now = Date.now();
  const criticalThreshold = 150000; // 2.5 minutes
  
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
      this.logWithTime(`🚨 CRITICAL: ${eventIds.length} events >2.5min stale - immediate recovery`, "error");
      
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
  const CRITICAL_THRESHOLD = 180000; // 3 minutes
  const STOP_THRESHOLD = 600000; // 10 minutes
  
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
      
      // Auto-stop if stale for >10 minutes
      if (timeStale > STOP_THRESHOLD) {
        await Event.updateOne(
          { Event_ID: eventId },
          { $set: { Status: "STOPPED", Stop_Reason: "stale_timeout" }}
        );
        this.logWithTime(`⚠️ STOPPED event ${eventId} (stale for ${Math.round(timeStale/60000)} minutes)`, 'warning');
        continue;
      }
      
      // Skip if in cooldown (30 seconds)
      if (this.cooldownEvents.has(eventId)) continue;
      
      // Skip if already processed
      if (processedEvents.has(eventId)) continue;

      try {
        // Track attempt (max 3 attempts in 3 minutes)
        const attempts = (this.eventAttempts.get(eventId) || 0) + 1;
        this.eventAttempts.set(eventId, attempts);
        
        // Short cooldown (30 seconds) after 3 attempts
        if (attempts > 3) {
          this.cooldownEvents.add(eventId);
          setTimeout(() => this.cooldownEvents.delete(eventId), 30000);
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

/**
 * Handle auto-stop for events stale >10 minutes
 */
async function handleAutoStopEvents() {
  const now = Date.now();
  
  try {
    // Verify database connection first
    if (!this.db || !this.db.readyState) {
      throw new Error('Database connection not ready');
    }

    const eventsToStop = await Event.find({
      Skip_Scraping: { $ne: true },
      Last_Updated: { $lt: new Date(now - STALE_EVENT_THRESHOLD) }
    }).select("Event_ID").lean().catch(err => {
      throw new Error(`Database query failed: ${err.message}`);
    });

    if (eventsToStop.length > 0) {
      const eventIds = eventsToStop.map(e => e.Event_ID);
      this.logWithTime(`🛑 AUTO-STOP: ${eventIds.length} events >10min stale - stopping`, "error");
      await this.autoStopStaleEvents(eventIds);
    }
  } catch (error) {
    this.logWithTime(`Error in auto-stop check: ${error.message}`, "error");
    // Clear intervals if database connection failed
    if (error.message.includes('Database')) {
      this.stopRecoveryMonitoring();
    }
  }
}

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
