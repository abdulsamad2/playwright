import { createRequire } from "module";
const require = createRequire(import.meta.url);
import { Event } from '../models/index.js';
import scraperManager from '../scraperManager.js';
import { exec } from 'child_process';
import { promisify } from 'util';
import fs from 'fs/promises';
import path from 'path';

const execAsync = promisify(exec);

/**
 * AutoRestartMonitor - Monitors event failures and automatically restarts server
 * when critical failure thresholds are reached
 */
export class AutoRestartMonitor {
  constructor(logger) {
    this.logger = logger;
    this.isMonitoring = false;
    this.monitoringInterval = null;
    this.restartInProgress = false;
    
    // Configuration
    this.config = {
      // Monitoring intervals
      CHECK_INTERVAL: 60000, // Check every minute
      FAILURE_CHECK_WINDOW: 5 * 60 * 1000, // 5 minute window for failure analysis
      
      // Failure thresholds
      TOTAL_EVENTS_THRESHOLD: 50, // Minimum total events to trigger monitoring
      FAILURE_PERCENTAGE_THRESHOLD: 75, // 75% failure rate triggers restart
      ABSOLUTE_FAILURE_THRESHOLD: 50, // Absolute number of failures that triggers restart
      CONSECUTIVE_FAILURES_THRESHOLD: 3, // Consecutive monitoring cycles with high failures
      
      // Restart configuration
      RESTART_COOLDOWN: 10 * 60 * 1000, // 10 minutes between restarts
      MAX_RESTARTS_PER_HOUR: 3, // Maximum restarts per hour
      
      // Recovery settings
      POST_RESTART_DELAY: 30000, // 30 seconds delay after restart before reactivating events
      EVENT_REACTIVATION_BATCH_SIZE: 20, // Reactivate events in batches
      EVENT_REACTIVATION_DELAY: 2000, // 2 seconds between batches
    };
    
    // State tracking
    this.failureHistory = [];
    this.consecutiveHighFailures = 0;
    this.lastRestartTime = 0;
    this.restartCount = 0;
    this.restartHistory = [];
    
    // Bind methods
    this.startMonitoring = this.startMonitoring.bind(this);
    this.stopMonitoring = this.stopMonitoring.bind(this);
    this.checkEventFailures = this.checkEventFailures.bind(this);
    this.performRestart = this.performRestart.bind(this);
  }

  /**
   * Start monitoring event failures
   */
  startMonitoring() {
    if (this.isMonitoring) {
      this.log('Auto-restart monitoring is already running', 'warning');
      return;
    }

    this.isMonitoring = true;
    this.log('🔍 Starting auto-restart monitoring for event failures', 'info');
    
    // Start monitoring interval
    this.monitoringInterval = setInterval(() => {
      this.checkEventFailures().catch(error => {
        this.log(`Error in failure monitoring: ${error.message}`, 'error');
      });
    }, this.config.CHECK_INTERVAL);
    
    // Initial check
    setTimeout(() => {
      this.checkEventFailures().catch(error => {
        this.log(`Error in initial failure check: ${error.message}`, 'error');
      });
    }, 5000); // Wait 5 seconds for system to stabilize
  }

  /**
   * Stop monitoring
   */
  stopMonitoring() {
    if (!this.isMonitoring) {
      return;
    }

    this.isMonitoring = false;
    if (this.monitoringInterval) {
      clearInterval(this.monitoringInterval);
      this.monitoringInterval = null;
    }
    
    this.log('🛑 Stopped auto-restart monitoring', 'info');
  }

  /**
   * Check current event failure rates and determine if restart is needed
   */
  async checkEventFailures() {
    if (this.restartInProgress) {
      this.log('Restart in progress, skipping failure check', 'info');
      return;
    }

    try {
      // Get current event statistics
      const stats = await this.getEventFailureStats();
      
      if (!stats) {
        this.log('Unable to get event statistics', 'warning');
        return;
      }

      // Log current status
      this.log(
        `📊 Event Status: ${stats.totalEvents} total, ${stats.activeEvents} active, ` +
        `${stats.failedEvents} failed (${stats.failurePercentage.toFixed(1)}%), ` +
        `${stats.staleEvents} stale events`,
        'info'
      );

      // Check if we meet minimum threshold for monitoring
      if (stats.totalEvents < this.config.TOTAL_EVENTS_THRESHOLD) {
        this.log(`Total events (${stats.totalEvents}) below threshold (${this.config.TOTAL_EVENTS_THRESHOLD}), skipping restart check`, 'info');
        this.consecutiveHighFailures = 0;
        return;
      }

      // Record failure data
      this.recordFailureData(stats);

      // Check if restart is needed
      const shouldRestart = this.shouldTriggerRestart(stats);
      
      if (shouldRestart) {
        await this.performRestart(stats);
      } else {
        // Reset consecutive failures if we're below threshold
        if (stats.failurePercentage < this.config.FAILURE_PERCENTAGE_THRESHOLD) {
          this.consecutiveHighFailures = 0;
        }
      }

    } catch (error) {
      this.log(`Error checking event failures: ${error.message}`, 'error');
    }
  }

  /**
   * Get comprehensive event failure statistics
   */
  async getEventFailureStats() {
    try {
      // Get all events
      const allEvents = await Event.find({});
      const activeEvents = allEvents.filter(event => !event.Skip_Scraping);
      
      if (activeEvents.length === 0) {
        return null;
      }

      const now = new Date();
      const failureThreshold = new Date(now.getTime() - this.config.FAILURE_CHECK_WINDOW);
      
      // Analyze event states
      let failedEvents = 0;
      let staleEvents = 0;
      let recentlyUpdatedEvents = 0;
      
      for (const event of activeEvents) {
        const lastUpdate = event.Last_Updated || event.updatedAt;
        
        if (!lastUpdate) {
          failedEvents++;
          continue;
        }
        
        const timeSinceUpdate = now.getTime() - new Date(lastUpdate).getTime();
        
        // Consider events failed if not updated in the failure window
        if (timeSinceUpdate > this.config.FAILURE_CHECK_WINDOW) {
          failedEvents++;
          
          // Consider events stale if not updated in 10 minutes
          if (timeSinceUpdate > 10 * 60 * 1000) {
            staleEvents++;
          }
        } else {
          recentlyUpdatedEvents++;
        }
      }

      const failurePercentage = (failedEvents / activeEvents.length) * 100;

      return {
        totalEvents: allEvents.length,
        activeEvents: activeEvents.length,
        failedEvents,
        staleEvents,
        recentlyUpdatedEvents,
        failurePercentage,
        timestamp: now
      };

    } catch (error) {
      this.log(`Error getting event failure stats: ${error.message}`, 'error');
      return null;
    }
  }

  /**
   * Record failure data for trend analysis
   */
  recordFailureData(stats) {
    this.failureHistory.push({
      timestamp: stats.timestamp,
      failurePercentage: stats.failurePercentage,
      failedEvents: stats.failedEvents,
      totalActiveEvents: stats.activeEvents
    });

    // Keep only recent history (last hour)
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
    this.failureHistory = this.failureHistory.filter(
      record => record.timestamp > oneHourAgo
    );
  }

  /**
   * Determine if restart should be triggered based on failure statistics
   */
  shouldTriggerRestart(stats) {
    // Check restart cooldown
    const timeSinceLastRestart = Date.now() - this.lastRestartTime;
    if (timeSinceLastRestart < this.config.RESTART_COOLDOWN) {
      this.log(`Restart on cooldown (${Math.round((this.config.RESTART_COOLDOWN - timeSinceLastRestart) / 1000)}s remaining)`, 'info');
      return false;
    }

    // Check hourly restart limit
    const oneHourAgo = Date.now() - 60 * 60 * 1000;
    const recentRestarts = this.restartHistory.filter(time => time > oneHourAgo).length;
    if (recentRestarts >= this.config.MAX_RESTARTS_PER_HOUR) {
      this.log(`Maximum restarts per hour reached (${recentRestarts}/${this.config.MAX_RESTARTS_PER_HOUR})`, 'warning');
      return false;
    }

    // Check failure percentage threshold
    const highFailureRate = stats.failurePercentage >= this.config.FAILURE_PERCENTAGE_THRESHOLD;
    
    // Check absolute failure threshold
    const highAbsoluteFailures = stats.failedEvents >= this.config.ABSOLUTE_FAILURE_THRESHOLD;
    
    if (highFailureRate || highAbsoluteFailures) {
      this.consecutiveHighFailures++;
      
      this.log(
        `🚨 High failure rate detected: ${stats.failurePercentage.toFixed(1)}% ` +
        `(${stats.failedEvents}/${stats.activeEvents}) - Consecutive: ${this.consecutiveHighFailures}`,
        'warning'
      );
      
      // Trigger restart after consecutive high failures
      if (this.consecutiveHighFailures >= this.config.CONSECUTIVE_FAILURES_THRESHOLD) {
        return true;
      }
    } else {
      this.consecutiveHighFailures = 0;
    }

    return false;
  }

  /**
   * Perform server restart and event reactivation
   */
  async performRestart(stats) {
    if (this.restartInProgress) {
      this.log('Restart already in progress', 'warning');
      return;
    }

    this.restartInProgress = true;
    this.lastRestartTime = Date.now();
    this.restartCount++;
    this.restartHistory.push(this.lastRestartTime);

    this.log(
      `🔄 TRIGGERING AUTO-RESTART #${this.restartCount}: ` +
      `${stats.failurePercentage.toFixed(1)}% failure rate ` +
      `(${stats.failedEvents}/${stats.activeEvents} events failed)`,
      'error'
    );

    try {
      // Step 1: Stop current scraper
      this.log('🛑 Stopping scraper manager...', 'info');
      if (scraperManager && scraperManager.isRunning) {
        await scraperManager.stopScraping();
      }

      // Step 2: Save restart info
      await this.saveRestartInfo(stats);

      // Step 3: Restart the server process
      this.log('🔄 Restarting server process...', 'info');
      await this.restartServerProcess();

    } catch (error) {
      this.log(`Error during restart process: ${error.message}`, 'error');
      this.restartInProgress = false;
    }
  }

  /**
   * Save restart information for post-restart recovery
   */
  async saveRestartInfo(stats) {
    const restartInfo = {
      timestamp: new Date().toISOString(),
      reason: 'auto_restart_high_failures',
      stats: stats,
      restartCount: this.restartCount,
      shouldReactivateEvents: true,
      reactivationConfig: {
        batchSize: this.config.EVENT_REACTIVATION_BATCH_SIZE,
        delay: this.config.EVENT_REACTIVATION_DELAY
      }
    };

    try {
      const restartInfoPath = path.join(process.cwd(), 'restart_info.json');
      await fs.writeFile(restartInfoPath, JSON.stringify(restartInfo, null, 2));
      this.log('💾 Saved restart information for post-restart recovery', 'info');
    } catch (error) {
      this.log(`Error saving restart info: ${error.message}`, 'error');
    }
  }

  /**
   * Restart the server process
   */
  async restartServerProcess() {
    try {
      // Close current server gracefully
      if (global.serverInstance) {
        this.log('🔄 Closing current server instance...', 'info');
        global.serverInstance.close();
      }

      // Wait a moment for cleanup
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Restart the process
      this.log('🚀 Restarting process...', 'info');
      
      // Use PM2 restart if available, otherwise exit for process manager restart
      try {
        await execAsync('pm2 restart all');
        this.log('✅ PM2 restart initiated', 'info');
      } catch (pm2Error) {
        // If PM2 is not available, exit the process
        // Process manager (like systemd, docker, etc.) should restart it
        this.log('🔄 PM2 not available, exiting for process manager restart...', 'info');
        process.exit(1);
      }

    } catch (error) {
      this.log(`Error restarting server: ${error.message}`, 'error');
      // Force exit as last resort
      process.exit(1);
    }
  }

  /**
   * Handle post-restart event reactivation
   * This should be called after server restart
   */
  static async handlePostRestartRecovery() {
    try {
      const restartInfoPath = path.join(process.cwd(), 'restart_info.json');
      
      // Check if restart info exists
      try {
        await fs.access(restartInfoPath);
      } catch {
        // No restart info, normal startup
        return;
      }

      // Read restart info
      const restartInfoContent = await fs.readFile(restartInfoPath, 'utf8');
      const restartInfo = JSON.parse(restartInfoContent);

      console.log(`🔄 Post-restart recovery: Detected restart from ${restartInfo.timestamp}`);
      console.log(`📊 Previous stats: ${restartInfo.stats.failurePercentage.toFixed(1)}% failure rate`);

      if (restartInfo.shouldReactivateEvents) {
        console.log('🔄 Starting event reactivation process...');
        
        // Wait for system to stabilize
        await new Promise(resolve => setTimeout(resolve, 30000)); // 30 seconds
        
        // Reactivate all events
        await AutoRestartMonitor.reactivateAllEvents(restartInfo.reactivationConfig);
        
        // Start scraper if configured
        if (scraperManager && !scraperManager.isRunning) {
          console.log('🚀 Starting scraper manager...');
          await scraperManager.startScraping();
        }
      }

      // Clean up restart info
      await fs.unlink(restartInfoPath);
      console.log('✅ Post-restart recovery completed');

    } catch (error) {
      console.error(`Error in post-restart recovery: ${error.message}`);
    }
  }

  /**
   * Reactivate all events (set Skip_Scraping = false)
   */
  static async reactivateAllEvents(config = {}) {
    const batchSize = config.batchSize || 20;
    const delay = config.delay || 2000;

    try {
      // Get all events that are currently skipped
      const skippedEvents = await Event.find({ Skip_Scraping: true });
      
      if (skippedEvents.length === 0) {
        console.log('📊 No events to reactivate');
        return;
      }

      console.log(`🔄 Reactivating ${skippedEvents.length} events in batches of ${batchSize}...`);

      // Process in batches
      for (let i = 0; i < skippedEvents.length; i += batchSize) {
        const batch = skippedEvents.slice(i, i + batchSize);
        const eventIds = batch.map(event => event.Event_ID);
        
        // Reactivate batch
        await Event.updateMany(
          { Event_ID: { $in: eventIds } },
          { 
            Skip_Scraping: false,
            Last_Updated: new Date()
          }
        );
        
        console.log(`✅ Reactivated batch ${Math.floor(i / batchSize) + 1}: ${eventIds.length} events`);
        
        // Wait between batches
        if (i + batchSize < skippedEvents.length) {
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }

      console.log(`✅ Successfully reactivated ${skippedEvents.length} events`);

    } catch (error) {
      console.error(`Error reactivating events: ${error.message}`);
    }
  }

  /**
   * Get monitoring status
   */
  getStatus() {
    return {
      isMonitoring: this.isMonitoring,
      restartInProgress: this.restartInProgress,
      consecutiveHighFailures: this.consecutiveHighFailures,
      restartCount: this.restartCount,
      lastRestartTime: this.lastRestartTime,
      config: this.config,
      failureHistoryLength: this.failureHistory.length
    };
  }

  /**
   * Update configuration
   */
  updateConfig(newConfig) {
    this.config = { ...this.config, ...newConfig };
    this.log('📝 Updated auto-restart configuration', 'info');
  }

  /**
   * Log with timestamp
   */
  log(message, level = 'info') {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] [AutoRestartMonitor] ${message}`;
    
    if (this.logger) {
      this.logger.logWithTime(logMessage, level);
    } else {
      console.log(logMessage);
    }
  }
}

export default AutoRestartMonitor;