import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

class SessionResetManager {
  constructor(sessionManager, cookieManager, scraperManager) {
    this.sessionManager = sessionManager;
    this.cookieManager = cookieManager;
    this.scraperManager = scraperManager;
    this.isResetting = false;
  }

  /**
   * Force a complete session reset - clears everything and starts fresh
   */
  async forceCompleteSessionReset() {
    if (this.isResetting) {
      this.scraperManager.logWithTime('Session reset already in progress, skipping...', 'warning');
      return false;
    }

    this.isResetting = true;
    const resetStartTime = Date.now();
    
    try {
      this.scraperManager.logWithTime('Starting complete session reset...', 'info');
      
      // Step 1: Clear all existing sessions
      this.scraperManager.logWithTime('Clearing existing sessions...', 'info');
      await this.sessionManager.forceSessionRotation();
      
      // Step 2: Delete sessions.json file if it exists
      try {
        const sessionsPath = path.join(process.cwd(), 'sessions.json');
        await fs.unlink(sessionsPath);
        this.scraperManager.logWithTime('Deleted sessions.json file', 'info');
      } catch (error) {
        if (error.code !== 'ENOENT') {
          this.scraperManager.logWithTime(`Warning: Could not delete sessions.json: ${error.message}`, 'warning');
        }
      }
      
      // Step 3: Reset cookies and headers
      this.scraperManager.logWithTime('Resetting cookies and headers...', 'info');
      if (this.cookieManager && typeof this.cookieManager.resetCookiesAndHeaders === 'function') {
        await this.cookieManager.resetCookiesAndHeaders();
      }
      
      // Step 4: Clear all caches and timestamps in ScraperManager
      this.scraperManager.logWithTime('Clearing caches and timestamps...', 'info');
      this.scraperManager.headerCache.clear();
      this.scraperManager.headerRefreshTimestamps.clear();
      this.scraperManager.eventFailureCounts.clear();
      this.scraperManager.eventLastUpdateTimes.clear();
      this.scraperManager.eventLastSuccessTimes.clear();
      this.scraperManager.lastCookieRefresh = 0;
      this.scraperManager.lastHeaderRefresh = 0;
      
      // Step 5: Reset session manager state
      this.scraperManager.logWithTime('Resetting session manager state...', 'info');
      if (this.sessionManager.sessions) {
        this.sessionManager.sessions.clear();
      }
      if (this.sessionManager.sessionStats) {
        this.sessionManager.sessionStats = {
          totalSessions: 0,
          activeSessions: 0,
          expiredSessions: 0,
          totalRequests: 0,
          successfulRequests: 0,
          failedRequests: 0
        };
      }
      
      // Step 6: Generate fresh test sessions
      this.scraperManager.logWithTime('Generating fresh test sessions...', 'info');
      await this.generateFreshTestSessions();
      
      const resetDuration = Date.now() - resetStartTime;
      this.scraperManager.logWithTime(`Complete session reset finished in ${resetDuration}ms`, 'success');
      
      return true;
      
    } catch (error) {
      this.scraperManager.logWithTime(`Error during complete session reset: ${error.message}`, 'error');
      return false;
    } finally {
      this.isResetting = false;
    }
  }

  /**
   * Generate fresh test sessions using available event IDs
   */
  async generateFreshTestSessions() {
    try {
      // Try to get event IDs from various sources
      let testEventIds = [];
      
      // Try cached event IDs first
      if (this.scraperManager.cachedEventIds && this.scraperManager.cachedEventIds.length > 0) {
        testEventIds = this.scraperManager.cachedEventIds.slice(0, 3);
        this.scraperManager.logWithTime(`Using ${testEventIds.length} cached event IDs for session generation`, 'info');
      }
      
      // If no cached IDs, try to get from database
      if (testEventIds.length === 0) {
        try {
          const { Event } = await import('../models/index.js');
          const events = await Event.find({}).limit(3).lean();
          testEventIds = events.map(event => event.Event_ID);
          this.scraperManager.logWithTime(`Using ${testEventIds.length} database event IDs for session generation`, 'info');
        } catch (dbError) {
          this.scraperManager.logWithTime(`Could not fetch events from database: ${dbError.message}`, 'warning');
        }
      }
      
      // Generate sessions for each test event ID
      if (testEventIds.length > 0) {
        for (const eventId of testEventIds) {
          try {
            await Promise.race([
              this.scraperManager.refreshEventHeaders(eventId, true),
              new Promise((_, reject) => 
                setTimeout(() => reject(new Error('Session generation timeout')), 15000)
              )
            ]);
            this.scraperManager.logWithTime(`Generated fresh session for event ${eventId}`, 'info');
          } catch (sessionError) {
            this.scraperManager.logWithTime(`Failed to generate session for event ${eventId}: ${sessionError.message}`, 'warning');
          }
        }
      } else {
        this.scraperManager.logWithTime('No event IDs available for session generation', 'warning');
      }
      
    } catch (error) {
      this.scraperManager.logWithTime(`Error generating fresh test sessions: ${error.message}`, 'error');
    }
  }

  /**
   * Start automatic hourly session refresh
   */
  startHourlySessionRefresh() {
    const HOURLY_REFRESH_INTERVAL = 60 * 60 * 1000; // 1 hour
    
    this.hourlyRefreshInterval = setInterval(async () => {
      this.scraperManager.logWithTime('Starting hourly automatic session refresh...', 'info');
      await this.forceCompleteSessionReset();
    }, HOURLY_REFRESH_INTERVAL);
    
    this.scraperManager.logWithTime('Hourly session refresh started', 'info');
  }

  /**
   * Stop automatic hourly session refresh
   */
  stopHourlySessionRefresh() {
    if (this.hourlyRefreshInterval) {
      clearInterval(this.hourlyRefreshInterval);
      this.hourlyRefreshInterval = null;
      this.scraperManager.logWithTime('Hourly session refresh stopped', 'info');
    }
  }

  /**
   * Get reset status
   */
  getResetStatus() {
    return {
      isResetting: this.isResetting,
      hourlyRefreshActive: !!this.hourlyRefreshInterval
    };
  }
}

export default SessionResetManager;