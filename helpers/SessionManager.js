import { createRequire } from "module";
const require = createRequire(import.meta.url);
const fs = require("fs");
const path = require("path");
import moment from "moment";
import { setTimeout } from "timers/promises";
import { BrowserFingerprint } from '../browserFingerprint.js';
import { refreshHeaders } from "../scraper.js";

/**
 * SessionManager handles browser session management for robust scraping
 * Maintains persistent sessions and rotates them every 30 minutes
 */
export class SessionManager {
  constructor(logger) {
    this.logger = logger;
    this.sessions = new Map(); // sessionId -> sessionData
    this.activeSessions = new Map(); // eventId -> sessionId
    this.sessionRotationTimestamps = new Map(); // sessionId -> timestamp
    this.sessionsPath = path.join(process.cwd(), "sessions.json");
    this.isRotatingSession = false;
    this.sessionRotationQueue = [];
    
    // Session configuration
    // Updated session configuration to align intervals with session expiration
    this.SESSION_CONFIG = {
      ROTATION_INTERVAL: 25 * 60 * 1000, // 25 minutes, shorter than session expiration
      MAX_SESSION_AGE: 2 * 60 * 60 * 1000, // 2 hours maximum
      MAX_SESSIONS: 200, // Maximum number of concurrent sessions
      SESSION_WARMUP_TIME: 5000, // 5 seconds to warm up new sessions
      SESSION_VALIDATION_INTERVAL: 4 * 60 * 1000, // Validate sessions every 4 minutes
      SESSION_HEALTH_CHECK_TIMEOUT: 30000, // 30 seconds timeout for health checks
    };

    // Start session validation interval
    this.startSessionValidation();
    
    // Load existing sessions from file
    this.loadSessionsFromFile();
  }

  /**
   * Get or create a session for an event
   * @param {string} eventId - The event ID
   * @param {object} proxy - Proxy configuration
   * @returns {Promise<object>} Session data with cookies and headers
   */
  async getSessionForEvent(eventId, proxy = null) {
    // Check if event already has an active session
    let sessionId = this.activeSessions.get(eventId);
    let sessionData = null;

    if (sessionId) {
      sessionData = this.sessions.get(sessionId);
      
      // Check if session needs rotation (30 minutes old)
      const sessionAge = Date.now() - (this.sessionRotationTimestamps.get(sessionId) || 0);
      if (sessionAge > this.SESSION_CONFIG.ROTATION_INTERVAL) {
        this.logger?.logWithTime(`Session ${sessionId} for event ${eventId} needs rotation (age: ${Math.floor(sessionAge / 60000)} minutes)`, "info");
        sessionData = null; // Force new session creation
      }
      
      // Validate session health
      if (sessionData && !(await this.validateSession(sessionData))) {
        this.logger?.logWithTime(`Session ${sessionId} for event ${eventId} failed validation`, "warning");
        sessionData = null; // Force new session creation
      }
    }

    // Create new session if needed
    if (!sessionData) {
      sessionData = await this.createNewSession(eventId, proxy);
      if (sessionData) {
        // Clean up old session if it exists
        if (sessionId) {
          await this.cleanupSession(sessionId);
        }
        
        // Register new session
        sessionId = sessionData.sessionId;
        this.sessions.set(sessionId, sessionData);
        this.activeSessions.set(eventId, sessionId);
        this.sessionRotationTimestamps.set(sessionId, Date.now());
        
        this.logger?.logWithTime(`Created new session ${sessionId} for event ${eventId}`, "success");
      }
    }

    return sessionData;
  }

  /**
   * Create a new session with fresh cookies and browser context
   * @param {string} eventId - The event ID  
   * @param {object} proxy - Proxy configuration
   * @returns {Promise<object>} New session data
   */
  async createNewSession(eventId, proxy = null) {
    try {
      this.logger?.logWithTime(`Creating new session for event ${eventId}`, "info");

      // Get random event ID from database for session creation
      let sessionEventId = eventId;
      try {
        const { Event } = await import('../models/index.js');
        const randomEvents = await Event.aggregate([
          {
            $match: {
              Skip_Scraping: { $ne: true },
              url: { $exists: true, $ne: "" },
            },
          },
          { $sample: { size: 3 } },
          { $project: { Event_ID: 1, url: 1 } },
        ]);

        if (randomEvents && randomEvents.length > 0) {
          const selectedEvent = randomEvents[Math.floor(Math.random() * randomEvents.length)];
          sessionEventId = selectedEvent.Event_ID;
          this.logger?.logWithTime(`Using random event ${sessionEventId} for session creation`, "debug");
        }
      } catch (dbError) {
        this.logger?.logWithTime(`Failed to get random event from database: ${dbError.message}`, "warning");
      }

      // Generate session ID
      const sessionId = this.generateSessionId();
      
      // Create fresh cookies and headers for this session
      const sessionState = await refreshHeaders(sessionEventId, proxy);
      
      if (!sessionState || !sessionState.cookies || sessionState.cookies.length === 0) {
        throw new Error("Failed to create session: No valid cookies obtained");
      }

      // Create session data structure
      const sessionData = {
        sessionId,
        eventId: sessionEventId,
        originalEventId: eventId,
        cookies: sessionState.cookies,
        headers: sessionState.headers || {},
        fingerprint: sessionState.fingerprint || BrowserFingerprint.generate(),
        proxy: proxy,
        createdAt: Date.now(),
        lastUsed: Date.now(),
        lastValidated: Date.now(),
        usageCount: 0,
        isValid: true,
        metadata: {
          userAgent: sessionState.headers?.['User-Agent'] || '',
          referer: sessionState.headers?.['Referer'] || '',
          origin: sessionState.headers?.['Origin'] || '',
        }
      };

      // Warm up the session
      await this.warmupSession(sessionData);

      // Save sessions to file
      await this.saveSessionsToFile();

      return sessionData;
    } catch (error) {
      this.logger?.logWithTime(`Failed to create new session for event ${eventId}: ${error.message}`, "error");
      return null;
    }
  }

  /**
   * Warm up a new session by making a test request
   * @param {object} sessionData - Session data to warm up
   * @returns {Promise<boolean>} Success status
   */
  async warmupSession(sessionData) {
    try {
      this.logger?.logWithTime(`Warming up session ${sessionData.sessionId}`, "debug");
      
      // Simple warmup by validating the session
      const isValid = await this.validateSession(sessionData);
      
      if (isValid) {
        sessionData.lastValidated = Date.now();
        this.logger?.logWithTime(`Session ${sessionData.sessionId} warmed up successfully`, "debug");
      }
      
      await setTimeout(this.SESSION_CONFIG.SESSION_WARMUP_TIME);
      return isValid;
    } catch (error) {
      this.logger?.logWithTime(`Failed to warm up session ${sessionData.sessionId}: ${error.message}`, "warning");
      return false;
    }
  }

  /**
   * Validate if a session is still healthy and usable
   * @param {object} sessionData - Session data to validate
   * @returns {Promise<boolean>} Validation result
   */
  async validateSession(sessionData) {
    try {
      // Basic validation checks
      if (!sessionData || !sessionData.cookies || sessionData.cookies.length === 0) {
        return false;
      }

      // Check session age
      const sessionAge = Date.now() - sessionData.createdAt;
      if (sessionAge > this.SESSION_CONFIG.MAX_SESSION_AGE) {
        this.logger?.logWithTime(`Session ${sessionData.sessionId} expired (age: ${Math.floor(sessionAge / 60000)} minutes)`, "info");
        return false;
      }

      // Check cookie expiration
      const expiredCookies = sessionData.cookies.filter(cookie => {
        if (cookie.expires && cookie.expires * 1000 < Date.now()) {
          return true;
        }
        return false;
      });

      if (expiredCookies.length > sessionData.cookies.length * 0.5) {
        this.logger?.logWithTime(`Session ${sessionData.sessionId} has too many expired cookies`, "warning");
        return false;
      }

      // Update validation timestamp
      sessionData.lastValidated = Date.now();
      sessionData.isValid = true;

      return true;
    } catch (error) {
      this.logger?.logWithTime(`Session validation error for ${sessionData.sessionId}: ${error.message}`, "error");
      return false;
    }
  }

  /**
   * Update session usage statistics
   * @param {string} sessionId - Session ID
   * @param {boolean} success - Whether the usage was successful
   */
  updateSessionUsage(sessionId, success = true) {
    const sessionData = this.sessions.get(sessionId);
    if (sessionData) {
      sessionData.lastUsed = Date.now();
      sessionData.usageCount = (sessionData.usageCount || 0) + 1;
      
      if (!success) {
        sessionData.failureCount = (sessionData.failureCount || 0) + 1;
        
        // Mark session as invalid if too many failures
        if (sessionData.failureCount > 5) {
          sessionData.isValid = false;
          this.logger?.logWithTime(`Session ${sessionId} marked as invalid due to failures`, "warning");
        }
      }
    }
  }

  /**
   * Get session headers for making requests
   * @param {string} eventId - Event ID
   * @returns {object} Headers object ready for HTTP requests
   */
  async getSessionHeaders(eventId) {
    const sessionId = this.activeSessions.get(eventId);
    if (!sessionId) {
      return null;
    }

    const sessionData = this.sessions.get(sessionId);
    if (!sessionData || !sessionData.isValid) {
      return null;
    }

    // Generate dynamic headers based on session data
    const cookieString = sessionData.cookies
      .map(cookie => `${cookie.name}=${cookie.value}`)
      .join('; ');

    const headers = {
      'Cookie': cookieString,
      'User-Agent': sessionData.metadata.userAgent || sessionData.headers?.['User-Agent'] || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.5',
      'Accept-Encoding': 'gzip, deflate, br',
      'Referer': sessionData.metadata.referer || 'https://www.ticketmaster.com/',
      'Origin': sessionData.metadata.origin || 'https://www.ticketmaster.com',
      'Connection': 'keep-alive',
      'Upgrade-Insecure-Requests': '1',
      'Sec-Fetch-Dest': 'document',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': 'same-origin',
      'Cache-Control': 'max-age=0',
      ...sessionData.headers, // Include any additional headers from session
    };

    // Update session usage
    this.updateSessionUsage(sessionId, true);

    return headers;
  }

  /**
   * Force rotation of sessions (for manual triggering)
   * @returns {Promise<number>} Number of sessions rotated
   */
  async forceSessionRotation() {
    this.logger?.logWithTime("Force rotating all sessions", "info");
    
    let rotatedCount = 0;
    const eventIds = Array.from(this.activeSessions.keys());
    
    for (const eventId of eventIds) {
      try {
        const sessionId = this.activeSessions.get(eventId);
        if (sessionId) {
          await this.cleanupSession(sessionId);
          this.activeSessions.delete(eventId);
          rotatedCount++;
        }
      } catch (error) {
        this.logger?.logWithTime(`Error rotating session for event ${eventId}: ${error.message}`, "error");
      }
    }

    await this.saveSessionsToFile();
    this.logger?.logWithTime(`Force rotated ${rotatedCount} sessions`, "success");
    
    return rotatedCount;
  }

  /**
   * Start automatic session validation and cleanup
   */
  startSessionValidation() {
    setInterval(async () => {
      try {
        // Validate and cleanup sessions
        await this.validateAndCleanupSessions();

        // Force session rotation after ROTATION_INTERVAL
        const now = Date.now();
        for (const [sessionId, timestamp] of this.sessionRotationTimestamps.entries()) {
          if (now - timestamp >= this.SESSION_CONFIG.ROTATION_INTERVAL) {
            this.logger?.logWithTime(`Session ${sessionId} exceeded rotation interval, forcing rotation`, "info");
            await this.cleanupSession(sessionId);
          }
        }

        // Generate fresh sessions for active events
        for (const eventId of this.activeSessions.keys()) {
          await this.getSessionForEvent(eventId);
        }
      } catch (error) {
        this.logger?.logWithTime(`Error in session validation cycle: ${error.message}`, "error");
      }
    }, this.SESSION_CONFIG.SESSION_VALIDATION_INTERVAL);
  }

  /**
   * Validate all sessions and cleanup invalid ones
   */
  async validateAndCleanupSessions() {
    const sessionIds = Array.from(this.sessions.keys());
    let cleanedCount = 0;

    for (const sessionId of sessionIds) {
      const sessionData = this.sessions.get(sessionId);
      if (!sessionData) continue;

      const isValid = await this.validateSession(sessionData);
      if (!isValid) {
        await this.cleanupSession(sessionId);
        cleanedCount++;
      }
    }

    if (cleanedCount > 0) {
      this.logger?.logWithTime(`Cleaned up ${cleanedCount} invalid sessions`, "info");
      await this.saveSessionsToFile();
    }
  }

  /**
   * Cleanup a specific session
   * @param {string} sessionId - Session ID to cleanup
   */
  async cleanupSession(sessionId) {
    try {
      this.sessions.delete(sessionId);
      this.sessionRotationTimestamps.delete(sessionId);
      
      // Remove from active sessions
      for (const [eventId, activeSessionId] of this.activeSessions.entries()) {
        if (activeSessionId === sessionId) {
          this.activeSessions.delete(eventId);
        }
      }
      
      this.logger?.logWithTime(`Cleaned up session ${sessionId}`, "debug");
    } catch (error) {
      this.logger?.logWithTime(`Error cleaning up session ${sessionId}: ${error.message}`, "error");
    }
  }

  /**
   * Generate a unique session ID
   * @returns {string} Unique session ID
   */
  generateSessionId() {
    return `session_${Date.now()}_${Math.random().toString(36).substring(2, 15)}`;
  }

  /**
   * Load sessions from file
   */
  async loadSessionsFromFile() {
    try {
      if (fs.existsSync(this.sessionsPath)) {
        const data = await fs.promises.readFile(this.sessionsPath, 'utf8');
        const sessionsData = JSON.parse(data);
        
        if (sessionsData.sessions) {
          for (const [sessionId, sessionData] of Object.entries(sessionsData.sessions)) {
            // Only load sessions that are less than max age
            const sessionAge = Date.now() - sessionData.createdAt;
            if (sessionAge < this.SESSION_CONFIG.MAX_SESSION_AGE) {
              this.sessions.set(sessionId, sessionData);
              if (sessionData.originalEventId) {
                this.activeSessions.set(sessionData.originalEventId, sessionId);
              }
              this.sessionRotationTimestamps.set(sessionId, sessionData.createdAt);
            }
          }
          
          this.logger?.logWithTime(`Loaded ${this.sessions.size} sessions from file`, "info");
        }
      }
    } catch (error) {
      this.logger?.logWithTime(`Error loading sessions from file: ${error.message}`, "error");
    }
  }

  /**
   * Save sessions to file
   */
  async saveSessionsToFile() {
    try {
      const sessionsData = {
        lastUpdated: Date.now(),
        sessions: Object.fromEntries(this.sessions),
        activeSessions: Object.fromEntries(this.activeSessions),
      };
      
      await fs.promises.writeFile(
        this.sessionsPath,
        JSON.stringify(sessionsData, null, 2)
      );
    } catch (error) {
      this.logger?.logWithTime(`Error saving sessions to file: ${error.message}`, "error");
    }
  }

  /**
   * Get session statistics
   * @returns {object} Session statistics
   */
  getSessionStats() {
    const totalSessions = this.sessions.size;
    const activeSessions = this.activeSessions.size;
    const validSessions = Array.from(this.sessions.values()).filter(s => s.isValid).length;
    
    const sessionAges = Array.from(this.sessions.values()).map(s => Date.now() - s.createdAt);
    const avgAge = sessionAges.length > 0 ? sessionAges.reduce((a, b) => a + b, 0) / sessionAges.length : 0;

    return {
      totalSessions,
      activeSessions,
      validSessions,
      averageAge: Math.floor(avgAge / 60000), // in minutes
      oldestSession: sessionAges.length > 0 ? Math.floor(Math.max(...sessionAges) / 60000) : 0,
    };
  }

  /**
   * Clean up all sessions and stop the session manager
   */
  async shutdown() {
    this.logger?.logWithTime("Shutting down SessionManager", "info");
    
    // Save current sessions
    await this.saveSessionsToFile();
    
    // Clear all sessions
    this.sessions.clear();
    this.activeSessions.clear();
    this.sessionRotationTimestamps.clear();
    
    this.logger?.logWithTime("SessionManager shutdown complete", "success");
  }
}

export default SessionManager;