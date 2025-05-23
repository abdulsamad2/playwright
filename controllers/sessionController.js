import SessionManager from '../helpers/SessionManager.js';

// Initialize a shared session manager instance
let sessionManager = null;

/**
 * Initialize session manager with logger
 */
const getSessionManager = () => {
  if (!sessionManager) {
    // Create a simple logger for the session manager
    const logger = {
      logWithTime: (message, level = 'info') => {
        const timestamp = new Date().toISOString();
        const levelEmoji = {
          'info': 'ℹ️',
          'debug': '🔍',
          'warning': '⚠️',
          'error': '❌',
          'success': '✅'
        }[level] || '📝';
        console.log(`${levelEmoji} [${timestamp}] SessionController: ${message}`);
      }
    };
    
    sessionManager = new SessionManager(logger);
  }
  return sessionManager;
};

/**
 * Get session statistics
 * @param {object} req - Express request object
 * @param {object} res - Express response object
 */
export const getSessionStats = async (req, res) => {
  try {
    const manager = getSessionManager();
    const stats = manager.getSessionStats();
    
    res.json({
      success: true,
      data: {
        stats,
        sessionConfig: {
          rotationInterval: manager.SESSION_CONFIG.ROTATION_INTERVAL,
          maxSessionAge: manager.SESSION_CONFIG.MAX_SESSION_AGE,
          maxSessions: manager.SESSION_CONFIG.MAX_SESSIONS,
          validationInterval: manager.SESSION_CONFIG.SESSION_VALIDATION_INTERVAL
        }
      }
    });
  } catch (error) {
    console.error('Error getting session stats:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
};

/**
 * Get active sessions
 * @param {object} req - Express request object
 * @param {object} res - Express response object
 */
export const getActiveSessions = async (req, res) => {
  try {
    const manager = getSessionManager();
    const limit = parseInt(req.query.limit) || 50;
    
    const activeSessions = Array.from(manager.activeSessions.entries()).slice(0, limit);
    const sessionDetails = [];
    
    for (const [eventId, sessionId] of activeSessions) {
      const sessionData = manager.sessions.get(sessionId);
      if (sessionData) {
        sessionDetails.push({
          eventId,
          sessionId,
          createdAt: new Date(sessionData.createdAt).toISOString(),
          lastUsed: new Date(sessionData.lastUsed).toISOString(),
          usageCount: sessionData.usageCount || 0,
          isValid: sessionData.isValid,
          age: Math.floor((Date.now() - sessionData.createdAt) / 60000), // in minutes
          cookieCount: sessionData.cookies?.length || 0,
          failureCount: sessionData.failureCount || 0
        });
      }
    }
    
    res.json({
      success: true,
      data: {
        activeSessions: sessionDetails,
        totalActiveSessions: manager.activeSessions.size,
        totalSessions: manager.sessions.size
      }
    });
  } catch (error) {
    console.error('Error getting active sessions:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
};

/**
 * Force session rotation
 * @param {object} req - Express request object
 * @param {object} res - Express response object
 */
export const forceSessionRotation = async (req, res) => {
  try {
    const manager = getSessionManager();
    const rotatedCount = await manager.forceSessionRotation();
    
    res.json({
      success: true,
      data: {
        message: `Successfully rotated ${rotatedCount} sessions`,
        rotatedSessions: rotatedCount,
        timestamp: new Date().toISOString()
      }
    });
  } catch (error) {
    console.error('Error forcing session rotation:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
};

/**
 * Get session details for a specific event
 * @param {object} req - Express request object
 * @param {object} res - Express response object
 */
export const getSessionForEvent = async (req, res) => {
  try {
    const { eventId } = req.params;
    const manager = getSessionManager();
    
    const sessionId = manager.activeSessions.get(eventId);
    if (!sessionId) {
      return res.json({
        success: true,
        data: {
          eventId,
          hasSession: false,
          message: 'No active session found for this event'
        }
      });
    }
    
    const sessionData = manager.sessions.get(sessionId);
    if (!sessionData) {
      return res.json({
        success: true,
        data: {
          eventId,
          hasSession: false,
          message: 'Session ID found but session data missing'
        }
      });
    }
    
    const sessionDetails = {
      eventId,
      sessionId,
      hasSession: true,
      createdAt: new Date(sessionData.createdAt).toISOString(),
      lastUsed: new Date(sessionData.lastUsed).toISOString(),
      lastValidated: new Date(sessionData.lastValidated).toISOString(),
      usageCount: sessionData.usageCount || 0,
      failureCount: sessionData.failureCount || 0,
      isValid: sessionData.isValid,
      age: Math.floor((Date.now() - sessionData.createdAt) / 60000), // in minutes
      cookieCount: sessionData.cookies?.length || 0,
      userAgent: sessionData.metadata?.userAgent || 'Unknown',
      proxy: sessionData.proxy ? `${sessionData.proxy.host}:${sessionData.proxy.port}` : 'None'
    };
    
    res.json({
      success: true,
      data: sessionDetails
    });
  } catch (error) {
    console.error('Error getting session for event:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
};

/**
 * Create a new session for an event
 * @param {object} req - Express request object
 * @param {object} res - Express response object
 */
export const createSessionForEvent = async (req, res) => {
  try {
    const { eventId } = req.params;
    const { proxy } = req.body; // Optional proxy configuration
    const manager = getSessionManager();
    
    // Get or create session for the event
    const sessionData = await manager.getSessionForEvent(eventId, proxy);
    
    if (!sessionData) {
      return res.status(500).json({
        success: false,
        error: 'Failed to create session for event'
      });
    }
    
    res.json({
      success: true,
      data: {
        eventId,
        sessionId: sessionData.sessionId,
        created: true,
        createdAt: new Date(sessionData.createdAt).toISOString(),
        cookieCount: sessionData.cookies?.length || 0,
        message: 'Session created successfully'
      }
    });
  } catch (error) {
    console.error('Error creating session for event:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
};

/**
 * Validate all sessions and get validation report
 * @param {object} req - Express request object
 * @param {object} res - Express response object
 */
export const validateSessions = async (req, res) => {
  try {
    const manager = getSessionManager();
    await manager.validateAndCleanupSessions();
    
    const stats = manager.getSessionStats();
    
    res.json({
      success: true,
      data: {
        message: 'Session validation completed',
        stats,
        timestamp: new Date().toISOString()
      }
    });
  } catch (error) {
    console.error('Error validating sessions:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
};

export default {
  getSessionStats,
  getActiveSessions,
  forceSessionRotation,
  getSessionForEvent,
  createSessionForEvent,
  validateSessions
}; 