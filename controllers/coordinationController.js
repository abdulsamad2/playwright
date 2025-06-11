import { CoordinationService } from '../services/coordinationService.js';
import { getCurrentInstanceConfig } from '../config/instanceConfig.js';
import { CookieManager } from '../helpers/CookieManager.js';
import SessionManager from '../helpers/SessionManager.js';

// Initialize coordination service
let coordinationService = null;
let cookieManager = null;
let sessionManager = null;

// Create a simple logger wrapper for console
const createLogger = () => ({
  logWithTime: (message, type = 'info') => {
    const timestamp = new Date().toISOString().replace('T', ' ').substr(0, 19);
    const emoji = { info: 'ℹ️', error: '❌', warning: '⚠️', success: '✅' }[type] || '📝';
    console.log(`${emoji} [${timestamp}] ${message}`);
  }
});

// Initialize services based on instance type
function initializeServices() {
  if (!coordinationService) {
    coordinationService = new CoordinationService(createLogger());
  }
  
  const instanceConfig = getCurrentInstanceConfig();
  
  // Only initialize cookie and session managers for primary instance
  if (instanceConfig.type === 'primary') {
    if (!cookieManager) {
      cookieManager = new CookieManager(createLogger());
    }
    if (!sessionManager) {
      sessionManager = new SessionManager(createLogger());
    }
  }
}

// Initialize services
initializeServices();

/**
 * Register a new instance with the coordinator
 */
export const registerInstance = (req, res) => {
  try {
    const instanceConfig = getCurrentInstanceConfig();
    
    // Only primary instances can accept registrations
    if (instanceConfig.type !== 'primary') {
      return res.status(403).json({
        success: false,
        message: 'Only primary instances can accept registrations'
      });
    }
    
    const result = coordinationService.registerInstance(req.body);
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
};

/**
 * Update heartbeat from an instance
 */
export const updateHeartbeat = (req, res) => {
  try {
    const { instanceId } = req.body;
    const result = coordinationService.updateHeartbeat(instanceId, req.body);
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
};

/**
 * Get coordination status
 */
export const getCoordinationStatus = (req, res) => {
  try {
    const status = coordinationService.getCoordinationStatus();
    res.json({
      success: true,
      data: status
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
};

/**
 * Get list of registered instances
 */
export const getInstances = (req, res) => {
  try {
    const instances = coordinationService.getHealthyInstances();
    res.json({
      success: true,
      data: instances
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
};

/**
 * Get events assigned to requesting instance
 */
export const getEventsForInstance = async (req, res) => {
  try {
    const { instanceId } = req.query;
    const limit = parseInt(req.query.limit) || 100;
    
    if (!instanceId) {
      return res.status(400).json({
        success: false,
        message: 'instanceId is required'
      });
    }
    
    const events = await coordinationService.getEventsForInstance(instanceId, limit);
    res.json({
      success: true,
      data: events
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
};

/**
 * Update event status
 */
export const updateEventStatus = async (req, res) => {
  try {
    const { eventId, status, instanceId } = req.body;
    
    // Update event status in database
    // This should integrate with your existing event update logic
    
    res.json({
      success: true,
      message: 'Event status updated'
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
};

/**
 * Manually trigger event redistribution
 */
export const redistributeEvents = async (req, res) => {
  try {
    const instanceConfig = getCurrentInstanceConfig();
    
    // Only primary instances can redistribute events
    if (instanceConfig.type !== 'primary') {
      return res.status(403).json({
        success: false,
        message: 'Only primary instances can redistribute events'
      });
    }
    
    await coordinationService.redistributeEvents();
    res.json({
      success: true,
      message: 'Events redistributed successfully'
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
};

/**
 * Get cookies for secondary instances
 */
export const getCookies = async (req, res) => {
  try {
    const instanceConfig = getCurrentInstanceConfig();
    
    // Only primary instances can provide cookies
    if (instanceConfig.type !== 'primary' || !cookieManager) {
      return res.status(403).json({
        success: false,
        message: 'Cookies only available from primary instance'
      });
    }
    
    // Get fresh cookies from cookie manager
    const cookies = await cookieManager.getFreshCookies();
    
    res.json({
      success: true,
      data: {
        cookies,
        timestamp: Date.now()
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
};

/**
 * Get sessions for secondary instances
 */
export const getSessions = async (req, res) => {
  try {
    const instanceConfig = getCurrentInstanceConfig();
    
    // Only primary instances can provide sessions
    if (instanceConfig.type !== 'primary' || !sessionManager) {
      return res.status(403).json({
        success: false,
        message: 'Sessions only available from primary instance'
      });
    }
    
    const { eventId } = req.query;
    
    if (eventId) {
      // Get session for specific event
      const session = await sessionManager.getSessionForEvent(eventId);
      res.json({
        success: true,
        data: session
      });
    } else {
      // Get all active sessions
      const sessions = sessionManager.getAllActiveSessions();
      res.json({
        success: true,
        data: sessions
      });
    }
  } catch (error) {
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
};

/**
 * Refresh cookies (primary instance only)
 */
export const refreshCookies = async (req, res) => {
  try {
    const instanceConfig = getCurrentInstanceConfig();
    
    // Only primary instances can refresh cookies
    if (instanceConfig.type !== 'primary' || !cookieManager) {
      return res.status(403).json({
        success: false,
        message: 'Cookie refresh only available on primary instance'
      });
    }
    
    await cookieManager.refreshCookies();
    
    res.json({
      success: true,
      message: 'Cookies refreshed successfully'
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
};

/**
 * Get health status
 */
export const getHealth = (req, res) => {
  try {
    const instanceConfig = getCurrentInstanceConfig();
    const status = coordinationService.getCoordinationStatus();
    
    res.json({
      success: true,
      data: {
        instanceId: status.instanceId,
        instanceType: instanceConfig.type,
        isHealthy: true,
        uptime: process.uptime(),
        memoryUsage: process.memoryUsage(),
        coordination: status
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
};

/**
 * Stop a specific instance (primary only)
 */
export const stopInstance = async (req, res) => {
  try {
    const instanceConfig = getCurrentInstanceConfig();
    
    // Only primary instances can stop other instances
    if (instanceConfig.type !== 'primary') {
      return res.status(403).json({
        success: false,
        message: 'Only primary instances can stop other instances'
      });
    }
    
    const { instanceId } = req.params;
    
    // Remove instance and reassign its events
    await coordinationService.reassignEventsFromInstance(instanceId);
    
    res.json({
      success: true,
      message: `Instance ${instanceId} stopped and events reassigned`
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
};

export const coordinationController = {
  registerInstance,
  updateHeartbeat,
  getCoordinationStatus,
  getInstances,
  getEventsForInstance,
  updateEventStatus,
  redistributeEvents,
  getCookies,
  getSessions,
  refreshCookies,
  getHealth,
  stopInstance
};