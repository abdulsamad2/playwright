// Instance configuration for horizontal scaling
export const INSTANCE_TYPES = {
  PRIMARY: 'primary',
  SECONDARY: 'secondary'
};

// Configuration for different instance types
export const INSTANCE_CONFIG = {
  [INSTANCE_TYPES.PRIMARY]: {
    // Primary instance handles cookies, CSV upload, and coordination
    enableCookieRefresh: true,
    enableCsvUpload: true,
    enableCsvProcessing: true,
    enableScraping: true,
    enableCoordination: true,
    enableSessionManagement: true,
    // Primary can handle fewer events since it has additional responsibilities
    maxEvents: 100,
    port: 3000,
    role: 'coordinator'
  },
  [INSTANCE_TYPES.SECONDARY]: {
    // Secondary instances only handle scraping
    enableCookieRefresh: false,
    enableCsvUpload: false,
    enableCsvProcessing: false,
    enableScraping: true,
    enableCoordination: false,
    enableSessionManagement: false,
    // Secondary instances can handle more events
    maxEvents: 100,
    port: null, // Will be auto-assigned
    role: 'worker'
  }
};

// Get current instance configuration based on environment
export function getCurrentInstanceConfig() {
  const instanceType = process.env.INSTANCE_TYPE || INSTANCE_TYPES.PRIMARY;
  const config = { ...INSTANCE_CONFIG[instanceType] };
  
  // Override port if specified in environment
  if (process.env.PORT) {
    config.port = parseInt(process.env.PORT, 10);
  }
  
  // Auto-assign port for secondary instances if not specified
  if (instanceType === INSTANCE_TYPES.SECONDARY && !config.port) {
    config.port = 3000 + Math.floor(Math.random() * 1000) + 1;
  }
  
  return {
    type: instanceType,
    ...config
  };
}

// Coordination endpoints for primary instance
export const COORDINATION_ENDPOINTS = {
  REGISTER_INSTANCE: '/api/coordination/register',
  HEARTBEAT: '/api/coordination/heartbeat',
  GET_EVENTS: '/api/coordination/events',
  UPDATE_EVENT_STATUS: '/api/coordination/events/status',
  GET_COOKIES: '/api/coordination/cookies',
  GET_SESSIONS: '/api/coordination/sessions'
};

// Health check intervals
export const HEALTH_CHECK_CONFIG = {
  HEARTBEAT_INTERVAL: 30000, // 30 seconds
  INSTANCE_TIMEOUT: 120000, // 2 minutes
  COORDINATION_RETRY_INTERVAL: 10000 // 10 seconds
};

export default {
  INSTANCE_TYPES,
  INSTANCE_CONFIG,
  getCurrentInstanceConfig,
  COORDINATION_ENDPOINTS,
  HEALTH_CHECK_CONFIG
};