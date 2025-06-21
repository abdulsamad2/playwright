import AdvancedScraperLogger from './AdvancedScraperLogger.js';

/**
 * Integration helper to add advanced file-based logging to ScraperManager
 * This replaces the existing console-only logging with comprehensive file logging
 */
class LoggerIntegration {
  constructor(scraperManager) {
    this.scraperManager = scraperManager;
    this.advancedLogger = new AdvancedScraperLogger(scraperManager);
    this.originalLogWithTime = scraperManager.logWithTime.bind(scraperManager);
    this.originalLogError = scraperManager.logError.bind(scraperManager);
    
    this.integrateLogger();
  }
  
  integrateLogger() {
    // Replace the original logWithTime method
    this.scraperManager.logWithTime = async (message, type = "info") => {
      // Call original method for console output
      this.originalLogWithTime(message, type);
      
      // Add file logging
      const level = this.mapTypeToLevel(type);
      await this.advancedLogger.logGeneral(level, message);
    };
    
    // Replace the original logError method
    this.scraperManager.logError = async (eventId, errorType, error, metadata = {}) => {
      // Call original method for database logging
      await this.originalLogError(eventId, errorType, error, metadata);
      
      // Add enhanced file logging based on error type
      await this.logEnhancedError(eventId, errorType, error, metadata);
    };
    
    // Add new logging methods to scraperManager
    this.scraperManager.logProxyFailure = this.logProxyFailure.bind(this);
    this.scraperManager.logSessionIssue = this.logSessionIssue.bind(this);
    this.scraperManager.logCookieIssue = this.logCookieIssue.bind(this);
    this.scraperManager.logPerformanceMetric = this.logPerformanceMetric.bind(this);
    this.scraperManager.generateLogSummary = this.generateLogSummary.bind(this);
    
    console.log('✅ Advanced file-based logging integrated with ScraperManager');
  }
  
  mapTypeToLevel(type) {
    const mapping = {
      'success': 'SUCCESS',
      'error': 'ERROR',
      'warning': 'WARN',
      'info': 'INFO',
      'debug': 'INFO'
    };
    return mapping[type] || 'INFO';
  }
  
  async logEnhancedError(eventId, errorType, error, metadata = {}) {
    // Determine the type of error and log accordingly
    const errorMessage = error.message || error.toString();
    
    // Proxy-related errors
    if (this.isProxyError(errorMessage, errorType)) {
      const proxyString = this.extractProxyFromMetadata(metadata) || 'unknown';
      await this.advancedLogger.logProxyIssue(proxyString, eventId, error, metadata);
    }
    
    // Session-related errors
    else if (this.isSessionError(errorMessage, errorType)) {
      const sessionInfo = this.extractSessionFromMetadata(metadata);
      await this.advancedLogger.logSessionIssue(sessionInfo, eventId, error, metadata);
    }
    
    // Cookie-related errors
    else if (this.isCookieError(errorMessage, errorType)) {
      const cookieType = this.extractCookieTypeFromError(errorMessage, errorType);
      const cookieData = this.extractCookieDataFromMetadata(metadata);
      await this.advancedLogger.logCookieIssue(cookieType, eventId, error, cookieData, metadata);
    }
    
    // General error with failure pattern tracking
    else {
      const retryCount = metadata.retryCount || 0;
      await this.advancedLogger.logEventFailurePattern(eventId, error, retryCount, metadata);
    }
  }
  
  isProxyError(errorMessage, errorType) {
    const proxyKeywords = [
      'proxy', 'ECONNREFUSED', 'ENOTFOUND', 'ETIMEDOUT', 
      'tunnel', 'connect', 'connection refused', 'network error',
      'PROXY_CONNECTION_FAILED', 'PROXY_AUTH_FAILED'
    ];
    
    return proxyKeywords.some(keyword => 
      errorMessage.toLowerCase().includes(keyword.toLowerCase()) ||
      errorType.toLowerCase().includes(keyword.toLowerCase())
    );
  }
  
  isSessionError(errorMessage, errorType) {
    const sessionKeywords = [
      'session', 'authentication', 'unauthorized', '401', '403',
      'login', 'auth', 'SESSION_EXPIRED', 'INVALID_SESSION',
      'csrf', 'token', 'captcha'
    ];
    
    return sessionKeywords.some(keyword => 
      errorMessage.toLowerCase().includes(keyword.toLowerCase()) ||
      errorType.toLowerCase().includes(keyword.toLowerCase())
    );
  }
  
  isCookieError(errorMessage, errorType) {
    const cookieKeywords = [
      'cookie', 'COOKIE_EXPIRED', 'INVALID_COOKIES', 'COOKIE_REFRESH',
      'set-cookie', 'cookie jar', 'cookie validation'
    ];
    
    return cookieKeywords.some(keyword => 
      errorMessage.toLowerCase().includes(keyword.toLowerCase()) ||
      errorType.toLowerCase().includes(keyword.toLowerCase())
    );
  }
  
  extractProxyFromMetadata(metadata) {
    return metadata.proxy || metadata.proxyString || metadata.proxyUrl || null;
  }
  
  extractSessionFromMetadata(metadata) {
    return {
      sessionId: metadata.sessionId || metadata.session || 'unknown',
      age: metadata.sessionAge || metadata.age || 'unknown'
    };
  }
  
  extractCookieTypeFromError(errorMessage, errorType) {
    if (errorMessage.includes('TMUO') || errorMessage.includes('TMPS')) {
      return 'AUTH_COOKIES';
    }
    if (errorMessage.includes('SESSION')) {
      return 'SESSION_COOKIES';
    }
    if (errorType.includes('COOKIE_REFRESH')) {
      return 'REFRESH_COOKIES';
    }
    return 'GENERAL_COOKIES';
  }
  
  extractCookieDataFromMetadata(metadata) {
    return {
      count: metadata.cookieCount || 0,
      age: metadata.cookieAge || 'unknown'
    };
  }
  
  async logProxyFailure(proxyString, eventId, error, additionalMetadata = {}) {
    await this.advancedLogger.logProxyIssue(proxyString, eventId, error, additionalMetadata);
  }
  
  async logSessionIssue(sessionInfo, eventId, error, additionalMetadata = {}) {
    await this.advancedLogger.logSessionIssue(sessionInfo, eventId, error, additionalMetadata);
  }
  
  async logCookieIssue(cookieType, eventId, error, cookieData = {}, additionalMetadata = {}) {
    await this.advancedLogger.logCookieIssue(cookieType, eventId, error, cookieData, additionalMetadata);
  }
  
  async logPerformanceMetric(eventId, responseTime, success = true, additionalMetadata = {}) {
    await this.advancedLogger.logPerformance(eventId, responseTime, success, additionalMetadata);
  }
  
  async generateLogSummary() {
    return await this.advancedLogger.generateSummaryReport();
  }
  
  async shutdown() {
    await this.advancedLogger.shutdown();
  }
}

export default LoggerIntegration;