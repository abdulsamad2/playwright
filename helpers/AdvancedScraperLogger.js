import fs from 'fs/promises';
import path from 'path';
import moment from 'moment';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

/**
 * Advanced Scraper Logger with file-based logging for debugging scraper issues
 * Tracks proxy failures, session issues, cookie problems, and general errors
 */
class AdvancedScraperLogger {
  constructor(scraperManager) {
    this.scraperManager = scraperManager;
    this.startTime = moment();
    this.logDir = path.join(__dirname, '..', 'logs');
    this.sessionId = this.generateSessionId();
    
    // Log file paths
    this.logFiles = {
      main: path.join(this.logDir, `scraper-${this.getDateString()}.log`),
      proxy: path.join(this.logDir, `proxy-issues-${this.getDateString()}.log`),
      session: path.join(this.logDir, `session-issues-${this.getDateString()}.log`),
      cookie: path.join(this.logDir, `cookie-issues-${this.getDateString()}.log`),
      performance: path.join(this.logDir, `performance-${this.getDateString()}.log`),
      errors: path.join(this.logDir, `errors-${this.getDateString()}.log`)
    };
    
    // Tracking maps for analysis
    this.proxyFailures = new Map(); // proxy -> failure count
    this.sessionIssues = new Map(); // session -> issue count
    this.cookieProblems = new Map(); // cookie type -> problem count
    this.eventFailurePatterns = new Map(); // eventId -> failure pattern
    
    // Performance tracking
    this.performanceMetrics = {
      startTime: Date.now(),
      lastHeartbeat: Date.now(),
      totalRequests: 0,
      successfulRequests: 0,
      failedRequests: 0,
      avgResponseTime: 0,
      responseTimes: []
    };
    
    this.initializeLogger();
  }
  
  generateSessionId() {
    return `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }
  
  getDateString() {
    return moment().format('YYYY-MM-DD');
  }
  
  async initializeLogger() {
    try {
      // Create logs directory if it doesn't exist
      await fs.mkdir(this.logDir, { recursive: true });
      
      // Initialize log files with headers
      await this.writeToFile(this.logFiles.main, `\n=== SCRAPER SESSION STARTED: ${this.sessionId} ===\n`);
      await this.writeToFile(this.logFiles.main, `Start Time: ${moment().format('YYYY-MM-DD HH:mm:ss')}\n`);
      await this.writeToFile(this.logFiles.main, `Session ID: ${this.sessionId}\n`);
      await this.writeToFile(this.logFiles.main, `=== CONFIGURATION ===\n`);
      
      // Log initial configuration
      const config = {
        concurrentLimit: this.scraperManager.concurrencySemaphore || 'N/A',
        maxRetries: 25,
        scrapeTimeout: 45000,
        proxyCount: this.scraperManager.proxyManager?.proxies?.length || 'N/A'
      };
      
      await this.writeToFile(this.logFiles.main, `Config: ${JSON.stringify(config, null, 2)}\n\n`);
      
      // Start heartbeat logging
      this.startHeartbeat();
      
      console.log(`✅ Advanced Scraper Logger initialized. Logs saved to: ${this.logDir}`);
    } catch (error) {
      console.error('Failed to initialize logger:', error);
    }
  }
  
  async writeToFile(filePath, content) {
    try {
      await fs.appendFile(filePath, content);
    } catch (error) {
      console.error(`Failed to write to log file ${filePath}:`, error);
    }
  }
  
  formatLogEntry(level, category, message, metadata = {}) {
    const timestamp = moment().format('YYYY-MM-DD HH:mm:ss.SSS');
    const runtime = moment.duration(moment().diff(this.startTime)).humanize();
    
    const logEntry = {
      timestamp,
      sessionId: this.sessionId,
      level,
      category,
      runtime,
      message,
      metadata: {
        ...metadata,
        activeJobs: this.scraperManager.activeJobs?.size || 0,
        failedEvents: this.scraperManager.failedEvents?.size || 0
      }
    };
    
    return `${timestamp} [${level}] [${category}] ${message}\n` +
           `  Runtime: ${runtime} | Active: ${logEntry.metadata.activeJobs} | Failed: ${logEntry.metadata.failedEvents}\n` +
           (Object.keys(metadata).length > 0 ? `  Metadata: ${JSON.stringify(metadata, null, 2)}\n` : '') +
           '\n';
  }
  
  async logGeneral(level, message, metadata = {}) {
    const logEntry = this.formatLogEntry(level, 'GENERAL', message, metadata);
    await this.writeToFile(this.logFiles.main, logEntry);
    
    // Also log errors to dedicated error file
    if (level === 'ERROR') {
      await this.writeToFile(this.logFiles.errors, logEntry);
    }
    
    // Console output with emoji
    const emoji = { INFO: 'ℹ️', WARN: '⚠️', ERROR: '❌', SUCCESS: '✅' }[level] || '📝';
    console.log(`${emoji} ${message}`);
  }
  
  async logProxyIssue(proxyString, eventId, error, metadata = {}) {
    // Track proxy failure
    const currentFailures = this.proxyFailures.get(proxyString) || 0;
    this.proxyFailures.set(proxyString, currentFailures + 1);
    
    const proxyMetadata = {
      ...metadata,
      proxyString,
      eventId,
      errorType: error.name || 'Unknown',
      errorMessage: error.message,
      totalFailuresForProxy: currentFailures + 1,
      timestamp: Date.now()
    };
    
    const logEntry = this.formatLogEntry('ERROR', 'PROXY', 
      `Proxy failure: ${proxyString} for event ${eventId} - ${error.message}`, 
      proxyMetadata
    );
    
    await this.writeToFile(this.logFiles.proxy, logEntry);
    await this.writeToFile(this.logFiles.main, logEntry);
    
    // Check if proxy has too many failures
    if (currentFailures + 1 >= 5) {
      await this.logGeneral('WARN', `Proxy ${proxyString} has ${currentFailures + 1} failures - consider removing`);
    }
  }
  
  async logSessionIssue(sessionInfo, eventId, error, metadata = {}) {
    const sessionKey = sessionInfo?.sessionId || 'unknown';
    const currentIssues = this.sessionIssues.get(sessionKey) || 0;
    this.sessionIssues.set(sessionKey, currentIssues + 1);
    
    const sessionMetadata = {
      ...metadata,
      sessionId: sessionKey,
      eventId,
      errorType: error.name || 'Unknown',
      errorMessage: error.message,
      sessionAge: sessionInfo?.age || 'unknown',
      totalIssuesForSession: currentIssues + 1,
      timestamp: Date.now()
    };
    
    const logEntry = this.formatLogEntry('ERROR', 'SESSION', 
      `Session issue: ${sessionKey} for event ${eventId} - ${error.message}`, 
      sessionMetadata
    );
    
    await this.writeToFile(this.logFiles.session, logEntry);
    await this.writeToFile(this.logFiles.main, logEntry);
  }
  
  async logCookieIssue(cookieType, eventId, error, cookieData = {}, metadata = {}) {
    const currentProblems = this.cookieProblems.get(cookieType) || 0;
    this.cookieProblems.set(cookieType, currentProblems + 1);
    
    const cookieMetadata = {
      ...metadata,
      cookieType,
      eventId,
      errorType: error.name || 'Unknown',
      errorMessage: error.message,
      cookieCount: cookieData.count || 0,
      cookieAge: cookieData.age || 'unknown',
      totalProblemsForCookieType: currentProblems + 1,
      timestamp: Date.now()
    };
    
    const logEntry = this.formatLogEntry('ERROR', 'COOKIE', 
      `Cookie issue: ${cookieType} for event ${eventId} - ${error.message}`, 
      cookieMetadata
    );
    
    await this.writeToFile(this.logFiles.cookie, logEntry);
    await this.writeToFile(this.logFiles.main, logEntry);
  }
  
  async logPerformance(eventId, responseTime, success = true, metadata = {}) {
    this.performanceMetrics.totalRequests++;
    if (success) {
      this.performanceMetrics.successfulRequests++;
    } else {
      this.performanceMetrics.failedRequests++;
    }
    
    // Track response times for average calculation
    this.performanceMetrics.responseTimes.push(responseTime);
    if (this.performanceMetrics.responseTimes.length > 100) {
      this.performanceMetrics.responseTimes.shift(); // Keep only last 100
    }
    
    this.performanceMetrics.avgResponseTime = 
      this.performanceMetrics.responseTimes.reduce((a, b) => a + b, 0) / 
      this.performanceMetrics.responseTimes.length;
    
    const perfMetadata = {
      ...metadata,
      eventId,
      responseTime,
      success,
      avgResponseTime: Math.round(this.performanceMetrics.avgResponseTime),
      successRate: ((this.performanceMetrics.successfulRequests / this.performanceMetrics.totalRequests) * 100).toFixed(2),
      timestamp: Date.now()
    };
    
    // Only log slow responses or failures
    if (!success || responseTime > 30000) {
      const logEntry = this.formatLogEntry(
        success ? 'WARN' : 'ERROR', 
        'PERFORMANCE', 
        `${success ? 'Slow response' : 'Failed request'}: ${eventId} took ${responseTime}ms`, 
        perfMetadata
      );
      
      await this.writeToFile(this.logFiles.performance, logEntry);
    }
  }
  
  async logEventFailurePattern(eventId, error, retryCount, metadata = {}) {
    const pattern = this.eventFailurePatterns.get(eventId) || {
      failures: [],
      firstFailure: Date.now(),
      totalFailures: 0
    };
    
    pattern.failures.push({
      timestamp: Date.now(),
      error: error.message,
      retryCount,
      metadata
    });
    pattern.totalFailures++;
    
    // Keep only last 10 failures
    if (pattern.failures.length > 10) {
      pattern.failures.shift();
    }
    
    this.eventFailurePatterns.set(eventId, pattern);
    
    // Analyze pattern
    const timeSinceFirst = Date.now() - pattern.firstFailure;
    const failureRate = pattern.totalFailures / (timeSinceFirst / 60000); // failures per minute
    
    if (pattern.totalFailures >= 5) {
      await this.logGeneral('WARN', 
        `Event ${eventId} has ${pattern.totalFailures} failures over ${Math.round(timeSinceFirst/60000)} minutes (rate: ${failureRate.toFixed(2)}/min)`,
        { eventId, pattern: pattern.failures.slice(-3) }
      );
    }
  }
  
  startHeartbeat() {
    setInterval(async () => {
      this.performanceMetrics.lastHeartbeat = Date.now();
      
      const heartbeatData = {
        timestamp: moment().format('YYYY-MM-DD HH:mm:ss'),
        runtime: moment.duration(moment().diff(this.startTime)).humanize(),
        performance: this.performanceMetrics,
        proxyFailureSummary: Object.fromEntries(this.proxyFailures),
        sessionIssueSummary: Object.fromEntries(this.sessionIssues),
        cookieProblemSummary: Object.fromEntries(this.cookieProblems),
        memoryUsage: process.memoryUsage()
      };
      
      await this.writeToFile(this.logFiles.main, 
        `\n=== HEARTBEAT ===\n${JSON.stringify(heartbeatData, null, 2)}\n\n`
      );
    }, 300000); // Every 5 minutes
  }
  
  async generateSummaryReport() {
    const runtime = moment.duration(moment().diff(this.startTime));
    const summaryPath = path.join(this.logDir, `summary-${this.sessionId}.json`);
    
    const summary = {
      sessionId: this.sessionId,
      startTime: this.startTime.format('YYYY-MM-DD HH:mm:ss'),
      endTime: moment().format('YYYY-MM-DD HH:mm:ss'),
      totalRuntime: runtime.humanize(),
      performance: this.performanceMetrics,
      topProxyFailures: Array.from(this.proxyFailures.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10),
      topSessionIssues: Array.from(this.sessionIssues.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10),
      cookieProblems: Object.fromEntries(this.cookieProblems),
      problematicEvents: Array.from(this.eventFailurePatterns.entries())
        .filter(([_, pattern]) => pattern.totalFailures >= 3)
        .sort((a, b) => b[1].totalFailures - a[1].totalFailures)
        .slice(0, 20)
    };
    
    await fs.writeFile(summaryPath, JSON.stringify(summary, null, 2));
    console.log(`📊 Summary report saved to: ${summaryPath}`);
    
    return summary;
  }
  
  async shutdown() {
    await this.logGeneral('INFO', 'Scraper logger shutting down...');
    await this.generateSummaryReport();
    await this.writeToFile(this.logFiles.main, 
      `\n=== SCRAPER SESSION ENDED: ${this.sessionId} ===\n`
    );
  }
}

export default AdvancedScraperLogger;