import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);
const __dirname = path.dirname(fileURLToPath(import.meta.url));

/**
 * Manages the health status of proxies with persistence
 */
class ProxyHealthManager {
  constructor(logger) {
    this.logger = logger;
    this.healthStore = {
      unhealthyProxies: new Map(),  // Maps proxy string to health data
      bannedProxies: new Set(),      // Set of permanently banned proxies
      lastReset: Date.now()
    };
    
    this.healthFilePath = path.join(__dirname, 'proxy-health-status.json');
    
    // Health status configuration
    this.config = {
      // How many consecutive failures before marking a proxy as potentially unhealthy
      failureThreshold: 8,

      // How many potential health issues before marking as unhealthy
      unhealthyThreshold: 15,

      // How many 403 errors before banning a proxy
      ban403Threshold: 8,

      // Initial cooldown time (15 minutes)
      initialCooldown: 15 * 60 * 1000,

      // Maximum cooldown time (12 hours)
      maxCooldown: 12 * 60 * 60 * 1000,

      // Minimum time to test a proxy before final health decision
      minTestPeriod: 3 * 60 * 1000,

      // Reset health metrics interval
      resetInterval: 4 * 60 * 60 * 1000,

      // Secondary test website for proxy validation
      secondaryTestUrl: "https://www.bing.com/",

      // Number of successful secondary tests required to rehabilitate
      secondaryTestSuccessThreshold: 2,

      // Ping configuration
      pingCount: 10,                    // Number of pings to send
      pingTimeout: 2000,                // Timeout for each ping in milliseconds
      pingFailureThreshold: 10,         // Number of failed pings before banning
      pingInterval: 60 * 1000,          // Interval between ping checks in milliseconds
    };
  }
  
  /**
   * Initialize the health manager and load persisted data
   */
  async initialize() {
    await this.loadHealthData();
    
    // Schedule periodic health reset
    setInterval(() => this.resetHealthMetrics(), this.config.resetInterval);
    
    // Start ping monitoring
    this.startPingMonitoring();
    
    this.log(`ProxyHealthManager initialized with ${this.healthStore.unhealthyProxies.size} unhealthy proxies and ${this.healthStore.bannedProxies.size} banned proxies`);
    return this;
  }
  
  /**
   * Load health data from file
   */
  async loadHealthData() {
    try {
      const fileExists = await fs.access(this.healthFilePath)
        .then(() => true)
        .catch(() => false);
        
      if (fileExists) {
        const data = await fs.readFile(this.healthFilePath, 'utf8');
        const parsed = JSON.parse(data);
        
        // Convert the unhealthy proxies object back to a Map
        this.healthStore.unhealthyProxies = new Map(
          Object.entries(parsed.unhealthyProxies || {}).map(([key, value]) => [
            key, 
            { 
              ...value,
              lastCheck: new Date(value.lastCheck || Date.now()),
              lastFailure: new Date(value.lastFailure || Date.now()),
              nextRetryTime: new Date(value.nextRetryTime || Date.now())
            }
          ])
        );
        
        // Convert the banned proxies array back to a Set
        this.healthStore.bannedProxies = new Set(parsed.bannedProxies || []);
        this.healthStore.lastReset = parsed.lastReset || Date.now();
        
        this.log(`Loaded health data from ${this.healthFilePath}: ${this.healthStore.unhealthyProxies.size} unhealthy, ${this.healthStore.bannedProxies.size} banned`);
      } else {
        this.log(`No health data file found at ${this.healthFilePath}, starting fresh`);
      }
    } catch (error) {
      this.log(`Error loading health data: ${error.message}`, 'error');
    }
  }
  
  /**
   * Save health data to file
   */
  async saveHealthData() {
    try {
      // Convert the Map and Set to objects for JSON serialization
      const data = {
        unhealthyProxies: Object.fromEntries(this.healthStore.unhealthyProxies),
        bannedProxies: Array.from(this.healthStore.bannedProxies),
        lastReset: this.healthStore.lastReset
      };
      
      await fs.writeFile(this.healthFilePath, JSON.stringify(data, null, 2), 'utf8');
      this.log(`Saved health data to ${this.healthFilePath}`);
    } catch (error) {
      this.log(`Error saving health data: ${error.message}`, 'error');
    }
  }
  
  /**
   * Simple logging function that uses the provided logger if available
   */
  log(message, level = "info") {
    if (this.logger) {
      if (typeof this.logger.logWithTime === 'function') {
        this.logger.logWithTime(message, level);
      } else if (typeof this.logger.log === 'function') {
        this.logger.log(message, level);
      } else {
        console.log(`[${level.toUpperCase()}] ${message}`);
      }
    } else {
      console.log(`[${level.toUpperCase()}] ${message}`);
    }
  }
  
  /**
   * Check if a proxy is healthy
   * @param {string} proxyString - The proxy string to check
   * @returns {boolean} Whether the proxy is healthy
   */
  isProxyHealthy(proxyString) {
    // If the proxy is banned, it's unhealthy
    if (this.healthStore.bannedProxies.has(proxyString)) {
      return false;
    }
    
    // If the proxy is not in the unhealthy set, it's healthy
    if (!this.healthStore.unhealthyProxies.has(proxyString)) {
      return true;
    }
    
    // Check if the proxy is in cooldown
    const healthData = this.healthStore.unhealthyProxies.get(proxyString);
    if (healthData && healthData.nextRetryTime) {
      if (Date.now() < healthData.nextRetryTime.getTime()) {
        return false;
      }
      
      // If we're past the retry time, give it another chance
      this.log(`Proxy ${proxyString} cooldown period ended, marking for retry`);
      
      // Don't immediately remove from unhealthy list, but let it be retried
      // We'll track success/failure in the success/failure recording methods
      return true;
    }
    
    return false;
  }
  
  /**
   * Record a successful proxy usage
   * @param {string} proxyString - The proxy string
   */
  recordProxySuccess(proxyString) {
    // If the proxy is banned, don't update
    if (this.healthStore.bannedProxies.has(proxyString)) {
      return;
    }
    
    // If the proxy was previously unhealthy, check if it's been successful enough to rehabilitate
    if (this.healthStore.unhealthyProxies.has(proxyString)) {
      const healthData = this.healthStore.unhealthyProxies.get(proxyString);
      
      // Increment success count
      healthData.successCount = (healthData.successCount || 0) + 1;
      healthData.lastCheck = new Date();
      
      // If we've had several successes in a row, rehabilitate the proxy
      if (healthData.successCount >= 2) {
        this.log(`Proxy ${proxyString} has been successfully rehabilitated after ${healthData.successCount} successful uses`);
        this.healthStore.unhealthyProxies.delete(proxyString);
        this.saveHealthData();
      } else {
        // Update the health data
        this.healthStore.unhealthyProxies.set(proxyString, healthData);
      }
    }
    
    // If proxy was healthy and stays healthy, no need to update persistent storage
  }
  
  /**
   * Record a failed proxy usage and determine if it should be marked unhealthy
   * @param {string} proxyString - The proxy string
   * @param {Object} error - The error object (optional)
   */
  recordProxyFailure(proxyString, error = null) {
    // If the proxy is already banned, don't update
    if (this.healthStore.bannedProxies.has(proxyString)) {
      return;
    }
    
    const now = new Date();
    const is403 = error && (
      (error.response && error.response.status === 403) ||
      (error.message && error.message.includes("403"))
    );
    
    // Get or create health data
    let healthData = this.healthStore.unhealthyProxies.get(proxyString) || {
      failureCount: 0,
      successCount: 0,
      count403: 0,
      firstFailure: now,
      lastFailure: now,
      lastCheck: now,
      cooldownMultiplier: 1,
      failureSequences: 0,
      secondaryTestSuccess: 0
    };
    
    // Update failure counts
    healthData.failureCount = (healthData.failureCount || 0) + 1;
    healthData.lastFailure = now;
    healthData.lastCheck = now;
    
    // Special handling for 403 errors
    if (is403) {
      healthData.count403 = (healthData.count403 || 0) + 1;
      this.log(`Proxy ${proxyString} received 403 error (total: ${healthData.count403})`, "warning");
      
      // Check if we should ban this proxy due to excessive 403 errors
      if (healthData.count403 >= this.config.ban403Threshold) {
        // Before banning, try secondary test
        if (healthData.secondaryTestSuccess < this.config.secondaryTestSuccessThreshold) {
          this.log(`Proxy ${proxyString} will be tested with secondary website before banning`, "warning");
          return;
        }
        this.banProxy(proxyString, "Excessive 403 errors");
        return;
      }
    }
    
    // Check if we should mark this proxy as unhealthy
    const timeSinceFirstFailure = now.getTime() - healthData.firstFailure.getTime();
    
    // We need both enough failures AND enough testing time before making decision
    if (healthData.failureCount >= this.config.unhealthyThreshold && 
        timeSinceFirstFailure >= this.config.minTestPeriod) {
      
      // Before marking as unhealthy, try secondary test
      if (healthData.secondaryTestSuccess < this.config.secondaryTestSuccessThreshold) {
        this.log(`Proxy ${proxyString} will be tested with secondary website before marking unhealthy`, "warning");
        return;
      }
      
      // Calculate cooldown period based on failure sequence
      healthData.failureSequences = (healthData.failureSequences || 0) + 1;
      
      // Exponential backoff for repeated failures, capped at maxCooldown
      const cooldownTime = Math.min(
        this.config.initialCooldown * Math.pow(1.5, healthData.failureSequences - 1),
        this.config.maxCooldown
      );
      
      // Add randomness to prevent all proxies retrying at the same time
      const jitter = Math.floor(Math.random() * 30000);
      healthData.nextRetryTime = new Date(now.getTime() + cooldownTime + jitter);
      
      if (healthData.failureSequences >= 4) {
        this.log(`Proxy ${proxyString} has failed ${healthData.failureSequences} consecutive cooldown periods, permanently banning`, "error");
        this.banProxy(proxyString, "Multiple consecutive failure sequences");
        return;
      } else {
        this.log(`Proxy ${proxyString} marked unhealthy (attempt ${healthData.failureSequences}). Will retry after ${healthData.nextRetryTime.toLocaleString()}`, "warning");
      }
      
      // Reset failure count for next attempt
      healthData.failureCount = 0;
      healthData.successCount = 0;
    }
    
    // Update the health data
    this.healthStore.unhealthyProxies.set(proxyString, healthData);
    this.saveHealthData();
  }
  
  /**
   * Permanently ban a proxy
   * @param {string} proxyString - The proxy string to ban
   * @param {string} reason - The reason for banning
   */
  banProxy(proxyString, reason = "Unknown") {
    if (!this.healthStore.bannedProxies.has(proxyString)) {
      this.healthStore.bannedProxies.add(proxyString);
      this.healthStore.unhealthyProxies.delete(proxyString); // No need to track it twice
      this.log(`Proxy ${proxyString} PERMANENTLY BANNED. Reason: ${reason}`, "error");
      this.saveHealthData();
    }
  }
  
  /**
   * Reset health metrics periodically
   */
  resetHealthMetrics() {
    const now = Date.now();
    
    // Don't reset too frequently
    if (now - this.healthStore.lastReset < this.config.resetInterval) {
      return;
    }
    
    this.healthStore.lastReset = now;
    this.log("Resetting proxy health metrics");
    
    // Give some banned proxies another chance after a long time
    // Note: this only applies to proxies banned for reasons other than 403s
    const bannedCount = this.healthStore.bannedProxies.size;
    let forgivenCount = 0;
    
    for (const proxyString of this.healthStore.bannedProxies) {
      const healthData = this.healthStore.unhealthyProxies.get(proxyString);
      
      // Only unban proxies that don't have 403 issues
      if (healthData && (!healthData.count403 || healthData.count403 < this.config.ban403Threshold)) {
        // Random chance to forgive a banned proxy after reset interval
        if (Math.random() < 0.1) { // 10% chance
          this.healthStore.bannedProxies.delete(proxyString);
          
          // Reset its health record but keep track that it was previously banned
          this.healthStore.unhealthyProxies.set(proxyString, {
            failureCount: 0,
            successCount: 0,
            count403: healthData.count403 || 0,
            firstFailure: new Date(),
            lastFailure: new Date(),
            lastCheck: new Date(),
            cooldownMultiplier: 1,
            failureSequences: healthData.failureSequences || 0,
            previouslyBanned: true
          });
          
          forgivenCount++;
        }
      }
    }
    
    if (forgivenCount > 0) {
      this.log(`Reset forgave ${forgivenCount} out of ${bannedCount} banned proxies`);
    }
    
    // Also reduce the cooldown times for proxies in recovery
    for (const [proxyString, healthData] of this.healthStore.unhealthyProxies.entries()) {
      // If a proxy has been in recovery for a long time, speed up its retry
      if (healthData.nextRetryTime && healthData.nextRetryTime.getTime() > now) {
        // Reduce the remaining cooldown time by half
        const remainingTime = healthData.nextRetryTime.getTime() - now;
        healthData.nextRetryTime = new Date(now + remainingTime / 2);
        this.log(`Reduced cooldown time for proxy ${proxyString}, new retry time: ${healthData.nextRetryTime.toLocaleString()}`);
      }
    }
    
    this.saveHealthData();
  }
  
  /**
   * Get proxy health statistics
   * @returns {Object} Statistics about proxy health
   */
  getHealthStats() {
    return {
      totalUnhealthy: this.healthStore.unhealthyProxies.size,
      totalBanned: this.healthStore.bannedProxies.size,
      lastReset: new Date(this.healthStore.lastReset).toLocaleString(),
      unhealthyDetails: Array.from(this.healthStore.unhealthyProxies.entries()).map(([proxy, data]) => ({
        proxy,
        failureCount: data.failureCount || 0,
        successCount: data.successCount || 0,
        count403: data.count403 || 0,
        firstFailure: data.firstFailure ? data.firstFailure.toLocaleString() : 'N/A',
        lastFailure: data.lastFailure ? data.lastFailure.toLocaleString() : 'N/A',
        nextRetryTime: data.nextRetryTime ? data.nextRetryTime.toLocaleString() : 'N/A',
        failureSequences: data.failureSequences || 0
      })),
      bannedProxies: Array.from(this.healthStore.bannedProxies)
    };
  }

  /**
   * Check proxy health using ping
   * @param {string} proxyString - The proxy string to check
   * @returns {Promise<boolean>} Whether the proxy is healthy based on ping
   */
  async checkProxyWithPing(proxyString) {
    try {
      // Extract host from proxy string (assuming format: protocol://host:port)
      const proxyUrl = new URL(proxyString);
      const host = proxyUrl.hostname;
      
      // Execute ping command
      const { stdout, stderr } = await execAsync(
        `ping -n ${this.config.pingCount} -w ${this.config.pingTimeout} ${host}`
      );
      
      // Count successful pings
      const successfulPings = (stdout.match(/Reply from/g) || []).length;
      const failedPings = this.config.pingCount - successfulPings;
      
      this.log(`Ping results for ${proxyString}: ${successfulPings} successful, ${failedPings} failed`);
      
      if (failedPings >= this.config.pingFailureThreshold) {
        this.log(`Proxy ${proxyString} failed ${failedPings} pings, marking as unhealthy`, 'warning');
        this.recordProxyFailure(proxyString, { message: `Failed ${failedPings} pings` });
        return false;
      }
      
      return true;
    } catch (error) {
      this.log(`Error pinging proxy ${proxyString}: ${error.message}`, 'error');
      this.recordProxyFailure(proxyString, error);
      return false;
    }
  }

  /**
   * Start periodic ping checks for all proxies
   */
  startPingMonitoring() {
    setInterval(async () => {
      const proxies = Array.from(this.healthStore.unhealthyProxies.keys());
      for (const proxy of proxies) {
        if (!this.healthStore.bannedProxies.has(proxy)) {
          await this.checkProxyWithPing(proxy);
        }
      }
    }, this.config.pingInterval);
  }
}

export default ProxyHealthManager; 