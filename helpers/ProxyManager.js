import proxyArray from "./proxy.js";
import { createRequire } from "module";

// Set up require for ES modules
const require = createRequire(import.meta.url);
const { HttpsProxyAgent } = require("https-proxy-agent");

/**
 * Manages proxy allocation and enforces usage limits for batches of events
 */
class ProxyManager {
  constructor(logger) {
    this.logger = logger;
    this.proxyUsage = new Map(); // Maps proxy IP to set of eventIds using it
    this.eventToProxy = new Map(); // Maps eventId to assigned proxy
    this.MAX_EVENTS_PER_PROXY = 1; // Ensuring each event gets a dedicated proxy
    this.BATCH_SIZE = 3; // Maximum batch size
    this.proxies = [...proxyArray.proxies];
    this.lastAssignedProxyIndex = -1;
    this.proxyHealth = new Map(); // Track proxy health status
    this.failedProxies = new Set(); // Track failed proxies
    this.proxyLastUsed = new Map(); // Track when proxies were last used
    this.proxyRetryTime = new Map(); // Track when to retry failed proxies
    this.proxyLastReset = Date.now(); // Track when we last reset proxy health
    this.proxy403Count = new Map(); // Specifically track 403 errors per proxy
    this.proxySuccessRate = new Map(); // Track success rate per proxy
    this.bannedProxies = new Set(); // Completely banned proxies that consistently get 403s
    
    // Initialize usage counts and health status
    this.proxies.forEach(proxy => {
      this.proxyUsage.set(proxy.proxy, new Set());
      this.proxyHealth.set(proxy.proxy, { 
        successCount: 0,
        failureCount: 0,
        last403Time: 0,
        lastCheck: 0,
        isHealthy: true
      });
      this.proxyRetryTime.set(proxy.proxy, 0);
      this.proxy403Count.set(proxy.proxy, 0);
      this.proxySuccessRate.set(proxy.proxy, 1.0); // Start with perfect score
    });
    
    // Set up periodic health reset to prevent permanently marking proxies as unhealthy
    setInterval(() => this.resetProxyHealthMetrics(), 5 * 60 * 1000); // Reset every 5 minutes
    
    this.log("ProxyManager initialized with " + this.proxies.length + " proxies");
  }
  
  /**
   * Reset proxy health metrics periodically to give all proxies another chance
   */
  resetProxyHealthMetrics() {
    const now = Date.now();
    if (now - this.proxyLastReset < 5 * 60 * 1000) return; // Don't reset too frequently
    
    this.proxyLastReset = now;
    this.log("Resetting proxy health metrics");
    
    // Reduce (but don't completely reset) failure counts to give proxies a new chance
    for (const [proxyString, health] of this.proxyHealth.entries()) {
      // If it's been more than 15 minutes since last 403, reduce the 403 count
      if (now - health.last403Time > 15 * 60 * 1000) {
        const current403Count = this.proxy403Count.get(proxyString) || 0;
        if (current403Count > 0) {
          this.proxy403Count.set(proxyString, Math.max(0, current403Count - 1));
          this.log(`Reduced 403 count for proxy ${proxyString} to ${this.proxy403Count.get(proxyString)}`);
        }
      }
      
      // Gradually restore health for all proxies
      if (health.failureCount > 0) {
        health.failureCount = Math.max(0, health.failureCount - 1);
      }
      
      // Mark as healthy again if it was unhealthy
      if (!health.isHealthy) {
        // Only restore health if not completely banned
        if (!this.bannedProxies.has(proxyString)) {
          health.isHealthy = true;
          this.log(`Restored health for proxy ${proxyString}`);
        }
      }
    }
    
    // Clear the failed proxies set (except banned ones)
    for (const proxy of this.failedProxies) {
      if (!this.bannedProxies.has(proxy)) {
        this.failedProxies.delete(proxy);
      }
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
   * Check if a proxy is healthy and available
   * @param {Object} proxy - The proxy to check
   * @returns {boolean} Whether the proxy is healthy
   */
  isProxyHealthy(proxy) {
    const health = this.proxyHealth.get(proxy.proxy);
    if (!health) return false;
    
    // If in retry timeout, consider unhealthy
    const retryTime = this.proxyRetryTime.get(proxy.proxy) || 0;
    if (retryTime > Date.now()) {
      return false;
    }
    
    // If proxy is banned, it's unhealthy
    if (this.bannedProxies.has(proxy.proxy)) {
      return false;
    }
    
    // If too many 403 errors, consider unhealthy
    const count403 = this.proxy403Count.get(proxy.proxy) || 0;
    if (count403 >= 3) { // 3 strikes and you're out
      return false;
    }
    
    // If proxy has too many failures, mark as unhealthy
    if (health.failureCount > 3 && health.successCount < health.failureCount) {
      return false;
    }
    
    // If proxy was used recently, add cooldown (short cooldown to prevent overuse)
    const lastUsed = this.proxyLastUsed.get(proxy.proxy) || 0;
    const cooldownTime = 3000; // 3 seconds cooldown
    if (Date.now() - lastUsed < cooldownTime) {
      return false;
    }
    
    // Check if proxy has capacity - this is critical for ensuring separate proxies
    const currentUsage = this.proxyUsage.get(proxy.proxy)?.size || 0;
    if (currentUsage >= this.MAX_EVENTS_PER_PROXY) {
      return false;
    }
    
    return health.isHealthy;
  }

  /**
   * Count how many healthy proxies are available
   * @returns {number} The number of healthy proxies
   */
  getAvailableProxyCount() {
    return this.proxies.filter(proxy => this.isProxyHealthy(proxy)).length;
  }

  /**
   * Record a successful proxy usage
   * @param {string} proxyString - The proxy string
   */
  recordProxySuccess(proxyString) {
    const health = this.proxyHealth.get(proxyString);
    if (health) {
      health.successCount++;
      health.lastCheck = Date.now();
      
      // Improve success rate
      const currentRate = this.proxySuccessRate.get(proxyString) || 0;
      const newRate = currentRate * 0.9 + 0.1; // Slowly improve success rate
      this.proxySuccessRate.set(proxyString, newRate);
      
      this.log(`Proxy ${proxyString} success rate: ${newRate.toFixed(2)}`);
    }
  }
  
  /**
   * Record a failed proxy usage
   * @param {string} proxyString - The proxy string 
   * @param {Object} error - The error object
   */
  recordProxyFailure(proxyString, error) {
    const health = this.proxyHealth.get(proxyString);
    if (health) {
      health.failureCount++;
      health.lastCheck = Date.now();
      
      // Reduce success rate
      const currentRate = this.proxySuccessRate.get(proxyString) || 1.0;
      const newRate = currentRate * 0.8; // Significant decrease in success rate
      this.proxySuccessRate.set(proxyString, newRate);
      
      // If error is a 403, specially track it
      const is403 = error && (
        (error.response && error.response.status === 403) ||
        (error.message && error.message.includes("403"))
      );
      
      if (is403) {
        const current403Count = this.proxy403Count.get(proxyString) || 0;
        this.proxy403Count.set(proxyString, current403Count + 1);
        health.last403Time = Date.now();
        
        this.log(`Proxy ${proxyString} received 403 error (count: ${current403Count + 1})`, "warning");
        
        // If too many 403s, ban this proxy
        if (current403Count + 1 >= 5) {
          this.bannedProxies.add(proxyString);
          this.log(`Proxy ${proxyString} BANNED due to excessive 403 errors`, "error");
        }
      }
      
      // Calculate retry time with exponential backoff
      const retryDelayBase = is403 ? 5 * 60 * 1000 : 30 * 1000; // 5 minutes for 403s, 30 seconds for others
      const jitter = Math.random() * 10000; // Add up to 10 seconds of jitter
      const retryTime = Date.now() + retryDelayBase * Math.pow(2, health.failureCount - 1) + jitter;
      
      this.proxyRetryTime.set(proxyString, retryTime);
      
      // Add to failed proxies set
      this.failedProxies.add(proxyString);
      
      this.log(`Proxy ${proxyString} failure recorded. Retry after ${new Date(retryTime).toLocaleTimeString()}`, "warning");
    }
  }
  
  /**
   * Update the health status of a proxy based on successful or failed usage
   * @param {string} proxyString - The proxy string
   * @param {boolean} isHealthy - Whether the proxy is healthy or not
   */
  updateProxyHealth(proxyString, isHealthy) {
    if (isHealthy) {
      this.recordProxySuccess(proxyString);
    } else {
      this.recordProxyFailure(proxyString, new Error("Proxy marked unhealthy"));
    }
    
    // Update the health status directly
    const health = this.proxyHealth.get(proxyString);
    if (health) {
      health.isHealthy = isHealthy;
      health.lastCheck = Date.now();
      this.log(`Proxy ${proxyString} health status updated to ${isHealthy ? 'healthy' : 'unhealthy'}`);
    }
  }

  /**
   * Get a proxy for a batch of events with improved selection logic
   * @param {string[]} eventIds - Array of event IDs to process
   * @returns {Object} Object containing proxy and first event ID
  =======
   * Record a failed proxy usage
   * @param {string} proxyString - The proxy string 
   * @param {Object} error - The error object
   */
  recordProxyFailure(proxyString, error) {
    const health = this.proxyHealth.get(proxyString);
    if (health) {
      health.failureCount++;
      health.lastCheck = Date.now();
      
      // Reduce success rate
      const currentRate = this.proxySuccessRate.get(proxyString) || 1.0;
      const newRate = currentRate * 0.8; // Significant decrease in success rate
      this.proxySuccessRate.set(proxyString, newRate);
      
      // If error is a 403, specially track it
      const is403 = error && (
        (error.response && error.response.status === 403) ||
        (error.message && error.message.includes("403"))
      );
      
      if (is403) {
        const current403Count = this.proxy403Count.get(proxyString) || 0;
        this.proxy403Count.set(proxyString, current403Count + 1);
        health.last403Time = Date.now();
        
        this.log(`Proxy ${proxyString} received 403 error (count: ${current403Count + 1})`, "warning");
        
        // If too many 403s, ban this proxy
        if (current403Count + 1 >= 5) {
          this.bannedProxies.add(proxyString);
          this.log(`Proxy ${proxyString} BANNED due to excessive 403 errors`, "error");
        }
      }
      
      // Calculate retry time with exponential backoff
      const retryDelayBase = is403 ? 5 * 60 * 1000 : 30 * 1000; // 5 minutes for 403s, 30 seconds for others
      const jitter = Math.random() * 10000; // Add up to 10 seconds of jitter
      const retryTime = Date.now() + retryDelayBase * Math.pow(2, health.failureCount - 1) + jitter;
      
      this.proxyRetryTime.set(proxyString, retryTime);
      
      // Add to failed proxies set
      this.failedProxies.add(proxyString);
      
      this.log(`Proxy ${proxyString} failure recorded. Retry after ${new Date(retryTime).toLocaleTimeString()}`, "warning");
    }
  }

  /**
   * Get a proxy for a batch of events with improved selection logic
   */
  getProxyForBatch(eventIds) {
    // Ensure we don't process too many events per proxy
    if (eventIds.length > this.MAX_EVENTS_PER_PROXY * this.getAvailableProxyCount()) {
      this.log(`Warning: Requested batch size ${eventIds.length} exceeds available proxy capacity`, "warning");
      // Only process as many events as we have proxies for
      eventIds = eventIds.slice(0, this.MAX_EVENTS_PER_PROXY * this.getAvailableProxyCount());
    }
    
    // Get first event from batch to determine requirements
    const firstEventId = eventIds[0];
    
    // Find the healthiest available proxy for the first event
    const proxy = this.getProxyForEvent(firstEventId);
    
    if (!proxy) {
      this.log(`No healthy proxy available for batch with event ${firstEventId}`, "error");
      throw new Error(`No healthy proxy available for batch (${this.getAvailableProxyCount()} healthy proxies, ${this.proxies.length} total)`);
    }
    
    // Mark this proxy as used by this event
    this.assignProxyToEvent(firstEventId, proxy.proxy);
    this.proxyLastUsed.set(proxy.proxy, Date.now());
    
    const proxyAgent = this.createProxyAgent(proxy);
    
    return { ...proxyAgent, firstEventId };
  }

  /**
   * Get a unique proxy for a single event
   * @param {string} eventId - The event ID
   * @returns {Object} The selected proxy object
   */
  getProxyForEvent(eventId) {
    // Try to use a different proxy than the last one
    const startIndex = (this.lastAssignedProxyIndex + 1) % this.proxies.length;
    
    // First do a full scan to find all healthy proxies
    const healthyProxies = [];
    for (let i = 0; i < this.proxies.length; i++) {
      const index = (startIndex + i) % this.proxies.length;
      const proxy = this.proxies[index];
      
      // Skip if proxy is unhealthy or in cooldown
      if (!this.isProxyHealthy(proxy)) {
        continue;
      }
      
      // Make sure this event isn't already using this proxy
      if (this.proxyUsage.get(proxy.proxy)?.has(eventId) ||
          this.eventToProxy.get(eventId) === proxy.proxy) {
        continue;
      }
      
      healthyProxies.push({ index, proxy });
    }
    
    // If we have healthy proxies, select the best one
    let bestProxy = null;
    if (healthyProxies.length > 0) {
      // Sort by success rate (higher is better)
      healthyProxies.sort((a, b) => {
        const rateA = this.proxySuccessRate.get(a.proxy.proxy) || 0;
        const rateB = this.proxySuccessRate.get(b.proxy.proxy) || 0;
        return rateB - rateA;
      });
      
      // Take one of the top 3 proxies (if available) to avoid always using the same ones
      const topCount = Math.min(3, healthyProxies.length);
      const randomIndex = Math.floor(Math.random() * topCount);
      const selected = healthyProxies[randomIndex];
      
      bestProxy = selected.proxy;
      this.lastAssignedProxyIndex = selected.index;
    }
    
    // If no healthy proxy is found, check if there are any proxies that are due for retry
    if (!bestProxy) {
      const now = Date.now();
      for (const proxy of this.proxies) {
        // Skip banned proxies
        if (this.bannedProxies.has(proxy.proxy)) {
          continue;
        }
        
        const retryTime = this.proxyRetryTime.get(proxy.proxy) || 0;
        if (now > retryTime) {
          // This proxy has waited long enough, give it another chance
          const health = this.proxyHealth.get(proxy.proxy);
          if (health) {
            health.failureCount = Math.max(0, health.failureCount - 1);
            health.isHealthy = true;
            this.failedProxies.delete(proxy.proxy);
            bestProxy = proxy;
            this.lastAssignedProxyIndex = this.proxies.indexOf(proxy);
            this.log(`Giving proxy ${proxy.proxy} another chance after timeout for event ${eventId}`, "info");
            break;
          }
        }
      }
    }
    
    // Return the best proxy or null if none found
    return bestProxy;
  }
  
  /**
   * Create a proxy agent from a proxy object
   * @param {Object} proxy - The proxy configuration
   * @returns {Object} The proxy with agent
   */
  createProxyAgent(proxy) {
    try {
      const proxyUrl = new URL(`http://${proxy.proxy}`);
      const proxyURl = `http://${proxy.username}:${proxy.password}@${
        proxyUrl.hostname
      }:${proxyUrl.port || 80}`;
      const proxyAgent = new HttpsProxyAgent(proxyURl);
      return { proxyAgent, proxy };
    } catch (error) {
      this.log(`Invalid proxy URL format: ${error.message}`, "error");
      throw new Error("Invalid proxy URL format");
    }
  }
  
  /**
   * Assign a proxy to an event and track the usage
   * @param {string} eventId - The event ID
   * @param {string} proxyString - The proxy string (IP:port)
   */
  assignProxyToEvent(eventId, proxyString) {
    // Remove event from previous proxy if it was assigned
    if (this.eventToProxy.has(eventId)) {
      const oldProxy = this.eventToProxy.get(eventId);
      const oldUsageSet = this.proxyUsage.get(oldProxy);
      if (oldUsageSet) {
        oldUsageSet.delete(eventId);
      }
    }
    
    // Assign new proxy
    this.eventToProxy.set(eventId, proxyString);
    
    // Add to usage tracking
    if (!this.proxyUsage.has(proxyString)) {
      this.proxyUsage.set(proxyString, new Set());
    }
    this.proxyUsage.get(proxyString).add(eventId);
  }
  
  /**
   * Release a proxy assignment when an event is done
   * @param {string} eventId - The event ID to release
   * @param {boolean} success - Whether the event was processed successfully
   * @param {Object} error - Optional error object if failed
   */
  releaseProxy(eventId, success = true, error = null) {
    if (this.eventToProxy.has(eventId)) {
      const proxyString = this.eventToProxy.get(eventId);
      const usageSet = this.proxyUsage.get(proxyString);
      
      if (usageSet) {
        usageSet.delete(eventId);
        
        // Record success or failure
        if (success) {
          this.recordProxySuccess(proxyString);
        } else if (error) {
          this.recordProxyFailure(proxyString, error);
        }
        
        this.log(
          `Released proxy ${proxyString} from event ${eventId}. Current usage: ${
            usageSet.size
          }/${this.MAX_EVENTS_PER_PROXY}. Status: ${success ? 'Success' : 'Failed'}`
        );
      }
      
      this.eventToProxy.delete(eventId);
    }
  }
  
  /**
   * Release proxies for a batch of events
   * @param {string[]} eventIds - Array of event IDs to release
   * @param {boolean} success - Whether the batch was processed successfully
   * @param {Object} error - Optional error object if failed
   */
  releaseProxyBatch(eventIds, success = true, error = null) {
    for (const eventId of eventIds) {
      this.releaseProxy(eventId, success, error);
    }
  }
  
  /**
   * Get the current proxy usage statistics
   * @returns {Object} Object with usage statistics
   */
  getUsageStats() {
    const stats = {
      totalProxies: this.proxies.length,
      usedProxies: 0,
      totalAssignments: 0,
      healthyProxies: this.getAvailableProxyCount(),
      bannedProxies: this.bannedProxies.size,
      proxyDetails: []
    };
    
    for (const [proxyString, usageSet] of this.proxyUsage.entries()) {
      if (usageSet.size > 0) {
        stats.usedProxies++;
      }
      stats.totalAssignments += usageSet.size;
      
      const health = this.proxyHealth.get(proxyString);
      const count403 = this.proxy403Count.get(proxyString) || 0;
      const successRate = this.proxySuccessRate.get(proxyString) || 0;
      
      stats.proxyDetails.push({
        proxy: proxyString,
        eventsCount: usageSet.size,
        health: health ? {
          success: health.successCount,
          failures: health.failureCount,
          last403: health.last403Time ? new Date(health.last403Time).toISOString() : null,
          isHealthy: health.isHealthy
        } : null,
        count403,
        successRate: successRate.toFixed(2),
        isBanned: this.bannedProxies.has(proxyString),
        events: Array.from(usageSet)
      });
    }
    
    return stats;
  }
}

export default ProxyManager; 