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
    this.MAX_EVENTS_PER_PROXY = 3; // Reduced from 5 to 3 to avoid overloading
    this.BATCH_SIZE = 3; // Reduced from 5 to 3
    this.proxies = [...proxyArray.proxies];
    this.lastAssignedProxyIndex = -1;
    this.proxyHealth = new Map(); // Track proxy health status
    this.failedProxies = new Set(); // Track failed proxies
    this.proxyLastUsed = new Map(); // Track when proxies were last used
    this.proxyRetryTime = new Map(); // Track when to retry failed proxies
    this.proxyLastReset = Date.now(); // Track when we last reset proxy health
    
    // Initialize usage counts and health status
    this.proxies.forEach(proxy => {
      this.proxyUsage.set(proxy.proxy, new Set());
      this.proxyHealth.set(proxy.proxy, { 
        successCount: 0,
        failureCount: 0,
        lastCheck: 0,
        isHealthy: true
      });
      this.proxyRetryTime.set(proxy.proxy, 0);
    });
    
    // Set up periodic health reset to prevent permanently marking proxies as unhealthy
    setInterval(() => this.resetProxyHealthMetrics(), 5 * 60 * 1000); // Reset every 5 minutes
    
    this.log("ProxyManager initialized with " + this.proxies.length + " proxies");
  }
  
  /**
   * Reset proxy health metrics periodically to prevent permanent bans
   */
  resetProxyHealthMetrics() {
    // Only reset if it's been at least 5 minutes since the last reset
    if (Date.now() - this.proxyLastReset < 5 * 60 * 1000) {
      return;
    }
    
    this.log("Resetting proxy health metrics", "info");
    this.proxyLastReset = Date.now();
    this.failedProxies.clear();
    
    // Reset health metrics but maintain some memory of past performance
    for (const [proxyString, health] of this.proxyHealth.entries()) {
      // Reduce failure count but don't completely reset
      health.failureCount = Math.floor(health.failureCount / 2);
      
      // If failures are low enough, mark as healthy again
      if (health.failureCount <= 1) {
        health.isHealthy = true;
      }
    }
    
    // Clear retry times
    for (const proxyString of this.proxyRetryTime.keys()) {
      this.proxyRetryTime.set(proxyString, 0);
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
    
    // If proxy has too many failures, mark as unhealthy
    if (health.failureCount > 3 && health.successCount < health.failureCount) {
      return false;
    }
    
    // If proxy was used recently, add cooldown (short cooldown to prevent overuse)
    const lastUsed = this.proxyLastUsed.get(proxy.proxy) || 0;
    const cooldownTime = 2000; // Reduced to 2 seconds for higher throughput
    if (Date.now() - lastUsed < cooldownTime) {
      return false;
    }
    
    // Check if proxy has capacity
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
   * Update proxy health status
   * @param {string} proxyString - The proxy string
   * @param {boolean} success - Whether the request was successful
   */
  updateProxyHealth(proxyString, success) {
    const health = this.proxyHealth.get(proxyString);
    if (!health) return;
    
    if (success) {
      health.successCount++;
      health.failureCount = Math.max(0, health.failureCount - 1);
      if (health.failureCount <= 1) {
        health.isHealthy = true;
        this.failedProxies.delete(proxyString);
      }
    } else {
      health.failureCount++;
      
      // Progressive backoff for failed proxies
      if (health.failureCount > 3) {
        health.isHealthy = false;
        this.failedProxies.add(proxyString);
        
        // Set retry timeout with progressive backoff
        const backoffTime = Math.min(30000 * Math.pow(2, health.failureCount - 3), 10 * 60 * 1000);
        this.proxyRetryTime.set(proxyString, Date.now() + backoffTime);
        
        this.log(`Proxy ${proxyString} marked unhealthy, retry after ${backoffTime/1000}s`, "warning");
      }
    }
    
    health.lastCheck = Date.now();
  }

  /**
   * Mark a proxy as failed/blacklisted for an event
   * @param {string} eventId - The event ID that had a problem with the proxy
   */
  blacklistCurrentProxy(eventId) {
    if (this.eventToProxy.has(eventId)) {
      const proxyString = this.eventToProxy.get(eventId);
      this.updateProxyHealth(proxyString, false);
      this.log(`Blacklisted proxy ${proxyString} due to failure with event ${eventId}`, "warning");
    }
  }

  /**
   * Get a proxy for a batch of events with improved selection logic
   */
  getProxyForBatch(eventIds) {
    // Ensure we don't process too many events per proxy
    if (eventIds.length > this.MAX_EVENTS_PER_PROXY) {
      this.log(`Batch size ${eventIds.length} exceeds max events per proxy (${this.MAX_EVENTS_PER_PROXY})`, "warning");
      // Instead of truncating, split into smaller batches if we have enough proxies
      const availableProxies = this.getAvailableProxyCount();
      if (availableProxies >= Math.ceil(eventIds.length / this.MAX_EVENTS_PER_PROXY)) {
        // We have enough proxies to handle this batch, so we'll process all events
        // The caller will handle creating multiple batches
        // Just limit this batch to the max per proxy
        eventIds = eventIds.slice(0, this.MAX_EVENTS_PER_PROXY);
      } else {
        // We're truly limited on proxies, so restrict batch size
        eventIds = eventIds.slice(0, this.MAX_EVENTS_PER_PROXY);
      }
    }
    
    // Find the healthiest available proxy
    let bestProxy = null;
    let bestScore = -Infinity;
    
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
      
      // Check if any events in this batch are already using this proxy
      const alreadyUsingProxy = eventIds.some(eventId => 
        this.proxyUsage.get(proxy.proxy)?.has(eventId) || 
        this.eventToProxy.get(eventId) === proxy.proxy
      );
      
      if (alreadyUsingProxy) {
        continue;
      }
      
      healthyProxies.push({ index, proxy });
    }
    
    // If we have healthy proxies, select the best one
    if (healthyProxies.length > 0) {
      for (const { index, proxy } of healthyProxies) {
        // Calculate proxy score based on health and usage
        const health = this.proxyHealth.get(proxy.proxy);
        const usageCount = this.proxyUsage.get(proxy.proxy)?.size || 0;
        const lastUsed = this.proxyLastUsed.get(proxy.proxy) || 0;
        const timeSinceLastUse = Date.now() - lastUsed;
        
        const score = 
          (health.successCount - health.failureCount) * 10 + // Health score
          (this.MAX_EVENTS_PER_PROXY - usageCount) * 5 + // Usage score
          Math.min(timeSinceLastUse / 1000, 30); // Time score
        
        if (score > bestScore) {
          bestScore = score;
          bestProxy = proxy;
          this.lastAssignedProxyIndex = index;
        }
      }
    }
    
    // If no healthy proxy is found, check if there are any proxies that are due for retry
    if (!bestProxy) {
      const now = Date.now();
      for (const proxy of this.proxies) {
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
            this.log(`Giving proxy ${proxy.proxy} another chance after timeout`, "info");
            break;
          }
        }
      }
    }
    
    // If still no proxy, use a less strict fallback strategy
    if (!bestProxy) {
      this.log("No healthy proxies available, using fallback", "warning");
      
      // Find the least loaded proxy regardless of health
      let leastLoadedProxy = null;
      let lowestUsage = Infinity;
      
      for (const proxy of this.proxies) {
        const usageCount = this.proxyUsage.get(proxy.proxy)?.size || 0;
        if (usageCount < lowestUsage) {
          lowestUsage = usageCount;
          leastLoadedProxy = proxy;
        }
      }
      
      // If we found a proxy with some capacity, use it
      if (leastLoadedProxy && lowestUsage < this.MAX_EVENTS_PER_PROXY) {
        bestProxy = leastLoadedProxy;
        this.lastAssignedProxyIndex = this.proxies.indexOf(bestProxy);
      } else {
        // Absolute last resort - just use the first proxy
        bestProxy = this.proxies[0];
        this.lastAssignedProxyIndex = 0;
      }
      
      // Reset its health since we're going to use it anyway
      const health = this.proxyHealth.get(bestProxy.proxy);
      if (health) {
        health.isHealthy = true;
        health.failureCount = Math.max(0, health.failureCount - 1);
      }
    }
    
    // Assign proxy to events
    for (const eventId of eventIds) {
      this.assignProxyToEvent(eventId, bestProxy.proxy);
    }
    
    // Update last used time
    this.proxyLastUsed.set(bestProxy.proxy, Date.now());
    
    this.log(`Assigned proxy ${bestProxy.proxy} to batch of ${eventIds.length} events`);
    
    return this.createProxyAgent(bestProxy);
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
   */
  releaseProxy(eventId) {
    if (this.eventToProxy.has(eventId)) {
      const proxyString = this.eventToProxy.get(eventId);
      const usageSet = this.proxyUsage.get(proxyString);
      
      if (usageSet) {
        usageSet.delete(eventId);
        this.log(
          `Released proxy ${proxyString} from event ${eventId}. Current usage: ${
            usageSet.size
          }/${this.MAX_EVENTS_PER_PROXY}`
        );
      }
      
      this.eventToProxy.delete(eventId);
    }
  }
  
  /**
   * Release proxies for a batch of events
   * @param {string[]} eventIds - Array of event IDs to release
   */
  releaseProxyBatch(eventIds) {
    for (const eventId of eventIds) {
      this.releaseProxy(eventId);
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
      proxyDetails: []
    };
    
    for (const [proxyString, usageSet] of this.proxyUsage.entries()) {
      if (usageSet.size > 0) {
        stats.usedProxies++;
      }
      stats.totalAssignments += usageSet.size;
      
      stats.proxyDetails.push({
        proxy: proxyString,
        eventsCount: usageSet.size,
        events: Array.from(usageSet)
      });
    }
    
    return stats;
  }
}

export default ProxyManager; 