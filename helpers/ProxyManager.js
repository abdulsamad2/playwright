import proxyArray from "./proxy.js";
import { createRequire } from "module";
import ProxyHealthManager from "./ProxyHealthManager.js";

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
    this.MAX_EVENTS_PER_PROXY = 5; // Increased from 1 to 5 to allow more events per proxy
    this.BATCH_SIZE = 15; // Increased from 3 to 15 for larger batches
    this.proxies = [...proxyArray.proxies];
    this.lastAssignedProxyIndex = -1;
    this.proxyLastUsed = new Map(); // Track when proxies were last used
    
    // Initialize the health manager
    this.healthManager = new ProxyHealthManager(logger);
    
    // Initialize usage counts
    this.proxies.forEach(proxy => {
      this.proxyUsage.set(proxy.proxy, new Set());
    });
    
    this.log("ProxyManager initialized with " + this.proxies.length + " proxies");
  }
  
  /**
   * Initialize the proxy manager
   */
  async initialize() {
    // Initialize the health manager
    await this.healthManager.initialize();
    return this;
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
    // First check with the health manager
    if (!this.healthManager.isProxyHealthy(proxy.proxy)) {
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
    
    return true;
  }

  /**
   * Count how many healthy proxies are available
   * @returns {number} The number of healthy proxies
   */
  getAvailableProxyCount() {
    return this.proxies.filter(proxy => this.isProxyHealthy(proxy)).length;
  }

  /**
   * Get a list of all available healthy proxies
   * @returns {Array} Array of available proxy objects
   */
  getAvailableProxies() {
    return this.proxies.filter(proxy => this.isProxyHealthy(proxy));
  }

  /**
   * Record a successful proxy usage
   * @param {string} proxyString - The proxy string
   */
  recordProxySuccess(proxyString) {
    this.healthManager.recordProxySuccess(proxyString);
  }
  
  /**
   * Record a failed proxy usage
   * @param {string} proxyString - The proxy string 
   * @param {Object} error - The error object
   */
  recordProxyFailure(proxyString, error) {
    this.healthManager.recordProxyFailure(proxyString, error);
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
  }

  /**
   * Get proxies for a batch of events
   * @param {string[]} eventIds - The event IDs to get proxies for
   * @returns {Object} Object containing the proxy mappings
   */
  getProxyForBatch(eventIds) {
    // Ensure we don't process too many events per proxy
    if (eventIds.length > this.MAX_EVENTS_PER_PROXY * this.getAvailableProxyCount()) {
      this.log(`Warning: Requested batch size ${eventIds.length} exceeds available proxy capacity`, "warning");
      // Only process as many events as we have proxies for
      eventIds = eventIds.slice(0, this.MAX_EVENTS_PER_PROXY * this.getAvailableProxyCount());
    }
    
    // Create a map to store proxy assignments for each event
    const proxyMap = new Map();
    
    // Get available proxies
    const availableProxies = this.getAvailableProxies();
    if (availableProxies.length === 0) {
      this.log("No available proxies for batch", "error");
      return { proxyAgent: null, proxy: null, eventProxyMap: proxyMap };
    }
    
    // Select a random proxy from available ones
    const randomProxy = availableProxies[Math.floor(Math.random() * availableProxies.length)];
    const proxyAgent = this.createProxyAgent(randomProxy);
    
    // Assign the same random proxy to all events in the batch
    for (const eventId of eventIds) {
      this.assignProxyToEvent(eventId, randomProxy.proxy);
      this.proxyLastUsed.set(randomProxy.proxy, Date.now());
      proxyMap.set(eventId, { proxyAgent, proxy: randomProxy });
    }
    
    return { proxyAgent, proxy: randomProxy, eventProxyMap: proxyMap };
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
      // Get top proxies by random selection to avoid always using the same ones
      const topCount = Math.min(3, healthyProxies.length);
      const randomIndex = Math.floor(Math.random() * topCount);
      const selected = healthyProxies[randomIndex];
      
      bestProxy = selected.proxy;
      this.lastAssignedProxyIndex = selected.index;
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
    const healthStats = this.healthManager.getHealthStats();
    
    const stats = {
      totalProxies: this.proxies.length,
      usedProxies: 0,
      totalAssignments: 0,
      healthyProxies: this.getAvailableProxyCount(),
      bannedProxies: healthStats.totalBanned,
      unhealthyProxies: healthStats.totalUnhealthy,
      proxyDetails: []
    };
    
    for (const [proxyString, usageSet] of this.proxyUsage.entries()) {
      if (usageSet.size > 0) {
        stats.usedProxies++;
      }
      stats.totalAssignments += usageSet.size;
      
      // Find in unhealthy details if it exists
      const unhealthyDetails = healthStats.unhealthyDetails.find(p => p.proxy === proxyString);
      const isBanned = healthStats.bannedProxies.includes(proxyString);
      
      stats.proxyDetails.push({
        proxy: proxyString,
        eventsCount: usageSet.size,
        isHealthy: !unhealthyDetails && !isBanned,
        isBanned: isBanned,
        healthDetails: unhealthyDetails || null,
        events: Array.from(usageSet)
      });
    }
    
    return stats;
  }

  /**
   * Blacklist the current proxy for an event
   * @param {string} eventId - The event ID
   */
  blacklistCurrentProxy(eventId) {
    const currentProxy = this.eventToProxy.get(eventId);
    if (currentProxy) {
      this.log(`Blacklisting proxy ${currentProxy} for event ${eventId}`, "warning");
      this.healthManager.recordProxyFailure(currentProxy, new Error("Proxy blacklisted"));
      this.releaseProxy(eventId, false, new Error("Proxy blacklisted"));
    }
  }
}

export default ProxyManager; 