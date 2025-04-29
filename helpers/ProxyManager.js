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
    this.MAX_EVENTS_PER_PROXY = 1; // Ensuring each event gets a dedicated proxy
    this.BATCH_SIZE = 3; // Maximum batch size
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
    // Check if any events were provided
    if (!eventIds || eventIds.length === 0) {
      this.log('No events provided for proxy batch processing', "warning");
      // Return empty result rather than throwing an error
      return {
        proxyAgent: null,
        proxy: null,
        eventProxyMap: new Map(),
        firstEventId: null
      };
    }
    
    // Get current available proxy count
    const availableCount = this.getAvailableProxyCount();
    
    // Log the current proxy state for debugging
    this.log(`Proxy availability: ${availableCount}/${this.proxies.length} healthy proxies for ${eventIds.length} events`);
    
    // If we have no healthy proxies, attempt to reset some for emergency use
    if (availableCount === 0 && this.proxies.length > 0) {
      this.log('No healthy proxies available, attempting to reset a few for emergency use', "warning");
      if (this.healthManager) {
        // Force reset some proxies for emergency use
        this.healthManager.resetHealthMetrics(true);
        
        // Check if we've recovered any proxies
        const recoveredCount = this.getAvailableProxyCount();
        this.log(`Emergency reset recovered ${recoveredCount} proxies`);
      }
    }
    
    // Recheck available count after potential recovery
    const updatedAvailableCount = this.getAvailableProxyCount();
    
    // Second-level fallback: If still no healthy proxies, reset ALL proxies in emergency mode
    if (updatedAvailableCount === 0 && this.proxies.length > 0) {
      this.log(`Still no healthy proxies available, forcing FULL RESET of all proxies for emergency use`, "warning");
      
      // Temporarily consider all proxies healthy for this operation
      if (this.healthManager) {
        // Force full reset of ALL proxies as last resort
        this.healthManager.resetAllProxies();
        
        // Recheck available count
        const emergencyCount = this.getAvailableProxyCount();
        this.log(`Emergency FULL RESET recovered ${emergencyCount} proxies`);
        
        // If we've recovered proxies, proceed with these emergency proxies
        if (emergencyCount > 0) {
          // Continue with the processing below
        } else {
          // If still no proxies after full reset, pick ONE proxy randomly as absolute last resort
          if (this.proxies.length > 0) {
            const randomIndex = Math.floor(Math.random() * this.proxies.length);
            const fallbackProxy = this.proxies[randomIndex];
            
            try {
              const proxyAgentData = this.createProxyAgent(fallbackProxy);
              this.log(`Using last-resort emergency proxy for batch processing`, "warning");
              
              // Return a minimal result with just this one proxy
              return {
                proxyAgent: proxyAgentData.proxyAgent,
                proxy: proxyAgentData.proxy,
                eventProxyMap: new Map(),
                firstEventId: eventIds[0],
                isEmergencyMode: true
              };
            } catch (error) {
              this.log(`Failed to create emergency proxy agent: ${error.message}`, "error");
            }
          }
        }
      }
    }
    
    // Final check if we truly have no healthy proxies
    const finalAvailableCount = this.getAvailableProxyCount();
    if (finalAvailableCount === 0) {
      this.log(`No healthy proxies available for batch with ${eventIds.length} events`, "error");
      
      // Instead of throwing an error, return an empty result
      // This lets the calling code handle the situation more gracefully
      return {
        proxyAgent: null,
        proxy: null,
        eventProxyMap: new Map(),
        firstEventId: eventIds[0] || null,
        noHealthyProxies: true  // Flag to indicate no proxies were available
      };
    }
    
    // Ensure we don't process too many events per proxy
    // Limit the batch size to what we can handle
    const maxBatchSize = this.MAX_EVENTS_PER_PROXY * finalAvailableCount;
    if (eventIds.length > maxBatchSize) {
      this.log(`Warning: Requested batch size ${eventIds.length} exceeds available proxy capacity (${maxBatchSize})`, "warning");
      // Only process as many events as we have proxies for
      eventIds = eventIds.slice(0, maxBatchSize);
    }
    
    // Create a map to store proxy assignments for each event
    const proxyMap = new Map();
    
    // Assign separate proxy for each event in the batch
    for (const eventId of eventIds) {
      // Find a healthy available proxy for this specific event
      const proxy = this.getProxyForEvent(eventId);
      
      if (!proxy) {
        this.log(`No healthy proxy available for event ${eventId} in batch`, "warning");
        continue; // Skip this event if no proxy available
      }
      
      // Mark this proxy as used by this event
      this.assignProxyToEvent(eventId, proxy.proxy);
      this.proxyLastUsed.set(proxy.proxy, Date.now());
      
      // Create proxy agent for this proxy
      try {
        const proxyAgent = this.createProxyAgent(proxy);
        proxyMap.set(eventId, { ...proxyAgent, eventId });
      } catch (error) {
        this.log(`Failed to create proxy agent for event ${eventId}: ${error.message}`, "error");
      }
    }
    
    // For backward compatibility, still return a single proxy for the first event
    const firstEventId = eventIds[0];
    const firstProxyAgent = proxyMap.get(firstEventId);
    
    if (!firstProxyAgent && proxyMap.size > 0) {
      // If first event doesn't have a proxy but other events do, use the first available
      const firstAvailable = Array.from(proxyMap.values())[0];
      return { 
        proxyAgent: firstAvailable.proxyAgent,
        proxy: firstAvailable.proxy,
        eventProxyMap: proxyMap,  // Include the full map for more advanced usage
        firstEventId
      };
    }
    
    // If we couldn't assign any proxies, return an empty result but don't throw an error
    if (proxyMap.size === 0) {
      this.log(`Warning: Failed to assign any proxies for batch with ${eventIds.length} events despite healthy proxies being available`, "warning");
      return {
        proxyAgent: null,
        proxy: null,
        eventProxyMap: new Map(),
        firstEventId: eventIds[0] || null,
        assignmentFailed: true  // Flag to indicate proxy assignment failed
      };
    }
    
    return { 
      proxyAgent: firstProxyAgent ? firstProxyAgent.proxyAgent : null,
      proxy: firstProxyAgent ? firstProxyAgent.proxy : null,
      eventProxyMap: proxyMap,  // Include the full map for more advanced usage
      firstEventId
    };
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
   * Get the total number of proxies in the system (healthy or not)
   * @returns {number} The total number of proxies
   */
  getTotalProxies() {
    return this.proxies.length;
  }
}

export default ProxyManager; 