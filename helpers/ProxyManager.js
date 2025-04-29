import proxyArray from "./proxy.js";
import { createRequire } from "module";
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
    this.MAX_EVENTS_PER_PROXY = 2; // Increased from 1 to allow more events per proxy
    this.BATCH_SIZE = 25; // Significantly increased for maximum batch throughput
    this.proxies = [...proxyArray.proxies];
    this.lastAssignedProxyIndex = -1;
    this.proxyLastUsed = new Map(); // Track when proxies were last used
    
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
   * Check if a proxy is available
   * @param {Object} proxy - The proxy to check
   * @returns {boolean} Whether the proxy is available
   */
  isProxyHealthy(proxy) {
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
   * Count how many available proxies are available
   * @returns {number} The number of available proxies
   */
  getAvailableProxyCount() {
    return this.proxies.filter(proxy => this.isProxyHealthy(proxy)).length;
  }

  /**
   * Record a successful proxy usage
   * @param {string} proxyString - The proxy string
   */
  recordProxySuccess(proxyString) {
    // Just update the last used time
    this.proxyLastUsed.set(proxyString, Date.now());
  }
  
  /**
   * Record a failed proxy usage
   * @param {string} proxyString - The proxy string 
   * @param {Object} error - The error object
   */
  recordProxyFailure(proxyString, error) {
    // Just log the failure
    this.log(`Proxy ${proxyString} failed: ${error.message}`, "warning");
  }

  /**
   * Update the status of a proxy based on successful or failed usage
   * @param {string} proxyString - The proxy string
   * @param {boolean} isHealthy - Whether the proxy is successful or not
   */
  updateProxyHealth(proxyString, isHealthy) {
    if (isHealthy) {
      this.recordProxySuccess(proxyString);
    } else {
      this.recordProxyFailure(proxyString, new Error("Proxy marked as failed"));
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
      return {
        proxyAgent: null,
        proxy: null,
        eventProxyMap: new Map(),
        firstEventId: null
      };
    }
    
    this.log(`Assigning proxies for batch of ${eventIds.length} events`);
    
    // Create a map to store proxy assignments for each event
    const proxyMap = new Map();
    
    // Get all available proxies and shuffle them to randomize assignment
    const allProxies = [...this.proxies];
    const shuffledProxies = allProxies.sort(() => 0.5 - Math.random());
    
    // If we have no proxies at all, return empty result
    if (shuffledProxies.length === 0) {
      this.log('No proxies available for batch processing', "error");
      return {
        proxyAgent: null,
        proxy: null,
        eventProxyMap: new Map(),
        firstEventId: eventIds[0] || null,
        noHealthyProxies: true
      };
    }
    
    // Assign proxies to events by cycling through the shuffled proxies
    for (let i = 0; i < eventIds.length; i++) {
      const eventId = eventIds[i];
      const proxyIndex = i % shuffledProxies.length;
      const proxy = shuffledProxies[proxyIndex];
      
      try {
        const proxyAgentData = this.createProxyAgent(proxy);
        proxyMap.set(eventId, { ...proxyAgentData, eventId });
      } catch (error) {
        this.log(`Failed to create proxy agent for event ${eventId}: ${error.message}`, "error");
      }
    }
    
    // Log the number of unique proxies used
    const uniqueProxiesCount = new Set(Array.from(proxyMap.values()).map(p => p.proxy.proxy)).size;
    this.log(`Assigned ${uniqueProxiesCount} unique proxies for ${eventIds.length} events (random assignment)`);
    
    // For backward compatibility, return a single proxy for the first event
    const firstEventId = eventIds[0];
    const firstProxyAgent = proxyMap.get(firstEventId);
    
    return { 
      proxyAgent: firstProxyAgent ? firstProxyAgent.proxyAgent : null,
      proxy: firstProxyAgent ? firstProxyAgent.proxy : null,
      eventProxyMap: proxyMap,
      firstEventId
    };
  }

  /**
   * Get a random proxy for a single event
   * @param {string} eventId - The event ID
   * @returns {Object} The selected proxy object
   */
  getProxyForEvent(eventId) {
    // Simply pick a random proxy
    if (this.proxies.length === 0) {
      return null;
    }
    
    const randomIndex = Math.floor(Math.random() * this.proxies.length);
    return this.proxies[randomIndex];
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
        isHealthy: this.isProxyHealthy({ proxy: proxyString }),
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