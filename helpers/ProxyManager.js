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
    this.MAX_EVENTS_PER_PROXY = 5; // Maximum number of events per proxy
    this.BATCH_SIZE = 5; // Maximum events per batch
    this.proxies = [...proxyArray.proxies]; // Copy the proxy list
    this.lastAssignedProxyIndex = -1; // Track the last proxy we used
    
    // Initialize usage counts for each proxy
    this.proxies.forEach(proxy => {
      this.proxyUsage.set(proxy.proxy, new Set());
    });
    
    this.log("ProxyManager initialized with " + this.proxies.length + " proxies");
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
   * Get a proxy for a batch of events, ensuring different batches use different proxies
   * @param {string[]} eventIds - Array of event IDs in this batch
   * @returns {Object} The proxy configuration with agent
   */
  getProxyForBatch(eventIds) {
    // If we have too many events for one proxy, split them up
    if (eventIds.length > this.MAX_EVENTS_PER_PROXY) {
      this.log(`Batch size ${eventIds.length} exceeds max events per proxy (${this.MAX_EVENTS_PER_PROXY})`, "warning");
      // This method should be called with smaller batches
      eventIds = eventIds.slice(0, this.MAX_EVENTS_PER_PROXY);
    }
    
    // Find the least loaded proxy that isn't being used by these events
    let leastLoadedProxy = null;
    let minEvents = Infinity;
    
    // Try to use a different proxy than the last one
    const startIndex = (this.lastAssignedProxyIndex + 1) % this.proxies.length;
    
    // Cycle through proxies starting from the next available one
    for (let i = 0; i < this.proxies.length; i++) {
      const index = (startIndex + i) % this.proxies.length;
      const proxy = this.proxies[index];
      const usageSet = this.proxyUsage.get(proxy.proxy) || new Set();
      
      // Check if any events in this batch are already using this proxy
      const alreadyUsingProxy = eventIds.some(eventId => 
        usageSet.has(eventId) || 
        (this.eventToProxy.has(eventId) && this.eventToProxy.get(eventId) === proxy.proxy)
      );
      
      // Skip this proxy if it's already being used by any event in this batch
      if (alreadyUsingProxy) {
        continue;
      }
      
      // Check if this proxy has less load than our current minimum
      if (usageSet.size < minEvents) {
        minEvents = usageSet.size;
        leastLoadedProxy = proxy;
        this.lastAssignedProxyIndex = index;
        
        // If we found an unused proxy, no need to check further
        if (minEvents === 0) {
          break;
        }
      }
    }
    
    // If all proxies are at capacity or being used by these events,
    // fall back to the least loaded proxy
    if (!leastLoadedProxy) {
      this.log("All proxies are being used by events in this batch, finding least loaded proxy", "warning");
      
      minEvents = Infinity;
      for (const proxy of this.proxies) {
        const usageSet = this.proxyUsage.get(proxy.proxy) || new Set();
        if (usageSet.size < minEvents) {
          minEvents = usageSet.size;
          leastLoadedProxy = proxy;
          
          if (minEvents === 0) break;
        }
      }
    }
    
    // If all proxies are at capacity, log warning
    if (minEvents >= this.MAX_EVENTS_PER_PROXY) {
      this.log(
        `Warning: All proxies at capacity (${this.MAX_EVENTS_PER_PROXY}+ events). Using least loaded proxy with ${minEvents} events.`,
        "warning"
      );
    }
    
    // Assign this proxy to all events in the batch
    for (const eventId of eventIds) {
      this.assignProxyToEvent(eventId, leastLoadedProxy.proxy);
    }
    
    // Log the batch assignment
    this.log(`Assigned proxy ${leastLoadedProxy.proxy} to batch of ${eventIds.length} events. Usage: ${minEvents + eventIds.length}/${this.MAX_EVENTS_PER_PROXY}`);
    
    return this.createProxyAgent(leastLoadedProxy);
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