import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { exec } from 'child_process';
import { promisify } from 'util';
import fetch from 'node-fetch';

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
      lastReset: Date.now(),
      activeProxyIndex: 0,           // For rotation tracking
      activeProxies: []              // List of currently active proxies
    };
    
    this.healthFilePath = path.join(__dirname, 'proxy-health-status.json');
    
    // Health status configuration
    this.config = {
      // How many consecutive failures before marking a proxy as potentially unhealthy
      failureThreshold: 15,          // Increased from 8

      // How many potential health issues before marking as unhealthy
      unhealthyThreshold: 25,        // Increased from 15

      // How many 403 errors before banning a proxy
      ban403Threshold: 12,           // Increased from 8

      // Initial cooldown time (10 minutes)
      initialCooldown: 10 * 60 * 1000, // Reduced from 15 minutes

      // Maximum cooldown time (6 hours)
      maxCooldown: 6 * 60 * 60 * 1000, // Reduced from 12 hours

      // Minimum time to test a proxy before final health decision
      minTestPeriod: 5 * 60 * 1000,  // Increased from 3 minutes

      // Reset health metrics interval
      resetInterval: 2 * 60 * 60 * 1000, // Reduced from 4 hours to 2 hours

      // Secondary test websites for proxy validation
      secondaryTestUrls: [
        "https://www.bing.com/",
        "https://www.reddit.com/",
        "https://www.youtube.com/"
      ],

      // Number of successful secondary tests required to rehabilitate
      secondaryTestSuccessThreshold: 1, // Reduced from 2 to 1

      // Ping configuration
      pingCount: 5,                     // Reduced from 10
      pingTimeout: 3000,                // Increased from 2000
      pingFailureThreshold: 4,          // Reduced from 10
      pingInterval: 3 * 60 * 1000,      // Increased from 1 minute to 3 minutes
      
      // Rotation settings
      rotationInterval: 30 * 60 * 1000, // Rotate proxies every 30 minutes
      rotationEnabled: true,            // Enable rotation by default
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
    
    // Start proxy rotation if enabled
    if (this.config.rotationEnabled) {
      this.startProxyRotation();
    }
    
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
        this.healthStore.activeProxyIndex = parsed.activeProxyIndex || 0;
        this.healthStore.activeProxies = parsed.activeProxies || [];
        
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
        lastReset: this.healthStore.lastReset,
        activeProxyIndex: this.healthStore.activeProxyIndex,
        activeProxies: this.healthStore.activeProxies
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
      
      // Auto-reset some metrics when giving a proxy another chance
      healthData.failureCount = Math.max(0, healthData.failureCount - 5);
      healthData.successCount = 0;
      
      // Don't immediately remove from unhealthy list, but let it be retried
      this.healthStore.unhealthyProxies.set(proxyString, healthData);
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
      
      // Decrease failure count with each success
      if (healthData.failureCount > 0) {
        healthData.failureCount--;
      }
      
      // If we've had several successes in a row, rehabilitate the proxy
      if (healthData.successCount >= 3) {
        this.log(`Proxy ${proxyString} has been successfully rehabilitated after ${healthData.successCount} successful uses`);
        this.healthStore.unhealthyProxies.delete(proxyString);
        this.saveHealthData();
      } else {
        // Update the health data
        this.healthStore.unhealthyProxies.set(proxyString, healthData);
      }
      
      // Make sure this proxy is in the active proxies list
      if (!this.healthStore.activeProxies.includes(proxyString)) {
        this.healthStore.activeProxies.push(proxyString);
      }
    } else {
      // If proxy was already healthy, make sure it's in the active proxies list
      if (!this.healthStore.activeProxies.includes(proxyString)) {
        this.healthStore.activeProxies.push(proxyString);
        this.saveHealthData();
      }
    }
  }
  
  /**
   * Record a failed proxy usage and determine if it should be marked unhealthy
   * @param {string} proxyString - The proxy string
   * @param {Object} error - The error object (optional)
   */
  async recordProxyFailure(proxyString, error = null) {
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
      secondaryTestSuccess: 0,
      verifiedFailing: false
    };
    
    // Update failure counts
    healthData.failureCount = (healthData.failureCount || 0) + 1;
    healthData.lastFailure = now;
    healthData.lastCheck = now;
    
    // Special handling for 403 errors
    if (is403) {
      healthData.count403 = (healthData.count403 || 0) + 1;
      this.log(`Proxy ${proxyString} received 403 error (total: ${healthData.count403})`, "warning");
      
      // Check if we should test this proxy with secondary sites due to 403 errors
      if (healthData.count403 >= this.config.ban403Threshold / 2) {
        const verified = await this.verifyProxyWithSecondarySites(proxyString);
        if (verified) {
          // If secondary tests passed, reduce 403 count as it might be site-specific
          healthData.count403 = Math.max(0, healthData.count403 - 2);
          healthData.failureCount = Math.max(0, healthData.failureCount - 2);
          this.log(`Proxy ${proxyString} passed secondary site tests, reducing failure count`, "info");
        } else {
          healthData.verifiedFailing = true;
        }
      }
      
      // Check if we should ban this proxy due to excessive 403 errors and verified failing
      if (healthData.count403 >= this.config.ban403Threshold && healthData.verifiedFailing) {
        this.banProxy(proxyString, "Excessive 403 errors verified with secondary sites");
        return;
      }
    }
    
    // Check if we should mark this proxy as unhealthy
    const timeSinceFirstFailure = now.getTime() - healthData.firstFailure.getTime();
    
    // We need both enough failures AND enough testing time before making decision
    if (healthData.failureCount >= this.config.unhealthyThreshold && 
        timeSinceFirstFailure >= this.config.minTestPeriod) {
      
      // Before marking as unhealthy, verify with secondary sites
      if (!healthData.verifiedFailing) {
        const verified = await this.verifyProxyWithSecondarySites(proxyString);
        if (verified) {
          // If secondary tests passed, reduce failure count
          healthData.failureCount = Math.max(0, healthData.failureCount - 5);
          this.log(`Proxy ${proxyString} passed secondary site tests, reducing failure count`, "info");
          this.healthStore.unhealthyProxies.set(proxyString, healthData);
          return;
        } else {
          healthData.verifiedFailing = true;
        }
      }
      
      // Calculate cooldown period based on failure sequence
      healthData.failureSequences = (healthData.failureSequences || 0) + 1;
      
      // Exponential backoff for repeated failures, but with a gentler curve
      const cooldownTime = Math.min(
        this.config.initialCooldown * Math.pow(1.3, healthData.failureSequences - 1),
        this.config.maxCooldown
      );
      
      // Add randomness to prevent all proxies retrying at the same time
      const jitter = Math.floor(Math.random() * 30000);
      healthData.nextRetryTime = new Date(now.getTime() + cooldownTime + jitter);
      
      // Only ban after many consecutive failures and verification
      if (healthData.failureSequences >= 6 && healthData.verifiedFailing) {
        this.log(`Proxy ${proxyString} has failed ${healthData.failureSequences} consecutive cooldown periods, permanently banning`, "error");
        this.banProxy(proxyString, "Multiple consecutive failure sequences verified with secondary sites");
        return;
      } else {
        this.log(`Proxy ${proxyString} marked unhealthy (attempt ${healthData.failureSequences}). Will retry after ${healthData.nextRetryTime.toLocaleString()}`, "warning");
      }
      
      // Reset failure count for next attempt
      healthData.failureCount = 0;
      healthData.successCount = 0;
      
      // Remove from active proxies list temporarily
      const index = this.healthStore.activeProxies.indexOf(proxyString);
      if (index !== -1) {
        this.healthStore.activeProxies.splice(index, 1);
      }
    }
    
    // Update the health data
    this.healthStore.unhealthyProxies.set(proxyString, healthData);
    this.saveHealthData();
  }
  
  /**
   * Verify a proxy with secondary test sites
   * @param {string} proxyString - The proxy to test
   * @returns {Promise<boolean>} Whether the proxy passed secondary tests
   */
  async verifyProxyWithSecondarySites(proxyString) {
    this.log(`Testing proxy ${proxyString} with secondary sites for verification`, "info");
    
    // Parse proxy for fetch options
    let proxyOptions = {};
    try {
      const proxyUrl = new URL(proxyString);
      proxyOptions = {
        proxy: proxyString,
        timeout: 10000 // 10 second timeout
      };
    } catch (error) {
      this.log(`Invalid proxy URL format: ${proxyString}`, "error");
      return false;
    }
    
    // Test with multiple sites
    let successCount = 0;
    
    for (const testUrl of this.config.secondaryTestUrls) {
      try {
        this.log(`Testing proxy ${proxyString} with ${testUrl}`, "info");
        
        // Using node-fetch with proxy
        const response = await fetch(testUrl, proxyOptions);
        
        if (response.ok) {
          successCount++;
          this.log(`Proxy ${proxyString} successfully connected to ${testUrl}`, "info");
        } else {
          this.log(`Proxy ${proxyString} got status ${response.status} from ${testUrl}`, "warning");
        }
      } catch (error) {
        this.log(`Proxy ${proxyString} failed to connect to ${testUrl}: ${error.message}`, "warning");
      }
    }
    
    const verified = successCount >= this.config.secondaryTestSuccessThreshold;
    this.log(`Proxy ${proxyString} verification result: ${verified ? 'PASSED' : 'FAILED'} (${successCount}/${this.config.secondaryTestUrls.length} successful)`, 
      verified ? "info" : "warning");
    
    return verified;
  }
  
  /**
   * Permanently ban a proxy
   * @param {string} proxyString - The proxy string to ban
   * @param {string} reason - The reason for banning
   */
  banProxy(proxyString, reason = "Unknown") {
    if (!this.healthStore.bannedProxies.has(proxyString)) {
      this.log(`Banning proxy ${proxyString} permanently: ${reason}`, 'warning');
      this.healthStore.bannedProxies.add(proxyString);
      
      // Remove from unhealthy set if it's there
      this.healthStore.unhealthyProxies.delete(proxyString);
      
      // Save the banned status
      this.saveHealthData();
      
      return true;
    }
    return false;
  }
  
  /**
   * Reset health metrics for all proxies - emergency method
   * This completely resets all proxy health metrics in emergency situations
   * @returns {number} The number of proxies reset
   */
  resetAllProxies() {
    const unhealthyCount = this.healthStore.unhealthyProxies.size;
    this.log(`EMERGENCY: Resetting ALL proxy health metrics (${unhealthyCount} unhealthy proxies)`, 'warning');
    
    // Clear all unhealthy proxies
    this.healthStore.unhealthyProxies.clear();
    
    // Temporarily clear banned proxies as well
    const bannedCount = this.healthStore.bannedProxies.size;
    const bannedProxies = Array.from(this.healthStore.bannedProxies);
    this.healthStore.bannedProxies.clear();
    
    // Save this change to the file immediately
    this.saveHealthData().catch(err => {
      this.log(`Error saving emergency proxy reset: ${err.message}`, 'error');
    });
    
    // Set a timeout to restore banned proxies after 5 minutes
    setTimeout(() => {
      this.log(`Restoring ${bannedCount} banned proxies after emergency reset period`, 'warning');
      bannedProxies.forEach(proxy => {
        this.healthStore.bannedProxies.add(proxy);
      });
      this.saveHealthData().catch(err => {
        this.log(`Error restoring banned proxies: ${err.message}`, 'error');
      });
    }, 5 * 60 * 1000); // 5 minutes
    
    return unhealthyCount + bannedCount;
  }
  
  /**
   * Reset health metrics periodically to prevent permanently marking proxies as unhealthy
   * @param {boolean} forceReset - Whether to force reset some proxies for emergency use
   */
  resetHealthMetrics(forceReset = false) {
    // If force reset is requested, reset some proxies no matter what
    if (forceReset && this.healthStore.unhealthyProxies.size > 0) {
      this.log(`Force resetting some proxies due to emergency request`);
      
      // Get all unhealthy proxies
      const unhealthyProxies = Array.from(this.healthStore.unhealthyProxies.keys());
      
      // Reset up to 5 proxies (or 25% of unhealthy ones, whichever is more)
      const resetCount = Math.max(5, Math.ceil(unhealthyProxies.length * 0.25));
      const proxiesToReset = unhealthyProxies.slice(0, resetCount);
      
      this.log(`Emergency resetting ${proxiesToReset.length} proxies out of ${unhealthyProxies.length} unhealthy proxies`);
      
      // Reset these proxies
      for (const proxy of proxiesToReset) {
        this.healthStore.unhealthyProxies.delete(proxy);
      }
      
      // Save the updated health data
      this.saveHealthData();
      return;
    }
    
    // Only reset metrics periodically
    const timeSinceLastReset = Date.now() - this.healthStore.lastReset;
    if (timeSinceLastReset < this.config.resetInterval) {
      return;
    }
    
    this.log('Resetting proxy health metrics...');
    
    // Record the reset time
    this.healthStore.lastReset = Date.now();
    
    // Add some unhealthy proxies that might be ready for retry
    const proxiesToRehabilitate = [];
    
    for (const [proxy, data] of this.healthStore.unhealthyProxies.entries()) {
      // If this proxy has been unhealthy for a long time, give it another chance
      const unhealthyTime = Date.now() - data.lastFailure.getTime();
      if (unhealthyTime > this.config.maxCooldown) {
        proxiesToRehabilitate.push(proxy);
        this.log(`Rehabilitating proxy ${proxy} after ${Math.round(unhealthyTime/3600000)} hours in unhealthy state`);
      }
    }
    
    // Remove rehabilitated proxies from unhealthy list
    for (const proxy of proxiesToRehabilitate) {
      this.healthStore.unhealthyProxies.delete(proxy);
    }
    
    // Save the updated health data
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
      totalActive: this.healthStore.activeProxies.length,
      currentRotationIndex: this.healthStore.activeProxyIndex,
      lastReset: new Date(this.healthStore.lastReset).toLocaleString(),
      unhealthyDetails: Array.from(this.healthStore.unhealthyProxies.entries()).map(([proxy, data]) => ({
        proxy,
        failureCount: data.failureCount || 0,
        successCount: data.successCount || 0,
        count403: data.count403 || 0,
        firstFailure: data.firstFailure ? data.firstFailure.toLocaleString() : 'N/A',
        lastFailure: data.lastFailure ? data.lastFailure.toLocaleString() : 'N/A',
        nextRetryTime: data.nextRetryTime ? data.nextRetryTime.toLocaleString() : 'N/A',
        failureSequences: data.failureSequences || 0,
        verifiedFailing: data.verifiedFailing || false
      })),
      bannedProxies: Array.from(this.healthStore.bannedProxies),
      activeProxies: this.healthStore.activeProxies
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
      
      // More permissive ping failure threshold
      if (failedPings >= this.config.pingFailureThreshold) {
        // Before marking as unhealthy, verify with secondary sites
        const verified = await this.verifyProxyWithSecondarySites(proxyString);
        if (!verified) {
          this.log(`Proxy ${proxyString} failed ${failedPings} pings and secondary site tests, marking as unhealthy`, 'warning');
          this.recordProxyFailure(proxyString, { message: `Failed ${failedPings} pings and secondary tests` });
          return false;
        } else {
          this.log(`Proxy ${proxyString} failed pings but passed secondary site tests, keeping healthy`, 'info');
          return true;
        }
      }
      
      return true;
    } catch (error) {
      this.log(`Error pinging proxy ${proxyString}: ${error.message}`, 'error');
      
      // Before marking as unhealthy, verify with secondary sites
      const verified = await this.verifyProxyWithSecondarySites(proxyString);
      if (!verified) {
        this.recordProxyFailure(proxyString, error);
        return false;
      } else {
        this.log(`Proxy ${proxyString} failed ping but passed secondary site tests, keeping healthy`, 'info');
        return true;
      }
    }
  }

  /**
   * Start periodic ping checks for all proxies
   */
  startPingMonitoring() {
    setInterval(async () => {
      // Check a random sample of proxies, not all at once
      const allProxies = [...this.healthStore.activeProxies];
      
      // Add some unhealthy proxies that might be ready for retry
      const unhealthyReadyForRetry = Array.from(this.healthStore.unhealthyProxies.entries())
        .filter(([proxy, data]) => {
          return !this.healthStore.bannedProxies.has(proxy) && 
                 (!data.nextRetryTime || data.nextRetryTime.getTime() <= Date.now());
        })
        .map(([proxy]) => proxy);
      
      allProxies.push(...unhealthyReadyForRetry);
      
      // Shuffle and take a subset
      const shuffled = allProxies.sort(() => 0.5 - Math.random());
      const samplesToCheck = shuffled.slice(0, Math.min(5, shuffled.length));
      
      for (const proxy of samplesToCheck) {
        if (!this.healthStore.bannedProxies.has(proxy)) {
          await this.checkProxyWithPing(proxy);
        }
      }
    }, this.config.pingInterval);
  }
  
  /**
   * Get the next proxy in rotation
   * @returns {string|null} The next proxy to use or null if none available
   */
  getNextProxy() {
    if (!this.healthStore.activeProxies.length) {
      return null;
    }
    
    // Update index
    this.healthStore.activeProxyIndex = (this.healthStore.activeProxyIndex + 1) % this.healthStore.activeProxies.length;
    
    // Get proxy at current index
    return this.healthStore.activeProxies[this.healthStore.activeProxyIndex];
  }
  
  /**
   * Add a new proxy to the rotation
   * @param {string} proxyString - The proxy to add
   */
  addProxyToRotation(proxyString) {
    if (!this.healthStore.activeProxies.includes(proxyString) && 
        !this.healthStore.bannedProxies.has(proxyString)) {
      this.healthStore.activeProxies.push(proxyString);
      this.log(`Added proxy ${proxyString} to rotation`, 'info');
      this.saveHealthData();
      return true;
    }
    return false;
  }
  
  /**
   * Start the automatic proxy rotation
   */
  startProxyRotation() {
    setInterval(() => {
      if (this.healthStore.activeProxies.length > 1) {
        const nextProxy = this.getNextProxy();
        this.log(`Rotating to next proxy: ${nextProxy}`, 'info');
      }
    }, this.config.rotationInterval);
  }
}

export default ProxyHealthManager; 