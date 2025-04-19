import ProxyManager from './helpers/ProxyManager.js';

/**
 * Setup global components and configuration for the application
 */
function setupGlobals() {
  console.log('Initializing global components...');

  // Create a global ProxyManager instance
  if (!global.proxyManager) {
    global.proxyManager = new ProxyManager({
      info: console.log,
      warn: console.warn,
      error: console.error
    });
    console.log(`Global ProxyManager initialized with ${global.proxyManager.proxies.length} proxies`);
  }
  
  // Track global statistics
  global.stats = {
    startTime: new Date(),
    totalRequests: 0,
    successfulRequests: 0,
    failedRequests: 0,
    requestsByProxy: new Map(),
    errors: {
      '403': 0,
      '429': 0,
      'network': 0,
      'timeout': 0,
      'other': 0
    }
  };
  
  // Set up periodic stats logging
  setInterval(() => {
    const uptime = Math.floor((new Date() - global.stats.startTime) / 1000 / 60);
    console.log(`=== STATS (${uptime} minutes uptime) ===`);
    console.log(`Total Requests: ${global.stats.totalRequests}`);
    console.log(`Success Rate: ${(global.stats.successfulRequests / global.stats.totalRequests * 100 || 0).toFixed(2)}%`);
    console.log(`Errors: 403=${global.stats.errors['403']}, 429=${global.stats.errors['429']}, network=${global.stats.errors['network']}, timeout=${global.stats.errors['timeout']}, other=${global.stats.errors['other']}`);
    
    // Log proxy usage statistics
    const proxyStats = global.proxyManager.getUsageStats();
    console.log(`Proxy Usage: ${proxyStats.usedProxies}/${proxyStats.totalProxies} proxies used`);
    console.log(`Healthy Proxies: ${proxyStats.healthyProxies}/${proxyStats.totalProxies}`);
    console.log(`Banned Proxies: ${proxyStats.bannedProxies}`);
    console.log('==============================');
  }, 5 * 60 * 1000); // Every 5 minutes
  
  // Set up error tracking
  process.on('uncaughtException', (err) => {
    console.error('Uncaught Exception:', err);
    // Continue running despite error
  });
  
  process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    // Continue running despite error
  });
  
  console.log('Global components initialized successfully');
}

// Export setup function
export default setupGlobals; 