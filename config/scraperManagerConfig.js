import { cpus } from "os";

// Centralized configuration for all scraper components
export default {
  // CSV Processing
  ENABLE_CSV_PROCESSING: true, // Set to false to disable all CSV generation
  ENABLE_CSV_UPLOAD: false, // Set to false to disable all_events_combined.csv upload
  
  // Time limits
  MAX_UPDATE_INTERVAL: 180000, // Extended to 3 minutes for better stability
  CONCURRENT_LIMIT: 150, // Reduced for better stability while maintaining throughput
  MAX_RETRIES: 40, // Increased significantly for more persistent recovery attempts
  SCRAPE_TIMEOUT: 60000, // Increased to 60 seconds to handle slower responses
  MIN_TIME_BETWEEN_EVENT_SCRAPES: 45000, // Increased to reduce pressure on the system
  MAX_ALLOWED_UPDATE_INTERVAL: 240000, // Extended to 4 minutes for more flexibility
  EVENT_FAILURE_THRESHOLD: 300000, // Extended to 5 minutes before marking as failed
  STALE_EVENT_THRESHOLD: 600000, // Maintained at 10 minutes as requested - final stopping point
  
  // Additional settings from scraperConfig.js
  URGENT_THRESHOLD: 110000, // Events needing update within 10 seconds of deadline
  BATCH_SIZE: Math.max(Math.floor(cpus().length * 0.9) * 2, 10), // Dynamic batch size based on CPU
  RETRY_BACKOFF_MS: 8000, // Increased base backoff time for more gradual retry attempts
  CHUNK_SIZE: 100, // Chunk size for batch DB operations
  COOKIE_RESET_COOLDOWN: 30 * 60 * 1000, // 1 hour between cookie resets
  COOKIE_REGENERATION_DELAY: 30000, // 30 seconds to allow cookie regeneration
  HEADER_REFRESH_INTERVAL: 300000, // 5 minutes between header refreshes
  STALE_TASK_TIMEOUT: 5 * 60 * 1000, // 5 minutes
  FAILURE_HISTORY_EXPIRY: 10 * 60 * 1000, // 10 minutes
  
  // Enhanced recovery settings for 1000+ events with gradual approach
  RECOVERY_BATCH_SIZE: 75, // Reduced for more stable processing
  MAX_RECOVERY_BATCHES: 30, // Increased for more thorough recovery attempts
  PARALLEL_BATCH_SIZE: 100, // Adjusted for better stability
  MAX_PARALLEL_BATCHES: 20, // Optimized for stability while maintaining throughput
  
  // Multi-tier recovery intervals with progressive scaling
  CRITICAL_RECOVERY_INTERVAL: 20000, // Extended to 20 seconds to reduce system pressure
  AGGRESSIVE_RECOVERY_INTERVAL: 40000, // Extended to 40 seconds for gradual recovery
  STANDARD_RECOVERY_INTERVAL: 60000, // Extended to 60 seconds for normal cases
  AUTO_STOP_CHECK_INTERVAL: 120000, // Extended to 2 minutes for less aggressive stopping
  
  // Logging levels: 0 = errors only, 1 = warnings + errors, 2 = info + warnings + errors, 3 = all (verbose)
  LOG_LEVEL: 3, // Default to warnings and errors only
  
  // Cookie expiration threshold: refresh cookies every 15 minutes
  COOKIE_EXPIRATION_MS: 15 * 60 * 1000, // 15 minutes (reduced for more frequent rotation)
  SESSION_REFRESH_INTERVAL: 15 * 60 * 1000, // 15 minutes for session refresh
  
  // Cookie management
  COOKIE_MANAGEMENT: {
    ESSENTIAL_COOKIES: [
      "TMUO",
      "TMPS",
      "TM_TKTS",
      "SESSION",
      "audit",
      "CMPS",
      "CMID",
      "MUID",
      "au_id",
      "aud",
      "tmTrackID",
      "TapAd_DID",
      "uid",
    ],
    AUTH_COOKIES: ["TMUO", "TMPS", "TM_TKTS", "SESSION", "audit"],
    MAX_COOKIE_LENGTH: 8000,
    COOKIE_REFRESH_INTERVAL: 15 * 60 * 1000, // 15 minutes (reduced for more frequent rotation)
    MAX_COOKIE_AGE: 30 * 60 * 60 * 1000,
    COOKIE_ROTATION: {
      ENABLED: true,
      MAX_STORED_COOKIES: 100,
      ROTATION_INTERVAL: 15 * 60 * 1000, // 15 minutes (reduced for more frequent rotation)
      LAST_ROTATION: Date.now(),
      ENFORCE_UNIQUE: true, // Ensure unique cookies are generated
    },
  },
  
  // CSV Upload Constants
  CSV_UPLOAD_INTERVAL: 6 * 60 * 1000, // 6 minutes in milliseconds
  COMPANY_ID: '702',
  API_TOKEN: 'OaJwtlUQiriMSrnGd7cauDWtIyAMnS363icaz-7t1vJ7bjIBe9ZFjBwgPYY1Q9eKV_Jt',
  
  // Fingerprint options to mimic human browsers
  FINGERPRINT_POOL: [
    { language: 'en-US', timezone: 'America/Los_Angeles', platform: 'Win32', screen: { width:1920, height:1080 }, deviceMemory: 8, hardwareConcurrency: 8, plugins: ['Widevine Content Decryption Module','Chrome PDF Viewer','Native Client'] },
    { language: 'en-GB', timezone: 'Europe/London', platform: 'MacIntel', screen: { width:1440, height:900 }, deviceMemory: 8, hardwareConcurrency: 4, plugins: ['PDF Viewer','QuickTime Plug-in 7.7.9','Java(TM) Platform SE 8 U211'] },
    { language: 'fr-FR', timezone: 'Europe/Paris', platform: 'Win32', screen: { width:1366, height:768 }, deviceMemory: 4, hardwareConcurrency: 4, plugins: ['Chrome PDF Viewer','Widevine Content Decryption Module'] },
    { language: 'de-DE', timezone: 'Europe/Berlin', platform: 'Linux x86_64', screen: { width:1920, height:1080 }, deviceMemory: 16, hardwareConcurrency: 8, plugins: ['Flash','QuickTime Plug-in','Java Bridge'] },
    // Firefox fingerprint with common privacy/bot-buster add-ons for human-like behavior
    { language: 'en-US', timezone: 'America/New_York', platform: 'MacIntel', screen: { width:1680, height:1050 }, deviceMemory: 8, hardwareConcurrency: 4, plugins: ['uBlock Origin','Privacy Badger','CanvasBlocker','NoScript','Video DownloadHelper'] }
  ],
  
  // Helper function to pick a random fingerprint
  randomFingerprint: function() {
    return this.FINGERPRINT_POOL[Math.floor(Math.random() * this.FINGERPRINT_POOL.length)];
  },
  
  // Helper function to generate random IP
  generateRandomIp: function() {
    return Array.from({ length: 4 }, () => Math.floor(Math.random() * 256)).join('.');
  },
  
  // Minimum valid cookies
  MIN_VALID_COOKIES: 3
};