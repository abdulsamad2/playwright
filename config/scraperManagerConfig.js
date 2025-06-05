import { cpus } from "os";

// Centralized configuration for all scraper components
export default {
  // CSV Processing
  ENABLE_CSV_PROCESSING: true, // Set to false to disable all CSV generation
  ENABLE_CSV_UPLOAD: false, // Set to false to disable all_events_combined.csv upload
  
  // Time limits
  MAX_UPDATE_INTERVAL: 120000, // Strict 2-minute update requirement (reduced from 160000)
  CONCURRENT_LIMIT: 200, // Increased from 100 for 1000+ events
  MAX_RETRIES: 25, // Increased from 20 for better recovery
  SCRAPE_TIMEOUT: 45000, // Increased from 20 seconds to 45 seconds for better success rate
  MIN_TIME_BETWEEN_EVENT_SCRAPES: 30000, // Reduced to 30 seconds for faster cycles
  MAX_ALLOWED_UPDATE_INTERVAL: 180000, // Maximum 3 minutes allowed between updates
  EVENT_FAILURE_THRESHOLD: 120000, // Reduced to 2 minutes for faster recovery
  STALE_EVENT_THRESHOLD: 600000, // 10 minutes - events will be stopped after this
  
  // Additional settings from scraperConfig.js
  URGENT_THRESHOLD: 110000, // Events needing update within 10 seconds of deadline
  BATCH_SIZE: Math.max(Math.floor(cpus().length * 0.9) * 2, 10), // Dynamic batch size based on CPU
  RETRY_BACKOFF_MS: 5000, // Base backoff time for retries
  CHUNK_SIZE: 100, // Chunk size for batch DB operations
  COOKIE_RESET_COOLDOWN: 30 * 60 * 1000, // 1 hour between cookie resets
  COOKIE_REGENERATION_DELAY: 30000, // 30 seconds to allow cookie regeneration
  HEADER_REFRESH_INTERVAL: 300000, // 5 minutes between header refreshes
  STALE_TASK_TIMEOUT: 5 * 60 * 1000, // 5 minutes
  FAILURE_HISTORY_EXPIRY: 60 * 60 * 1000, // 1 hour
  
  // Enhanced recovery settings for 1000+ events
  RECOVERY_BATCH_SIZE: 100, // Increased from 50 for better throughput
  MAX_RECOVERY_BATCHES: 20, // Increased from 10 for parallel processing
  PARALLEL_BATCH_SIZE: 150, // Increased from 100 for better batching
  MAX_PARALLEL_BATCHES: 25, // Increased from 20 for 1000+ events
  
  // Multi-tier recovery intervals for aggressive processing
  CRITICAL_RECOVERY_INTERVAL: 10000, // Check critical events every 10 seconds
  AGGRESSIVE_RECOVERY_INTERVAL: 20000, // Check stale events every 20 seconds
  STANDARD_RECOVERY_INTERVAL: 30000, // Check standard events every 30 seconds
  AUTO_STOP_CHECK_INTERVAL: 60000, // Check auto-stop events every minute
  
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