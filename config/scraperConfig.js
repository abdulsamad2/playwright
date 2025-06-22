import { cpus } from "os";

// Scraper configuration and constants
export default {
  // Time limits
  MAX_UPDATE_INTERVAL: 120000, // Strict 2-minute update requirement
  SCRAPE_TIMEOUT: 45000, // Timeout for each scrape operation (increased from 30000)
  MIN_TIME_BETWEEN_EVENT_SCRAPES: 10000, // Minimum time between scrapes of the same event
  URGENT_THRESHOLD: 110000, // Events needing update within 10 seconds of deadline
  PROCESSING_INTERVAL: 1000, // 1-second interval between batch processing cycles for higher throughput
  
  // Concurrency settings - optimized for 2-second intervals with higher throughput
  CONCURRENT_LIMIT: Math.max(8, Math.floor(cpus().length * 1.5)), // Increased CPU utilization for better throughput
  BATCH_SIZE: 20, // Increased batch size for better event handling capacity
  
  // Retry settings
  MAX_RETRIES: 5, // Increased from 3
  RETRY_BACKOFF_MS: 5000, // Base backoff time for retries
  
  // Batch processing
  CHUNK_SIZE: 100, // Chunk size for batch DB operations
  
  // Cookie reset settings
  COOKIE_RESET_COOLDOWN: 60 * 60 * 1000, // 1 hour between cookie resets
  COOKIE_REGENERATION_DELAY: 30000, // 30 seconds to allow cookie regeneration
  
  // Header refresh delay
  HEADER_REFRESH_INTERVAL: 300000, // 5 minutes between header refreshes
  
  // Stale task cleanup
  STALE_TASK_TIMEOUT: 5 * 60 * 1000, // 5 minutes
  
  // Failure cleanup
  FAILURE_HISTORY_EXPIRY: 60 * 60 * 1000, // 1 hour
};