import { cpus } from "os";

// Scraper configuration and constants
export default {
  // Time limits - optimized for better flow
  MAX_UPDATE_INTERVAL: 120000, // Strict 2-minute update requirement
  SCRAPE_TIMEOUT: 30000, // Reduced timeout to prevent hanging (reduced from 45000)
  MIN_TIME_BETWEEN_EVENT_SCRAPES: 5000, // Reduced from 10000 to allow faster retries
  URGENT_THRESHOLD: 110000, // Events needing update within 10 seconds of deadline
  PROCESSING_INTERVAL: 500, // Faster processing interval (reduced from 1000ms)
  
  // Concurrency settings - optimized for 2-second intervals with higher throughput
  CONCURRENT_LIMIT: Math.max(8, Math.floor(cpus().length * 1.5)), // Increased CPU utilization for better throughput
  BATCH_SIZE: 20, // Increased batch size for better event handling capacity
  
  // Retry settings - optimized for resilience
  MAX_RETRIES: 8, // Increased from 5 for better persistence
  RETRY_BACKOFF_MS: 3000, // Reduced base backoff (from 5000) for faster retries
  
  // Batch processing
  CHUNK_SIZE: 100, // Chunk size for batch DB operations
  
  // Cookie reset settings
  COOKIE_RESET_COOLDOWN: 60 * 60 * 1000, // 1 hour between cookie resets
  COOKIE_REGENERATION_DELAY: 30000, // 30 seconds to allow cookie regeneration
  
  // Header refresh delay
  HEADER_REFRESH_INTERVAL: 300000, // 5 minutes between header refreshes
  
  // Stale task cleanup - more aggressive
  STALE_TASK_TIMEOUT: 2 * 60 * 1000, // Reduced to 2 minutes for faster recovery
  
  // Failure cleanup - shorter memory for faster recovery
  FAILURE_HISTORY_EXPIRY: 30 * 60 * 1000, // Reduced to 30 minutes
};