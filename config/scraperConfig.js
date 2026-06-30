import { cpus } from "os";

// Scraper configuration and constants
export default {
  // Time limits - optimized for better flow
  MAX_UPDATE_INTERVAL: 120000, // Strict 2-minute update requirement
  SCRAPE_TIMEOUT: 90000, // 90s max per event — smaller batches complete in ~25-35s
  MIN_TIME_BETWEEN_EVENT_SCRAPES: 500, // Minimal delay - page pool handles concurrency naturally
  URGENT_THRESHOLD: 110000, // Events needing update within 10 seconds of deadline
  PROCESSING_INTERVAL: 500, // Faster processing interval (reduced to 500ms for better throughput)
  
  // Concurrency — keep near pool capacity (POOL_SIZE pages = that many proxies at
  // once). Tune via env WITHOUT code edits, e.g. CONCURRENT_LIMIT=16 BATCH_SIZE=16.
  // NOTE: the 2nd parseInt arg is the RADIX (base 10) — the DEFAULT is after `||`.
  CONCURRENT_LIMIT: parseInt(process.env.CONCURRENT_LIMIT, 10) || 12,
  BATCH_SIZE: parseInt(process.env.BATCH_SIZE, 10) || 12,
  
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