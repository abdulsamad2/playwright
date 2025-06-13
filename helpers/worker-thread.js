/**
 * Worker Thread for Performance Optimizer
 * 
 * This worker thread handles CPU-intensive tasks in parallel to improve
 * scraping performance. Each worker manages its own set of events and
 * maintains its own connection pool.
 */

import { parentPort, workerData } from 'worker_threads';
import { setTimeout } from 'timers/promises';

// Worker configuration
const workerId = workerData.id;
let maxConcurrent = workerData.maxConcurrent || 50;
const options = workerData.options || {};

// Worker state
const activeJobs = new Map();
let isReady = false;
let isShuttingDown = false;

// Performance metrics
const stats = {
  eventsProcessed: 0,
  successCount: 0,
  failureCount: 0,
  avgResponseTime: 0,
  totalResponseTime: 0,
  activeCount: 0,
  peakConcurrency: 0,
  lastReportTime: Date.now()
};

/**
 * Initialize the worker
 */
async function initialize() {
  try {
    // Set up any worker-specific resources
    console.log(`[Worker ${workerId}] Initializing...`);
    
    // Signal that the worker is ready
    isReady = true;
    parentPort.postMessage({ type: 'ready' });
    
    // Start reporting stats periodically
    startStatsReporting();
    
    console.log(`[Worker ${workerId}] Ready with max concurrency: ${maxConcurrent}`);
  } catch (error) {
    console.error(`[Worker ${workerId}] Initialization error:`, error);
    parentPort.postMessage({ 
      type: 'error', 
      error: error.message,
      stack: error.stack 
    });
  }
}

/**
 * Process a batch of events
 */
async function processBatch(messageId, events) {
  console.log(`[Worker ${workerId}] Received batch of ${events.length} events`);
  
  const results = [];
  const promises = [];
  
  // Process events with concurrency control
  const semaphore = createSemaphore(maxConcurrent);
  
  for (const event of events) {
    const promise = processEventWithSemaphore(semaphore, event)
      .then(result => {
        results.push(result);
        return result;
      })
      .catch(error => {
        console.error(`[Worker ${workerId}] Error processing event:`, error);
        const result = {
          eventId: event.id || 'unknown',
          success: false,
          error: error.message,
          responseTime: 0
        };
        results.push(result);
        return result;
      });
    
    promises.push(promise);
  }
  
  await Promise.all(promises);
  
  // Send batch completion message
  parentPort.postMessage({
    type: 'batch_complete',
    messageId,
    results
  });
  
  return results;
}

/**
 * Process a single event with semaphore for concurrency control
 */
async function processEventWithSemaphore(semaphore, event) {
  const release = await semaphore.acquire();
  
  try {
    // Track active count for stats
    stats.activeCount++;
    if (stats.activeCount > stats.peakConcurrency) {
      stats.peakConcurrency = stats.activeCount;
    }
    
    // Process the event
    const result = await processEvent(event);
    return result;
  } finally {
    stats.activeCount--;
    release();
  }
}

/**
 * Process a single event
 */
async function processEvent(event) {
  const startTime = Date.now();
  const eventId = event.id || 'unknown';
  
  try {
    // Add to active jobs
    activeJobs.set(eventId, {
      event,
      startTime
    });
    
    // Simulate event processing
    // In a real implementation, this would call the actual scraping logic
    // This is where you would integrate with the existing scraper code
    
    // Simulate varying processing times
    const processingTime = Math.floor(Math.random() * 2000) + 1000;
    await setTimeout(processingTime);
    
    // Simulate success/failure ratio
    const success = Math.random() > 0.1; // 90% success rate
    
    const endTime = Date.now();
    const responseTime = endTime - startTime;
    
    // Update stats
    stats.eventsProcessed++;
    if (success) {
      stats.successCount++;
    } else {
      stats.failureCount++;
    }
    stats.totalResponseTime += responseTime;
    stats.avgResponseTime = stats.totalResponseTime / stats.eventsProcessed;
    
    // Remove from active jobs
    activeJobs.delete(eventId);
    
    // Create result
    const result = {
      eventId,
      success,
      responseTime,
      error: success ? null : 'Simulated failure'
    };
    
    // Send result to parent
    parentPort.postMessage({
      type: 'result',
      data: result
    });
    
    return result;
  } catch (error) {
    const endTime = Date.now();
    const responseTime = endTime - startTime;
    
    // Update stats
    stats.eventsProcessed++;
    stats.failureCount++;
    stats.totalResponseTime += responseTime;
    stats.avgResponseTime = stats.totalResponseTime / stats.eventsProcessed;
    
    // Remove from active jobs
    activeJobs.delete(eventId);
    
    // Create error result
    const result = {
      eventId,
      success: false,
      responseTime,
      error: error.message
    };
    
    // Send result to parent
    parentPort.postMessage({
      type: 'result',
      data: result
    });
    
    return result;
  }
}

/**
 * Create a semaphore for concurrency control
 */
function createSemaphore(max) {
  let count = 0;
  const queue = [];
  
  return {
    acquire: () => {
      return new Promise(resolve => {
        if (count < max) {
          count++;
          resolve(() => {
            count--;
            if (queue.length > 0) {
              const next = queue.shift();
              count++;
              next(() => {
                count--;
              });
            }
          });
        } else {
          queue.push(resolve);
        }
      });
    }
  };
}

/**
 * Start periodic stats reporting
 */
function startStatsReporting() {
  setInterval(() => {
    // Calculate current stats
    const currentStats = {
      ...stats,
      timestamp: Date.now()
    };
    
    // Send stats to parent
    parentPort.postMessage({
      type: 'stats',
      data: currentStats
    });
    
    // Log stats
    console.log(`[Worker ${workerId}] Stats: ${stats.eventsProcessed} events processed, ${stats.activeCount} active, ${stats.avgResponseTime.toFixed(2)}ms avg response time`);
  }, 5000); // Report every 5 seconds
}

/**
 * Handle shutdown request
 */
async function shutdown() {
  console.log(`[Worker ${workerId}] Shutting down...`);
  isShuttingDown = true;
  
  // Wait for active jobs to complete (with timeout)
  if (activeJobs.size > 0) {
    console.log(`[Worker ${workerId}] Waiting for ${activeJobs.size} active jobs to complete...`);
    
    // Wait up to 10 seconds for jobs to complete
    const timeout = setTimeout(() => {
      console.log(`[Worker ${workerId}] Timeout waiting for jobs to complete, forcing shutdown`);
      process.exit(0);
    }, 10000);
    
    // Check every 100ms if jobs are done
    while (activeJobs.size > 0) {
      await setTimeout(100);
    }
    
    clearTimeout(timeout);
  }
  
  console.log(`[Worker ${workerId}] Shutdown complete`);
  process.exit(0);
}

// Set up message handler
parentPort.on('message', async (message) => {
  if (!isReady && message.type !== 'shutdown') {
    console.log(`[Worker ${workerId}] Received message before ready: ${message.type}`);
    return;
  }
  
  if (isShuttingDown && message.type !== 'shutdown') {
    console.log(`[Worker ${workerId}] Ignoring message during shutdown: ${message.type}`);
    return;
  }
  
  try {
    switch (message.type) {
      case 'process_batch':
        await processBatch(message.messageId, message.events);
        break;
        
      case 'update_concurrency':
        maxConcurrent = message.maxConcurrent;
        console.log(`[Worker ${workerId}] Updated max concurrency to ${maxConcurrent}`);
        break;
        
      case 'shutdown':
        await shutdown();
        break;
        
      default:
        console.log(`[Worker ${workerId}] Unknown message type: ${message.type}`);
    }
  } catch (error) {
    console.error(`[Worker ${workerId}] Error handling message:`, error);
    parentPort.postMessage({
      type: 'error',
      error: error.message,
      stack: error.stack
    });
  }
});

// Handle process termination
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// Initialize the worker
initialize().catch(error => {
  console.error(`[Worker ${workerId}] Fatal initialization error:`, error);
  process.exit(1);
});