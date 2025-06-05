import { parentPort, workerData } from 'worker_threads';
import { setTimeout } from 'timers/promises';

/**
 * Real Event Worker - Handles event processing in a separate thread
 * 
 * This worker receives events to process and communicates with the main thread
 * to perform the actual scraping operations. This is necessary because the scraping
 * functionality depends on shared resources managed by the main thread.
 */

// Store worker-specific state
const workerState = {
  workerId: workerData.workerId,
  isProcessing: false,
  eventsProcessed: 0,
  eventsSucceeded: 0,
  eventsFailed: 0,
  startTime: Date.now(),
  pendingEvents: new Map(), // eventId -> { resolve, reject, startTime }
  scrapeTimeout: workerData.scrapeTimeout || 30000 // Use configured timeout, default to 30s
};

/**
 * Process a batch of events
 * @param {Array} batch - Array of events to process
 */
async function processBatch(batch) {
  try {
    // Mark worker as busy
    workerState.isProcessing = true;
    
    // Process all events in parallel
    const promises = batch.map(eventItem => processEvent(eventItem));
    
    // Wait for all events to be processed
    const results = await Promise.all(promises);
    
    return results;
  } finally {
    workerState.isProcessing = false;
  }
}

/**
 * Process a single event
 * @param {Object|string} eventItem - Event to process
 */
async function processEvent(eventItem) {
  const eventId = eventItem.eventId || eventItem;
  const retryCount = eventItem.retryCount || 0;
  
  try {
    // Notify parent we're starting to process this event
    parentPort.postMessage({
      type: 'processing_started',
      eventId,
      workerId: workerState.workerId
    });
    
    // Create a promise that will be resolved when the main thread responds
    const result = await requestEventProcessing(eventId, retryCount);
    
    // Update stats
    workerState.eventsProcessed++;
    if (result.success) {
      workerState.eventsSucceeded++;
    } else {
      workerState.eventsFailed++;
    }
    
    // Notify parent of completion
    parentPort.postMessage({
      type: 'processing_complete',
      eventId,
      success: result.success,
      workerId: workerState.workerId
    });
    
    return { eventId, success: result.success };
  } catch (error) {
    // Update stats
    workerState.eventsProcessed++;
    workerState.eventsFailed++;
    
    // Notify parent of error
    parentPort.postMessage({
      type: 'processing_error',
      eventId,
      error: error.message,
      workerId: workerState.workerId
    });
    
    return { eventId, success: false, error: error.message };
  }
}

/**
 * Request event processing from the main thread
 * @param {string} eventId - Event ID to process
 * @param {number} retryCount - Retry count
 * @returns {Promise} - Promise that resolves when the event is processed
 */
function requestEventProcessing(eventId, retryCount) {
  return new Promise((resolve, reject) => {
    // Generate a unique request ID
    const requestId = `${eventId}_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
    
    // Store the promise callbacks
    workerState.pendingEvents.set(requestId, {
      resolve,
      reject,
      startTime: Date.now(),
      eventId,
      retryCount
    });
    
    // Send the request to the main thread
    parentPort.postMessage({
      type: 'scrape_event_request',
      eventId,
      retryCount,
      requestId
    });
    
    // Set a timeout to prevent hanging
    setTimeout(workerState.scrapeTimeout).then(() => {
      // Check if the request is still pending
      if (workerState.pendingEvents.has(requestId)) {
        // Remove the pending request
        workerState.pendingEvents.delete(requestId);
        
        // Reject the promise
        reject(new Error(`Request timed out after ${workerState.scrapeTimeout / 1000} seconds: ${eventId}`));
      }
    });
  });
}

// Handle scrape event responses from the main thread
function handleScrapeEventResponse(message) {
  const { requestId, success, result, error } = message;
  
  // Get the pending request
  const pendingRequest = workerState.pendingEvents.get(requestId);
  if (!pendingRequest) {
    // Request not found (may have timed out)
    return;
  }
  
  // Remove the pending request
  workerState.pendingEvents.delete(requestId);
  
  // Resolve or reject the promise
  if (success) {
    pendingRequest.resolve({ success: true, result });
  } else {
    pendingRequest.resolve({ success: false, error });
  }
}

// Listen for messages from the main thread
parentPort.on('message', async (message) => {
  try {
    switch (message.type) {
      case 'process_batch':
        const results = await processBatch(message.batch);
        parentPort.postMessage({
          type: 'batch_complete',
          results,
          workerId: workerState.workerId
        });
        break;
        
      case 'scrape_event_response':
        handleScrapeEventResponse(message);
        break;
        
      case 'status_request':
        parentPort.postMessage({
          type: 'status_response',
          status: {
            workerId: workerState.workerId,
            isProcessing: workerState.isProcessing,
            eventsProcessed: workerState.eventsProcessed,
            eventsSucceeded: workerState.eventsSucceeded,
            eventsFailed: workerState.eventsFailed,
            pendingRequests: workerState.pendingEvents.size,
            uptime: Date.now() - workerState.startTime
          }
        });
        break;
        
      case 'terminate':
        // Clean up and prepare for termination
        parentPort.postMessage({
          type: 'terminating',
          workerId: workerState.workerId
        });
        // Worker will terminate after this handler completes
        break;
        
      default:
        parentPort.postMessage({
          type: 'error',
          error: `Unknown message type: ${message.type}`,
          workerId: workerState.workerId
        });
    }
  } catch (error) {
    parentPort.postMessage({
      type: 'error',
      error: error.message,
      workerId: workerState.workerId
    });
  }
});

// Notify parent that worker is ready
parentPort.postMessage({
  type: 'ready',
  workerId: workerState.workerId
});