import { Worker } from 'worker_threads';
import path from 'path';
import { fileURLToPath } from 'url';
import os from 'os';

// Get directory name in ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * WorkerPool - Manages a pool of worker threads for event processing
 * 
 * This class creates and manages worker threads to process events in parallel,
 * maximizing CPU utilization without blocking the main thread.
 */
export class WorkerPool {
  /**
   * Create a new worker pool
   * @param {Object} options - Worker pool options
   * @param {number} options.maxWorkers - Maximum number of workers (defaults to CPU count)
   * @param {Function} options.scrapeEventHandler - Function to handle scrape event requests
   * @param {Function} options.logger - Logger function
   * @param {number} options.scrapeTimeout - Timeout for scrape operations in milliseconds
   */
  constructor(options = {}) {
    // Set default options
    this.options = {
      maxWorkers: options.maxWorkers || Math.max(1, os.cpus().length - 1), // Leave one CPU for main thread
      workerScript: options.workerScript || path.join(__dirname, 'eventWorker.js'),
      logger: options.logger || console.log,
      scrapeEventHandler: options.scrapeEventHandler || null,
      scrapeTimeout: options.scrapeTimeout || 30000 // Default to 30 seconds if not provided
    };
    
    // Initialize worker pool
    this.workers = [];
    this.activeWorkers = new Map(); // workerId -> busy status
    this.taskQueue = [];
    this.messageHandlers = new Map();
    
    // Statistics
    this.stats = {
      tasksProcessed: 0,
      tasksQueued: 0,
      tasksSucceeded: 0,
      tasksFailed: 0,
      startTime: Date.now()
    };
    
    // Register default message handlers
    this._registerDefaultHandlers();
  }
  
  /**
   * Initialize the worker pool
   */
  async initialize() {
    this.log(`Initializing worker pool with ${this.options.maxWorkers} workers`);
    
    // Create workers
    const initPromises = [];
    
    for (let i = 0; i < this.options.maxWorkers; i++) {
      initPromises.push(this._createWorker(i));
    }
    
    // Wait for all workers to initialize
    await Promise.all(initPromises);
    
    this.log(`Worker pool initialized with ${this.workers.length} workers`);
    return this.workers.length;
  }
  
  /**
   * Submit a batch of tasks to the worker pool
   * @param {Array} batch - Batch of events to process
   * @returns {Promise} - Promise that resolves when the batch is processed
   */
  async submitBatch(batch) {
    if (!batch || batch.length === 0) return [];
    
    this.stats.tasksQueued += batch.length;
    
    // Create a promise that will resolve when the batch is complete
    return new Promise((resolve, reject) => {
      const taskId = `batch_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
      
      // Find an available worker
      const availableWorker = this._getAvailableWorker();
      
      if (availableWorker) {
        // Process batch immediately
        this._processBatchWithWorker(availableWorker, batch, taskId)
          .then(resolve)
          .catch(reject);
      } else {
        // Queue the batch for later processing
        this.taskQueue.push({
          id: taskId,
          batch,
          resolve,
          reject
        });
        
        this.log(`No available workers. Queued batch ${taskId} with ${batch.length} events. Queue size: ${this.taskQueue.length}`);
      }
    });
  }
  
  /**
   * Process the next batch in the queue if a worker is available
   */
  _processNextBatch() {
    if (this.taskQueue.length === 0) return;
    
    const availableWorker = this._getAvailableWorker();
    if (!availableWorker) return;
    
    const task = this.taskQueue.shift();
    
    this._processBatchWithWorker(availableWorker, task.batch, task.id)
      .then(task.resolve)
      .catch(task.reject);
  }
  
  /**
   * Process a batch with a specific worker
   * @param {Worker} worker - Worker to use
   * @param {Array} batch - Batch of events to process
   * @param {string} taskId - Task ID
   * @returns {Promise} - Promise that resolves when the batch is processed
   */
  async _processBatchWithWorker(worker, batch, taskId) {
    return new Promise((resolve, reject) => {
      // Set up one-time handler for batch completion
      const batchCompleteHandler = (message) => {
        if (message.type === 'batch_complete' && message.workerId === worker.workerId) {
          // Remove the handler
          worker.removeListener('message', batchCompleteHandler);
          
          // Mark worker as available
          this.activeWorkers.set(worker.workerId, false);
          
          // Update stats
          this.stats.tasksProcessed += batch.length;
          const successCount = message.results.filter(r => r.success).length;
          this.stats.tasksSucceeded += successCount;
          this.stats.tasksFailed += (batch.length - successCount);
          
          // Process next batch if available
          this._processNextBatch();
          
          // Resolve with results
          resolve(message.results);
        }
      };
      
      // Add the handler
      worker.on('message', batchCompleteHandler);
      
      // Mark worker as busy
      this.activeWorkers.set(worker.workerId, true);
      
      // Send the batch to the worker
      worker.postMessage({
        type: 'process_batch',
        batch,
        taskId
      });
    });
  }
  
  /**
   * Get an available worker
   * @returns {Worker|null} - Available worker or null if none available
   */
  _getAvailableWorker() {
    for (const worker of this.workers) {
      if (!this.activeWorkers.get(worker.workerId)) {
        return worker;
      }
    }
    return null;
  }
  
  /**
   * Create a new worker
   * @param {number} id - Worker ID
   * @returns {Promise} - Promise that resolves when the worker is ready
   */
  async _createWorker(id) {
    return new Promise((resolve, reject) => {
      try {
        // Create the worker
        const worker = new Worker(this.options.workerScript, {
          workerData: { 
            workerId: id,
            scrapeTimeout: this.options.scrapeTimeout // Pass scrapeTimeout to worker
          }
        });
        
        // Add worker ID for tracking
        worker.workerId = id;
        
        // Set up message handler
        worker.on('message', (message) => this._handleWorkerMessage(worker, message));
        
        // Set up error handler
        worker.on('error', (error) => {
          this.log(`Worker ${id} error: ${error.message}`, 'error');
          this._recreateWorker(worker);
        });
        
        // Set up exit handler
        worker.on('exit', (code) => {
          if (code !== 0) {
            this.log(`Worker ${id} exited with code ${code}`, 'error');
            this._recreateWorker(worker);
          }
        });
        
        // Wait for ready message
        const readyHandler = (message) => {
          if (message.type === 'ready' && message.workerId === id) {
            worker.removeListener('message', readyHandler);
            
            // Add to workers list
            this.workers.push(worker);
            this.activeWorkers.set(worker.workerId, false);
            
            this.log(`Worker ${id} is ready`);
            resolve(worker);
          }
        };
        
        worker.on('message', readyHandler);
      } catch (error) {
        reject(error);
      }
    });
  }
  
  /**
   * Recreate a worker that has crashed or exited
   * @param {Worker} worker - Worker to recreate
   */
  async _recreateWorker(worker) {
    try {
      // Remove from workers list
      const index = this.workers.findIndex(w => w.workerId === worker.workerId);
      if (index !== -1) {
        this.workers.splice(index, 1);
      }
      
      // Remove from active workers
      this.activeWorkers.delete(worker.workerId);
      
      // Create a new worker with the same ID
      await this._createWorker(worker.workerId);
      
      // Process next batch if available
      this._processNextBatch();
    } catch (error) {
      this.log(`Failed to recreate worker ${worker.workerId}: ${error.message}`, 'error');
    }
  }
  
  /**
   * Handle a message from a worker
   * @param {Worker} worker - Worker that sent the message
   * @param {Object} message - Message from the worker
   */
  _handleWorkerMessage(worker, message) {
    // Check if we have a handler for this message type
    const handler = this.messageHandlers.get(message.type);
    if (handler) {
      handler(worker, message);
    }
  }
  
  /**
   * Register default message handlers
   */
  _registerDefaultHandlers() {
    // Handle scrape_event_request messages
    this.messageHandlers.set('scrape_event_request', (worker, message) => {
      if (this.options.scrapeEventHandler) {
        // Call the scrape event handler
        this.options.scrapeEventHandler(message.eventId, message.retryCount)
          .then(result => {
            // Send the result back to the worker
            worker.postMessage({
              type: 'scrape_event_response',
              requestId: message.requestId,
              success: true,
              result
            });
          })
          .catch(error => {
            // Send the error back to the worker
            worker.postMessage({
              type: 'scrape_event_response',
              requestId: message.requestId,
              success: false,
              error: error.message
            });
          });
      }
    });
    
    // Handle processing_started messages
    this.messageHandlers.set('processing_started', (worker, message) => {
      // Could be used for tracking/logging
    });
    
    // Handle processing_complete messages
    this.messageHandlers.set('processing_complete', (worker, message) => {
      // Could be used for tracking/logging
    });
    
    // Handle processing_error messages
    this.messageHandlers.set('processing_error', (worker, message) => {
      this.log(`Worker ${message.workerId} error processing event ${message.eventId}: ${message.error}`, 'error');
    });
    
    // Handle error messages
    this.messageHandlers.set('error', (worker, message) => {
      this.log(`Worker ${message.workerId} error: ${message.error}`, 'error');
    });
  }
  
  /**
   * Register a custom message handler
   * @param {string} type - Message type
   * @param {Function} handler - Message handler
   */
  registerMessageHandler(type, handler) {
    this.messageHandlers.set(type, handler);
  }
  
  /**
   * Get worker pool statistics
   * @returns {Object} - Worker pool statistics
   */
  getStats() {
    const activeWorkerCount = Array.from(this.activeWorkers.values()).filter(Boolean).length;
    
    return {
      ...this.stats,
      workerCount: this.workers.length,
      activeWorkerCount,
      queueSize: this.taskQueue.length,
      uptime: Date.now() - this.stats.startTime
    };
  }
  
  /**
   * Shut down the worker pool
   */
  async shutdown() {
    this.log('Shutting down worker pool');
    
    // Send terminate message to all workers
    const terminatePromises = this.workers.map(worker => {
      return new Promise((resolve) => {
        // Set up one-time handler for termination
        const terminateHandler = (message) => {
          if (message.type === 'terminating' && message.workerId === worker.workerId) {
            worker.removeListener('message', terminateHandler);
            resolve();
          }
        };
        
        worker.on('message', terminateHandler);
        
        // Send terminate message
        worker.postMessage({ type: 'terminate' });
        
        // Force terminate after timeout
        setTimeout(() => {
          worker.terminate();
          resolve();
        }, 5000);
      });
    });
    
    // Wait for all workers to terminate
    await Promise.all(terminatePromises);
    
    this.log('Worker pool shut down');
  }
  
  /**
   * Log a message
   * @param {string} message - Message to log
   * @param {string} level - Log level
   */
  log(message, level = 'info') {
    if (this.options.logger) {
      this.options.logger(`[WorkerPool] ${message}`, level);
    }
  }
}