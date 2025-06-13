/**
 * Performance Optimizer for Scraper
 * 
 * This module provides performance optimizations for the scraper system to handle
 * 5000 events in 3 minutes (approximately 28 events per second).
 * 
 * Key optimizations:
 * 1. MongoDB connection pooling and optimization
 * 2. Worker threads for CPU-intensive tasks
 * 3. Batch processing for network requests
 * 4. Memory management and garbage collection optimization
 * 5. Proxy rotation and connection pooling
 * 6. Dynamic concurrency adjustment
 */

import { Worker } from 'worker_threads';
import os from 'os';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import MongoDBConnectionManager from './MongoDBConnectionManager.js';
import genericPool from 'generic-pool';
import { EventEmitter } from 'events';
import { setTimeout } from 'timers/promises';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

class PerformanceOptimizer extends EventEmitter {
  constructor(options = {}) {
    super();
    
    this.options = {
      // Worker thread settings
      workerCount: options.workerCount || Math.max(1, os.cpus().length - 1),
      workerScript: options.workerScript || path.join(__dirname, 'worker-thread.js'),
      
      // Concurrency settings
      maxConcurrentEvents: options.maxConcurrentEvents || 200,
      maxConcurrentPerWorker: options.maxConcurrentPerWorker || 50,
      
      // Batch processing settings
      batchSize: options.batchSize || 50,
      batchTimeoutMs: options.batchTimeoutMs || 1000,
      
      // Memory management
      memoryLimit: options.memoryLimit || 0.8, // 80% of available memory
      gcInterval: options.gcInterval || 60000, // 1 minute
      
      // Database settings
      dbPoolSize: options.dbPoolSize || 20,
      dbConnectionTimeout: options.dbConnectionTimeout || 30000,
      
      // HTTP settings
      httpPoolSize: options.httpPoolSize || 100,
      httpTimeout: options.httpTimeout || 30000,
      
      // Target throughput
      targetEventsPerSecond: options.targetEventsPerSecond || 28, // 5000 events in 3 minutes
      
      ...options
    };
    
    // Initialize components
    this.workers = [];
    this.dbManager = null;
    this.httpPool = null;
    this.proxyPool = null;
    this.stats = {
      startTime: null,
      eventsProcessed: 0,
      successCount: 0,
      failureCount: 0,
      currentThroughput: 0,
      peakThroughput: 0,
      avgResponseTime: 0,
      totalResponseTime: 0,
      memoryUsage: {}
    };
    
    // Performance monitoring
    this.monitoringInterval = null;
    this.lastProcessedCount = 0;
    this.lastMonitoringTime = null;
    
    // Batch processing
    this.eventBatch = [];
    this.batchTimer = null;
    
    // Bind methods
    this.initialize = this.initialize.bind(this);
    this.startWorkers = this.startWorkers.bind(this);
    this.processEvent = this.processEvent.bind(this);
    this.processBatch = this.processBatch.bind(this);
    this.startMonitoring = this.startMonitoring.bind(this);
    this.optimizeMemory = this.optimizeMemory.bind(this);
    this.shutdown = this.shutdown.bind(this);
  }
  
  /**
   * Initialize the performance optimizer
   */
  async initialize() {
    console.log('🚀 Initializing Performance Optimizer');
    console.log(`📊 Target throughput: ${this.options.targetEventsPerSecond} events/second`);
    console.log(`🧵 Worker threads: ${this.options.workerCount}`);
    console.log(`🔄 Max concurrency: ${this.options.maxConcurrentEvents}`);
    
    try {
      // Initialize MongoDB connection manager
      this.dbManager = new MongoDBConnectionManager({
        poolSize: this.options.dbPoolSize,
        connectTimeoutMS: this.options.dbConnectionTimeout
      });
      
      await this.dbManager.connect();
      
      // Initialize HTTP connection pool
      this.initializeHttpPool();
      
      // Initialize proxy pool if available
      this.initializeProxyPool();
      
      // Start worker threads
      await this.startWorkers();
      
      // Start performance monitoring
      this.startMonitoring();
      
      // Start memory optimization
      this.optimizeMemory();
      
      this.stats.startTime = Date.now();
      this.lastMonitoringTime = Date.now();
      
      console.log('✅ Performance Optimizer initialized successfully');
      this.emit('initialized');
      
      return true;
    } catch (error) {
      console.error('❌ Failed to initialize Performance Optimizer:', error.message);
      this.emit('error', error);
      return false;
    }
  }
  
  /**
   * Initialize HTTP connection pool
   */
  initializeHttpPool() {
    const factory = {
      create: async () => {
        // Create an axios instance or other HTTP client
        return { id: Math.random().toString(36).substring(2, 15) };
      },
      destroy: async (client) => {
        // Clean up resources
        return;
      },
    };
    
    this.httpPool = genericPool.createPool(factory, {
      max: this.options.httpPoolSize,
      min: 5,
      testOnBorrow: true,
      acquireTimeoutMillis: 10000,
      idleTimeoutMillis: 30000,
      evictionRunIntervalMillis: 30000,
    });
    
    console.log(`🌐 HTTP connection pool created (size: ${this.options.httpPoolSize})`);
  }
  
  /**
   * Initialize proxy pool
   */
  initializeProxyPool() {
    try {
      // Check if ProxyManager is available
      const ProxyManager = require('./ProxyManager.js').default;
      if (ProxyManager) {
        this.proxyManager = new ProxyManager();
        console.log('🔄 Proxy pool initialized');
      }
    } catch (error) {
      console.log('⚠️ ProxyManager not available, skipping proxy pool initialization');
    }
  }
  
  /**
   * Start worker threads
   */
  async startWorkers() {
    console.log(`🧵 Starting ${this.options.workerCount} worker threads...`);
    
    const workerPromises = [];
    
    for (let i = 0; i < this.options.workerCount; i++) {
      workerPromises.push(this.startWorker(i));
    }
    
    await Promise.all(workerPromises);
    console.log(`✅ All worker threads started`);
  }
  
  /**
   * Start a single worker thread
   */
  async startWorker(id) {
    return new Promise((resolve, reject) => {
      try {
        const worker = new Worker(this.options.workerScript, {
          workerData: {
            id,
            maxConcurrent: this.options.maxConcurrentPerWorker,
            options: this.options
          }
        });
        
        worker.on('message', (message) => {
          if (message.type === 'ready') {
            console.log(`🧵 Worker ${id} ready`);
            this.workers.push(worker);
            resolve(worker);
          } else if (message.type === 'result') {
            this.handleWorkerResult(message.data);
          } else if (message.type === 'error') {
            console.error(`❌ Worker ${id} error:`, message.error);
          } else if (message.type === 'stats') {
            this.updateWorkerStats(id, message.data);
          }
        });
        
        worker.on('error', (error) => {
          console.error(`❌ Worker ${id} error:`, error);
          this.emit('worker_error', { id, error });
          reject(error);
        });
        
        worker.on('exit', (code) => {
          if (code !== 0) {
            console.error(`❌ Worker ${id} exited with code ${code}`);
            this.emit('worker_exit', { id, code });
            
            // Restart worker after a delay
            setTimeout(5000).then(() => {
              console.log(`🔄 Restarting worker ${id}...`);
              this.startWorker(id).catch(console.error);
            });
          }
        });
      } catch (error) {
        console.error(`❌ Failed to start worker ${id}:`, error.message);
        reject(error);
      }
    });
  }
  
  /**
   * Process a single event
   */
  async processEvent(event) {
    // Add to batch for processing
    this.eventBatch.push(event);
    
    // Process batch if it reaches the batch size
    if (this.eventBatch.length >= this.options.batchSize) {
      clearTimeout(this.batchTimer);
      await this.processBatch();
    } else if (!this.batchTimer) {
      // Start a timer to process the batch if it doesn't fill up
      this.batchTimer = setTimeout(() => {
        this.processBatch().catch(console.error);
      }, this.options.batchTimeoutMs);
    }
    
    return true;
  }
  
  /**
   * Process a batch of events
   */
  async processBatch() {
    if (this.eventBatch.length === 0) return;
    
    const batch = [...this.eventBatch];
    this.eventBatch = [];
    clearTimeout(this.batchTimer);
    this.batchTimer = null;
    
    console.log(`🔄 Processing batch of ${batch.length} events`);
    
    try {
      // Distribute events among workers
      const workerCount = this.workers.length;
      const eventsPerWorker = Math.ceil(batch.length / workerCount);
      
      const workerPromises = [];
      
      for (let i = 0; i < workerCount; i++) {
        const workerBatch = batch.slice(i * eventsPerWorker, (i + 1) * eventsPerWorker);
        if (workerBatch.length > 0) {
          workerPromises.push(this.sendToWorker(i, workerBatch));
        }
      }
      
      await Promise.all(workerPromises);
      
      // Update stats
      this.stats.eventsProcessed += batch.length;
      
      return true;
    } catch (error) {
      console.error('❌ Error processing batch:', error.message);
      this.emit('batch_error', { error, batchSize: batch.length });
      return false;
    }
  }
  
  /**
   * Send events to a worker
   */
  async sendToWorker(workerId, events) {
    return new Promise((resolve, reject) => {
      const worker = this.workers[workerId % this.workers.length];
      
      if (!worker) {
        reject(new Error(`Worker ${workerId} not found`));
        return;
      }
      
      const messageId = Date.now().toString() + Math.random().toString(36).substring(2, 15);
      
      const timeout = setTimeout(() => {
        reject(new Error(`Worker ${workerId} timed out processing events`));
      }, this.options.httpTimeout);
      
      const messageHandler = (message) => {
        if (message.type === 'batch_complete' && message.messageId === messageId) {
          clearTimeout(timeout);
          worker.removeListener('message', messageHandler);
          resolve(message.results);
        }
      };
      
      worker.on('message', messageHandler);
      
      worker.postMessage({
        type: 'process_batch',
        messageId,
        events
      });
    });
  }
  
  /**
   * Handle worker result
   */
  handleWorkerResult(result) {
    if (result.success) {
      this.stats.successCount++;
    } else {
      this.stats.failureCount++;
    }
    
    this.stats.totalResponseTime += result.responseTime;
    this.stats.avgResponseTime = this.stats.totalResponseTime / this.stats.eventsProcessed;
    
    this.emit('event_processed', result);
  }
  
  /**
   * Update worker stats
   */
  updateWorkerStats(workerId, stats) {
    // Update worker-specific stats
    if (!this.stats.workers) {
      this.stats.workers = {};
    }
    
    this.stats.workers[workerId] = stats;
  }
  
  /**
   * Start performance monitoring
   */
  startMonitoring() {
    this.monitoringInterval = setInterval(() => {
      const now = Date.now();
      const elapsedSec = (now - this.lastMonitoringTime) / 1000;
      const newEvents = this.stats.eventsProcessed - this.lastProcessedCount;
      
      // Calculate current throughput
      this.stats.currentThroughput = newEvents / elapsedSec;
      
      // Update peak throughput
      if (this.stats.currentThroughput > this.stats.peakThroughput) {
        this.stats.peakThroughput = this.stats.currentThroughput;
      }
      
      // Update memory usage
      const memoryUsage = process.memoryUsage();
      this.stats.memoryUsage = {
        rss: memoryUsage.rss / 1024 / 1024, // MB
        heapTotal: memoryUsage.heapTotal / 1024 / 1024, // MB
        heapUsed: memoryUsage.heapUsed / 1024 / 1024, // MB
        external: memoryUsage.external / 1024 / 1024, // MB
        arrayBuffers: memoryUsage.arrayBuffers / 1024 / 1024 // MB
      };
      
      // Calculate runtime
      const runtime = (now - this.stats.startTime) / 1000; // seconds
      
      // Log performance stats
      console.log('📊 Performance Stats:');
      console.log(`   Runtime: ${Math.floor(runtime / 60)}m ${Math.floor(runtime % 60)}s`);
      console.log(`   Events Processed: ${this.stats.eventsProcessed}`);
      console.log(`   Success: ${this.stats.successCount}, Failed: ${this.stats.failureCount}`);
      console.log(`   Current Throughput: ${this.stats.currentThroughput.toFixed(2)} events/sec`);
      console.log(`   Peak Throughput: ${this.stats.peakThroughput.toFixed(2)} events/sec`);
      console.log(`   Avg Response Time: ${this.stats.avgResponseTime.toFixed(2)}ms`);
      console.log(`   Memory Usage: ${this.stats.memoryUsage.heapUsed.toFixed(2)}MB / ${this.stats.memoryUsage.heapTotal.toFixed(2)}MB`);
      
      // Emit monitoring event
      this.emit('monitoring', { ...this.stats, runtime });
      
      // Update for next interval
      this.lastProcessedCount = this.stats.eventsProcessed;
      this.lastMonitoringTime = now;
      
      // Dynamic concurrency adjustment based on performance
      this.adjustConcurrency();
    }, 10000); // Report every 10 seconds
  }
  
  /**
   * Dynamically adjust concurrency based on performance
   */
  adjustConcurrency() {
    // If we're below target throughput and not memory constrained, increase concurrency
    if (this.stats.currentThroughput < this.options.targetEventsPerSecond * 0.9 && 
        this.stats.memoryUsage.heapUsed / this.stats.memoryUsage.heapTotal < 0.8) {
      
      const newConcurrency = Math.min(
        this.options.maxConcurrentEvents * 1.1, // Increase by 10%
        this.options.maxConcurrentEvents + 20 // Or add 20, whichever is smaller
      );
      
      if (newConcurrency > this.options.maxConcurrentEvents) {
        this.options.maxConcurrentEvents = Math.floor(newConcurrency);
        console.log(`🔄 Increasing concurrency to ${this.options.maxConcurrentEvents}`);
        
        // Notify workers of new concurrency
        this.workers.forEach(worker => {
          worker.postMessage({
            type: 'update_concurrency',
            maxConcurrent: Math.ceil(this.options.maxConcurrentEvents / this.workers.length)
          });
        });
      }
    }
    // If we're memory constrained or error rate is high, decrease concurrency
    else if (this.stats.memoryUsage.heapUsed / this.stats.memoryUsage.heapTotal > 0.9 || 
             this.stats.failureCount / (this.stats.successCount + this.stats.failureCount) > 0.2) {
      
      const newConcurrency = Math.max(
        this.options.maxConcurrentEvents * 0.9, // Decrease by 10%
        50 // Don't go below 50
      );
      
      if (newConcurrency < this.options.maxConcurrentEvents) {
        this.options.maxConcurrentEvents = Math.floor(newConcurrency);
        console.log(`🔄 Decreasing concurrency to ${this.options.maxConcurrentEvents} due to resource constraints`);
        
        // Notify workers of new concurrency
        this.workers.forEach(worker => {
          worker.postMessage({
            type: 'update_concurrency',
            maxConcurrent: Math.ceil(this.options.maxConcurrentEvents / this.workers.length)
          });
        });
      }
    }
  }
  
  /**
   * Optimize memory usage
   */
  optimizeMemory() {
    // Schedule periodic garbage collection
    if (global.gc) {
      setInterval(() => {
        const beforeMemory = process.memoryUsage().heapUsed / 1024 / 1024;
        global.gc();
        const afterMemory = process.memoryUsage().heapUsed / 1024 / 1024;
        console.log(`🧹 Garbage collection: ${beforeMemory.toFixed(2)}MB → ${afterMemory.toFixed(2)}MB (freed ${(beforeMemory - afterMemory).toFixed(2)}MB)`);
      }, this.options.gcInterval);
    } else {
      console.log('⚠️ Explicit garbage collection not available. Run with --expose-gc flag for better memory management.');
    }
  }
  
  /**
   * Shutdown the performance optimizer
   */
  async shutdown() {
    console.log('🛑 Shutting down Performance Optimizer...');
    
    // Clear intervals
    clearInterval(this.monitoringInterval);
    clearTimeout(this.batchTimer);
    
    // Process any remaining events in batch
    if (this.eventBatch.length > 0) {
      await this.processBatch().catch(console.error);
    }
    
    // Terminate workers
    const workerPromises = this.workers.map(worker => {
      return new Promise((resolve) => {
        worker.on('exit', () => resolve());
        worker.terminate();
      });
    });
    
    await Promise.all(workerPromises);
    
    // Close database connection
    if (this.dbManager) {
      await this.dbManager.disconnect();
    }
    
    // Drain connection pools
    if (this.httpPool) {
      await this.httpPool.drain();
      await this.httpPool.clear();
    }
    
    console.log('✅ Performance Optimizer shut down successfully');
    this.emit('shutdown');
  }
  
  /**
   * Get current performance statistics
   */
  getStats() {
    const now = Date.now();
    const runtime = (now - this.stats.startTime) / 1000; // seconds
    
    return {
      ...this.stats,
      runtime,
      runtimeFormatted: `${Math.floor(runtime / 3600)}h ${Math.floor((runtime % 3600) / 60)}m ${Math.floor(runtime % 60)}s`,
      avgThroughput: this.stats.eventsProcessed / runtime,
      projectedCompletion: this.stats.currentThroughput > 0 ? 
        (5000 - this.stats.eventsProcessed) / this.stats.currentThroughput : 
        null
    };
  }
}

export default PerformanceOptimizer;