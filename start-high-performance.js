/**
 * High-Performance Scraper Startup Script
 * 
 * This script initializes and runs the scraper in high-performance mode,
 * utilizing the PerformanceOptimizer to achieve the target of processing
 * 5000 events in 3 minutes (approximately 28 events per second).
 */

import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import PerformanceOptimizer from './helpers/PerformanceOptimizer.js';
import MongoDBConnectionManager from './helpers/MongoDBConnectionManager.js';
import mongoose from 'mongoose';
import os from 'os';

// Load environment variables
dotenv.config();

// Get directory name
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration
const config = {
  // Performance settings
  workerCount: parseInt(process.env.WORKER_COUNT, 10) || Math.max(1, os.cpus().length - 1),
  maxConcurrentEvents: parseInt(process.env.MAX_CONCURRENT_EVENTS, 10) || 200,
  maxConcurrentPerWorker: parseInt(process.env.MAX_CONCURRENT_PER_WORKER, 10) || 50,
  batchSize: parseInt(process.env.BATCH_SIZE, 10) || 50,
  
  // Database settings
  mongoUri: process.env.MONGODB_URI || 'mongodb://localhost:27017/scraper',
  dbPoolSize: parseInt(process.env.DB_POOL_SIZE, 10) || 20,
  
  // HTTP settings
  httpPoolSize: parseInt(process.env.HTTP_POOL_SIZE, 10) || 100,
  httpTimeout: parseInt(process.env.HTTP_TIMEOUT, 10) || 30000,
  
  // Target throughput
  targetEventsPerSecond: parseInt(process.env.TARGET_EVENTS_PER_SECOND, 10) || 28, // 5000 events in 3 minutes
  
  // Logging
  logLevel: process.env.LOG_LEVEL || 'info',
  
  // Mode
  dryRun: process.env.DRY_RUN === 'true',
  
  // Custom event query
  eventQuery: process.env.EVENT_QUERY ? JSON.parse(process.env.EVENT_QUERY) : null,
  
  // Event limit
  eventLimit: parseInt(process.env.EVENT_LIMIT, 10) || 5000
};

/**
 * Main function to start the high-performance scraper
 */
async function main() {
  console.log('🚀 Starting High-Performance Scraper');
  console.log('📊 Configuration:');
  console.log(`   Worker Count: ${config.workerCount}`);
  console.log(`   Max Concurrent Events: ${config.maxConcurrentEvents}`);
  console.log(`   Max Concurrent Per Worker: ${config.maxConcurrentPerWorker}`);
  console.log(`   Batch Size: ${config.batchSize}`);
  console.log(`   Target Throughput: ${config.targetEventsPerSecond} events/second`);
  console.log(`   Event Limit: ${config.eventLimit}`);
  console.log(`   Dry Run: ${config.dryRun ? 'Yes' : 'No'}`);
  
  try {
    // Initialize MongoDB connection manager
    const dbManager = new MongoDBConnectionManager({
      uri: config.mongoUri,
      poolSize: config.dbPoolSize
    });
    
    await dbManager.connect();
    console.log('✅ Connected to MongoDB');
    
    // Initialize performance optimizer
    const optimizer = new PerformanceOptimizer({
      workerCount: config.workerCount,
      maxConcurrentEvents: config.maxConcurrentEvents,
      maxConcurrentPerWorker: config.maxConcurrentPerWorker,
      batchSize: config.batchSize,
      dbPoolSize: config.dbPoolSize,
      httpPoolSize: config.httpPoolSize,
      httpTimeout: config.httpTimeout,
      targetEventsPerSecond: config.targetEventsPerSecond
    });
    
    await optimizer.initialize();
    console.log('✅ Performance Optimizer initialized');
    
    // Load models
    await loadModels();
    console.log('✅ Models loaded');
    
    // If dry run, just test the setup and exit
    if (config.dryRun) {
      console.log('🧪 Dry run completed successfully');
      await optimizer.shutdown();
      await dbManager.disconnect();
      process.exit(0);
    }
    
    // Start processing events
    await processEvents(optimizer);
    
    // Shutdown
    await optimizer.shutdown();
    await dbManager.disconnect();
    
    console.log('✅ High-Performance Scraper completed successfully');
    process.exit(0);
  } catch (error) {
    console.error('❌ Error starting High-Performance Scraper:', error);
    process.exit(1);
  }
}

/**
 * Load mongoose models
 */
async function loadModels() {
  try {
    // Import models dynamically
    const Event = (await import('./models/Event.js')).default;
    const ErrorLog = (await import('./models/ErrorLog.js')).default;
    const Session = (await import('./models/Session.js')).default;
    
    console.log('✅ Models imported successfully');
    return { Event, ErrorLog, Session };
  } catch (error) {
    console.error('❌ Error loading models:', error);
    throw error;
  }
}

/**
 * Process events using the performance optimizer
 */
async function processEvents(optimizer) {
  console.log('🔄 Starting event processing');
  
  try {
    // Import Event model
    const { Event } = await import('./models/Event.js');
    
    // Build query for events
    const query = buildEventQuery();
    console.log('📊 Event query:', JSON.stringify(query));
    
    // Count total events matching query
    const totalEvents = await Event.countDocuments(query);
    console.log(`📊 Found ${totalEvents} events matching query`);
    
    // Limit the number of events if specified
    const limit = config.eventLimit > 0 ? Math.min(config.eventLimit, totalEvents) : totalEvents;
    console.log(`📊 Processing ${limit} events`);
    
    // Fetch events in batches to avoid memory issues
    const batchSize = 500;
    let processedCount = 0;
    
    while (processedCount < limit) {
      const batchLimit = Math.min(batchSize, limit - processedCount);
      
      const events = await Event.find(query)
        .sort({ priority: -1, updatedAt: 1 })
        .skip(processedCount)
        .limit(batchLimit)
        .lean();
      
      if (events.length === 0) {
        console.log('⚠️ No more events to process');
        break;
      }
      
      console.log(`📊 Fetched batch of ${events.length} events`);
      
      // Process events in parallel using the optimizer
      for (const event of events) {
        await optimizer.processEvent(event);
      }
      
      processedCount += events.length;
      console.log(`📊 Progress: ${processedCount}/${limit} events (${Math.round(processedCount / limit * 100)}%)`);
    }
    
    console.log(`✅ Processed ${processedCount} events`);
    
    // Wait for any remaining events to be processed
    console.log('⏳ Waiting for remaining events to complete processing...');
    await new Promise(resolve => setTimeout(resolve, 10000));
    
    return processedCount;
  } catch (error) {
    console.error('❌ Error processing events:', error);
    throw error;
  }
}

/**
 * Build query for events to process
 */
function buildEventQuery() {
  // Start with base query for active events
  const baseQuery = {
    status: { $in: ['EVENT_ACTIVE', 'EVENT_PENDING'] },
    isDeleted: { $ne: true }
  };
  
  // If custom query is provided, use it
  if (config.eventQuery) {
    return { ...baseQuery, ...config.eventQuery };
  }
  
  return baseQuery;
}

/**
 * Handle process termination
 */
function handleTermination() {
  console.log('🛑 Received termination signal, shutting down...');
  process.exit(0);
}

// Handle termination signals
process.on('SIGINT', handleTermination);
process.on('SIGTERM', handleTermination);

// Start the application
main().catch(error => {
  console.error('❌ Fatal error:', error);
  process.exit(1);
});