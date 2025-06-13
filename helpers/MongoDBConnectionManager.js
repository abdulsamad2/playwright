/**
 * MongoDB Connection Manager
 * 
 * Provides robust MongoDB connection handling with:
 * - Connection pooling
 * - Automatic reconnection
 * - Connection monitoring
 * - Performance optimization
 */

import mongoose from 'mongoose';
import { EventEmitter } from 'events';
import genericPool from 'generic-pool';

class MongoDBConnectionManager extends EventEmitter {
  constructor(options = {}) {
    super();
    
    this.options = {
      uri: process.env.DATABASE_URL || 'mongodb://localhost:27017/ticketScraper',
      poolSize: options.poolSize || 10,
      connectTimeoutMS: options.connectTimeoutMS || 30000,
      socketTimeoutMS: options.socketTimeoutMS || 45000,
      maxIdleTimeMS: options.maxIdleTimeMS || 60000,
      heartbeatFrequencyMS: options.heartbeatFrequencyMS || 10000,
      retryWrites: options.retryWrites !== undefined ? options.retryWrites : true,
      retryReads: options.retryReads !== undefined ? options.retryReads : true,
      maxPoolSize: options.maxPoolSize || 100,
      minPoolSize: options.minPoolSize || 5,
      maxConnecting: options.maxConnecting || 10,
      maxRetryAttempts: options.maxRetryAttempts || 5,
      retryInterval: options.retryInterval || 5000,
      ...options
    };
    
    this.isConnected = false;
    this.connectionAttempts = 0;
    this.connectionPool = null;
    this.healthCheckInterval = null;
    this.connectionPromise = null;
    
    // Bind methods
    this.connect = this.connect.bind(this);
    this.disconnect = this.disconnect.bind(this);
    this.getConnection = this.getConnection.bind(this);
    this.releaseConnection = this.releaseConnection.bind(this);
    this.healthCheck = this.healthCheck.bind(this);
    this.setupConnectionPool = this.setupConnectionPool.bind(this);
  }
  
  /**
   * Connect to MongoDB with retry logic
   */
  async connect() {
    if (this.connectionPromise) {
      return this.connectionPromise;
    }
    
    this.connectionPromise = new Promise(async (resolve, reject) => {
      while (this.connectionAttempts < this.options.maxRetryAttempts) {
        try {
          this.connectionAttempts++;
          
          console.log(`Connecting to MongoDB (Attempt ${this.connectionAttempts}/${this.options.maxRetryAttempts})...`);
          
          // Configure mongoose connection
          mongoose.set('strictQuery', false);
          
          await mongoose.connect(this.options.uri, {
            connectTimeoutMS: this.options.connectTimeoutMS,
            socketTimeoutMS: this.options.socketTimeoutMS,
            maxPoolSize: this.options.maxPoolSize,
            minPoolSize: this.options.minPoolSize,
            maxConnecting: this.options.maxConnecting,
            heartbeatFrequencyMS: this.options.heartbeatFrequencyMS,
            retryWrites: this.options.retryWrites,
            retryReads: this.options.retryReads
          });
          
          this.isConnected = true;
          this.connectionAttempts = 0;
          
          // Setup connection pool
          this.setupConnectionPool();
          
          // Start health check
          this.startHealthCheck();
          
          console.log('✅ Connected to MongoDB successfully');
          this.emit('connected');
          
          resolve();
          return;
        } catch (error) {
          console.error(`MongoDB connection error (Attempt ${this.connectionAttempts}/${this.options.maxRetryAttempts}):`, error.message);
          
          if (this.connectionAttempts >= this.options.maxRetryAttempts) {
            this.emit('error', error);
            reject(error);
            return;
          }
          
          console.log(`Retrying in ${this.options.retryInterval / 1000} seconds...`);
          await new Promise(r => setTimeout(r, this.options.retryInterval));
        }
      }
    });
    
    return this.connectionPromise;
  }
  
  /**
   * Setup connection pooling
   */
  setupConnectionPool() {
    // Create a pool factory
    const factory = {
      create: async () => {
        const conn = mongoose.connection.db;
        return conn;
      },
      destroy: async (client) => {
        // Connection will be automatically managed by mongoose
        return;
      },
    };
    
    // Create pool
    this.connectionPool = genericPool.createPool(factory, {
      max: this.options.poolSize,
      min: 2,
      testOnBorrow: true,
      acquireTimeoutMillis: 10000,
      idleTimeoutMillis: this.options.maxIdleTimeMS,
      evictionRunIntervalMillis: 30000,
    });
    
    console.log(`MongoDB connection pool created (size: ${this.options.poolSize})`);
  }
  
  /**
   * Get a connection from the pool
   */
  async getConnection() {
    if (!this.isConnected) {
      await this.connect();
    }
    
    if (this.connectionPool) {
      return this.connectionPool.acquire();
    }
    
    return mongoose.connection.db;
  }
  
  /**
   * Release a connection back to the pool
   */
  async releaseConnection(connection) {
    if (this.connectionPool && connection) {
      return this.connectionPool.release(connection);
    }
  }
  
  /**
   * Disconnect from MongoDB
   */
  async disconnect() {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = null;
    }
    
    if (this.connectionPool) {
      await this.connectionPool.drain();
      await this.connectionPool.clear();
      this.connectionPool = null;
    }
    
    if (this.isConnected) {
      await mongoose.disconnect();
      this.isConnected = false;
      this.connectionPromise = null;
      console.log('Disconnected from MongoDB');
      this.emit('disconnected');
    }
  }
  
  /**
   * Start periodic health check
   */
  startHealthCheck() {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
    }
    
    this.healthCheckInterval = setInterval(() => {
      this.healthCheck();
    }, this.options.heartbeatFrequencyMS);
    
    console.log(`MongoDB health check started (interval: ${this.options.heartbeatFrequencyMS / 1000}s)`);
  }
  
  /**
   * Check MongoDB connection health
   */
  async healthCheck() {
    try {
      if (!this.isConnected) {
        throw new Error('Not connected to MongoDB');
      }
      
      const adminDb = mongoose.connection.db.admin();
      const result = await adminDb.ping();
      
      if (result && result.ok === 1) {
        this.emit('healthCheck', { status: 'ok' });
      } else {
        throw new Error('MongoDB ping failed');
      }
    } catch (error) {
      console.error('MongoDB health check failed:', error.message);
      this.emit('healthCheck', { status: 'error', error });
      
      // Try to reconnect
      this.isConnected = false;
      this.connectionPromise = null;
      this.connect().catch(err => {
        console.error('Failed to reconnect to MongoDB:', err.message);
      });
    }
  }
  
  /**
   * Get MongoDB connection status
   */
  getStatus() {
    return {
      isConnected: this.isConnected,
      connectionAttempts: this.connectionAttempts,
      poolStats: this.connectionPool ? this.connectionPool.stats() : null,
      uri: this.options.uri.replace(/\/\/([^:]+):([^@]+)@/, '//***:***@') // Hide credentials
    };
  }
}

// Create singleton instance
let instance = null;

export const getMongoDBConnectionManager = (options = {}) => {
  if (!instance) {
    instance = new MongoDBConnectionManager(options);
  }
  return instance;
};

export default MongoDBConnectionManager;