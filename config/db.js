import mongoose from "mongoose";
import { createClient } from "redis";
import dotenv from "dotenv";

dotenv.config();

// MongoDB configuration
const mongoUri = process.env.MONGODB_URI || 'mongodb://admin:password123@localhost:27017/syncdb?authSource=admin';

// Redis configuration
const redisUri = process.env.REDIS_URI || 'redis://:redis123@localhost:6380';
let redisClient;

const connectDB = async () => {
  try {
    // Connect to MongoDB
    await mongoose.connect(mongoUri, {
      // Increase connection timeout from default 10s to 30s
      serverSelectionTimeoutMS: 30000,
      // Add socket timeout to prevent hanging connections
      socketTimeoutMS: 45000,
      // Connection pool settings for better reliability
      maxPoolSize: 10,
      minPoolSize: 2,
      // Heartbeat frequency
      heartbeatFrequencyMS: 10000
    });
    console.log("Connected to MongoDB with enhanced timeout settings");
    
    // Connect to Redis
    redisClient = createClient({ url: redisUri });
    
    redisClient.on('error', (err) => {
      console.error('Redis Client Error:', err);
    });
    
    redisClient.on('connect', () => {
      console.log('Connected to Redis');
    });
    
    await redisClient.connect();
    console.log("Redis connection established");
    
  } catch (error) {
    console.error("Database connection error:", error);
    process.exit(1);
  }
};

// Redis client getter
const getRedisClient = () => {
  if (!redisClient) {
    throw new Error('Redis client not initialized. Call connectDB() first.');
  }
  return redisClient;
};

// Graceful shutdown
const closeConnections = async () => {
  try {
    if (redisClient) {
      await redisClient.quit();
      console.log('Redis connection closed');
    }
    await mongoose.connection.close();
    console.log('MongoDB connection closed');
  } catch (error) {
    console.error('Error closing database connections:', error);
  }
};

export default connectDB;
export { getRedisClient, closeConnections };
