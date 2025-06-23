import mongoose from "mongoose";
import dotenv from "dotenv";

dotenv.config();

const mongoUri = process.env.MONGODB_URI || process.env.DATABASE_URL;

if (!mongoUri) {
  throw new Error("MongoDB URI is not defined in environment variables");
}

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
    
  } catch (error) {
    console.error("Database connection error:", error);
    process.exit(1);
  }
};

// Graceful shutdown
const closeConnections = async () => {
  try {
    await mongoose.connection.close();
    console.log('MongoDB connection closed');
  } catch (error) {
    console.error('Error closing database connections:', error);
  }
};

export default connectDB;
export { closeConnections };
