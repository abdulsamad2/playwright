import mongoose from "mongoose";
import dotenv from "dotenv";

dotenv.config();

const mongoUri =
  process.env.DATABASE_URL ;

const connectDB = async () => {
  try {
    await mongoose.connect(mongoUri, {
      // Increase connection timeout from default 10s to 30s
      serverSelectionTimeoutMS: 30000,
      // Add socket timeout to prevent hanging connections
      socketTimeoutMS: 45000,
      // Disable buffering to get immediate errors instead of timeouts
      bufferCommands: false,
      bufferMaxEntries: 0,
      // Connection pool settings for better reliability
      maxPoolSize: 10,
      minPoolSize: 2,
      // Heartbeat frequency
      heartbeatFrequencyMS: 10000
    });
    console.log("Connected to MongoDB with enhanced timeout settings");
  } catch (error) {
    console.error("MongoDB connection error:", error);
    process.exit(1);
  }
};

export default connectDB;
