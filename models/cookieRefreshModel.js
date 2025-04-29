import mongoose from "mongoose";

const cookieRefreshSchema = new mongoose.Schema(
  {
    // Unique identifier for the refresh operation
    refreshId: {
      type: String,
      required: true,
      unique: true,
    },
    // Status of the refresh operation
    status: {
      type: String,
      enum: ["success", "failed", "in_progress"],
      required: true,
    },
    // Event ID used for the refresh, if any
    eventId: {
      type: String,
      required: false,
    },
    // Timestamp when the refresh operation started
    startTime: {
      type: Date,
      required: true,
      default: Date.now,
    },
    // Timestamp when the refresh operation completed
    completionTime: {
      type: Date,
      required: false,
    },
    // Timestamp when the next refresh is scheduled
    nextScheduledRefresh: {
      type: Date,
      required: false,
    },
    // Number of cookies retrieved in this refresh
    cookieCount: {
      type: Number,
      required: false,
      default: 0,
    },
    // Number of retries performed for this refresh
    retryCount: {
      type: Number,
      required: true,
      default: 0,
    },
    // Error message if the refresh failed
    errorMessage: {
      type: String,
      required: false,
    },
    // Duration of the refresh operation in milliseconds
    duration: {
      type: Number,
      required: false,
    },
    // Proxy used for this refresh
    proxy: {
      type: String,
      required: false,
    },
    // Additional metadata
    metadata: {
      type: mongoose.Schema.Types.Mixed,
      required: false,
    },
  },
  {
    timestamps: true, // Adds createdAt and updatedAt fields
  }
);

// Create indexes for common queries
cookieRefreshSchema.index({ status: 1 });
cookieRefreshSchema.index({ startTime: -1 });
cookieRefreshSchema.index({ nextScheduledRefresh: 1 });

export const CookieRefresh = mongoose.model("CookieRefresh", cookieRefreshSchema); 