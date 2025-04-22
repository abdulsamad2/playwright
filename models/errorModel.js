import mongoose from "mongoose";

const errorLogSchema = new mongoose.Schema(
  {
    eventUrl: {
      type: String,
      required: true,
      index: true,
    },
    errorType: {
      type: String,
      required: true,
      enum: [
        "SCRAPE_ERROR",
        "PARSE_ERROR",
        "DATABASE_ERROR",
        "VALIDATION_ERROR",
        "LONG_COOLDOWN",
      ],
    },
    message: {
      type: String,
      required: true,
    },
    stack: String,
    metadata: {
      iteration: Number,
      timestamp: {
        type: Date,
        default: Date.now,
      },
      additionalInfo: mongoose.Schema.Types.Mixed,
    },
  },
  {
    timestamps: true,
  }
);

// Indexes
errorLogSchema.index({ eventUrl: 1, createdAt: -1 });

export const ErrorLog = mongoose.model("ErrorLog", errorLogSchema);
