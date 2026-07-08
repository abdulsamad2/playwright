import mongoose from "mongoose";

const seatValidationSchema = new mongoose.Schema(
  {
    eventId: {
      type: String,
      required: true,
      unique: true,
      index: true,
    },
    seatCount: {
      type: Number,
      required: true,
    },
    lastUpdated: {
      type: Date,
      required: true,
      default: Date.now,
      index: true,
    },
    validationCount: {
      type: Number,
      default: 1,
    },
    fluctuationCount: {
      type: Number,
      default: 0,
    },
    lastFluctuation: {
      type: Date,
      default: null,
    },
    delayedUntil: {
      type: Date,
      default: null,
      index: true,
    },
    previousCount: {
      type: Number,
      default: null,
    },
    instanceId: {
      type: String,
      default: null,
    },
    trendTracking: [{
      count: Number,
      timestamp: Date,
      previousCount: Number,
      instanceId: String
    }],
  },
  {
    timestamps: true,
    collection: "seat_validations",
  }
);

// Static method to clean up old records
seatValidationSchema.statics.cleanupOld = async function(maxAge = 24 * 60 * 60 * 1000) {
  const cutoffDate = new Date(Date.now() - maxAge);
  const result = await this.deleteMany({ lastUpdated: { $lt: cutoffDate } });
  return result.deletedCount;
};

// Static method to get delayed events
seatValidationSchema.statics.getDelayedEvents = async function() {
  const now = new Date();
  return await this.find({
    delayedUntil: { $gt: now }
  }).select('eventId delayedUntil seatCount previousCount');
};

// Static method to clear expired delays
seatValidationSchema.statics.clearExpiredDelays = async function() {
  const now = new Date();
  const result = await this.updateMany(
    { delayedUntil: { $lte: now, $ne: null } },
    { $set: { delayedUntil: null } }
  );
  return result.modifiedCount;
};

export const SeatValidation = mongoose.model("SeatValidation", seatValidationSchema);
