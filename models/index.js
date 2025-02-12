import mongoose from "mongoose";

// Event Schema
const eventSchema = new mongoose.Schema(
  {
    title: {
      type: String,
      required: true,
    },
    dateTime: {
      type: Date,
      required: true,
    },
    availableSeats: {
      type: Number,
      required: true,
    },
    url: {
      type: String,
      required: true,
      unique: true,
    },
    lastUpdated: {
      type: Date,
      default: Date.now,
    },
    metadata: {
      lastUpdate: String,
      iterationNumber: Number,
      scrapeStartTime: Date,
      scrapeEndTime: Date,
      scrapeDurationSeconds: Number,
      totalRunningTimeMinutes: String,
      ticketStats: {
        totalTickets: Number,
        ticketCountChange: Number,
        previousTicketCount: Number,
      },
    },
  },
  {
    timestamps: true,
  }
);

// Individual Seat Schema (as a subdocument)
const seatSchema = new mongoose.Schema({
  number: {
    type: String,
    required: true,
  },
  price: {
    type: Number,
    required: true,
  },
});

// Consecutive Group Schema
const consecutiveGroupSchema = new mongoose.Schema(
  {
    eventId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Event",
      required: true,
    },
    section: {
      type: String,
      required: true,
    },
    row: {
      type: String,
      required: true,
    },
    seatCount: {
      type: Number,
      required: true,
    },
    seatRange: {
      type: String,
      required: true,
    },
    seats: [seatSchema],
  },
  {
    timestamps: true,
  }
);

// Error Log Schema
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
// In models/index.js
export const SeatGroup = mongoose.model("ConsecutiveGroup", consecutiveGroupSchema);eventSchema.index({ url: 1 }, { unique: true });
errorLogSchema.index({ eventUrl: 1, createdAt: -1 });

// Models
export const Event = mongoose.model("Event", eventSchema);
export const ConsecutiveGroup = mongoose.model(
  "ConsecutiveGroup",
  consecutiveGroupSchema
);
export const ErrorLog = mongoose.model("ErrorLog", errorLogSchema);
