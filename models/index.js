import mongoose from "mongoose";

const eventSchema = new mongoose.Schema({
  title: {
    type: String,
    required: true,
  },
  dateTime: {
    type: Date,
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
});

const seatGroupSchema = new mongoose.Schema(
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
    startSeat: {
      type: Number,
      default: 1,
    },
    endSeat: {
      type: Number,
      required: true,
    },
    quantity: {
      type: Number,
      required: true,
    },
    price: {
      type: Number,
      required: true,
    },
    ticketType: {
      type: String,
      required: true,
    },
    rowList: {
      type: String,
      required: true,
    },
  },
  {
    timestamps: true,
  }
);
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

export const ErrorLog = mongoose.model("ErrorLog", errorLogSchema);

// Indexes for better query performance
seatGroupSchema.index({ eventId: 1, section: 1, row: 1 });
eventSchema.index({ url: 1 }, { unique: true });

export const Event = mongoose.model("Event", eventSchema);
export const SeatGroup = mongoose.model("SeatGroup", seatGroupSchema);
