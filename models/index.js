import mongoose from "mongoose";

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

// Ticket Schema (as a subdocument)
const ticketSchema = new mongoose.Schema({
  id: {
    type: Number,
    required: true,
  },
  seatNumber: {
    type: Number,
    required: true,
  },
  notes: {
    type: String,
  },
  cost: {
    type: Number,
    required: true,
  },
  faceValue: {
    type: Number,
    required: true,
  },
  taxedCost: {
    type: Number,
    required: true,
  },
  sellPrice: {
    type: Number,
    required: true,
  },
  stockType: {
    type: String,
    required: true,
  },
  eventId: {
    type: Number,
    required: true,
  },
  accountId: {
    type: Number,
    required: true,
  },
  status: {
    type: String,
    required: true,
  },
  auditNote: {
    type: String,
  },
});

// Consecutive Group Schema
const consecutiveGroupSchema = new mongoose.Schema(
  {
    eventId: {
      type: String,
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
    inventory: {
      quantity: {
        type: Number,
        required: true,
      },
      section: {
        type: String,
        required: true,
      },
      hideSeatNumbers: {
        type: Boolean,
        required: true,
      },
      row: {
        type: String,
        required: true,
      },
      cost: {
        type: Number,
        required: true,
      },
      stockType: {
        type: String,
        required: true,
      },
      lineType: {
        type: String,
        required: true,
      },
      seatType: {
        type: String,
        required: true,
      },
      inHandDate: {
        type: Date,
        required: true,
      },
      notes: {
        type: String,
      },
      tags: {
        type: String,
      },
      inventoryId: {
        type: Number,
        required: true,
      },
      offerId: {
        type: String,
        required: true,
      },
      splitType: {
        type: String,
        required: true,
      },
      publicNotes: {
        type: String,
      },
      listPrice: {
        type: Number,
        required: true,
      },
      customSplit: {
        type: String,
      },
      tickets: [ticketSchema],
    },
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

// Event Schema
const eventSchema = new mongoose.Schema(
  {
    Event_ID: {
      type: String,
      required: true,
      unique: true,
    },
    Event_Name: {
      type: String,
      required: true,
    },
    Event_DateTime: {
      type: Date,
      required: true,
    },
    Venue: String,
    URL: {
      type: String,
      required: true,
   
    },
    Zone: {
      type: String,
      default: "none",
    },
    Available_Seats: {
      type: Number,
      default: 0,
    },
    Skip_Scraping: {
      type: Boolean,
      default: true,
    },

    inHandDate:{
       type: Date,
      default: Date.now,
    },
    Last_Updated: {
      type: Date,
      default: Date.now,
    },
    metadata: {
      lastUpdate: String,
      iterationNumber: Number,
      scrapeStartTime: Date,
      scrapeEndTime: Date,
      inHandDate:Date,
      scrapeDurationSeconds: Number,
      totalRunningTimeMinutes: Number,
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

// Indexes
eventSchema.index({ URL: 1 }, { unique: true });
errorLogSchema.index({ eventUrl: 1, createdAt: -1 });

// Models
export const Event = mongoose.model("Event", eventSchema);
export const ConsecutiveGroup = mongoose.model(
  "ConsecutiveGroup",
  consecutiveGroupSchema
);
export const ErrorLog = mongoose.model("ErrorLog", errorLogSchema);
