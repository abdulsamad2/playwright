import mongoose from "mongoose";

// Seat Schema (as a subdocument)
const seatSchema = new mongoose.Schema({
  Seat_Number: {
    type: String,
    required: true,
  },
  Price: {
    type: Number,
    required: true,
  },
  Face_Value: {
    type: Number,
    required: true,
  },
  Unit_Taxed_Cost: {
    type: Number,
    required: true,
  },
  Sell_Price: {
    type: Number,
    required: true,
  },
  Stock_Type: {
    type: String,
    enum: ["MOBILE_TRANSFER", "HARD", "E-TICKET"],
    required: true,
  },
  Barcode: {
    type: String,
    required: true,
    unique: true,
  },
  Status: {
    type: String,
    enum: ["AVAILABLE", "SOLD", "PENDING"],
    required: true,
  },
});

// Event Schema
const eventSchema = new mongoose.Schema(
  {
    Event_ID: {
      type: String,
      required: true,
      unique: true,
      index: true,
    },
    Event_Name: {
      type: String,
      required: true,
    },
    Venue: {
      type: String,
      required: true,
    },
    Event_DateTime: {
      type: Date,
      required: true,
    },
    Available_Seats: {
      type: Number,
      required: true,
    },
    URL: {
      type: String,
      required: true,
      unique: true,
    },
    Instant_Download: {
      type: Boolean,
      default: false,
    },
    Instant_Transfer: {
      type: Boolean,
      default: false,
    },
    E_Ticket: {
      type: Boolean,
      default: false,
    },
    Zone: {
      type: String,
      required: true,
    },
    Last_Updated: {
      type: Date,
      default: Date.now,
    },
    Skip_Scraping: {
      type: Boolean,
      default: false, // By default, scraping is not skipped
    },
    Metadata: {
      Last_Update: String,
      Iteration_Number: Number,
      Scrape_Start_Time: Date,
      Scrape_End_Time: Date,
      Scrape_Duration_Seconds: Number,
      Total_Running_Time_Minutes: String,
      Ticket_Stats: {
        Total_Tickets: Number,
        Ticket_Count_Change: Number,
        Previous_Ticket_Count: Number,
      },
    },
  },
  {
    timestamps: true,
  }
);

// Consecutive Group Schema
const consecutiveGroupSchema = new mongoose.Schema(
  {
    Event_ID: {
      type: String,
      required: true,
      index: true,
      ref: "Event",
    },
    Section: {
      type: String,
      required: true,
    },
    Row: {
      type: String,
      required: true,
    },
    Seat_Count: {
      type: Number,
      required: true,
    },
    Seat_Range: {
      type: String,
      required: true,
    },
    Seats: [seatSchema],
    Unit_Taxed_Cost: {
      type: Number,
      required: true,
    },
  },
  {
    timestamps: true,
  }
);

// Error Log Schema
const errorLogSchema = new mongoose.Schema(
  {
    Event_URL: {
      type: String,
      required: true,
      index: true,
    },
    Error_Type: {
      type: String,
      required: true,
      enum: [
        "SCRAPE_ERROR",
        "PARSE_ERROR",
        "DATABASE_ERROR",
        "VALIDATION_ERROR",
      ],
    },
    Message: {
      type: String,
      required: true,
    },
    Stack: String,
    Metadata: {
      Iteration: Number,
      Timestamp: {
        type: Date,
        default: Date.now,
      },
      Additional_Info: mongoose.Schema.Types.Mixed,
    },
  },
  {
    timestamps: true,
  }
);

// Indexes for Optimization
eventSchema.index({ URL: 1 }, { unique: true });
eventSchema.index({ Event_ID: 1 }, { unique: true });
errorLogSchema.index({ Event_URL: 1, createdAt: -1 });

consecutiveGroupSchema.index(
  { Event_ID: 1, Section: 1, Row: 1, Seat_Range: 1 },
  { unique: true }
);

// Models
export const Event = mongoose.model("Event", eventSchema);
export const ConsecutiveGroup = mongoose.model(
  "ConsecutiveGroup",
  consecutiveGroupSchema
);
export const ErrorLog = mongoose.model("ErrorLog", errorLogSchema);
