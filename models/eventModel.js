import mongoose from "mongoose";

const eventSchema = new mongoose.Schema(
  {
    mapping_id: {
      type: String,
      required: true,
      unique: true,
    },
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
    // Which scraper owns this event. Defaults to "ticketmaster" so every row
    // that existed before tickets.com was added keeps its current behaviour.
    //
    // This field is LOad-BEARING, not decorative: the TM queue builder in
    // helpers/RedisLiveStore.js filters on it, and the tickets.com manager
    // claims only its own source. Without it, one scraper would pick up the
    // other's events and fail every scrape.
    source: {
      type: String,
      enum: ["ticketmaster", "ticketscom"],
      default: "ticketmaster",
      index: true,
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
    inHandDate: {
      type: Date,
      default: Date.now,
    },
    priceIncreasePercentage: {
      type: Number,
      default: 35, // Default 25% markup
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
      inHandDate: Date,
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

export const Event = mongoose.model("Event", eventSchema);
