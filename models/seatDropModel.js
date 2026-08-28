import mongoose from "mongoose";

const TTL_DAYS = parseInt(process.env.SEAT_DROP_TTL_DAYS, 10) || 7;

/**
 * SeatDrop — a record of NEW seats appearing on an event.
 *
 * One document per (event, section, row) drop occurrence. Written only when
 * seat numbers show up that were not present on the previous scrape, so a
 * price change or a quantity reshuffle on existing seats never lands here.
 *
 * The alerting layer consumes this collection; nothing in the scraper reads it
 * back except the concurrency guard (the unique index below).
 */
const seatDropSchema = new mongoose.Schema(
  {
    eventId: { type: String, required: true, index: true },
    mapping_id: { type: String },

    // Denormalized so an alert can be rendered without joining Event
    event_name: { type: String },
    venue_name: { type: String },
    event_date: { type: Date },
    event_url: { type: String },

    section: { type: String, required: true },
    row: { type: String, required: true },

    // The seat numbers that appeared on this scrape
    newSeats: [{ type: String }],
    newSeatCount: { type: Number, required: true },

    // Row state AFTER the drop
    totalSeatsInRow: { type: Number },

    // Price of the listing carrying the new seats (marked-up list price)
    listPrice: { type: Number },

    // true when the section/row had zero inventory before this scrape
    isNewListing: { type: Boolean, default: false },

    detectedAt: { type: Date, required: true, default: Date.now },

    // ── Lifecycle ─────────────────────────────────────────────────────────
    // A drop is "active" while its seats are still on sale, and "gone" once
    // they disappear again. Confirmation takes DROP_GONE_CONFIRM_CYCLES
    // consecutive absent scrapes, because TM regularly returns partial data.
    status: {
      type: String,
      enum: ["active", "gone"],
      default: "active",
      index: true,
    },

    // Seats from this drop still on sale as of the last scrape
    seatsRemaining: [{ type: String }],

    lastSeenAt: { type: Date },

    // Scrape cycles the seats were observed still present (starts at 1)
    cyclesSeen: { type: Number, default: 1 },

    // Consecutive scrapes the seats have been absent (resets on reappearance)
    missCount: { type: Number, default: 0 },

    // When the seats first went missing — becomes goneAt once confirmed
    firstMissAt: { type: Date, default: null },

    goneAt: { type: Date, default: null },

    // detectedAt → goneAt, in seconds. How long the drop lasted.
    secondsAlive: { type: Number, default: null },

    // Flipped by the alerting layer once the drop has been surfaced
    seen: { type: Boolean, default: false, index: true },

    // Which PM2 instance observed it (150+ instances run concurrently)
    instanceId: { type: String },

    // Identity of this drop independent of when it happened:
    // "eventId|section|row|seat,seat". Stable across repeats.
    dropBase: { type: String, required: true },

    // dropBase + a generation counter. Concurrency guard — see unique index.
    dropKey: { type: String, required: true },
  },
  {
    timestamps: true,
    collection: "seat_drops",
  }
);

// Alerting reads: newest drops for one event, and newest unseen drops overall
seatDropSchema.index({ eventId: 1, detectedAt: -1 });
// Lifecycle pass on every scrape: "which drops for this event are still active?"
seatDropSchema.index({ eventId: 1, status: 1 });
seatDropSchema.index({ seen: 1, detectedAt: -1 });

// Auto-expire old drops so the collection stays bounded
seatDropSchema.index({ detectedAt: 1 }, { expireAfterSeconds: TTL_DAYS * 24 * 60 * 60 });

// Counting prior generations of the same drop (see dropKey construction)
seatDropSchema.index({ eventId: 1, dropBase: 1 });

// Two instances racing the same scrape compute the same generation and collide
// here, so exactly one record is written. A genuine re-drop of the same seats
// later computes the next generation and is recorded as its own drop.
seatDropSchema.index({ dropKey: 1 }, { unique: true, name: "unique_seat_drop" });

export const SeatDrop = mongoose.model("SeatDrop", seatDropSchema);
