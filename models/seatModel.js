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
    inHandDate: {
      type: Date,
      required: true,
    },
    eventId: {
      type: String,
      required: true,
    },
    mapping_id: {
      type: String,
      required: true,
    },
    event_name: {
      type: String,
    },
    venue_name: {
      type: String,
    },
    event_date: {
      type: Date,
    },
    section: {
      type: String,
      required: true,
    },
    row: {
      type: String,
      required: true,
    },
    // Position of this row inside TM's SECTION.segments array: 0 is the row
    // closest to the field, 1 the next one back, and so on. Positional rather
    // than parsed from the label, so it holds for numeric, A/B/C and AA/A/B
    // schemes alike. Null for GA and anything TM gives us with no row ordering.
    rowRank: {
      type: Number,
      default: null,
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
      rowRank: {
        type: Number,
        default: null,
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
      face_price: {
        type: Number,
      },
      taxed_cost: {
        type: Number,
      },
      in_hand: {
        type: Boolean,
      },
      instant_transfer: {
        type: Boolean,
      },
      files_available: {
        type: Boolean,
      },
      zone: {
        type: String,
      },
      shown_quantity: {
        type: String,
      },
      passthrough: {
        type: String,
      },
      event_name: {
        type: String,
      },
      venue_name: {
        type: String,
      },
      event_date: {
        type: Date,
      },
      eventId: {
        type: String,
      },
      mapping_id: {
        type: String,
      },
      tickets: [ticketSchema],
    },
  },
  {
    timestamps: true,
  }
);

// Add compound unique index to prevent duplicate seat groups
// Includes listPrice so GA rows at different price tiers don't collide
// (same section/row/seatRange/quantity but different prices)
consecutiveGroupSchema.index(
  {
    eventId: 1,
    mapping_id: 1,
    section: 1,
    row: 1,
    seatRange: 1,
    seatCount: 1,
    "inventory.quantity": 1,
    "inventory.listPrice": 1
  },
  {
    unique: true,
    name: "unique_seat_group_v2"
  }
);

export const ConsecutiveGroup = mongoose.model(
  "ConsecutiveGroup",
  consecutiveGroupSchema
);
