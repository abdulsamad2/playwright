import mongoose from "mongoose";

/**
 * Tombstones — the record that a listing needs removing from the marketplace.
 *
 * Under the CSV this collection has no equivalent and needs none: a row simply
 * stops appearing in the next export and Automatiq infers the removal. An API
 * infers nothing. It removes what you name, by id, and nothing else.
 *
 * That creates a gap, because the scrapers hard-delete the Mongo document. The
 * instant the document is gone so is its stubhubListingId, and a row that no
 * longer exists cannot ask to be unlisted. So whoever removes inventory has to
 * record the removal here first, in the same transaction as the delete.
 *
 * Kept byte-identical to the portal's copy in models/inventoryTombstoneModel.js:
 * both write the same collection, so the schemas have to agree. Change one and
 * change the other.
 *
 * This is also the primary safety mechanism, and the reason it is a separate
 * collection rather than a flag. Deletes originate here and nowhere else — there
 * is no code path from "my query returned no rows" to "unlist everything". A
 * failed query, an expired token or a bad deploy therefore produces zero deletes,
 * not because a guard caught it but because the delete input is a different
 * collection that simply stays empty. The blank-CSV wipe has no analogue here.
 */
const inventoryTombstoneSchema = new mongoose.Schema(
  {
    /** Our scraper id — the externalId StubHub knows this listing by. */
    inventoryId: {
      type: Number,
      required: true,
      index: true,
    },

    /**
     * StubHub's own listing id, captured at create time. Null means the listing
     * was never successfully created, so there is nothing to remove — those
     * tombstones resolve immediately without a call.
     */
    stubhubListingId: {
      type: String,
    },

    mapping_id: { type: String },
    section: { type: String },
    row: { type: String },

    /**
     * Why the row went away. Drives the delete-vs-delist decision: scraper churn
     * is expected to flap and is cheaper to delist reversibly, while an expired
     * event is genuinely final.
     */
    reason: {
      type: String,
      enum: [
        "scraper-removed",     // row disappeared from a scrape cycle
        "seats-changed",       // different seats = a genuinely different listing
        "quantity-changed",    // count changed; StubHub cannot patch it, so recreate
        "event-deleted",       // event removed from the portal
        "event-expired",       // auto-delete cron, past or expired event
        "low-seat-auto-stop",  // below the configured minimum
        "manual",              // removed from the dashboard
      ],
      required: true,
    },

    /** Which system recorded it, for attribution when something goes wrong. */
    source: {
      type: String,
      enum: ["ticketmaster", "ticketscom", "evenue", "portal"],
      required: true,
    },

    syncState: {
      type: String,
      enum: ["pending", "deleting", "done", "failed"],
      default: "pending",
    },

    syncBatchId: { type: String },
    syncLeaseUntil: { type: Date },
    syncAttempts: { type: Number, default: 0 },
    syncError: { type: String },

    /**
     * Set once StubHub has confirmed the removal. Retained rather than deleted so
     * the audit sweep can tell "we removed this deliberately" apart from "this
     * vanished and we don't know why".
     */
    processedAt: { type: Date },
  },
  { timestamps: true }
);

/**
 * Outbox index. Partial on the *presence* of the lease-free pending marker would
 * be ideal, but syncState is a small closed set, so an equality predicate — which
 * partialFilterExpression does support — is enough to keep this to open work only.
 */
inventoryTombstoneSchema.index(
  { createdAt: 1 },
  {
    name: "tombstone_outbox",
    partialFilterExpression: { syncState: "pending" },
  }
);

/**
 * Processed tombstones are audit evidence, not queue entries. Ninety days is long
 * enough to explain any drift the sweep turns up and short enough that the
 * collection does not grow without bound.
 */
inventoryTombstoneSchema.index(
  { processedAt: 1 },
  { name: "tombstone_ttl", expireAfterSeconds: 60 * 60 * 24 * 90 }
);

export const InventoryTombstone =
  mongoose.models.InventoryTombstone ||
  mongoose.model("InventoryTombstone", inventoryTombstoneSchema);
