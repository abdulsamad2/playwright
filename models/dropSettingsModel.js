import mongoose from "mongoose";

/**
 * DropSettings — the one tunable the operator changes without a deploy.
 *
 * A single document, pinned to a fixed key so there can only ever be one. Both
 * the scraper (which deletes a drop once it matures) and the portal (which
 * withholds its listing from the CSV until then) read the same value, so they
 * cannot disagree about when a drop becomes ordinary inventory.
 */
const dropSettingsSchema = new mongoose.Schema(
  {
    key: { type: String, required: true, unique: true, default: "singleton" },

    /** Minutes a drop is held out of the CSV after it is first detected. */
    holdMinutes: { type: Number, required: true, default: 45, min: 1, max: 1440 },

    /** Who last changed it, for the audit trail the page shows. */
    updatedBy: { type: String },
  },
  { timestamps: true, collection: "drop_settings" }
);

export const DropSettings =
  mongoose.models.DropSettings || mongoose.model("DropSettings", dropSettingsSchema);
