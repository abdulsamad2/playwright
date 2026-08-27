/**
 * Inventory reconciliation — the execution half.
 *
 * Takes the plan produced by inventoryPlan.js and applies it. Everything runs in
 * the caller's transaction, and everything that touches ConsecutiveGroup goes in
 * a single bulkWrite.
 *
 * ── Why one bulkWrite ──────────────────────────────────────────────────────────
 *
 * The previous shape issued, per event and per cycle: a find to collect ids, a
 * deleteMany, an external HTTP delete, a second find, a second deleteMany, a
 * second HTTP delete, then insertMany in chunks of 100 or 500. That is up to eight
 * round trips for one event, most of them serialised, all inside an open
 * transaction. Multiply by ~2,000 events on a two-minute cycle and the cost is not
 * the writes — it is the latency of holding transactions open while waiting.
 *
 * bulkWrite sends deletes, inserts and updates as one ordered:false command. One
 * round trip, server-side parallelism, and the transaction closes sooner. The
 * external HTTP calls disappear entirely: the marketplace is no longer the
 * scraper's business.
 *
 * ── Ordering ───────────────────────────────────────────────────────────────────
 *
 * ordered:false lets the server apply operations in any order, which matters
 * because of the unique index on
 * (eventId, mapping_id, section, row, seatRange, seatCount, inventory.quantity).
 * A seats-changed row produces a delete and an insert whose keys differ — the
 * seatRange is what changed — so they cannot collide. Rows whose keys would
 * collide are the unchanged ones, and those generate no operations at all.
 */

import { ConsecutiveGroup } from "../models/seatModel.js";
import { buildGroupDocument, buildPatchFields } from "./inventoryPlan.js";
import { recordTombstones } from "./syncOutbox.js";

/**
 * Apply a reconciliation plan.
 *
 * @param {object} plan     from planInventoryChanges
 * @param {object} ctx      { eventId, mapping_id, event_name, venue_name, event_date }
 * @param {object} options  { session, source }
 * @returns {{deleted:number, created:number, patched:number, tombstoned:number}}
 */
export async function applyInventoryPlan(plan, ctx, { session, source = "ticketmaster" } = {}) {
  const { creates, patches, deletes } = plan;

  if (creates.length === 0 && patches.length === 0 && deletes.length === 0) {
    return { deleted: 0, created: 0, patched: 0, tombstoned: 0 };
  }

  // Tombstones first, and inside the same transaction as the delete they describe.
  //
  // This ordering is the whole guarantee. Once the document is gone so is its
  // stubhubListingId, and a row that no longer exists cannot ask to be unlisted.
  // Writing the tombstone first — and atomically — means a crash between the two
  // cannot leave a listing stranded on StubHub with nothing pointing at it.
  //
  // It is also why a failed scrape can never mass-delete: removals originate from
  // this collection, not from the absence of rows in a query result, so an empty
  // or failed cycle produces an empty tombstone set and therefore no deletions.
  let tombstoned = 0;
  if (deletes.length > 0) {
    const byReason = new Map();
    for (const d of deletes) {
      if (!byReason.has(d.reason)) byReason.set(d.reason, []);
      byReason.get(d.reason).push({
        mapping_id: d.mapping_id ?? ctx.mapping_id,
        section: d.section,
        row: d.row,
        inventory: { inventoryId: d.inventoryId, stubhubListingId: d.stubhubListingId },
      });
    }
    for (const [reason, groups] of byReason) {
      tombstoned += await recordTombstones(groups, { reason, source, session });
    }
  }

  const ops = [];

  if (deletes.length > 0) {
    ops.push({ deleteMany: { filter: { _id: { $in: deletes.map((d) => d._id) } } } });
  }

  for (const { data } of creates) {
    const doc = buildGroupDocument(data.group, ctx, data.price);
    // New rows enter the outbox as pending: nothing about them has reached the
    // marketplace yet, so the portal has a create to do rather than an update.
    doc.inventory.syncState = "pending";
    doc.inventory.syncPendingSince = new Date();
    doc.inventory.syncAttempts = 0;
    ops.push({ insertOne: { document: doc } });
  }

  for (const { _id, data, now } of patches) {
    ops.push({
      updateOne: {
        filter: { _id },
        update: { $set: buildPatchFields(data.group, ctx, data.price, now) },
      },
    });
  }

  // ordered:false so one duplicate-key rejection cannot abandon the rest of the
  // batch. Duplicates are expected under concurrent processing of the same event
  // and have always been tolerated here.
  const result = await ConsecutiveGroup.bulkWrite(ops, { ordered: false, session });

  return {
    deleted: result.deletedCount ?? 0,
    created: result.insertedCount ?? 0,
    patched: result.modifiedCount ?? 0,
    tombstoned,
  };
}

/**
 * Fields the planner needs from existing rows.
 *
 * Kept narrow on purpose — this query runs once per event per cycle, and pulling
 * whole documents (which carry a tickets[] array per row) would dominate the
 * cycle's memory and wire time for data the plan never reads.
 */
export const PLAN_PROJECTION = {
  _id: 1,
  section: 1,
  row: 1,
  seats: 1,
  seatCount: 1,
  mapping_id: 1,
  "inventory.listPrice": 1,
  "inventory.quantity": 1,
  "inventory.inventoryId": 1,
  "inventory.stubhubListingId": 1,
  "inventory.customSplit": 1,
  "inventory.splitType": 1,
};
