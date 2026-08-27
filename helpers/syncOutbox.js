/**
 * Sync outbox — how the scraper tells the portal what the marketplace needs to know.
 *
 * Until now the scraper has talked to the marketplace directly: any change to a
 * row meant deleting it from SeatScouts by inventoryId and letting the portal's
 * next CSV put it back. That was not a design choice so much as a workaround, and
 * the reason is recorded in the vendored client itself:
 *
 *     "SeatScouts has no update endpoint: changing a listing means deleting it
 *      by inventoryId and re-inserting."
 *
 * StubHub's Point of Sale API does have an update endpoint, so the workaround can
 * go. Two things change:
 *
 *   1. The scraper stops calling any marketplace. It records *intent* — this row
 *      is dirty, this row is gone — and the portal's sync worker is the only
 *      process that talks to StubHub. That matters because the POS API has no
 *      optimistic concurrency of any kind (no If-Match, no ETag, no 412 anywhere
 *      in its 160 operations), so two writers racing on a listing lose silently.
 *      Single-writer is a correctness requirement, not a preference.
 *
 *   2. A price change becomes an update rather than a destroy-and-rebuild, so the
 *      listing keeps its StubHub id, its age and its history, and never leaves the
 *      market in between.
 *
 * Everything here is written inside the caller's existing Mongo transaction. That
 * is what makes this a real outbox rather than a second system to keep in step:
 * the intent to sync commits atomically with the data it describes, so there is no
 * window in which a row changed but nothing recorded that it needs pushing.
 *
 * Behaviour is gated on INVENTORY_SYNC_PROVIDER and defaults to 'csv', which is
 * byte-for-byte what the scraper does today. Nothing below runs until someone
 * deliberately sets it to 'stubhub'.
 */

import { InventoryTombstone } from "../models/inventoryTombstoneModel.js";

/** Which system owns marketplace writes. env > default. */
export function syncProvider() {
  return (process.env.INVENTORY_SYNC_PROVIDER || "csv").toLowerCase();
}

/**
 * True when the portal's sync worker owns marketplace writes and the scraper must
 * not make external calls of its own.
 */
export function isStubhubMode() {
  return syncProvider() === "stubhub";
}

/**
 * Fields marking a row as needing a push.
 *
 * Deliberately does NOT touch stubhubListingId, syncHash or syncedAt. Those record
 * what StubHub last accepted, and the worker compares the row's freshly mapped
 * payload against syncHash to decide whether anything actually needs sending. A
 * scrape that rewrites a row to the same values leaves the hash matching and
 * therefore costs no API call at all.
 *
 * syncPendingSince is the queue marker: present while work is outstanding, unset
 * once synced. The portal's outbox index is partial on its presence, so the index
 * stays proportional to outstanding work rather than to the size of the book.
 */
export function dirtyFields(now = new Date()) {
  return {
    "inventory.syncState": "dirty",
    "inventory.syncPendingSince": now,
  };
}

/** Same, for a row being created for the first time. */
export function pendingFields(now = new Date()) {
  return {
    syncState: "pending",
    syncPendingSince: now,
    syncAttempts: 0,
  };
}

/**
 * Record that listings need removing from the marketplace.
 *
 * This is the piece with no CSV analogue, and the reason it has to exist. Under a
 * CSV the row simply stops appearing in the next file and Automatiq infers the
 * removal. An API infers nothing — it removes what you name, by id. But the
 * scraper hard-deletes the document, and the instant that document is gone so is
 * its stubhubListingId, so a row that no longer exists cannot ask to be unlisted.
 * The deleter has to record the deletion before performing it.
 *
 * Writing these in the caller's transaction is what makes the guarantee hold: the
 * tombstone and the delete commit together or not at all. A crash between them
 * cannot leave a listing orphaned on StubHub with nothing pointing at it.
 *
 * It is also the main safety property of the whole migration. Deletes originate
 * here and nowhere else, so there is no code path from "my query returned no rows"
 * to "unlist everything". A failed query or an expired token produces zero
 * deletes — not because a guard caught it, but because the delete input is a
 * different collection that simply stays empty.
 *
 * @param {Array} groups   documents about to be deleted, carrying inventory.*
 * @param {object} opts    { reason, source, session }
 * @returns {number}       tombstones written
 */
export async function recordTombstones(groups, { reason, source = "ticketmaster", session } = {}) {
  if (!isStubhubMode()) return 0;
  if (!Array.isArray(groups) || groups.length === 0) return 0;

  const docs = groups
    .filter((g) => g?.inventory?.inventoryId)
    .map((g) => ({
      inventoryId: g.inventory.inventoryId,
      // Null is meaningful: the listing was never created, so there is nothing
      // to remove and the worker resolves the tombstone without an API call.
      stubhubListingId: g.inventory.stubhubListingId ?? null,
      mapping_id: g.mapping_id ?? g.inventory.mapping_id ?? null,
      section: g.section ?? null,
      row: g.row ?? null,
      reason,
      source,
      syncState: "pending",
      syncAttempts: 0,
    }));

  if (docs.length === 0) return 0;

  await InventoryTombstone.insertMany(docs, { session, ordered: false });
  return docs.length;
}

/**
 * Projection needed to build a tombstone. The delete paths already load documents
 * to collect inventoryIds for the SeatScouts call; this widens that read by two
 * fields rather than adding a second query.
 */
export const TOMBSTONE_PROJECTION = {
  "inventory.inventoryId": 1,
  "inventory.stubhubListingId": 1,
  mapping_id: 1,
  section: 1,
  row: 1,
};
