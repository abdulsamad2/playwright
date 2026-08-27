/**
 * Sync outbox — how the scraper hands inventory changes to the portal.
 *
 * The scraper's job is now exactly one thing: keep the database describing what
 * is actually for sale. It does not call any marketplace, does not know which
 * marketplace exists, and does not decide whether a change means an update, a
 * delist or a delete. It records what happened; the portal decides what that
 * means and is the only process that talks to StubHub.
 *
 * Two reasons it has to be this way rather than merely tidier:
 *
 *   The POS API has no optimistic concurrency anywhere in its 160 operations —
 *   no If-Match, no ETag, no 412. Two writers racing on the same listing lose
 *   silently, with no error and no way to detect it afterwards. Single-writer is
 *   a correctness requirement.
 *
 *   Only the portal knows the final price. Markup is applied in two stages and
 *   the second one — the event's standard, resale and broker adjustments — lives
 *   in the exporter. A scraper that pushed prices would be pushing the wrong
 *   number the moment anyone set an adjustment in the dashboard.
 *
 * Everything here is written inside the caller's existing Mongo transaction. That
 * is what makes this an outbox rather than a second system to keep in step: the
 * intent to sync commits atomically with the data it describes, so there is no
 * window in which a row changed but nothing recorded that it needs pushing.
 *
 * Historical note, because it explains the shape of what was here before: the
 * scraper used to delete listings from SeatScouts itself, and rebuild a row from
 * scratch on any change. That was forced, not chosen — the vendored client says
 * so in its own header: "SeatScouts has no update endpoint: changing a listing
 * means deleting it by inventoryId and re-inserting." StubHub has PATCH, and the
 * CSV path is retired, so both the external call and the rebuild are gone.
 */

import { InventoryTombstone } from "../models/inventoryTombstoneModel.js";

/**
 * Fields marking a row as needing a push.
 *
 * Deliberately does NOT touch stubhubListingId, syncHash or syncedAt. Those record
 * what StubHub last accepted, and the portal compares the row's freshly mapped
 * payload against syncHash to decide whether anything actually needs sending. A
 * scrape that rewrites a row to identical values leaves the hash matching and
 * therefore costs no API call at all — which is what makes an aggressive scrape
 * cycle affordable against a per-row API.
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
 * This is the piece with no CSV analogue, and the reason it has to exist at all.
 * Under a CSV a row simply stopped appearing in the next file and the receiver
 * inferred the removal. An API infers nothing — it removes what you name, by id.
 * But the row is deleted here, and the instant the document is gone so is its
 * stubhubListingId, so a row that no longer exists cannot ask to be unlisted. The
 * deleter has to record the removal before performing it.
 *
 * Writing these in the caller's transaction is what makes the guarantee hold: the
 * tombstone and the delete commit together or not at all. A crash between them
 * cannot leave a listing stranded on StubHub with nothing pointing at it.
 *
 * It is also the main safety property of the migration. Removals originate here
 * and nowhere else, so there is no code path from "my query returned no rows" to
 * "unlist everything". A failed scrape or a bad deploy produces an empty tombstone
 * set and therefore zero deletions — not because a guard caught it, but because
 * the delete input is a different collection that simply stays empty.
 *
 * Note that the scraper says only *that* a row is gone and why. Whether that
 * becomes a delist, a delete, or nothing at all — because the row reappears
 * inside the grace window — is the portal's call.
 *
 * @param {Array} groups   documents about to be deleted, carrying inventory.*
 * @param {object} opts    { reason, source, session }
 * @returns {number}       tombstones written
 */
export async function recordTombstones(groups, { reason, source = "ticketmaster", session } = {}) {
  if (!Array.isArray(groups) || groups.length === 0) return 0;

  const docs = groups
    .filter((g) => g?.inventory?.inventoryId)
    .map((g) => ({
      inventoryId: g.inventory.inventoryId,
      // Null is meaningful: the listing was never created, so there is nothing
      // to remove and the portal resolves the tombstone without an API call.
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
