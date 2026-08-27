/**
 * Inventory reconciliation — the planning half.
 *
 * Pure. No Mongo, no network, no clock of its own, no mutation of its inputs.
 * Given what the database holds and what the scrape produced, it returns a plan
 * describing what should change. Nothing here performs a write, which means the
 * hardest logic in the scraper — deciding what a change *is* — can be tested
 * against real fixtures without a database or a browser.
 *
 * This used to live inline in scraperManager.js, interleaved with the writes it
 * implied and duplicated between the update and insert paths. Splitting the
 * decision from the execution is what lets the execution become a single
 * bulkWrite (see inventoryPersist.js) instead of a sequence of round trips per
 * event, which is the difference that matters once the book is large.
 *
 * ── Why change classification finally has more than one answer ──────────────────
 *
 * Every change used to take one path: destroy the row, rebuild it, and delete the
 * listing downstream on the way past. That was not a preference. The vendored
 * SeatScouts client explains it in its own header:
 *
 *     "SeatScouts has no update endpoint: changing a listing means deleting it
 *      by inventoryId and re-inserting."
 *
 * StubHub's POS API has PATCH and the CSV path is retired, so the two kinds of
 * change can finally be told apart:
 *
 *   seats changed   a different set of physical tickets — genuinely a different
 *                   block, so the old row is tombstoned and a new one created.
 *
 *   price, quantity the same tickets on different terms. Update in place and keep
 *   or split changed the inventoryId.
 *
 * Note what this module does NOT decide. It says a row changed, or a row is gone,
 * and why. Whether "gone" becomes a delist, a delete, or nothing at all because
 * the row reappears inside the grace window is the portal's call — the scraper
 * has no opinion about marketplaces and no way to act on one.
 */

import moment from "moment";

let inventoryIdCounter = 0;

/**
 * Ten-digit local id, unique across processes and restarts.
 *
 * Moved here from scraperManager because it belongs with row construction. It is
 * also, as of the POS migration, load-bearing in a way it was not before: this
 * value becomes StubHub's `externalId`, the key that survives every change and
 * ties a listing back to a row. A collision would merge two listings.
 */
export function generateUniqueInventoryId() {
  const timestamp = Date.now();
  const processId = process.pid % 1000;
  const random = Math.floor(Math.random() * 1000);

  inventoryIdCounter = (inventoryIdCounter + 1) % 1000;

  const fullUniqueString =
    `${timestamp}` +
    `${processId.toString().padStart(3, "0")}` +
    `${inventoryIdCounter.toString().padStart(3, "0")}` +
    `${random.toString().padStart(3, "0")}`;

  const hash = fullUniqueString.split("").reduce((acc, char, index) => {
    return ((acc << 5) - acc + char.charCodeAt(0) + index) & 0x7fffffff;
  }, 0);

  return (hash % 9000000000) + 1000000000;
}

/** Seat numbers as sorted strings, so two orderings compare equal. */
export function normaliseSeats(seats) {
  if (!Array.isArray(seats)) return [];
  return seats
    .map((s) => (s && typeof s === "object" && "number" in s ? s.number : s))
    .map((n) => String(n))
    .sort();
}

/**
 * Identity of a physical block of seats: section, row, and the outer seat numbers.
 *
 * Deliberately excludes price and quantity, so a re-priced block matches itself
 * across cycles instead of looking like a removal plus an arrival. That property
 * is what makes an in-place update possible at all.
 */
export function rowKeyFor(section, row, seats) {
  const range = seats.length ? `${seats[0]}-${seats[seats.length - 1]}` : "no-seats";
  return `${section}-${row}-${range}`;
}

/**
 * First-stage markup, applied at scrape time and persisted.
 *
 * The flat addition below $35 is deliberate and long-standing: a percentage on a
 * low-value ticket does not cover the cost of selling it. The portal applies a
 * second stage at export time using the event's standard/resale/broker
 * adjustments, and that stage divides this one back out — so this must not change
 * without changing the exporter to match, or every price in the book moves.
 */
export function applyScrapeMarkup(basePrice, priceIncreasePercentage) {
  const base = parseFloat(basePrice);
  if (!Number.isFinite(base)) return 0;
  return base < 35 ? base + 15 : base * (1 + (priceIncreasePercentage || 0) / 100);
}

function seatsEqual(a, b) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

/**
 * Build the persisted document for a scraped group.
 *
 * Single definition, used by both the create and the in-place-update paths. It
 * previously existed twice, ~90 lines each, and had already drifted: one copy
 * minted a fresh inventoryId on every change while the other preserved it.
 */
export function buildGroupDocument(group, ctx, price) {
  const { eventId, mapping_id, event_name, venue_name, event_date } = ctx;
  const eventDateObj = typeof event_date === "string" ? new Date(event_date) : event_date;
  const inHandDate = moment(eventDateObj).subtract(1, "day").toISOString();
  const seats = group.seats || [];

  return {
    eventId,
    mapping_id,
    event_name,
    venue_name,
    event_date: eventDateObj.toISOString(),
    inHandDate,
    section: group.section,
    row: group.row,
    seatCount: group.inventory.quantity,
    seatRange: `${Math.min(...seats)}-${Math.max(...seats)}`,
    seats: seats.map((seatNumber) => ({
      number: seatNumber.toString(),
      inHandDate,
      price,
      mapping_id,
    })),
    inventory: {
      inventoryId: group.inventory.inventoryId || generateUniqueInventoryId(),
      quantity: group.inventory.quantity,
      section: group.section,
      hideSeatNumbers: group.inventory.hideSeatNumbers || true,
      row: group.row,
      cost: group.inventory.cost,
      stockType: group.inventory.stockType || "MOBILE_TRANSFER",
      lineType: group.inventory.lineType,
      seatType: group.inventory.seatType,
      inHandDate,
      notes: group.inventory.notes,
      tags: group.inventory.tags,
      offerId: group.inventory.offerId,
      splitType: group.inventory.splitType || "CUSTOM",
      publicNotes: group.inventory.publicNotes,
      listPrice: price,
      face_price: group.inventory.faceValue,
      taxed_cost: group.inventory.taxedCost,
      hide_seats: group.inventory.hideSeatNumbers || true,
      in_hand: typeof group.inventory.inHand === "boolean" ? group.inventory.inHand : true,
      in_hand_date: inHandDate,
      instant_transfer:
        typeof group.inventory.instantTransfer === "boolean" ? group.inventory.instantTransfer : false,
      files_available:
        typeof group.inventory.filesAvailable === "boolean" ? group.inventory.filesAvailable : false,
      customSplit: group.inventory.customSplit,
      stock_type: group.inventory.stockType || "MOBILE_TRANSFER",
      zone: group.inventory.zone,
      shown_quantity: group.inventory.shownQuantity,
      passthrough: group.inventory.passthrough,
      mapping_id,
      event_name,
      venue_name,
      event_date: eventDateObj.toISOString(),
      eventId,
      tickets: (group.inventory.tickets || []).map((ticket) => ({
        id: ticket.id,
        seatNumber: ticket.seatNumber,
        notes: ticket.notes,
        cost: ticket.cost,
        faceValue: ticket.faceValue,
        taxedCost: ticket.taxedCost,
        sellPrice:
          typeof ticket?.sellPrice === "number" && !Number.isNaN(ticket?.sellPrice)
            ? ticket.sellPrice
            : parseFloat(ticket?.cost || ticket?.faceValue || 0),
        stockType: ticket.stockType,
        eventId: ticket.eventId,
        accountId: ticket.accountId,
        status: ticket.status,
        auditNote: ticket.auditNote,
        mapping_id,
      })),
    },
  };
}

/**
 * Field-level $set for an in-place update.
 *
 * Explicitly enumerated rather than `$set: { inventory: {...} }`, because
 * replacing the whole subdocument would wipe the portal's sync bookkeeping —
 * stubhubListingId, syncHash, syncedAt — and every listing would look brand new
 * on the next drain. Those four fields are the row's memory of what StubHub was
 * told; the scraper writes around them, never over them.
 */
export function buildPatchFields(group, ctx, price, now) {
  const doc = buildGroupDocument(group, ctx, price);
  const inv = doc.inventory;

  return {
    seats: doc.seats,
    seatCount: doc.seatCount,
    inHandDate: doc.inHandDate,
    "inventory.quantity": inv.quantity,
    "inventory.listPrice": inv.listPrice,
    "inventory.cost": inv.cost,
    "inventory.customSplit": inv.customSplit,
    "inventory.splitType": inv.splitType,
    "inventory.face_price": inv.face_price,
    "inventory.taxed_cost": inv.taxed_cost,
    "inventory.tags": inv.tags,
    "inventory.notes": inv.notes,
    "inventory.publicNotes": inv.publicNotes,
    "inventory.stockType": inv.stockType,
    "inventory.stock_type": inv.stock_type,
    "inventory.offerId": inv.offerId,
    "inventory.tickets": inv.tickets,
    // Outbox marker. The portal compares a freshly mapped payload against
    // syncHash, so a rewrite to identical values costs no API call.
    "inventory.syncState": "dirty",
    "inventory.syncPendingSince": now,
  };
}

/**
 * Compare what the database holds against what was scraped.
 *
 * @param {object[]} existing  lean docs with _id, section, row, seats, inventory.*
 * @param {object[]} scraped   validated scrape groups
 * @param {object}   ctx       { eventId, mapping_id, event_name, venue_name,
 *                               event_date, priceIncreasePercentage,
 *                               now }
 * @returns {{creates, patches, deletes, unchanged, stats}}
 */
export function planInventoryChanges(existing, scraped, ctx) {
  const { priceIncreasePercentage, now = new Date() } = ctx;

  const existingByKey = new Map();
  for (const doc of existing) {
    const seats = normaliseSeats(doc.seats);
    existingByKey.set(rowKeyFor(doc.section, doc.row, seats), {
      _id: doc._id,
      seats,
      price: doc.inventory?.listPrice,
      quantity: doc.inventory?.quantity,
      inventoryId: doc.inventory?.inventoryId,
      stubhubListingId: doc.inventory?.stubhubListingId ?? null,
      customSplit: doc.inventory?.customSplit,
      splitType: doc.inventory?.splitType,
      mapping_id: doc.mapping_id,
      section: doc.section,
      row: doc.row,
    });
  }

  const scrapedByKey = new Map();
  for (const group of scraped) {
    const seats = normaliseSeats(group.seats);
    scrapedByKey.set(rowKeyFor(group.section, group.row, seats), {
      seats,
      price: applyScrapeMarkup(group.inventory.listPrice, priceIncreasePercentage),
      quantity: group.inventory.quantity,
      customSplit: group.inventory.customSplit,
      splitType: group.inventory.splitType,
      group,
    });
  }

  const creates = [];
  const patches = [];
  const deletes = [];
  let unchanged = 0;
  let recreatedForSeats = 0;

  for (const [rowKey, prev] of existingByKey) {
    const next = scrapedByKey.get(rowKey);

    if (!next) {
      deletes.push({ ...prev, rowKey, reason: "scraper-removed" });
      continue;
    }

    // The id survives every kind of change. It is StubHub's externalId.
    next.group.inventory.inventoryId = prev.inventoryId;

    const seatsChanged = !seatsEqual(prev.seats, next.seats);
    const priceChanged = Math.abs(parseFloat(prev.price) - parseFloat(next.price)) > 0.01;
    const quantityChanged = Number(prev.quantity) !== Number(next.quantity);
    const customSplitChanged = (prev.customSplit || "") !== (next.customSplit || "");
    const splitTypeChanged = (prev.splitType || "") !== (next.splitType || "");

    if (seatsChanged || quantityChanged) {
      // Different physical tickets — a different listing, however similar.
      //
      // Quantity belongs here rather than with the patchable fields because the
      // marketplace cannot change it. StubHub's InventoryUpdateRequest carries a
      // `quantity`, but the spec restricts it to placeholder (SeatSaver)
      // listings behind the ExtApiPlaceholderListingQtyPatch feature; on an
      // ordinary seated listing a PATCH silently leaves the count alone. Treated
      // as a patch, a quantity change therefore hashed differently, sent an
      // update, changed nothing, and was recorded as synced — the listing kept
      // selling the old count.
      //
      // In practice this nearly always coincides with a seat change, so it is
      // usually already covered; nearly always is not a guarantee.
      deletes.push({ ...prev, rowKey, reason: seatsChanged ? "seats-changed" : "quantity-changed" });
      creates.push({ rowKey, data: next });
      recreatedForSeats++;
      continue;
    }

    if (priceChanged || customSplitChanged || splitTypeChanged) {
      patches.push({ rowKey, _id: prev._id, data: next, now });
      continue;
    }

    unchanged++;
  }

  for (const [rowKey, next] of scrapedByKey) {
    if (!existingByKey.has(rowKey)) creates.push({ rowKey, data: next });
  }

  return {
    creates,
    patches,
    deletes,
    unchanged,
    stats: {
      existing: existingByKey.size,
      scraped: scrapedByKey.size,
      creates: creates.length,
      patches: patches.length,
      deletes: deletes.length,
      unchanged,
      recreatedForSeats,
    },
  };
}
