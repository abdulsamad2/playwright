/**
 * SeatDropDetector — detects NEW seats appearing on an event ("a drop").
 *
 * A drop is new *seat numbers*, not a bigger number. The scrape diff in
 * scraperManager already re-keys a listing whenever its price or quantity
 * changes (rowKey embeds seatRange), so `rowsToInsert` conflates genuinely new
 * inventory with repriced inventory. This module ignores rowKeys entirely and
 * diffs the seat-number set per section+row, which is the only comparison that
 * survives a re-key.
 *
 * Anti-flap: Ticketmaster regularly returns partial data, so seats vanish and
 * come back a scrape later. Seats removed within DROP_GRACE_MS are remembered
 * in Redis and are NOT reported as a drop when they reappear. Redis being down
 * fails open (more alerts, never a crash).
 */

import { getRedisClient, isRedisReady } from "../config/redis.js";
import { SeatDrop } from "../models/seatDropModel.js";
import { INSTANCE_ID } from "./RedisLiveStore.js";

const ENABLED = process.env.SEAT_DROP_TRACKING !== "0";
const GRACE_MS =
  process.env.DROP_GRACE_MS !== undefined
    ? parseInt(process.env.DROP_GRACE_MS, 10)
    : 10 * 60 * 1000; // 10 minutes

// Consecutive absent scrapes before a drop is declared gone. TM returns partial
// data often enough that one absence means nothing.
const GONE_CONFIRM_CYCLES =
  parseInt(process.env.DROP_GONE_CONFIRM_CYCLES, 10) || 2;

const graceKey = (eventId) => `dropgrace:${eventId}`;

/** "section|row" — the identity a listing keeps across a reprice or resize. */
const srKey = (section, row) => `${section}|${row}`;

/**
 * Collapse a rowKey-keyed map into one entry per section+row.
 * Several rowKeys can share a section+row (different seat ranges, price tiers).
 */
function indexBySectionRow(rowMap) {
  const index = new Map();
  let skipped = 0;

  for (const value of rowMap.values()) {
    const section = value.section;
    const row = value.row;
    if (section === undefined || row === undefined) {
      skipped++;
      continue;
    }

    const key = srKey(section, row);
    let entry = index.get(key);
    if (!entry) {
      entry = { section, row, seats: new Set(), groups: [] };
      index.set(key, entry);
    }

    for (const seat of value.seats || []) entry.seats.add(seat);
    entry.groups.push(value);
  }

  index.skipped = skipped;
  return index;
}

/** Price of the group that actually carries `seat`, else the cheapest group. */
function priceForSeat(entry, seat) {
  const carrier = entry.groups.find((g) => (g.seats || []).includes(seat));
  const group =
    carrier ||
    entry.groups.reduce(
      (min, g) =>
        min === null || Number(g.price) < Number(min.price) ? g : min,
      null
    );
  const price = Number(group?.price);
  return Number.isFinite(price) ? price : null;
}

/**
 * Diff two scrape states and return the seats that appeared.
 *
 * @param {Map} existingRowMap - current DB state, values carry {section,row,seats,price}
 * @param {Map} newRowMap      - freshly scraped state, same shape
 * @returns {{drops: Array, removedSeats: Array<string>, isBaseline: boolean}}
 */
export function diffSeatDrops(existingRowMap, newRowMap) {
  const isBaseline = existingRowMap.size === 0;

  const before = indexBySectionRow(existingRowMap);
  const after = indexBySectionRow(newRowMap);

  const drops = [];
  const removedSeats = [];

  for (const [key, afterEntry] of after) {
    const beforeEntry = before.get(key);
    const beforeSeats = beforeEntry ? beforeEntry.seats : new Set();

    const newSeats = [...afterEntry.seats].filter((s) => !beforeSeats.has(s));
    if (newSeats.length === 0) continue;

    newSeats.sort((a, b) => {
      const na = Number(a);
      const nb = Number(b);
      return Number.isFinite(na) && Number.isFinite(nb)
        ? na - nb
        : String(a).localeCompare(String(b));
    });

    drops.push({
      section: afterEntry.section,
      row: afterEntry.row,
      newSeats,
      newSeatCount: newSeats.length,
      totalSeatsInRow: afterEntry.seats.size,
      listPrice: priceForSeat(afterEntry, newSeats[0]),
      isNewListing: !beforeEntry,
    });
  }

  // Seats that vanished — remembered so their return is not a false drop
  for (const [key, beforeEntry] of before) {
    const afterEntry = after.get(key);
    const afterSeats = afterEntry ? afterEntry.seats : new Set();
    for (const seat of beforeEntry.seats) {
      if (!afterSeats.has(seat)) removedSeats.push(`${key}|${seat}`);
    }
  }

  return {
    drops,
    removedSeats,
    isBaseline,
    // section|row → Set(seat numbers) as of this scrape. The lifecycle pass
    // reuses it to decide which recorded drops are still on sale.
    afterIndex: after,
    skipped: (before.skipped || 0) + (after.skipped || 0),
  };
}

/**
 * Remove seats that disappeared within the grace window — their reappearance is
 * Ticketmaster returning partial data, not a real drop.
 */
async function filterFlapping(eventId, drops) {
  if (GRACE_MS <= 0 || !isRedisReady()) return drops;

  const redis = getRedisClient();
  if (!redis) return drops;

  let grace;
  try {
    grace = await redis.hgetall(graceKey(eventId));
  } catch {
    return drops; // fail open
  }
  if (!grace || Object.keys(grace).length === 0) return drops;

  const cutoff = Date.now() - GRACE_MS;
  const filtered = [];

  for (const drop of drops) {
    const key = srKey(drop.section, drop.row);
    const genuinelyNew = drop.newSeats.filter((seat) => {
      const removedAt = Number(grace[`${key}|${seat}`]);
      return !(Number.isFinite(removedAt) && removedAt > cutoff);
    });

    if (genuinelyNew.length === 0) continue;

    filtered.push({
      ...drop,
      newSeats: genuinelyNew,
      newSeatCount: genuinelyNew.length,
      // a "new listing" whose seats were all just here is really a restore
      isNewListing:
        drop.isNewListing && genuinelyNew.length === drop.newSeats.length,
    });
  }

  return filtered;
}

/** Remember seats that vanished, so a bounce-back is not reported as a drop. */
async function rememberRemoved(eventId, removedSeats) {
  if (GRACE_MS <= 0 || removedSeats.length === 0 || !isRedisReady()) return;

  const redis = getRedisClient();
  if (!redis) return;

  try {
    const now = Date.now();
    const fields = {};
    for (const seat of removedSeats) fields[seat] = now;

    const pipe = redis.pipeline();
    pipe.hset(graceKey(eventId), fields);
    // Expire the whole hash a little after the window so it self-cleans
    pipe.pexpire(graceKey(eventId), GRACE_MS * 2);
    await pipe.exec();
  } catch {
    // Best effort — losing the grace record only costs an extra alert
  }
}

/**
 * Forget grace entries for seats whose drop has been CONFIRMED gone.
 *
 * The grace window exists to swallow TM's partial-data flapping. Once a drop
 * has been declared gone, that judgement is already made — so if those seats
 * come back they are genuine new inventory and must alert, not be suppressed.
 */
async function forgetRemoved(eventId, fields) {
  if (GRACE_MS <= 0 || fields.length === 0 || !isRedisReady()) return;
  const redis = getRedisClient();
  if (!redis) return;
  try {
    await redis.hdel(graceKey(eventId), ...fields);
  } catch {
    // Best effort — a stale grace entry only costs one missed alert
  }
}

/**
 * Age the drops already recorded for this event against the current scrape.
 *
 * Each active drop is checked seat-by-seat: still on sale → cyclesSeen++, all
 * gone for GONE_CONFIRM_CYCLES consecutive scrapes → status "gone". That is
 * what lets the portal say "appeared, then gone after N cycles".
 *
 * @returns {Promise<{aged: number, gone: number, activeCoverage: Map<string, Set<string>>}>}
 *   activeCoverage maps section|row → seats still covered by an active drop, so
 *   a flapping seat is never recorded as a second drop.
 */
async function ageActiveDrops(eventId, afterIndex) {
  const active = await SeatDrop.find(
    { eventId, status: "active" },
    { section: 1, row: 1, newSeats: 1, detectedAt: 1, missCount: 1, firstMissAt: 1 }
  ).lean();

  const activeCoverage = new Map();
  const cover = (section, row, seats) => {
    const k = srKey(section, row);
    if (!activeCoverage.has(k)) activeCoverage.set(k, new Set());
    const set = activeCoverage.get(k);
    for (const seat of seats) set.add(seat);
  };

  if (active.length === 0) return { aged: 0, gone: 0, activeCoverage };

  const now = new Date();
  const ops = [];
  const graceToClear = [];
  let gone = 0;

  for (const drop of active) {
    const entry = afterIndex.get(srKey(drop.section, drop.row));
    const onSale = entry ? entry.seats : new Set();
    const stillPresent = (drop.newSeats || []).filter((s) => onSale.has(s));

    if (stillPresent.length > 0) {
      // Still (at least partly) on sale — a reappearance clears any miss streak
      cover(drop.section, drop.row, drop.newSeats || []);
      ops.push({
        updateOne: {
          filter: { _id: drop._id },
          update: {
            $set: {
              seatsRemaining: stillPresent,
              lastSeenAt: now,
              missCount: 0,
              firstMissAt: null,
            },
            $inc: { cyclesSeen: 1 },
          },
        },
      });
      continue;
    }

    const misses = (drop.missCount || 0) + 1;

    if (misses < GONE_CONFIRM_CYCLES) {
      // Absent once — could be TM partial data. Wait for confirmation. The drop
      // is still active, so it keeps ownership of its seats: if they come back
      // next scrape that is this drop resuming, not a new one.
      cover(drop.section, drop.row, drop.newSeats || []);
      ops.push({
        updateOne: {
          filter: { _id: drop._id },
          update: {
            $set: {
              missCount: misses,
              firstMissAt: drop.firstMissAt || now,
              seatsRemaining: [],
            },
          },
        },
      });
      continue;
    }

    // Confirmed gone. Credit the disappearance to when it actually started.
    const goneAt = drop.firstMissAt || now;
    for (const seat of drop.newSeats || []) {
      graceToClear.push(`${srKey(drop.section, drop.row)}|${seat}`);
    }
    ops.push({
      updateOne: {
        filter: { _id: drop._id },
        update: {
          $set: {
            status: "gone",
            missCount: misses,
            goneAt,
            seatsRemaining: [],
            secondsAlive: Math.max(
              0,
              Math.round((goneAt - new Date(drop.detectedAt)) / 1000)
            ),
          },
        },
      },
    });
    gone++;
  }

  if (ops.length > 0) {
    try {
      await SeatDrop.bulkWrite(ops, { ordered: false });
    } catch (error) {
      console.error(`[SeatDrop] Lifecycle update failed for ${eventId}: ${error.message}`);
      return { aged: 0, gone: 0 };
    }
  }

  if (gone > 0) {
    // Their return is now news again, not a flap
    await forgetRemoved(eventId, graceToClear);
    console.log(`[DROP ${eventId}] ${gone} drop(s) confirmed GONE`);
  }

  return { aged: ops.length, gone, activeCoverage };
}

/**
 * Detect and persist drops for one event.
 *
 * Safe to call fire-and-forget: it never throws.
 *
 * @returns {Promise<number>} how many drop records were written
 */
export async function recordSeatDrops({
  eventId,
  existingRowMap,
  newRowMap,
  eventMeta = {},
}) {
  if (!ENABLED) return 0;

  try {
    const { drops, removedSeats, isBaseline, skipped, afterIndex } =
      diffSeatDrops(existingRowMap, newRowMap);

    // scraperManager must put section/row on every rowMap value. If that ever
    // regresses, detection degrades to a silent no-op — so say so loudly.
    if (skipped > 0) {
      console.error(
        `[SeatDrop] ${eventId}: ${skipped} listing(s) missing section/row — ` +
          `drop detection is INCOMPLETE for this scrape`
      );
    }

    await rememberRemoved(eventId, removedSeats);

    // Age drops already on record BEFORE writing new ones — a drop recorded by
    // this same scrape must not be aged against the scrape that created it.
    const { activeCoverage } = await ageActiveDrops(eventId, afterIndex);

    // First-ever scrape: every seat is "new". That is a baseline, not a drop.
    if (isBaseline) return 0;
    if (drops.length === 0) return 0;

    const unflapped = await filterFlapping(eventId, drops);

    // Seats an active drop already tracks are that drop resuming, not a new one
    const genuine = [];
    for (const drop of unflapped) {
      const covered = activeCoverage.get(srKey(drop.section, drop.row));
      const uncovered = covered
        ? drop.newSeats.filter((s) => !covered.has(s))
        : drop.newSeats;
      if (uncovered.length === 0) continue;
      genuine.push({
        ...drop,
        newSeats: uncovered,
        newSeatCount: uncovered.length,
        isNewListing: drop.isNewListing && uncovered.length === drop.newSeats.length,
      });
    }
    if (genuine.length === 0) return 0;

    const detectedAt = new Date();

    // Generation counter, not a timestamp: two instances racing the same scrape
    // compute the same generation and collide on the unique index, while a
    // genuine re-drop of the same seats later gets the next generation.
    const withKeys = await Promise.all(
      genuine.map(async (drop) => {
        const dropBase = `${eventId}|${drop.section}|${drop.row}|${drop.newSeats.join(",")}`;
        const generation = await SeatDrop.countDocuments({ eventId, dropBase });
        return { drop, dropBase, dropKey: `${dropBase}|${generation}` };
      })
    );

    const docs = withKeys.map(({ drop, dropBase, dropKey }) => ({
      eventId,
      mapping_id: eventMeta.mapping_id,
      event_name: eventMeta.event_name,
      venue_name: eventMeta.venue_name,
      event_date: eventMeta.event_date,
      event_url: eventMeta.event_url,
      section: drop.section,
      row: drop.row,
      newSeats: drop.newSeats,
      newSeatCount: drop.newSeatCount,
      totalSeatsInRow: drop.totalSeatsInRow,
      listPrice: drop.listPrice,
      isNewListing: drop.isNewListing,
      detectedAt,
      status: "active",
      seatsRemaining: drop.newSeats,
      lastSeenAt: detectedAt,
      cyclesSeen: 1,
      missCount: 0,
      seen: false,
      instanceId: INSTANCE_ID,
      dropBase,
      dropKey,
    }));

    let written = 0;
    try {
      const inserted = await SeatDrop.insertMany(docs, { ordered: false });
      written = inserted.length;
    } catch (error) {
      // Duplicate keys mean another instance recorded the same drop — expected
      written = error?.result?.insertedCount ?? error?.insertedDocs?.length ?? 0;
      if (error?.code !== 11000 && !error?.writeErrors) {
        console.error(
          `[SeatDrop] Insert error for ${eventId}: ${error.message}`
        );
      }
    }

    if (written > 0) {
      const totalSeats = genuine.reduce((sum, d) => sum + d.newSeatCount, 0);
      console.log(
        `[DROP ${eventId}] ${totalSeats} new seats across ${written} row(s)` +
          ` — ${genuine
            .slice(0, 3)
            .map((d) => `${d.section}/${d.row}:${d.newSeats.join(",")}`)
            .join(" ")}${genuine.length > 3 ? " …" : ""}`
      );
    }

    return written;
  } catch (error) {
    console.error(`[SeatDrop] Detection failed for ${eventId}: ${error.message}`);
    return 0;
  }
}

export default { diffSeatDrops, recordSeatDrops };
