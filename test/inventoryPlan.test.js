/**
 * Tests for the inventory reconciliation planner.
 *
 * This logic decides, every two minutes for every event, whether a listing gets
 * updated, rebuilt or removed — and until it was extracted from scraperManager it
 * could not be exercised without a database and a browser. It is pure now, so
 * these run in milliseconds against fixtures.
 *
 * The cases that matter most are the ones about identity: inventoryId becomes
 * StubHub's externalId, so anything that silently changes it detaches a live
 * listing from the row that owns it.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";

import {
  planInventoryChanges,
  buildPatchFields,
  buildGroupDocument,
  applyScrapeMarkup,
  rowKeyFor,
  normaliseSeats,
} from "../helpers/inventoryPlan.js";

const CTX = {
  eventId: "EVT1",
  mapping_id: "159262123",
  event_name: "Braves vs Nationals",
  venue_name: "Truist Park",
  event_date: "2026-05-22T19:15:00.000Z",
  priceIncreasePercentage: 30,
  now: new Date("2026-08-27T12:00:00Z"),
};

/** A row as stored, matching PLAN_PROJECTION. */
const existingRow = (over = {}) => ({
  _id: "doc1",
  section: "101",
  row: "A",
  seats: [{ number: "1" }, { number: "2" }],
  mapping_id: "159262123",
  inventory: {
    listPrice: 130,
    quantity: 2,
    inventoryId: 2540402267,
    stubhubListingId: "1146693166",
    customSplit: "2",
    splitType: "CUSTOM",
    ...over,
  },
});

/** A group as scraped. listPrice here is pre-markup. */
const scrapedGroup = (over = {}) => ({
  section: "101",
  row: "A",
  seats: [1, 2],
  inventory: {
    quantity: 2,
    listPrice: 100, // 100 * 1.30 = 130, matching existingRow
    cost: 100,
    customSplit: "2",
    splitType: "CUSTOM",
    tickets: [],
    ...over,
  },
});

const plan = (existing, scraped, over = {}) =>
  planInventoryChanges(existing, scraped, { ...CTX, stubhubMode: true, ...over });

describe("markup", () => {
  test("percentage above the flat-rate threshold", () => {
    assert.equal(applyScrapeMarkup(100, 30), 130);
    assert.equal(Math.round(applyScrapeMarkup(529.66, 30) * 1000) / 1000, 688.558);
  });

  test("flat addition below $35, where a percentage would not cover selling costs", () => {
    assert.equal(applyScrapeMarkup(20, 30), 35);
    assert.equal(applyScrapeMarkup(34.99, 30), 49.99);
  });

  test("the boundary itself takes the percentage", () => {
    assert.equal(applyScrapeMarkup(35, 30), 45.5);
  });
});

describe("row identity", () => {
  test("the key ignores price, so a re-priced block matches itself", () => {
    const a = rowKeyFor("101", "A", normaliseSeats([1, 2]));
    const b = rowKeyFor("101", "A", normaliseSeats([2, 1]));
    assert.equal(a, b, "seat order must not affect identity");
  });

  test("different seats are a different block", () => {
    assert.notEqual(
      rowKeyFor("101", "A", normaliseSeats([1, 2])),
      rowKeyFor("101", "A", normaliseSeats([1, 3]))
    );
  });
});

describe("planInventoryChanges", () => {
  test("an unchanged row produces no operations at all", () => {
    const p = plan([existingRow()], [scrapedGroup()]);
    assert.deepEqual(
      { c: p.creates.length, u: p.patches.length, d: p.deletes.length, same: p.unchanged },
      { c: 0, u: 0, d: 0, same: 1 }
    );
  });

  test("a price change becomes an in-place patch, not a rebuild", () => {
    const p = plan([existingRow()], [scrapedGroup({ listPrice: 150 })]);
    assert.equal(p.patches.length, 1);
    assert.equal(p.deletes.length, 0, "the listing must not be destroyed to re-price it");
    assert.equal(p.creates.length, 0);
    assert.equal(p.patches[0].data.price, 195); // 150 * 1.30
  });

  test("the inventoryId survives a price change — it is StubHub's externalId", () => {
    const p = plan([existingRow()], [scrapedGroup({ listPrice: 150 })]);
    assert.equal(p.patches[0].data.group.inventory.inventoryId, 2540402267);
  });

  test("in csv mode a price change still rebuilds, exactly as before", () => {
    const p = plan([existingRow()], [scrapedGroup({ listPrice: 150 })], { stubhubMode: false });
    assert.equal(p.patches.length, 0);
    assert.equal(p.deletes.length, 1);
    assert.equal(p.creates.length, 1);
    assert.equal(
      p.creates[0].data.group.inventory.inventoryId,
      2540402267,
      "even the legacy path preserves the id"
    );
  });

  test("changed seats rebuild in both modes — different tickets, different listing", () => {
    for (const stubhubMode of [true, false]) {
      const p = plan([existingRow()], [scrapedGroup({ quantity: 2 })].map(g => ({ ...g, seats: [1, 3] })), { stubhubMode });
      assert.equal(p.deletes.length, 1, `stubhubMode=${stubhubMode}`);
      assert.equal(p.creates.length, 1);
      assert.equal(p.deletes[0].reason, "scraper-removed");
    }
  });

  test("split and quantity changes patch in place", () => {
    const p = plan([existingRow()], [scrapedGroup({ customSplit: "1,2" })]);
    assert.equal(p.patches.length, 1);
    assert.equal(p.deletes.length, 0);
  });

  test("a vanished row is a delete carrying its listing id for the tombstone", () => {
    const p = plan([existingRow()], []);
    assert.equal(p.deletes.length, 1);
    assert.equal(p.deletes[0].reason, "scraper-removed");
    assert.equal(p.deletes[0].stubhubListingId, "1146693166");
    assert.equal(p.deletes[0].inventoryId, 2540402267);
  });

  test("a row never listed carries a null listing id, so removal costs no API call", () => {
    const p = plan([existingRow({ stubhubListingId: undefined })], []);
    assert.equal(p.deletes[0].stubhubListingId, null);
  });

  test("a brand new block is a create", () => {
    const p = plan([], [scrapedGroup()]);
    assert.equal(p.creates.length, 1);
    assert.equal(p.deletes.length, 0);
  });

  test("planning is idempotent — replanning the same scrape changes nothing", () => {
    const first = plan([existingRow()], [scrapedGroup()]);
    const second = plan([existingRow()], [scrapedGroup()]);
    assert.deepEqual(first.stats, second.stats);
    assert.equal(first.stats.unchanged, 1);
  });

  test("does not mutate the existing rows it was given", () => {
    const rows = [existingRow()];
    const snapshot = JSON.stringify(rows);
    plan(rows, [scrapedGroup({ listPrice: 150 })]);
    assert.equal(JSON.stringify(rows), snapshot);
  });

  test("stats account for every existing row exactly once", () => {
    const p = plan(
      [existingRow(), { ...existingRow(), _id: "doc2", row: "B" }],
      [scrapedGroup({ listPrice: 150 })]
    );
    const accounted = p.stats.patches + p.stats.deletes + p.stats.unchanged;
    assert.equal(accounted, 2);
  });
});

describe("buildPatchFields", () => {
  const fields = buildPatchFields(scrapedGroup({ listPrice: 150 }), CTX, 195, CTX.now);

  test("never writes over the portal's sync bookkeeping", () => {
    for (const owned of [
      "inventory.stubhubListingId",
      "inventory.syncHash",
      "inventory.syncedAt",
      "inventory.inventoryId",
    ]) {
      assert.equal(owned in fields, false, `${owned} is portal-owned and must survive a scraper write`);
    }
  });

  test("marks the row for the outbox", () => {
    assert.equal(fields["inventory.syncState"], "dirty");
    assert.equal(fields["inventory.syncPendingSince"], CTX.now);
  });

  test("carries the new price through", () => {
    assert.equal(fields["inventory.listPrice"], 195);
  });
});

describe("buildGroupDocument", () => {
  test("preserves a supplied inventoryId and mints one only when absent", () => {
    const withId = buildGroupDocument(
      scrapedGroup({ inventoryId: 999 }), CTX, 130
    );
    assert.equal(withId.inventory.inventoryId, 999);

    const withoutId = buildGroupDocument(scrapedGroup(), CTX, 130);
    assert.equal(String(withoutId.inventory.inventoryId).length, 10);
  });

  test("in-hand date is the day before the event", () => {
    const doc = buildGroupDocument(scrapedGroup(), CTX, 130);
    assert.match(doc.inHandDate, /^2026-05-21/);
  });
});
