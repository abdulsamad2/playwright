// rowRank tests for helpers/seatBatch.js.
//
// rowRank is the row's position within its section counting from the front, 0
// being closest, taken from the row number. Venues number from the front, so
// row 1 is nearest whatever the seats face. TM's own array order is unrelated
// to position, so these fixtures store the rows scrambled to prove the ranking
// does not follow it.
//
// Run: node --test test/
import test from "node:test";
import assert from "node:assert/strict";

import { AttachRowSection } from "../helpers/seatBatch.js";

const event = { eventMappingId: "evt-1", inHandDate: "2026-08-01" };
const descriptions = [{ descriptionId: "d1", descriptions: ["Standard Ticket"] }];

function offerFor(offerId) {
  return {
    offerId,
    name: "Standard Ticket",
    description: "Standard Ticket",
    inventoryType: "Standard",
    faceValue: 100,
    charges: [],
  };
}

function listingFor(offerId, places) {
  return { places, offerId, accessibility: "", descriptionId: "d1", attributes: [] };
}

function mapWith(sections) {
  return { pages: [{ segments: [{ segments: sections }] }] };
}

function run(mapData, listings) {
  return AttachRowSection(
    listings,
    mapData,
    listings.map((l) => offerFor(l.offerId)),
    event,
    descriptions,
  );
}

test("rows are ranked by row number, not by TM's array order", () => {
  // Stored 22, 3, 10, 1 — the order TM happens to draw them in. Ranking must
  // ignore that completely and follow the numbers.
  const mapData = mapWith([
    {
      name: "101",
      segments: [
        { name: "22", placesNoKeys: [["a-1", "1"], ["a-2", "2"]] },
        { name: "3", placesNoKeys: [["b-1", "1"], ["b-2", "2"]] },
        { name: "10", placesNoKeys: [["c-1", "1"], ["c-2", "2"]] },
        { name: "1", placesNoKeys: [["d-1", "1"], ["d-2", "2"]] },
      ],
    },
  ]);

  const listings = [
    listingFor("off-1", ["a-1", "a-2"]),
    listingFor("off-2", ["b-1", "b-2"]),
    listingFor("off-3", ["c-1", "c-2"]),
    listingFor("off-4", ["d-1", "d-2"]),
  ];
  const byRow = new Map(run(mapData, listings).map((r) => [r.row, r.rowRank]));

  assert.equal(byRow.get("1"), 0, "row 1 is the front, despite being stored last");
  assert.equal(byRow.get("3"), 1);
  assert.equal(byRow.get("10"), 2);
  assert.equal(byRow.get("22"), 3, "row 22 is the back, despite being stored first");
});

test("rank is a dense position, so gaps in the numbering do not leave gaps", () => {
  const mapData = mapWith([
    {
      name: "101",
      segments: [
        { name: "1", placesNoKeys: [["a-1", "1"], ["a-2", "2"]] },
        { name: "40", placesNoKeys: [["b-1", "1"], ["b-2", "2"]] },
        { name: "7", placesNoKeys: [["c-1", "1"], ["c-2", "2"]] },
      ],
    },
  ]);
  const listings = [
    listingFor("off-1", ["a-1", "a-2"]),
    listingFor("off-2", ["b-1", "b-2"]),
    listingFor("off-3", ["c-1", "c-2"]),
  ];
  const byRow = new Map(run(mapData, listings).map((r) => [r.row, r.rowRank]));

  assert.deepEqual([byRow.get("1"), byRow.get("7"), byRow.get("40")], [0, 1, 2]);
});

test("rank restarts per section, so the same row ranks differently elsewhere", () => {
  const mapData = mapWith([
    {
      name: "101",
      segments: [
        { name: "1", placesNoKeys: [["a-1", "1"], ["a-2", "2"]] },
        { name: "5", placesNoKeys: [["b-1", "1"], ["b-2", "2"]] },
      ],
    },
    {
      name: "102",
      segments: [{ name: "5", placesNoKeys: [["c-1", "1"], ["c-2", "2"]] }],
    },
  ]);
  const listings = [
    listingFor("off-1", ["b-1", "b-2"]),
    listingFor("off-2", ["c-1", "c-2"]),
  ];
  const result = run(mapData, listings);

  assert.equal(result.find((r) => r.section === "101" && r.row === "5").rowRank, 1);
  assert.equal(result.find((r) => r.section === "102" && r.row === "5").rowRank, 0);
});

test("rowRank is written to both the group and its inventory", () => {
  const mapData = mapWith([
    {
      name: "101",
      segments: [
        { name: "1", placesNoKeys: [["a-1", "1"], ["a-2", "2"]] },
        { name: "2", placesNoKeys: [["b-1", "1"], ["b-2", "2"]] },
      ],
    },
  ]);
  const [listing] = run(mapData, [listingFor("off-1", ["b-1", "b-2"])]);

  assert.equal(listing.rowRank, 1);
  assert.equal(listing.inventory.rowRank, 1);
});

test("a non-numeric row is left unranked, and the numbered rows still rank", () => {
  // 34W has no reliable place in the order, and guessing makes the pricing rule
  // drop the better seat. It is skipped; the rows that do carry a number are
  // still ranked among themselves.
  const mapData = mapWith([
    {
      name: "104",
      segments: [
        { name: "5", placesNoKeys: [["a-1", "1"], ["a-2", "2"]] },
        { name: "34W", placesNoKeys: [["b-1", "1"], ["b-2", "2"]] },
        { name: "1", placesNoKeys: [["c-1", "1"], ["c-2", "2"]] },
      ],
    },
  ]);
  const listings = [
    listingFor("off-1", ["a-1", "a-2"]),
    listingFor("off-2", ["b-1", "b-2"]),
    listingFor("off-3", ["c-1", "c-2"]),
  ];
  const byRow = new Map(run(mapData, listings).map((r) => [r.row, r.rowRank]));

  assert.equal(byRow.get("1"), 0);
  assert.equal(byRow.get("5"), 1);
  assert.equal(byRow.get("34W"), null, "no number, so no rank");
});

test("GA sections carry no rank", () => {
  const gaMapData = mapWith([
    { name: "LAWN", placesNoKeys: [["g-1", "1"], ["g-2", "2"]] },
  ]);
  const result = run(gaMapData, [listingFor("off-1", ["g-1", "g-2"])]);

  assert.equal(result.length, 1);
  assert.equal(result[0].rowRank, null);
  assert.equal(result[0].inventory.rowRank, null);
});
