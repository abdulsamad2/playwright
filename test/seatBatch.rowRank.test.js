// rowRank tests for helpers/seatBatch.js.
//
// rowRank is the row's position within its section counting from the field, 0
// being closest. It comes from the seat coordinates in placesNoKeys, never from
// the row label, so these fixtures use a non-numeric, non-alphabetical label
// scheme to prove no parsing is happening. Maps with no coordinates fall back
// to the array index, which the fixtures below also cover.
//
// Run: node --test test/
import test from "node:test";
import assert from "node:assert/strict";

import { AttachRowSection } from "../helpers/seatBatch.js";

const event = { eventMappingId: "evt-1", inHandDate: "2026-08-01" };
const descriptions = [{ descriptionId: "d1", descriptions: ["Standard Ticket"] }];

// Two sections; rows listed front-to-back with labels that sort differently
// from their physical order (AA, then A, then B) so a label-parsing
// implementation would produce a different ranking than the positional one.
const mapData = {
  pages: [
    {
      segments: [
        {
          segments: [
            {
              name: "FLOOR1",
              segments: [
                { name: "AA", placesNoKeys: [["s-1", "1"], ["s-2", "2"]] },
                { name: "A", placesNoKeys: [["s-3", "1"], ["s-4", "2"]] },
                { name: "B", placesNoKeys: [["s-5", "1"], ["s-6", "2"]] },
              ],
            },
            {
              // A second section restarts the ranking at 0 — rank is per section.
              name: "FLOOR2",
              segments: [{ name: "B", placesNoKeys: [["s-7", "1"], ["s-8", "2"]] }],
            },
          ],
        },
      ],
    },
  ],
};

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

function run(listings) {
  return AttachRowSection(
    listings,
    mapData,
    listings.map((l) => offerFor(l.offerId)),
    event,
    descriptions,
  );
}

test("rows are ranked by distance from the field, not by array order", () => {
  // TM lists rows in map-drawing order: this section's array runs C, A, B while
  // the seats put A nearest the field. Coordinates must win over array position.
  // Page is 1000x1000, so the field centre is (500, 500).
  const scrambledMap = {
    pages: [
      {
        width: 1000,
        height: 1000,
        segments: [
          {
            segments: [
              {
                name: "FLOOR1",
                segments: [
                  // index 0, but furthest from centre -> must rank last
                  { name: "C", placesNoKeys: [["c-1", "1", 500, 800], ["c-2", "2", 510, 800]] },
                  // index 1, nearest the centre -> must rank first
                  { name: "A", placesNoKeys: [["a-1", "1", 500, 600], ["a-2", "2", 510, 600]] },
                  { name: "B", placesNoKeys: [["b-1", "1", 500, 700], ["b-2", "2", 510, 700]] },
                ],
              },
            ],
          },
        ],
      },
    ],
  };

  const listings = [
    listingFor("off-1", ["a-1", "a-2"]),
    listingFor("off-2", ["b-1", "b-2"]),
    listingFor("off-3", ["c-1", "c-2"]),
  ];
  const result = AttachRowSection(
    listings,
    scrambledMap,
    listings.map((l) => offerFor(l.offerId)),
    event,
    descriptions,
  );

  const byRow = new Map(result.map((r) => [r.row, r.rowRank]));
  assert.equal(byRow.get("A"), 0, "A is closest to the field despite being second in the array");
  assert.equal(byRow.get("B"), 1);
  assert.equal(byRow.get("C"), 2, "C is furthest despite being first in the array");
});

test("a map with no coordinates falls back to the array index", () => {
  const result = run([
    listingFor("off-1", ["s-1", "s-2"]), // FLOOR1 row AA -> rank 0
    listingFor("off-2", ["s-3", "s-4"]), // FLOOR1 row A  -> rank 1
    listingFor("off-3", ["s-5", "s-6"]), // FLOOR1 row B  -> rank 2
  ]);

  const byRow = new Map(result.map((r) => [r.row, r]));
  assert.equal(byRow.size, 3, "expected one listing per row");
  assert.equal(byRow.get("AA").rowRank, 0);
  assert.equal(byRow.get("A").rowRank, 1);
  assert.equal(byRow.get("B").rowRank, 2);
});

test("rowRank is written to both the group and its inventory", () => {
  const [listing] = run([listingFor("off-2", ["s-3", "s-4"])]);
  assert.equal(listing.rowRank, 1);
  assert.equal(listing.inventory.rowRank, 1);
});

test("rank restarts per section, so the same label can rank differently", () => {
  const result = run([
    listingFor("off-3", ["s-5", "s-6"]), // FLOOR1 row B -> rank 2
    listingFor("off-4", ["s-7", "s-8"]), // FLOOR2 row B -> rank 0
  ]);

  const floor1B = result.find((r) => r.section === "FLOOR1" && r.row === "B");
  const floor2B = result.find((r) => r.section === "FLOOR2" && r.row === "B");
  assert.equal(floor1B.rowRank, 2);
  assert.equal(floor2B.rowRank, 0);
});

test("GA sections carry no rank", () => {
  const gaMapData = {
    pages: [
      {
        segments: [
          {
            segments: [
              { name: "LAWN", placesNoKeys: [["g-1", "1"], ["g-2", "2"]] },
            ],
          },
        ],
      },
    ],
  };

  const result = AttachRowSection(
    [listingFor("off-1", ["g-1", "g-2"])],
    gaMapData,
    [offerFor("off-1")],
    event,
    descriptions,
  );

  assert.equal(result.length, 1);
  assert.equal(result[0].rowRank, null);
  assert.equal(result[0].inventory.rowRank, null);
});
