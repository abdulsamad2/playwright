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

  assert.equal(byRow.get("1"), 1, "row 1 is the front, despite being stored last");
  assert.equal(byRow.get("3"), 3);
  assert.equal(byRow.get("10"), 10);
  assert.equal(byRow.get("22"), 22, "row 22 is the back, despite being stored first");
});

test("rank is the row number itself, so gaps in the numbering are preserved", () => {
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

  assert.deepEqual([byRow.get("1"), byRow.get("7"), byRow.get("40")], [1, 7, 40]);
});

test("the same row label ranks the same in every section", () => {
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

  assert.equal(result.find((r) => r.section === "101" && r.row === "5").rowRank, 5);
  assert.equal(result.find((r) => r.section === "102" && r.row === "5").rowRank, 5);
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

  assert.equal(listing.rowRank, 2);
  assert.equal(listing.inventory.rowRank, 2);
});

test("a number with a letter after it is left unranked", () => {
  // 26W is the wheelchair position in row 26, but the label is not a plain
  // number, so it sits the rule out rather than being placed by guesswork.
  const mapData = mapWith([
    {
      name: "102",
      segments: [
        { name: "26", placesNoKeys: [["a-1", "1"], ["a-2", "2"]] },
        { name: "26W", placesNoKeys: [["b-1", "1"], ["b-2", "2"]] },
        { name: "30", placesNoKeys: [["c-1", "1"], ["c-2", "2"]] },
        { name: "3", placesNoKeys: [["d-1", "1"], ["d-2", "2"]] },
      ],
    },
  ]);
  const listings = ["a", "b", "c", "d"].map((p, i) => listingFor(`off-${i}`, [`${p}-1`, `${p}-2`]));
  const byRow = new Map(run(mapData, listings).map((r) => [r.row, r.rowRank]));

  assert.equal(byRow.get("3"), 3);
  assert.equal(byRow.get("26"), 26);
  assert.equal(byRow.get("30"), 30);
  assert.equal(byRow.get("26W"), null, "not a plain number, so never ranked");
});

test("a venue lettered A,B,C ranks alphabetically", () => {
  // Some venues have no numbered rows at all. Real ones skip I and O, so the
  // letters are compared rather than counted.
  const mapData = mapWith([
    {
      name: "101",
      segments: [
        { name: "J", placesNoKeys: [["a-1", "1"], ["a-2", "2"]] },
        { name: "A", placesNoKeys: [["b-1", "1"], ["b-2", "2"]] },
        { name: "H", placesNoKeys: [["c-1", "1"], ["c-2", "2"]] },
      ],
    },
  ]);
  const listings = ["a", "b", "c"].map((p, i) => listingFor(`off-${i}`, [`${p}-1`, `${p}-2`]));
  const byRow = new Map(run(mapData, listings).map((r) => [r.row, r.rowRank]));

  assert.deepEqual([byRow.get("A"), byRow.get("H"), byRow.get("J")], [1, 8, 10]);
});

test("two-letter rows rank on their own scale, counting from AA", () => {
  // AA is ahead of A in some venues and behind Z in others, so the two widths
  // are separate scales and each counts from 1. The consumer never compares
  // across them, which is what makes the shared rank harmless.
  const mapData = mapWith([
    {
      name: "101",
      segments: [
        { name: "AA", placesNoKeys: [["a-1", "1"], ["a-2", "2"]] },
        { name: "BB", placesNoKeys: [["b-1", "1"], ["b-2", "2"]] },
        { name: "ZZ", placesNoKeys: [["c-1", "1"], ["c-2", "2"]] },
        { name: "A", placesNoKeys: [["d-1", "1"], ["d-2", "2"]] },
      ],
    },
  ]);
  const listings = ["a", "b", "c", "d"].map((p, i) => listingFor(`off-${i}`, [`${p}-1`, `${p}-2`]));
  const byRow = new Map(run(mapData, listings).map((r) => [r.row, r.rowRank]));

  assert.equal(byRow.get("AA"), 1);
  assert.equal(byRow.get("BB"), 28, "the pairs still count in base 26: BB is 26 + 2");
  assert.equal(byRow.get("ZZ"), 676);
  assert.equal(byRow.get("A"), 1, "rank 1 of a different scale");
});

test("three-letter rows rank one place wider still", () => {
  const mapData = mapWith([
    {
      name: "101",
      segments: [
        { name: "AAA", placesNoKeys: [["a-1", "1"], ["a-2", "2"]] },
        { name: "BBB", placesNoKeys: [["b-1", "1"], ["b-2", "2"]] },
        { name: "ZZZ", placesNoKeys: [["c-1", "1"], ["c-2", "2"]] },
      ],
    },
  ]);
  const listings = ["a", "b", "c"].map((p, i) => listingFor(`off-${i}`, [`${p}-1`, `${p}-2`]));
  const byRow = new Map(run(mapData, listings).map((r) => [r.row, r.rowRank]));

  assert.deepEqual([byRow.get("AAA"), byRow.get("BBB"), byRow.get("ZZZ")], [1, 704, 17576]);
});

test("a mixed pair or triple is a seat-type code, so it never ranks", () => {
  // Every two- and three-letter row in 1,090,781 production listings is a
  // repeated letter; every mixed one is a code. These are the twelve that the
  // old denylist did not name and so ranked as positions — ONE became row
  // 9,808 and CRT row 1,814, which is how courtside came to be judged as a
  // seat 1,813 rows behind row AAA.
  const codes = ["MW", "VW", "LR", "RL", "RW", "BX",
                 "JJW", "RAL", "BAR", "CRT", "EDG", "ONE", "TWO"];
  const mapData = mapWith([
    {
      name: "101",
      segments: codes.map((name, i) => ({
        name,
        placesNoKeys: [[`p${i}-1`, "1"], [`p${i}-2`, "2"]],
      })),
    },
  ]);
  const listings = codes.map((_, i) => listingFor(`off-${i}`, [`p${i}-1`, `p${i}-2`]));
  const byRow = new Map(run(mapData, listings).map((r) => [r.row, r.rowRank]));

  for (const code of codes) {
    assert.equal(byRow.get(code), null, `${code} is a code, not a row`);
  }
});

test("four letters and up are still a guess, so they never rank", () => {
  const mapData = mapWith([
    {
      name: "101",
      segments: [
        { name: "AAAA", placesNoKeys: [["a-1", "1"], ["a-2", "2"]] },
        { name: "12A", placesNoKeys: [["b-1", "1"], ["b-2", "2"]] },
        { name: "A", placesNoKeys: [["c-1", "1"], ["c-2", "2"]] },
      ],
    },
  ]);
  const listings = ["a", "b", "c"].map((p, i) => listingFor(`off-${i}`, [`${p}-1`, `${p}-2`]));
  const byRow = new Map(run(mapData, listings).map((r) => [r.row, r.rowRank]));

  assert.equal(byRow.get("AAAA"), null);
  assert.equal(byRow.get("12A"), null);
  assert.equal(byRow.get("A"), 1);
});

test("access and standing codes are not rows, whatever their length", () => {
  // WC and ADA are the shapes of a two- and a three-letter row and are neither.
  // 156 of the 16,024 two- and three-letter rows in one production export are
  // codes like these, and ranking one lets the pricing rule delete it.
  const mapData = mapWith([
    {
      name: "101",
      segments: [
        { name: "WC", placesNoKeys: [["a-1", "1"], ["a-2", "2"]] },
        { name: "ADA", placesNoKeys: [["b-1", "1"], ["b-2", "2"]] },
        { name: "GA", placesNoKeys: [["c-1", "1"], ["c-2", "2"]] },
        { name: "BB", placesNoKeys: [["d-1", "1"], ["d-2", "2"]] },
      ],
    },
  ]);
  const listings = ["a", "b", "c", "d"].map((p, i) => listingFor(`off-${i}`, [`${p}-1`, `${p}-2`]));
  const byRow = new Map(run(mapData, listings).map((r) => [r.row, r.rowRank]));

  assert.equal(byRow.get("WC"), null);
  assert.equal(byRow.get("ADA"), null);
  assert.equal(byRow.get("GA"), null);
  assert.equal(byRow.get("BB"), 28, "a real doubled-letter row still ranks");
});

test("numbers and letters share one scale, so row A and row 1 both rank 1", () => {
  // A section can carry both. Each label is read on its own terms: the number
  // for digits, the alphabet position for a letter.
  const mapData = mapWith([
    {
      name: "433",
      segments: [
        { name: "1", placesNoKeys: [["a-1", "1"], ["a-2", "2"]] },
        { name: "A", placesNoKeys: [["b-1", "1"], ["b-2", "2"]] },
        { name: "23", placesNoKeys: [["c-1", "1"], ["c-2", "2"]] },
      ],
    },
  ]);
  const listings = ["a", "b", "c"].map((p, i) => listingFor(`off-${i}`, [`${p}-1`, `${p}-2`]));
  const byRow = new Map(run(mapData, listings).map((r) => [r.row, r.rowRank]));

  assert.equal(byRow.get("1"), 1);
  assert.equal(byRow.get("23"), 23);
  assert.equal(byRow.get("A"), 1);
});

test("SRO is standing room, not a row, so it never ranks", () => {
  const mapData = mapWith([
    {
      name: "F28",
      segments: [
        { name: "SRO", placesNoKeys: [["a-1", "1"], ["a-2", "2"]] },
        { name: "1", placesNoKeys: [["b-1", "1"], ["b-2", "2"]] },
        { name: "2", placesNoKeys: [["c-1", "1"], ["c-2", "2"]] },
      ],
    },
  ]);
  const listings = ["a", "b", "c"].map((p, i) => listingFor(`off-${i}`, [`${p}-1`, `${p}-2`]));
  const byRow = new Map(run(mapData, listings).map((r) => [r.row, r.rowRank]));

  assert.equal(byRow.get("SRO"), null);
  assert.equal(byRow.get("1"), 1);
  assert.equal(byRow.get("2"), 2);
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
