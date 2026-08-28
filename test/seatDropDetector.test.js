import test from "node:test";
import assert from "node:assert/strict";

import { diffSeatDrops } from "../helpers/SeatDropDetector.js";

/** Build a rowMap the way scraperManager does: key = section-row-seatRange. */
function rowMap(rows) {
  const map = new Map();
  for (const { section, row, seats, price } of rows) {
    const sorted = seats.map(String).sort();
    const range = `${sorted[0]}-${sorted[sorted.length - 1]}`;
    map.set(`${section}-${row}-${range}`, {
      section,
      row,
      seats: sorted,
      price,
    });
  }
  return map;
}

test("reports seats added to an existing row", () => {
  const before = rowMap([{ section: "112", row: "F", seats: [1, 2, 3, 4], price: 180 }]);
  const after = rowMap([{ section: "112", row: "F", seats: [1, 2, 3, 4, 5, 6], price: 180 }]);

  const { drops } = diffSeatDrops(before, after);

  assert.equal(drops.length, 1);
  assert.deepEqual(drops[0].newSeats, ["5", "6"]);
  assert.equal(drops[0].newSeatCount, 2);
  assert.equal(drops[0].totalSeatsInRow, 6);
  assert.equal(drops[0].isNewListing, false);
});

test("reports a section/row that had no inventory as a new listing", () => {
  const before = rowMap([{ section: "112", row: "F", seats: [1, 2], price: 180 }]);
  const after = rowMap([
    { section: "112", row: "F", seats: [1, 2], price: 180 },
    { section: "118", row: "A", seats: [12, 13], price: 240 },
  ]);

  const { drops } = diffSeatDrops(before, after);

  assert.equal(drops.length, 1);
  assert.equal(drops[0].section, "118");
  assert.equal(drops[0].isNewListing, true);
  assert.deepEqual(drops[0].newSeats, ["12", "13"]);
  assert.equal(drops[0].listPrice, 240);
});

test("a reprice is not a drop, even though it re-keys the row", () => {
  const before = rowMap([{ section: "112", row: "F", seats: [1, 2, 3, 4], price: 180 }]);
  const after = rowMap([{ section: "112", row: "F", seats: [1, 2, 3, 4], price: 210 }]);

  const { drops } = diffSeatDrops(before, after);

  assert.equal(drops.length, 0);
});

test("seats being removed is not a drop, and is remembered", () => {
  const before = rowMap([{ section: "112", row: "F", seats: [1, 2, 3, 4], price: 180 }]);
  const after = rowMap([{ section: "112", row: "F", seats: [1, 2], price: 180 }]);

  const { drops, removedSeats } = diffSeatDrops(before, after);

  assert.equal(drops.length, 0);
  assert.deepEqual(removedSeats.sort(), ["112|F|3", "112|F|4"]);
});

test("merges seats across price tiers sharing one section/row", () => {
  // Same row split into two listings at different prices — a real GA pattern
  const before = rowMap([
    { section: "GA", row: "1", seats: [1, 2], price: 100 },
    { section: "GA", row: "1", seats: [3, 4], price: 150 },
  ]);
  const after = rowMap([
    { section: "GA", row: "1", seats: [1, 2], price: 100 },
    { section: "GA", row: "1", seats: [3, 4, 5], price: 150 },
  ]);

  const { drops } = diffSeatDrops(before, after);

  assert.equal(drops.length, 1);
  assert.deepEqual(drops[0].newSeats, ["5"]);
  assert.equal(drops[0].listPrice, 150, "price comes from the listing holding the new seat");
  assert.equal(drops[0].totalSeatsInRow, 5);
});

test("seats moving between rows credit only the receiving row", () => {
  const before = rowMap([{ section: "A", row: "1", seats: [1, 2, 3], price: 90 }]);
  const after = rowMap([
    { section: "A", row: "1", seats: [1], price: 90 },
    { section: "A", row: "2", seats: [2, 3], price: 90 },
  ]);

  const { drops, removedSeats } = diffSeatDrops(before, after);

  assert.equal(drops.length, 1);
  assert.equal(drops[0].row, "2");
  assert.deepEqual(drops[0].newSeats, ["2", "3"]);
  assert.deepEqual(removedSeats.sort(), ["A|1|2", "A|1|3"]);
});

test("first-ever scrape is a baseline, not a drop", () => {
  const before = new Map();
  const after = rowMap([{ section: "112", row: "F", seats: [1, 2, 3], price: 180 }]);

  const { drops, isBaseline } = diffSeatDrops(before, after);

  assert.equal(isBaseline, true);
  assert.equal(drops.length, 1, "the diff still reports them…");
  // …and recordSeatDrops discards them on isBaseline, so nothing is persisted.
});

test("sorts new seats numerically, not lexicographically", () => {
  const before = rowMap([{ section: "A", row: "1", seats: [1], price: 90 }]);
  const after = rowMap([{ section: "A", row: "1", seats: [1, 2, 10, 9], price: 90 }]);

  const { drops } = diffSeatDrops(before, after);

  assert.deepEqual(drops[0].newSeats, ["2", "9", "10"]);
});

test("no change produces nothing", () => {
  const rows = [
    { section: "112", row: "F", seats: [1, 2, 3], price: 180 },
    { section: "118", row: "A", seats: [7], price: 240 },
  ];
  const { drops, removedSeats } = diffSeatDrops(rowMap(rows), rowMap(rows));

  assert.equal(drops.length, 0);
  assert.equal(removedSeats.length, 0);
});

test("flags listings missing section/row instead of silently skipping them", () => {
  // Guards the scraperManager wiring: if section/row ever stop being written
  // into the rowMaps, detection degrades to a no-op — this makes it visible.
  const before = new Map([["k1", { section: "A", row: "1", seats: ["1"], price: 90 }]]);
  const after = new Map([
    ["k1", { section: "A", row: "1", seats: ["1", "2"], price: 90 }],
    ["k2", { seats: ["7"], price: 90 }], // no section/row
  ]);

  const { drops, skipped } = diffSeatDrops(before, after);

  assert.equal(skipped, 1);
  assert.equal(drops.length, 1, "well-formed listings are still detected");
  assert.deepEqual(drops[0].newSeats, ["2"]);
});
