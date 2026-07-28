// Offer-filter tests for helpers/seatBatch.js — HOLD and PACKAGE exclusions.
//
// These drive the real AttachRowSection export rather than re-testing a copy of
// the regexes, so a change to the filter block is actually caught. The module
// only imports moment + fs, so there is no DB or network to stub.
//
// Run: node --test test/
import test from "node:test";
import assert from "node:assert/strict";

import { AttachRowSection } from "../helpers/seatBatch.js";

// One section / one row / two consecutive seats. Section name is deliberately
// free of WC/ADA/WHEELCHAIR/COMPANION etc. so GLOBAL_FILTERS.excludeAccessibility
// does not drop the fixture before the offer filter is reached.
const mapData = {
  pages: [
    {
      segments: [
        {
          segments: [
            {
              name: "FLOOR1",
              segments: [
                {
                  name: "A",
                  placesNoKeys: [
                    ["seat-1", "1"],
                    ["seat-2", "2"],
                  ],
                },
              ],
            },
          ],
        },
      ],
    },
  ],
};

const event = { eventMappingId: "evt-1", inHandDate: "2026-08-01" };
const descriptions = [{ descriptionId: "d1", descriptions: ["Standard Ticket"] }];

// accessibility must be a string, not undefined: CreateInventoryAndLine calls
// data?.accessibility.includes(...) unguarded and would throw on undefined.
const listings = [
  {
    places: ["seat-1", "seat-2"],
    offerId: "off-1",
    accessibility: "",
    descriptionId: "d1",
    attributes: [],
  },
];

// inventoryType "Standard" matches GLOBAL_FILTERS.inventoryType, so the item
// survives the global-filter stage and reaches the offer filter under test.
function runFilter(offerOverrides) {
  const offer = {
    offerId: "off-1",
    name: "Standard Ticket",
    description: "Standard Ticket",
    inventoryType: "Standard",
    faceValue: 100,
    charges: [],
    ...offerOverrides,
  };
  return AttachRowSection(listings, mapData, [offer], event, descriptions);
}

// Control. If this ever fails, every "is excluded" assertion below becomes
// vacuous — a broken fixture would return [] and pass them all.
test("a plain offer is kept (fixture sanity)", () => {
  const result = runFilter({});
  assert.equal(result.length, 1, "plain offer should produce one listing");
  assert.equal(result[0].section, "FLOOR1");
  assert.deepEqual(result[0].seats, [1, 2]);
});

test("HOLD qualifier is excluded", async (t) => {
  for (const qualifier of [
    "VIP5HOLD",
    "2PACKHOLD",
    "222PA1HOLD",
    "22BOGOHOLD",
    "PRODUCTIONHOLD",
  ]) {
    await t.test(qualifier, () => {
      assert.deepEqual(
        runFilter({ ticketTypeUnsoldQualifier: qualifier }),
        [],
        `${qualifier} should be dropped`,
      );
    });
  }
});

test("HOLD qualifier match is case-insensitive", () => {
  assert.deepEqual(runFilter({ ticketTypeUnsoldQualifier: "vip5hold" }), []);
});

test("HOLD is anchored to the end of the qualifier", () => {
  // "HOLDBACK" contains HOLD but does not end in it, so /HOLD$/i keeps it.
  // Documents the anchor: widen the regex here if TM ships such a code.
  assert.equal(runFilter({ ticketTypeUnsoldQualifier: "HOLDBACK" }).length, 1);
});

test("non-string qualifier does not throw and is kept", () => {
  assert.equal(runFilter({ ticketTypeUnsoldQualifier: null }).length, 1);
  assert.equal(runFilter({ ticketTypeUnsoldQualifier: undefined }).length, 1);
});

test("PACKAGE in the offer description is excluded", async (t) => {
  for (const description of [
    "DEAD PRESIDENTS VIP PIT PACKAGE",
    "Platinum Package",
    "vip package",
  ]) {
    await t.test(description, () => {
      assert.deepEqual(
        runFilter({ description }),
        [],
        `"${description}" should be dropped`,
      );
    });
  }
});

test("HOLD and PACKAGE together are excluded", () => {
  assert.deepEqual(
    runFilter({
      ticketTypeUnsoldQualifier: "VIP5HOLD",
      description: "DEAD PRESIDENTS VIP PIT PACKAGE",
    }),
    [],
  );
});

// KNOWN GAP, not a passing requirement: the PACKAGE check reads description
// only, while every other package/4-pack exclusion in the block reads name.
// An offer named "...PACKAGE" with an unrelated description still gets listed.
// If the filter is widened to cover name, flip this to assert.deepEqual(..., []).
test("KNOWN GAP: PACKAGE in the offer name alone is NOT excluded", () => {
  const result = runFilter({
    name: "DEAD PRESIDENTS VIP PIT PACKAGE",
    description: "Standard Ticket",
  });
  assert.equal(
    result.length,
    1,
    "name-only PACKAGE currently slips through — see seatBatch.js offer filter",
  );
});
