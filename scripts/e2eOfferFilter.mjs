// End-to-end: run the REAL pipeline (GenerateNanoPlaces -> AttachRowSection) over
// a saved facets dump + live map, and diff listings with the offer filter applied
// vs. bypassed. Answers "what listings does this change actually remove?"
//
// Usage: node scripts/e2eOfferFilter.mjs <eventId>
import "dotenv/config";
import * as fs from "fs";
import mongoose from "mongoose";
import GenerateNanoPlaces from "../helpers/seats.js";
import { AttachRowSection } from "../helpers/seatBatch.js";
import { initApiBrowserContext, cleanupApiBrowser } from "../browser-cookies.js";

const eventId = process.argv[2];
if (!eventId) {
  console.error("usage: node scripts/e2eOfferFilter.mjs <eventId>");
  process.exit(1);
}
const dumpPath = `./debug/facets_${eventId}.json`;
if (!fs.existsSync(dumpPath)) {
  console.error(`missing ${dumpPath} — run checkOfferFilter.mjs ${eventId} first`);
  process.exit(1);
}
const DataFacets = JSON.parse(fs.readFileSync(dumpPath, "utf8"));

await mongoose.connect(process.env.MONGODB_URI);
const jarDocs = await mongoose.connection.db
  .collection("seed_jars")
  .find({ status: "healthy", expiresAt: { $gt: new Date() } })
  .limit(1)
  .toArray();
const jar = jarDocs[0]?.cookies;

const mapUrl =
  `https://mapsapi.tmol.io/maps/geometry/3/event/${eventId}/placeDetailNoKeys` +
  `?useHostGrids=true&app=CCP&sectionLevel=true&systemId=HOST`;
const { page } = await initApiBrowserContext(null, jar);
const mapResp = await page.context().request.get(mapUrl, { timeout: 30000 });
if (mapResp.status() !== 200) {
  console.error(`map fetch failed: ${mapResp.status()}`);
  process.exit(2);
}
const DataMap = await mapResp.json();
await cleanupApiBrowser().catch(() => {});

const offers = DataFacets?._embedded?.offer || [];
const event = { eventId, eventMappingId: eventId, inHandDate: "2026-10-22" };
const descriptions = DataFacets?._embedded?.description || [];
const places = GenerateNanoPlaces(DataFacets?.facets);

const isHold = (o) =>
  typeof o?.ticketTypeUnsoldQualifier === "string" &&
  /HOLD$/i.test(o.ticketTypeUnsoldQualifier);
const isPackage = (o) =>
  typeof o?.description === "string" && /package/i.test(o.description);

// "Filter off" is simulated by removing the offending fields from the offers, so
// the exact same seatBatch code path runs in both arms — the only difference is
// what the two new rules can see.
const neutered = offers.map((o) =>
  isHold(o) || isPackage(o)
    ? { ...o, ticketTypeUnsoldQualifier: null, description: "Standard Ticket" }
    : o,
);

const withFilter = AttachRowSection(places, DataMap, offers, event, descriptions);
const withoutFilter = AttachRowSection(places, DataMap, neutered, event, descriptions);

const key = (l) => `${l.section}|${l.row}|${l.seats.join(",")}`;
const keptKeys = new Set(withFilter.map(key));
const removed = withoutFilter.filter((l) => !keptKeys.has(key(l)));

const qty = (arr) => arr.reduce((n, l) => n + (l.inventory?.quantity || 0), 0);

console.log(`\nevent ${eventId}`);
console.log(`  offers: ${offers.length}  (hold/package: ${offers.filter((o) => isHold(o) || isPackage(o)).length})`);
console.log(`  listings WITHOUT filter: ${withoutFilter.length}  (${qty(withoutFilter)} seats)`);
console.log(`  listings WITH    filter: ${withFilter.length}  (${qty(withFilter)} seats)`);
console.log(`  REMOVED by filter:       ${removed.length} listings (${qty(removed)} seats)`);

const bySection = {};
for (const l of removed) bySection[l.section] = (bySection[l.section] || 0) + 1;
console.log(`\n  removed listings by section:`);
for (const [s, n] of Object.entries(bySection).sort((a, b) => b[1] - a[1])) {
  console.log(`    ${s.padEnd(12)} ${n}`);
}

// Sections that vanish entirely vs. sections that merely shrink.
const secWith = new Set(withFilter.map((l) => l.section));
const secWithout = new Set(withoutFilter.map((l) => l.section));
const gone = [...secWithout].filter((s) => !secWith.has(s)).sort();
console.log(`\n  sections fully removed: ${gone.length ? gone.join(", ") : "(none)"}`);
console.log(`  sections still present: ${secWith.size}`);

console.log(`\n  sample removed listings:`);
for (const l of removed.slice(0, 8)) {
  console.log(`    ${l.section} row ${l.row} seats ${l.seats.join(",")} — $${l.inventory?.listPrice}`);
}

await mongoose.disconnect().catch(() => {});
