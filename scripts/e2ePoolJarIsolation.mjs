// END-TO-END: does the real page pool now give every page its OWN jar, and still scrape?
//
// The unit tests cover the lease and the pacing in isolation. This drives the actual
// BrowserPagePool the scraper uses — real proxies, real farm jars, real facets calls —
// and asserts the property the whole fix rests on:
//
//   every bound page must hold a DIFFERENT tmpt.
//
// Before the lease, readFarmJar() handed the same 6-8 jars round-robin to every page, so a
// pool of 3 would routinely bind 3 pages to 1 or 2 tokens; each of those tokens then saw
// several exit IPs from this instance alone, and dozens across the fleet, and was revoked
// at the eleventh (scripts/jarIpBudget.mjs).
//
// Leases are released on cleanup, so this leaves the farm as it found it.
//
// Usage: node scripts/e2ePoolJarIsolation.mjs [poolSize] [events]
import "dotenv/config";
import mongoose from "mongoose";
import proxyArray, { loadProxies } from "../helpers/proxy.js";

const POOL = process.argv[2] || "3";
const N_EVENTS = parseInt(process.argv[3], 10) || 6;
process.env.POOL_SIZE = POOL;

const { browserPagePool: pool } = await import("../browser-cookies.js");

const EVENTS = [
  "15006331B08D75C0", "0A006331DC273765", "0600632E29196B3E",
  "0800632CA3272367", "17006441A531D6B5", "1E00644AE8CAE766",
];
const facetsUrl = (id) =>
  `https://services.ticketmaster.com/api/ismds/event/${id}/facets` +
  `?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description` +
  `&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places` +
  `&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us` +
  `&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse` +
  `&_=${Date.now()}&t=${Math.random().toString(36).slice(2)}`;

await mongoose.connect(process.env.MONGODB_URI);
await loadProxies();
const coll = mongoose.connection.db.collection("seed_jars");
const leasedBefore = await coll.countDocuments({ leaseUntil: { $gt: new Date() } });

let pass = 0, fail = 0;
const check = (name, ok, detail = "") => {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${name}${detail ? ` — ${detail}` : ""}`);
  ok ? pass++ : fail++;
};

console.log(`initialising the real pool at POOL_SIZE=${POOL}…\n`);
const t0 = Date.now();
await pool.init(null, null, EVENTS[0]);
console.log(`\npool init took ${((Date.now() - t0) / 1000).toFixed(1)}s\n`);

// --- the property under test
const metas = [...pool._pageMeta.values()];
const tmpts = metas.map((m) => m.tmpt).filter(Boolean);
const jarIds = metas.map((m) => m.jarId && String(m.jarId)).filter(Boolean);
const proxies = metas.map((m) => (m.proxy ? m.proxy.proxy : "direct"));

console.log("bound pages:");
metas.forEach((m, i) => console.log(
  `  page ${i + 1}  ${String(m.proxy ? m.proxy.proxy : "direct").padEnd(24)} tmpt ${String(m.tmpt || "none").slice(0, 14)}…  jar ${String(m.jarId || "-").slice(-12)}`
));
console.log("");

check("pool bound at least one page", metas.length > 0, `${metas.length} page(s)`);
check("every page holds a DISTINCT token", new Set(tmpts).size === tmpts.length,
  `${new Set(tmpts).size} distinct of ${tmpts.length}`);
check("every page holds a DISTINCT jar lease", new Set(jarIds).size === jarIds.length,
  `${new Set(jarIds).size} distinct of ${jarIds.length}`);
check("every page sits on its own exit IP", new Set(proxies).size === proxies.length,
  `${new Set(proxies).size} distinct of ${proxies.length}`);

// --- and it still scrapes
console.log("\nsubmitting real facets requests through the pool…");
const results = await Promise.all(
  EVENTS.slice(0, N_EVENTS).map((id) =>
    // The FULL header set the scraper sends. Minimal headers return 400 even on a
    // perfectly good session — tmps-correlation-id and x-request-id are what flip it.
    pool.submitRequests([{ url: facetsUrl(id), headers: {
      accept: "application/json",
      "x-api-key": "b462oi7fic6pehcdkzony5bxhe",
      "tmps-correlation-id": "e2e" + Math.floor(Math.random() * 1e9),
      "x-request-id": "e2e" + Math.floor(Math.random() * 1e9),
    } }])
      .then((r) => r[0])
      .catch((e) => ({ success: false, error: e.message }))
  )
);
const ok = results.filter((r) => r && r.success).length;
results.forEach((r, i) => console.log(
  `  ${EVENTS[i]}  ${r && r.success ? `200 (${r.data?.facets?.length ?? 0} facets)` : `FAILED ${String(r && (r.status || r.error)).slice(0, 50)}`}`
));
check("the pool still returns live facet data", ok > 0, `${ok}/${results.length} events`);

// --- and it gives the jars back
await pool.cleanup();
await new Promise((r) => setTimeout(r, 1500)); // release is fire-and-forget
const leasedAfter = await coll.countDocuments({ leaseUntil: { $gt: new Date() } });
check("cleanup released every lease", leasedAfter <= leasedBefore, `${leasedBefore} before, ${leasedAfter} after`);

console.log(`\n${pass} passed, ${fail} failed`);
await mongoose.disconnect().catch(() => {});
process.exit(fail ? 1 : 0);
