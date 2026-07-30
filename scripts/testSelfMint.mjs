// Test the SELF_MINT fallback: with the cookie farm dry, does a pool page mint its
// own tmpt on its proxy, validate against facets, and share the jar with siblings?
//
// The farm is simulated as DOWN by stubbing the `seed_jars` collection to return
// nothing — no DB writes, nothing else is touched.
//
// Usage:
//   SELF_MINT=1 node scripts/testSelfMint.mjs [eventId]   # expect self-mint
//   SELF_MINT=0 node scripts/testSelfMint.mjs [eventId]   # control: expect a clean bail
import "dotenv/config";
import mongoose from "mongoose";
import { loadProxies } from "../helpers/proxy.js";
import { browserPagePool } from "../browser-cookies.js";

const eventId = process.argv[2] || "0A006396BA615BBE";
process.env.POOL_SIZE = process.env.TEST_POOL_SIZE || "2";

await mongoose.connect(process.env.MONGODB_URI);
await loadProxies();

// ── Simulate a dry farm ────────────────────────────────────────────────────────
// readFarmJar() queries seed_jars; hand it an empty result set so it returns null,
// exactly as it does when the farm is down or every jar is over budget.
const DRY = {
  find: () => ({ sort: () => ({ toArray: async () => [] }), toArray: async () => [] }),
  findOne: async () => null,
  updateOne: async () => ({}),
  updateMany: async () => ({}),
  bulkWrite: async () => ({}),
  countDocuments: async () => 0,
};
const db = mongoose.connection.db;
const realCollection = db.collection.bind(db);
db.collection = (name, ...rest) =>
  name === "seed_jars" ? DRY : realCollection(name, ...rest);

const selfMint = process.env.SELF_MINT === "1";
console.log(
  `\n=== SELF_MINT=${selfMint ? 1 : 0} | engine=${process.env.BROWSER_ENGINE || "camoufox"} ` +
    `| POOL_SIZE=${process.env.POOL_SIZE} | event=${eventId} | farm=DRY (stubbed) ===\n`
);

const t0 = Date.now();
let initErr = null;
try {
  await browserPagePool.init(null, null, eventId);
} catch (e) {
  initErr = e;
}
const secs = ((Date.now() - t0) / 1000).toFixed(1);

const jar = browserPagePool._seedJar || [];
const tmpt = (jar.find((c) => c.name === "tmpt") || {}).value;
console.log(`\n--- RESULT after ${secs}s ---`);
console.log(`init error      : ${initErr ? initErr.message : "(none)"}`);
console.log(`pages bound     : ${browserPagePool.pages.length}/${browserPagePool.size}`);
console.log(`distinct proxies: ${browserPagePool._usedProxies.size}`);
console.log(
  `self-minted jar : ${jar.length ? `${jar.length} cookies, tmpt=${tmpt ? tmpt.slice(0, 16) + "…" : "NONE"}` : "(not cached)"}`
);

// ── Proof: does a bound page actually get facets 200 with its own session? ──────
if (browserPagePool.pages.length) {
  const url =
    `https://services.ticketmaster.com/api/ismds/event/${eventId}/facets` +
    `?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description` +
    `&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places` +
    `&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us` +
    `&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse` +
    `&_=${Date.now()}&t=${Math.random().toString(36).slice(2)}`;
  // Retry over ~60s: a freshly minted tmpt can 403 until EPS finishes scoring the
  // session, so one immediate 403 doesn't prove the jar is dead.
  const waits = [0, 5000, 10000, 15000, 30000];
  for (let i = 0; i < browserPagePool.pages.length; i++) {
    const page = browserPagePool.pages[i];
    let elapsed = 0;
    for (const w of waits) {
      if (w) await new Promise((r) => setTimeout(r, w));
      elapsed += w;
      try {
        const resp = await page.context().request.get(url.replace(/&_=\d+/, `&_=${Date.now()}`), {
          headers: {
            "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe",
            Accept: "application/json",
            "tmps-correlation-id": `selfmint-${Date.now()}-${i}`,
          },
          timeout: 25000,
        });
        const body = resp.status() === 200 ? await resp.json().catch(() => null) : null;
        console.log(
          `page ${i + 1} @+${elapsed / 1000}s: facets ${resp.status()}` +
            (body ? ` — ${body?.facets?.length ?? 0} facets, ${body?._embedded?.offer?.length ?? 0} offers` : "")
        );
        if (resp.status() === 200) break;
      } catch (e) {
        console.log(`page ${i + 1} @+${elapsed / 1000}s: facets FAILED — ${e.message}`);
      }
    }
  }
}

await browserPagePool.cleanup().catch(() => {});
await mongoose.disconnect().catch(() => {});
process.exit(0);
