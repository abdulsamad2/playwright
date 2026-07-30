// Double-check of "proxies, not cookies": run a full jar x egress matrix.
//
// Controls this adds over probeJar.mjs:
//   - proxies are tested BEFORE direct, so a 200 on direct can't be an ordering artifact
//   - a jar our DB marked status:"dead" is replayed too — if it 200s on a clean IP,
//     our "dead" bookkeeping was wrong and the token was never the problem
//   - the same proxy is retried with a different jar, isolating IP from token
//
// Usage: node scripts/probeMatrix.mjs [eventId] [goodProxy] [badProxy]
import "dotenv/config";
import mongoose from "mongoose";
import proxyArray, { loadProxies } from "../helpers/proxy.js";
import { initApiBrowserContext, cleanupApiBrowser } from "../browser-cookies.js";

const eventId = process.argv[2] || "0A006396BA615BBE";
const goodProxyArg = process.argv[3] || "149.143.130.25:14128";
const badProxyArg = process.argv[4] || "209.20.216.100:15260";

await mongoose.connect(process.env.MONGODB_URI);
await loadProxies();
const sj = mongoose.connection.db.collection("seed_jars");
const now = new Date();

const pick = async (status) =>
  (
    await sj
      .find({ status, expiresAt: { $gt: now } })
      .sort({ mintedAt: -1, createdAt: -1 })
      .limit(1)
      .toArray()
  )[0];

const healthy = await pick("healthy");
const deadJar = await pick("dead"); // marked dead by US, but not yet expired
if (!healthy && !deadJar) {
  console.error("no unexpired jars at all");
  await mongoose.disconnect();
  process.exit(2);
}

const jars = [
  healthy && { label: `HEALTHY jar (slot${healthy.slot}, useCount=${healthy.useCount ?? 0})`, cookies: healthy.cookies },
  deadJar && { label: `DEAD-flagged jar (slot${deadJar.slot}, useCount=${deadJar.useCount ?? 0})`, cookies: deadJar.cookies },
].filter(Boolean);

const byName = (s) => (proxyArray.proxies || []).find((p) => p.proxy === s);
const targets = [
  { label: `PROXY ${badProxyArg} (expected bad)`, proxy: byName(badProxyArg) },
  { label: `PROXY ${goodProxyArg} (expected good)`, proxy: byName(goodProxyArg) },
  { label: "DIRECT (this host IP)", proxy: null },
];

const url = () =>
  `https://services.ticketmaster.com/api/ismds/event/${eventId}/facets` +
  `?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description` +
  `&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places` +
  `&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us` +
  `&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse` +
  `&_=${Date.now()}&t=${Math.random().toString(36).slice(2)}`;

console.log(`event=${eventId} engine=${process.env.BROWSER_ENGINE || "camoufox"}\n`);

for (const jar of jars) {
  console.log(`--- ${jar.label} ---`);
  for (const t of targets) {
    if (t.proxy === undefined) {
      console.log(`  ${t.label.padEnd(42)} SKIPPED (not in the live proxy list)`);
      continue;
    }
    try {
      const { page } = await initApiBrowserContext(t.proxy, jar.cookies);
      const resp = await page.context().request.get(url(), {
        headers: {
          "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe",
          Accept: "application/json",
          "tmps-correlation-id": `matrix-${Date.now()}`,
        },
        timeout: 25000,
      });
      const s = resp.status();
      let detail = "";
      if (s === 200) {
        const b = await resp.json().catch(() => null);
        detail = ` — ${b?.facets?.length ?? 0} facets`;
      } else {
        detail = ` — ${(await resp.text().catch(() => "")).slice(0, 80)}`;
      }
      console.log(`  ${t.label.padEnd(42)} ${s}${detail}`);
    } catch (e) {
      console.log(`  ${t.label.padEnd(42)} FAILED — ${e.message.slice(0, 90)}`);
    } finally {
      await cleanupApiBrowser().catch(() => {});
    }
  }
  console.log("");
}

await mongoose.disconnect().catch(() => {});
process.exit(0);
