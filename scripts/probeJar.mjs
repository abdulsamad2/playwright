// Is the jar dead, or is the IP blocked? Takes the freshest unexpired jar from
// seed_jars (regardless of our own status flag — that's our bookkeeping, not TM's)
// and replays the SAME jar against facets on several IPs: this host directly, then
// N proxies. Same cookies, different egress → isolates token validity from IP block.
//
// Usage: BROWSER_ENGINE=camoufox node scripts/probeJar.mjs [eventId] [proxyCount]
import "dotenv/config";
import mongoose from "mongoose";
import proxyArray, { loadProxies } from "../helpers/proxy.js";
import { initApiBrowserContext, cleanupApiBrowser } from "../browser-cookies.js";

const eventId = process.argv[2] || "0A006396BA615BBE";
const proxyCount = parseInt(process.argv[3], 10) || 3;

await mongoose.connect(process.env.MONGODB_URI);
await loadProxies();

// Freshest jar that hasn't expired, ignoring status — we want to know whether TM
// still accepts the token, not whether we already gave up on it.
const [doc] = await mongoose.connection.db
  .collection("seed_jars")
  .find({ expiresAt: { $gt: new Date() } })
  .sort({ mintedAt: -1, createdAt: -1 })
  .limit(1)
  .toArray();

if (!doc) {
  console.error("no unexpired jar in seed_jars at all");
  await mongoose.disconnect();
  process.exit(2);
}
const jar = doc.cookies;
const tmpt = (jar.find((c) => c.name === "tmpt") || {}).value;
const ageMin = Math.round((Date.now() - (doc.mintedAt || doc.createdAt)) / 60000);
console.log(
  `jar: slot=${doc.slot} status=${doc.status} useCount=${doc.useCount ?? "-"} ` +
    `age=${ageMin}min tmpt=${tmpt ? tmpt.slice(0, 16) + "…" : "NONE"} cookies=${jar.length}`
);
console.log(`engine: ${process.env.BROWSER_ENGINE || "camoufox"}   event: ${eventId}\n`);

const url = () =>
  `https://services.ticketmaster.com/api/ismds/event/${eventId}/facets` +
  `?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description` +
  `&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places` +
  `&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us` +
  `&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse` +
  `&_=${Date.now()}&t=${Math.random().toString(36).slice(2)}`;

async function probe(label, proxy) {
  try {
    const { page } = await initApiBrowserContext(proxy, jar);
    const resp = await page.context().request.get(url(), {
      headers: {
        "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe",
        Accept: "application/json",
        "tmps-correlation-id": `probe-${Date.now()}`,
      },
      timeout: 25000,
    });
    const status = resp.status();
    let detail = "";
    if (status === 200) {
      const b = await resp.json().catch(() => null);
      detail = ` — ${b?.facets?.length ?? 0} facets, ${b?._embedded?.offer?.length ?? 0} offers`;
    } else {
      detail = ` — body: ${(await resp.text().catch(() => "")).slice(0, 160)}`;
    }
    console.log(`${label.padEnd(28)} facets ${status}${detail}`);
  } catch (e) {
    console.log(`${label.padEnd(28)} FAILED — ${e.message.slice(0, 120)}`);
  } finally {
    await cleanupApiBrowser().catch(() => {});
  }
}

// 1) this host's own IP — no proxy in the path at all
await probe("DIRECT (this host IP)", null);

// 2) N proxies from the live Mongo list
const proxies = (proxyArray.proxies || []).filter((p) => p && p.proxy);
for (let i = 0; i < Math.min(proxyCount, proxies.length); i++) {
  const p = proxies[Math.floor(Math.random() * proxies.length)];
  await probe(`PROXY ${p.proxy}`, p);
}

await mongoose.disconnect().catch(() => {});
process.exit(0);
