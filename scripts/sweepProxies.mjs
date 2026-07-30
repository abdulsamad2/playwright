// How many read proxies is TM actually blocking right now?
//
// The trap this avoids: a jar can die mid-sweep, after which every proxy looks
// blocked. So a 403 is never trusted on its own — it's re-checked against the SAME
// jar on the direct IP. Jar good + proxy 403 = the proxy is blocked. Jar 403 direct
// too = the jar died, so we swap in a fresh one and retry that proxy.
//
// Read-only: reports counts, writes nothing.
//
// Usage: node scripts/sweepProxies.mjs [sampleSize] [eventId]
import "dotenv/config";
import mongoose from "mongoose";
import proxyArray, { loadProxies } from "../helpers/proxy.js";
import { initApiBrowserContext, cleanupApiBrowser } from "../browser-cookies.js";

const sampleSize = parseInt(process.argv[2], 10) || 25;
const eventId = process.argv[3] || "0A006396BA615BBE";

await mongoose.connect(process.env.MONGODB_URI);
await loadProxies();
const sj = mongoose.connection.db.collection("seed_jars");

const url = () =>
  `https://services.ticketmaster.com/api/ismds/event/${eventId}/facets` +
  `?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description` +
  `&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places` +
  `&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us` +
  `&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse` +
  `&_=${Date.now()}&t=${Math.random().toString(36).slice(2)}`;

async function hit(cookies, proxy) {
  try {
    const { page } = await initApiBrowserContext(proxy, cookies);
    const resp = await page.context().request.get(url(), {
      headers: {
        "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe",
        Accept: "application/json",
        "tmps-correlation-id": `sweep-${Date.now()}`,
      },
      timeout: 25000,
    });
    return resp.status();
  } catch {
    return -1; // connection/tunnel failure — not a TM verdict
  } finally {
    await cleanupApiBrowser().catch(() => {});
  }
}

// Find a jar that TM currently accepts on the direct IP.
async function freshJar(exclude = new Set()) {
  const docs = await sj
    .find({ expiresAt: { $gt: new Date() } })
    .sort({ mintedAt: -1, createdAt: -1 })
    .limit(12)
    .toArray();
  for (const d of docs) {
    const key = String(d._id);
    if (exclude.has(key)) continue;
    if ((await hit(d.cookies, null)) === 200) {
      console.log(`[jar] using slot${d.slot} (status=${d.status}, useCount=${d.useCount ?? 0}) — verified 200 direct`);
      return { key, cookies: d.cookies };
    }
    exclude.add(key);
  }
  return null;
}

const tried = new Set();
let jar = await freshJar(tried);
if (!jar) {
  console.error("no jar in seed_jars is accepted by TM on the direct IP — cannot attribute blame");
  await mongoose.disconnect();
  process.exit(2);
}

const all = (proxyArray.proxies || []).filter((p) => p && p.proxy);
const sample = all.sort(() => Math.random() - 0.5).slice(0, sampleSize);
console.log(`sweeping ${sample.length} of ${all.length} proxies\n`);

const res = { ok: [], blocked: [], connErr: [] };
for (let i = 0; i < sample.length; i++) {
  const p = sample[i];
  let s = await hit(jar.cookies, p);

  if (s === 403) {
    // Blame check: is the jar still alive on the clean IP?
    if ((await hit(jar.cookies, null)) !== 200) {
      tried.add(jar.key);
      const next = await freshJar(tried);
      if (!next) {
        console.log("[jar] ran out of TM-accepted jars — stopping early");
        break;
      }
      jar = next;
      s = await hit(jar.cookies, p); // retry this proxy with the good jar
    }
  }

  const bucket = s === 200 ? "ok" : s === -1 ? "connErr" : "blocked";
  res[bucket].push(p.proxy);
  console.log(`${String(i + 1).padStart(3)}/${sample.length}  ${p.proxy.padEnd(24)} ${s === -1 ? "CONN-FAIL" : s}`);
}

const n = res.ok.length + res.blocked.length;
console.log(`\n--- RESULT (${n} proxies given a TM verdict) ---`);
console.log(`  passing  : ${res.ok.length}`);
console.log(`  blocked  : ${res.blocked.length}`);
console.log(`  conn-fail: ${res.connErr.length} (not counted — never reached TM)`);
if (n) {
  const rate = res.blocked.length / n;
  console.log(`  blocked rate: ${(rate * 100).toFixed(0)}%  →  ~${Math.round(rate * all.length)} of ${all.length} pool proxies`);
  console.log(`  usable estimate: ~${Math.round((1 - rate) * all.length)} proxies`);
}
if (res.ok.length) console.log(`\npassing proxies:\n  ${res.ok.join("\n  ")}`);

await mongoose.disconnect().catch(() => {});
process.exit(0);
