// HOW MANY EXIT IPs ARE ACTUALLY USABLE? — parallel, and it tells the two 403s apart.
//
// scripts/sweepProxies.mjs answers a similar question but launches a whole browser per
// proxy (≈10s each, so ~25 IPs before you give up) and records only the status code.
// That conflates the two very different 403s TM returns:
//
//   {"response":"block"}          returned both by a revoked TOKEN and by an over-driven IP
//   {"response":"dynamic_block"}  a rate trip, clears on its own once the rate drops
//
// Neither body means "burned IP" — that assumption is what this script originally got
// wrong, see below.
//
// CORRECTED 2026-08-09. The first version of this script reported 90% of the pool as hard
// blocked, and that was its own doing: it drove ONE jar across 120 exit IPs. A tmpt
// session is revoked permanently at the ELEVENTH distinct IP (scripts/jarIpBudget.mjs),
// so it measured exactly 10 × 200 and then 98 × 403 — the token dying, not the proxies.
// Re-probed with a fresh token, those same IPs answer 200.
//
// So each jar here is retired after IPS_PER_JAR (default 8, under the limit of 10) and the
// sweep moves to the next one. Sweep size is therefore bounded by jar supply, which is the
// honest constraint. Both 403 bodies are also transient — jarRateRamp.mjs drove an IP into
// {"response":"block"} and it answered 200 again 75s later — so neither is proof of a
// burned IP; only a repeated 403 at a low rate on a fresh token is.
//
// Read-only. Usage: node scripts/proxyHealthSweep.mjs [sampleSize] [concurrency]
import "dotenv/config";
import mongoose from "mongoose";
import { Camoufox } from "camoufox-js";
import proxyArray, { loadProxies } from "../helpers/proxy.js";

const SAMPLE = parseInt(process.argv[2], 10) || 120;
const CONC = parseInt(process.argv[3], 10) || 12;

const EVENTS = ["15006331B08D75C0", "0A006331DC273765", "0600632E29196B3E", "0800632CA3272367"];
const facetsUrl = (id) =>
  `https://services.ticketmaster.com/api/ismds/event/${id}/facets` +
  `?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description` +
  `&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places` +
  `&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us` +
  `&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse` +
  `&_=${Date.now()}&t=${Math.random().toString(36).slice(2)}`;

await mongoose.connect(process.env.MONGODB_URI);
await loadProxies();

// One tmpt survives ~10 distinct exit IPs. Stay under it.
const IPS_PER_JAR = parseInt(process.env.IPS_PER_JAR, 10) || 8;
const norm = (cookies) => cookies.map((c) => ({
  name: c.name, value: c.value, domain: c.domain || ".ticketmaster.com", path: c.path || "/",
  expires: c.expires || -1, httpOnly: !!c.httpOnly, secure: c.secure !== false, sameSite: c.sameSite || "Lax",
}));
const jarDocs = await mongoose.connection.db.collection("seed_jars")
  .find({ status: "healthy", expiresAt: { $gt: new Date() } })
  .sort({ mintedAt: -1 }).toArray();
if (!jarDocs.length) { console.error("no healthy jar to sweep with"); process.exit(2); }
const jars = jarDocs.map((d) => norm(d.cookies));

const all = (proxyArray.proxies || []).filter((p) => p && p.proxy);
const capacity = jars.length * IPS_PER_JAR;
const wanted = Math.min(SAMPLE, all.length);
const sample = all.slice().sort(() => Math.random() - 0.5).slice(0, Math.min(wanted, capacity));
console.log(`${jars.length} healthy jar(s) available, ${IPS_PER_JAR} IPs each -> capacity ${capacity} IPs`);
if (wanted > capacity) {
  console.log(`NOTE: asked for ${wanted} but only ${capacity} can be swept without burning tokens — sweeping ${sample.length}.`);
}
console.log(`sweeping ${sample.length} of ${all.length} proxies, ${CONC} at a time, ONE call each\n`);

const browser = await Camoufox({ headless: true, humanize: false, geoip: false });
const res = { ok: [], hardBlock: [], dynBlock: [], other: [], connFail: [] };
let done = 0;

async function check(p, jarIdx) {
  const [host, port] = p.proxy.split(":");
  let ctx = null;
  try {
    ctx = await browser.newContext({
      ignoreHTTPSErrors: true, bypassCSP: true,
      proxy: { server: `http://${host}:${port}`, username: p.username, password: p.password },
    });
    await ctx.addCookies(jars[jarIdx]);
    const r = await ctx.request.get(facetsUrl(EVENTS[done % EVENTS.length]), {
      headers: { "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe", Accept: "application/json", "tmps-correlation-id": `sw-${Date.now()}` },
      timeout: 30000,
    });
    const s = r.status();
    if (s === 200) { res.ok.push(p.proxy); return "200"; }
    const body = (await r.text().catch(() => "")).slice(0, 80).replace(/\s+/g, " ");
    if (/dynamic_block/.test(body)) { res.dynBlock.push(p.proxy); return `403 dynamic_block`; }
    if (/"block"/.test(body)) { res.hardBlock.push(p.proxy); return `403 block`; }
    res.other.push(p.proxy); return `${s} ${body.slice(0, 40)}`;
  } catch (e) {
    res.connFail.push(p.proxy);
    return `CONN-FAIL ${e.message.slice(0, 40)}`;
  } finally {
    if (ctx) await ctx.close().catch(() => {});
  }
}

// Index the queue so each proxy is permanently tied to one jar — a worker picking jars
// dynamically would let a single token drift across every IP again.
const queue = sample.map((p, i) => ({ p, jarIdx: Math.floor(i / IPS_PER_JAR) }));
await Promise.all(Array.from({ length: CONC }, async () => {
  while (queue.length) {
    const { p, jarIdx } = queue.shift();
    const verdict = await check(p, jarIdx);
    done++;
    if (done % 10 === 0 || !/200/.test(verdict)) {
      console.log(`${String(done).padStart(4)}/${sample.length}  ${p.proxy.padEnd(24)} ${verdict}`);
    }
  }
}));

const verdicted = res.ok.length + res.hardBlock.length + res.dynBlock.length + res.other.length;
console.log(`\n--- RESULT (${verdicted} IPs got a TM verdict, ${res.connFail.length} never reached TM) ---`);
console.log(`  200 OK              : ${res.ok.length}`);
console.log(`  403 block (real)    : ${res.hardBlock.length}`);
console.log(`  403 dynamic_block   : ${res.dynBlock.length}   <- transient rate trip, not a burned IP`);
console.log(`  other               : ${res.other.length}`);
console.log(`  conn-fail           : ${res.connFail.length}   <- proxy/tunnel problem, no TM verdict`);
if (verdicted) {
  const passRate = res.ok.length / verdicted;
  console.log(`\n  pass rate: ${(passRate * 100).toFixed(0)}%  ->  ~${Math.round(passRate * all.length)} usable of ${all.length} pool IPs`);
  console.log(`  hard-blocked: ${((res.hardBlock.length / verdicted) * 100).toFixed(0)}%  ->  ~${Math.round((res.hardBlock.length / all.length) * all.length)} IPs to actually replace`);
}
if (res.hardBlock.length) console.log(`\nhard-blocked IPs (first 20):\n  ${res.hardBlock.slice(0, 20).join("\n  ")}`);

await browser.close().catch(() => {});
await mongoose.disconnect().catch(() => {});
process.exit(0);
