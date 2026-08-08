// DOES REUSING ONE TOKEN ACROSS MANY IPs CAUSE THE HARD BLOCK?
//
// proxyHealthSweep.mjs called 90% of the pool "hard blocked" — including
// 142.173.45.176, which jarRateRamp.mjs was at that same moment driving at 97% success.
// Both cannot be true of the IP. The one thing the sweep did differently is what every
// diagnostic in this repo does (probeJar, sweepProxies, auditJars): replay ONE jar across
// many exit IPs. That is also exactly what production does — 6-8 farm tokens spread over
// 60+ pages on 60+ proxies.
//
// So the "burned proxy pool" and the token-sharing pattern are confounded, and this
// separates them. Same IPs, same low rate (one call each, spaced), only the token
// assignment differs:
//
//   ARM DISTINCT : each IP gets its OWN token          -> isolates the IP
//   ARM SHARED   : every IP replays ONE token          -> isolates cross-IP reuse
//
// The IPs are drawn from the ones the sweep just condemned, so:
//   DISTINCT passes, SHARED fails -> the IPs were never blocked; token fan-out is the
//                                    trigger, and prod's jar-sharing is self-inflicted.
//   both fail                     -> the IPs really are burned; buy new ones.
//   both pass                     -> the sweep's concurrency was the artifact, not sharing.
//
// Calls are spaced 6s apart and interleaved between arms, so neither arm can be the one
// that "went first" and neither can trip a rate limit.
//
// Read-only. Usage: node scripts/tokenIpBinding.mjs [ipsPerArm] [blockedIpFile]
import "dotenv/config";
import mongoose from "mongoose";
import { Camoufox } from "camoufox-js";
import proxyArray, { loadProxies } from "../helpers/proxy.js";

const PER_ARM = parseInt(process.argv[2], 10) || 6;
const SPACING_MS = 6000;

const EVENTS = ["15006331B08D75C0", "0A006331DC273765", "0600632E29196B3E", "0800632CA3272367"];
const facetsUrl = (id) =>
  `https://services.ticketmaster.com/api/ismds/event/${id}/facets` +
  `?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description` +
  `&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places` +
  `&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us` +
  `&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse` +
  `&_=${Date.now()}&t=${Math.random().toString(36).slice(2)}`;

const norm = (cookies) => cookies.map((c) => ({
  name: c.name, value: c.value, domain: c.domain || ".ticketmaster.com", path: c.path || "/",
  expires: c.expires || -1, httpOnly: !!c.httpOnly, secure: c.secure !== false, sameSite: c.sameSite || "Lax",
}));

await mongoose.connect(process.env.MONGODB_URI);
await loadProxies();

const jars = await mongoose.connection.db.collection("seed_jars")
  .find({ status: "healthy", expiresAt: { $gt: new Date() } })
  .sort({ mintedAt: -1 }).toArray();
if (jars.length < 2) { console.error(`need >=2 healthy jars, have ${jars.length}`); process.exit(2); }

// DISTINCT arm needs one jar per IP; SHARED arm needs exactly one, kept out of DISTINCT
// so the two arms never contaminate each other.
const perArm = Math.min(PER_ARM, jars.length - 1);
const distinctJars = jars.slice(0, perArm);
const sharedJar = jars[jars.length - 1];
console.log(`DISTINCT arm: ${perArm} jars (slots ${distinctJars.map((j) => j.slot).join(",")})`);
console.log(`SHARED   arm: 1 jar  (slot ${sharedJar.slot}, useCount ${sharedJar.useCount ?? 0}) replayed on every IP\n`);

const pool = (proxyArray.proxies || []).filter((p) => p && p.proxy);
const shuffled = pool.slice().sort(() => Math.random() - 0.5).slice(0, perArm * 2);
const ipsDistinct = shuffled.slice(0, perArm);
const ipsShared = shuffled.slice(perArm, perArm * 2);

const browser = await Camoufox({ headless: true, humanize: false, geoip: false });

async function probe(p, jarDoc) {
  const [host, port] = p.proxy.split(":");
  let ctx = null;
  try {
    ctx = await browser.newContext({
      ignoreHTTPSErrors: true, bypassCSP: true,
      proxy: { server: `http://${host}:${port}`, username: p.username, password: p.password },
    });
    await ctx.addCookies(norm(jarDoc.cookies));
    const r = await ctx.request.get(facetsUrl(EVENTS[Math.floor(Math.random() * EVENTS.length)]), {
      headers: { "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe", Accept: "application/json", "tmps-correlation-id": `bind-${Date.now()}` },
      timeout: 30000,
    });
    const s = r.status();
    if (s === 200) return "200";
    const b = (await r.text().catch(() => "")).slice(0, 60).replace(/\s+/g, " ");
    return /dynamic_block/.test(b) ? "403 dynamic" : /"block"/.test(b) ? "403 block" : `${s} ${b.slice(0, 30)}`;
  } catch (e) {
    return `CONN-FAIL`;
  } finally {
    if (ctx) await ctx.close().catch(() => {});
  }
}

const tally = { distinct: {}, shared: {} };
const bump = (arm, v) => { tally[arm][v] = (tally[arm][v] || 0) + 1; };

console.log("#   arm       IP                        jar      verdict");
console.log("-".repeat(66));
for (let i = 0; i < perArm; i++) {
  // interleave so ordering / time-of-day cannot favour one arm
  const d = await probe(ipsDistinct[i], distinctJars[i]);
  bump("distinct", d);
  console.log(`${String(i + 1).padStart(3)} DISTINCT  ${ipsDistinct[i].proxy.padEnd(24)}  slot${String(distinctJars[i].slot).padEnd(4)} ${d}`);
  await new Promise((r) => setTimeout(r, SPACING_MS));

  const s = await probe(ipsShared[i], sharedJar);
  bump("shared", s);
  console.log(`${String(i + 1).padStart(3)} SHARED    ${ipsShared[i].proxy.padEnd(24)}  slot${String(sharedJar.slot).padEnd(4)} ${s}`);
  await new Promise((r) => setTimeout(r, SPACING_MS));
}

const pass = (t) => t["200"] || 0;
console.log("\n--- RESULT ---");
console.log(`  DISTINCT (one token per IP) : ${pass(tally.distinct)}/${perArm} passed   ${JSON.stringify(tally.distinct)}`);
console.log(`  SHARED   (one token, N IPs) : ${pass(tally.shared)}/${perArm} passed   ${JSON.stringify(tally.shared)}`);
const dp = pass(tally.distinct) / perArm, sp = pass(tally.shared) / perArm;
console.log(
  dp > 0.6 && sp < 0.4 ? "\n  -> TOKEN FAN-OUT is the trigger. The IPs are fine; replaying one jar across many exit IPs is what TM blocks."
  : dp < 0.4 && sp < 0.4 ? "\n  -> Both arms fail: these exit IPs really are blocked, independent of how tokens are assigned."
  : dp > 0.6 && sp > 0.6 ? "\n  -> Both arms pass: neither the IPs nor sharing is the problem at this rate — the earlier sweep's CONCURRENCY was the artifact."
  : "\n  -> Mixed. Re-run with more IPs per arm before concluding."
);

await browser.close().catch(() => {});
await mongoose.disconnect().catch(() => {});
process.exit(0);
