// PER-IP RATE CEILING — how fast can ONE egress IP call facets before TM dynamic_blocks?
//
// jarBurnTest.mjs showed the 403 {"response":"dynamic_block"} is NOT token death: two
// different tokens on the same IP blocked at the same MOMENT (call 61 and call 22), and
// the surviving token recovered on its own ~40s after the aggregate rate dropped. So the
// governed resource is requests/minute per exit IP, not calls per jar.
//
// This measures that ceiling directly. One jar, one IP, rate stepped up stage by stage
// until dynamic_block appears, then idle to measure how long recovery takes.
//
// Runs on a POOL PROXY by default so it does not disturb anything measuring the host IP,
// and so the number we get is the one that actually governs production reads.
// Pass "direct" as the proxy arg to measure this host's own IP instead.
//
// Read-only w.r.t. Mongo. Usage: node scripts/jarRateRamp.mjs [proxy|direct] [outfile]
import "dotenv/config";
import fs from "fs";
import mongoose from "mongoose";
import proxyArray, { loadProxies } from "../helpers/proxy.js";
import { initApiBrowserContext, cleanupApiBrowser } from "../browser-cookies.js";

const PROXY_ARG = process.argv[2] || "auto";
const OUT = process.argv[3] || `/tmp/jarramp-${Date.now()}.jsonl`;

// rate (calls/min) held for `min` minutes, low to high
const STAGES = [
  { rpm: 10, min: 2 },
  { rpm: 20, min: 2 },
  { rpm: 30, min: 2 },
  { rpm: 45, min: 2 },
  { rpm: 60, min: 2 },
  { rpm: 90, min: 2 },
  { rpm: 120, min: 2 },
];

const EVENTS = [
  "15006331B08D75C0", "0A006331DC273765", "0600632E29196B3E", "0800632CA3272367",
  "17006441A531D6B5", "1E00644AE8CAE766", "08006317220953D2", "0200631CB2212963",
  "0D00646DB574C935", "21006375DFD22BFA", "06006375D990CD6C", "1B006482B88D3A9A",
];
const facetsUrl = (id) =>
  `https://services.ticketmaster.com/api/ismds/event/${id}/facets` +
  `?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description` +
  `&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places` +
  `&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us` +
  `&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse` +
  `&_=${Date.now()}&t=${Math.random().toString(36).slice(2)}`;

const T0 = Date.now();
const el = () => ((Date.now() - T0) / 1000).toFixed(1);
const out = fs.createWriteStream(OUT, { flags: "a" });
const rec = (o) => out.write(JSON.stringify({ t: +el(), ...o }) + "\n");
const say = (m) => console.log(`[${el().padStart(7)}s] ${m}`);

await mongoose.connect(process.env.MONGODB_URI);
await loadProxies();

const [doc] = await mongoose.connection.db.collection("seed_jars")
  .find({ status: "healthy", expiresAt: { $gt: new Date() } })
  .sort({ mintedAt: -1 }).limit(1).toArray();
if (!doc) { console.error("no healthy jar available"); process.exit(2); }
const jar = doc.cookies;
const ageMin = ((Date.now() - new Date(doc.mintedAt || doc.createdAt)) / 60000).toFixed(1);
say(`jar slot=${doc.slot} useCount=${doc.useCount ?? 0} age=${ageMin}min cookies=${jar.length}`);

let proxy = null;
if (PROXY_ARG !== "direct") {
  const pool = (proxyArray.proxies || []).filter((p) => p && p.proxy);
  proxy = PROXY_ARG === "auto" ? pool[Math.floor(Math.random() * pool.length)] : pool.find((p) => p.proxy === PROXY_ARG);
  if (!proxy) { console.error(`proxy ${PROXY_ARG} not in pool (${pool.length} available)`); process.exit(2); }
}
say(`egress: ${proxy ? proxy.proxy : "DIRECT (this host IP)"}`);
rec({ ev: "start", proxy: proxy ? proxy.proxy : "direct", jarSlot: doc.slot, jarAgeMin: +ageMin });

const { page } = await initApiBrowserContext(proxy, jar);
let i = 0;

async function call(stageRpm) {
  const id = EVENTS[i++ % EVENTS.length];
  const t = Date.now();
  try {
    const r = await page.context().request.get(facetsUrl(id), {
      headers: { "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe", Accept: "application/json", "tmps-correlation-id": `ramp-${Date.now()}` },
      timeout: 25000,
    });
    const status = r.status();
    const note = status === 200 ? "" : (await r.text().catch(() => "")).slice(0, 60).replace(/\s+/g, " ");
    rec({ ev: "call", rpm: stageRpm, status, ms: Date.now() - t, note });
    return { status, note };
  } catch (e) {
    rec({ ev: "call", rpm: stageRpm, status: "ERR", ms: Date.now() - t, note: e.message.slice(0, 60) });
    return { status: "ERR", note: e.message.slice(0, 60) };
  }
}

let blockedAtRpm = null;
console.log("\nrpm   calls  ok   403-dynamic  403-block  other   verdict");
console.log("-".repeat(70));

for (const st of STAGES) {
  const gap = 60000 / st.rpm;
  const until = Date.now() + st.min * 60000;
  const tally = { calls: 0, ok: 0, dyn: 0, blk: 0, other: 0 };
  while (Date.now() < until) {
    const t = Date.now();
    const { status, note } = await call(st.rpm);
    tally.calls++;
    if (status === 200) tally.ok++;
    else if (/dynamic_block/.test(note)) tally.dyn++;
    else if (/"block"/.test(note)) tally.blk++;
    else tally.other++;
    const wait = gap - (Date.now() - t);
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
  }
  const failRate = ((tally.calls - tally.ok) / tally.calls) * 100;
  const verdict = failRate === 0 ? "clean" : failRate < 10 ? `${failRate.toFixed(0)}% fail` : `BLOCKING (${failRate.toFixed(0)}% fail)`;
  console.log(
    `${String(st.rpm).padEnd(5)} ${String(tally.calls).padEnd(6)} ${String(tally.ok).padEnd(4)} ` +
    `${String(tally.dyn).padEnd(12)} ${String(tally.blk).padEnd(10)} ${String(tally.other).padEnd(7)} ${verdict}`
  );
  rec({ ev: "stage", ...st, ...tally });
  if (failRate > 25 && !blockedAtRpm) { blockedAtRpm = st.rpm; break; }
}

if (blockedAtRpm) {
  say(`\nceiling crossed at ${blockedAtRpm}/min — now measuring RECOVERY (1 probe every 15s, silent otherwise)`);
  let recoveredAt = null;
  for (let k = 0; k < 24 && !recoveredAt; k++) {
    await new Promise((r) => setTimeout(r, 15000));
    const { status } = await call(0);
    say(`  recovery probe +${((k + 1) * 15)}s -> ${status}`);
    if (status === 200) recoveredAt = (k + 1) * 15;
  }
  rec({ ev: "recovery", seconds: recoveredAt });
  console.log(recoveredAt
    ? `\nRECOVERED after ~${recoveredAt}s of reduced load — the block is TEMPORARY, the IP is not burned.`
    : `\nStill blocked after 6 minutes of near-zero load — this looks like a durable IP block, not a rate trip.`);
} else {
  console.log(`\nNo blocking through ${STAGES[STAGES.length - 1].rpm}/min on this egress.`);
}

console.log(`\nlog: ${OUT}`);
await cleanupApiBrowser().catch(() => {});
await mongoose.disconnect().catch(() => {});
process.exit(0);
