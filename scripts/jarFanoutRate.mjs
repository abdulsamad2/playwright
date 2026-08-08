// DOES A TOKEN'S OWN RATE MATTER ONCE THE PER-IP RATE IS REMOVED?
//
// Two results have to be reconciled:
//   jarRateRamp.mjs   one token, ONE IP        -> clean to ~30/min, breaks at 45/min
//   jarFanoutVsDepth  one token, 12 IPs, 10/min -> 12/12 clean, fan-out costs nothing
//   proxyHealthSweep  one token, 120 IPs, ~25/min, 10 in flight -> 90% blocked
//
// The sweep cannot be explained by per-IP rate (each IP got exactly ONE call) and it
// cannot be explained by fan-out alone (that came back clean). What is left is the
// token's OWN rate while fanned out — and possibly concurrency.
//
// This runs the identical rate ladder to jarRateRamp.mjs, sequentially, with one change:
// every call goes out through a DIFFERENT exit IP, so per-IP rate stays pinned at ~1 and
// the only variable is how fast the token itself is being used. Overlay the two curves:
//
//   fan-out curve matches the single-IP curve -> the token is not rate-limited as a
//       session; per-IP rate is the whole story and the sweep's blocks came from its
//       10-way concurrency.
//   fan-out curve breaks down EARLIER          -> a token spread across IPs has its own,
//       tighter budget, and prod's 6-tokens-over-60-pages design is the bottleneck.
//
// Read-only. Usage: node scripts/jarFanoutRate.mjs [outfile]
import "dotenv/config";
import fs from "fs";
import mongoose from "mongoose";
import { chromium } from "patchright";
import { Camoufox } from "camoufox-js";
import proxyArray, { loadProxies } from "../helpers/proxy.js";

const OUT = process.argv[2] || `/tmp/fanrate-${Date.now()}.jsonl`;
const STAGES = [
  { rpm: 10, min: 2 },
  { rpm: 20, min: 2 },
  { rpm: 30, min: 2 },
  { rpm: 45, min: 2 },
  { rpm: 60, min: 2 },
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

await mongoose.connect(process.env.MONGODB_URI).catch(() => {});
await loadProxies();
const pool = (proxyArray.proxies || []).filter((p) => p && p.proxy).sort(() => Math.random() - 0.5);
say(`${pool.length} exit IPs available — each call uses the next one, so per-IP rate stays ~1`);

// private jar so production consumers add no uncontrolled fan-out of their own
const mintBrowser = await chromium.launch({
  headless: false, channel: "chrome",
  args: ["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"],
});
let jar = null;
for (let a = 1; a <= 4 && !jar; a++) {
  let ctx = null;
  try {
    ctx = await mintBrowser.newContext({ viewport: { width: 1440, height: 900 }, ignoreHTTPSErrors: true, bypassCSP: true });
    await ctx.route("**/*", (r) => {
      const t = r.request().resourceType();
      return t === "image" || t === "media" || t === "font" ? r.abort() : r.continue();
    }).catch(() => {});
    const page = await ctx.newPage();
    const seed = EVENTS[Math.floor(Math.random() * EVENTS.length)];
    await page.goto("https://www.ticketmaster.com/", { waitUntil: "domcontentloaded", timeout: 45000 }).catch(() => null);
    await page.waitForTimeout(2500);
    await page.goto(`https://www.ticketmaster.com/event/${seed}`, { waitUntil: "domcontentloaded", timeout: 45000 }).catch(() => null);
    let has = false;
    for (let w = 0; w < 30 && !has; w++) { has = (await ctx.cookies()).some((c) => c.name === "tmpt"); if (!has) await page.waitForTimeout(750); }
    await page.waitForTimeout(2500);
    const r = await ctx.request.get(facetsUrl(seed), {
      headers: { "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe", Accept: "application/json", "tmps-correlation-id": `m-${Date.now()}` },
      timeout: 25000,
    });
    if (r.status() === 200) { jar = await ctx.cookies(); say(`minted private jar (${jar.length} cookies, validated 200)`); }
    await ctx.close().catch(() => {});
  } catch { if (ctx) await ctx.close().catch(() => {}); }
}
await mintBrowser.close().catch(() => {});
if (!jar) { console.error("could not mint a private jar"); process.exit(3); }

const norm = jar.map((c) => ({
  name: c.name, value: c.value, domain: c.domain || ".ticketmaster.com", path: c.path || "/",
  expires: c.expires || -1, httpOnly: !!c.httpOnly, secure: c.secure !== false, sameSite: c.sameSite || "Lax",
}));

const browser = await Camoufox({ headless: true, humanize: false, geoip: false });

// Contexts are built ONCE up front. Creating a Camoufox context costs ~10s, so building
// one per call silently caps the achievable rate around 6/min — far below the 45-60/min
// this ladder has to reach. With a standing ring of contexts each call is just a request,
// and rotating through the ring keeps per-IP rate at (achieved rate / ring size).
const RING = 24;
const ring = [];
for (const p of pool.slice(0, RING)) {
  const [host, port] = p.proxy.split(":");
  try {
    const ctx = await browser.newContext({
      ignoreHTTPSErrors: true, bypassCSP: true,
      proxy: { server: `http://${host}:${port}`, username: p.username, password: p.password },
    });
    await ctx.addCookies(norm);
    ring.push({ ip: p.proxy, ctx });
  } catch { /* proxy that won't even open a context is no use here */ }
}
if (ring.length < 8) { console.error(`only ${ring.length} contexts came up — need >=8`); process.exit(3); }
say(`${ring.length} standing contexts, all carrying the SAME token`);

let ipIdx = 0, evIdx = 0;

async function callNextIp(stageRpm) {
  const w = ring[ipIdx++ % ring.length];
  const t = Date.now();
  try {
    const r = await w.ctx.request.get(facetsUrl(EVENTS[evIdx++ % EVENTS.length]), {
      headers: { "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe", Accept: "application/json", "tmps-correlation-id": `fr-${Date.now()}` },
      timeout: 30000,
    });
    const s = r.status();
    const note = s === 200 ? "" : (await r.text().catch(() => "")).slice(0, 50).replace(/\s+/g, " ");
    rec({ ev: "call", rpm: stageRpm, ip: w.ip, status: s, ms: Date.now() - t, note });
    return { status: s, note };
  } catch (e) {
    rec({ ev: "call", rpm: stageRpm, ip: w.ip, status: "ERR", ms: Date.now() - t, note: e.message.slice(0, 50) });
    return { status: "ERR", note: e.message.slice(0, 50) };
  }
}

console.log("\n(compare against jarRateRamp.mjs, which ran the same ladder on ONE IP)");
console.log("target  achieved  perIP   calls  ok   403  connErr  failRate  vs single-IP run");
console.log("-".repeat(84));
const SINGLE_IP = { 10: 0, 20: 3, 30: 4, 45: 12, 60: 37 }; // measured fail% in jarRateRamp.mjs

for (const st of STAGES) {
  const gap = 60000 / st.rpm;
  const stageStart = Date.now();
  const until = stageStart + st.min * 60000;
  const tally = { calls: 0, ok: 0, f403: 0, err: 0 };
  while (Date.now() < until) {
    const t = Date.now();
    const { status } = await callNextIp(st.rpm);
    tally.calls++;
    if (status === 200) tally.ok++;
    else if (status === 403) tally.f403++;
    else tally.err++;
    const wait = gap - (Date.now() - t);
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
  }
  // Report the rate actually achieved, not the target — if request latency exceeds the
  // gap the loop silently runs slower, and comparing a 6/min run against a 45/min run
  // would invent a difference that was never tested.
  const mins = (Date.now() - stageStart) / 60000;
  const achieved = tally.calls / mins;
  const verdicted = tally.calls - tally.err;
  const failPct = verdicted ? (tally.f403 / verdicted) * 100 : 0;
  const delta = failPct - (SINGLE_IP[st.rpm] ?? 0);
  console.log(
    `${String(st.rpm).padEnd(7)} ${achieved.toFixed(1).padEnd(9)} ${(achieved / ring.length).toFixed(1).padEnd(7)} ` +
    `${String(tally.calls).padEnd(6)} ${String(tally.ok).padEnd(4)} ${String(tally.f403).padEnd(4)} ${String(tally.err).padEnd(8)} ` +
    `${failPct.toFixed(0).padStart(7)}%  ${SINGLE_IP[st.rpm] ?? "-"}% single-IP  ` +
    `${achieved < st.rpm * 0.7 ? "(rate not reached)" : delta > 15 ? "<-- WORSE fanned out" : delta < -15 ? "<-- BETTER fanned out" : "same"}`
  );
  rec({ ev: "stage", ...st, ...tally, achievedRpm: achieved, failPct });
}

console.log(`\nlog: ${OUT}`);
await browser.close().catch(() => {});
await mongoose.disconnect().catch(() => {});
process.exit(0);
