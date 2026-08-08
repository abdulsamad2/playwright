// IS SPREADING ONE TOKEN ACROSS MANY IPs ITSELF A TRIGGER?
//
// This is the question production's design hangs on. The fleet runs ~60 pages per machine
// but the farm only supplies 6-8 tokens, so readFarmJar() hands the SAME token to ~10
// pages, each sitting on a different exit IP. If TM treats one session appearing from
// many IPs as a signal in its own right, the fleet manufactures its own 403s and no
// amount of extra proxies fixes it.
//
// Everything else is held equal. Two privately-minted jars (never written to seed_jars,
// so production consumers cannot add uncontrolled fan-out to either one), same age, same
// total call count, same per-token rate, same 6s spacing, interleaved:
//
//   FANOUT : N calls, each on a DIFFERENT exit IP   (per-IP rate = 1 call total)
//   DEPTH  : N calls, all on ONE exit IP            (per-IP rate = 10/min, measured clean)
//
// Per-IP rate is the confound that ruined the earlier sweep, so DEPTH deliberately runs
// at 10/min — the rate jarRateRamp.mjs measured as 100% clean. If DEPTH stays clean and
// FANOUT fails, the only remaining difference is how many IPs the token was seen from.
//
// Phase 2 exonerates the IPs: every IP that failed under FANOUT is re-probed once with a
// third, untouched jar. If those same IPs answer 200, they were never blocked — the
// token's fan-out was.
//
// Read-only w.r.t. Mongo. Usage: node scripts/jarFanoutVsDepth.mjs [callsPerArm]
import "dotenv/config";
import mongoose from "mongoose";
import { chromium } from "patchright";
import { Camoufox } from "camoufox-js";
import proxyArray, { loadProxies } from "../helpers/proxy.js";

const N = parseInt(process.argv[2], 10) || 12;
const SPACING_MS = 6000; // 10 calls/min per token in BOTH arms

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
const say = (m) => console.log(`[${((Date.now() - T0) / 1000).toFixed(1).padStart(7)}s] ${m}`);

await mongoose.connect(process.env.MONGODB_URI).catch(() => {});
await loadProxies();
const pool = (proxyArray.proxies || []).filter((p) => p && p.proxy);
if (pool.length < N + 2) { console.error(`need >=${N + 2} proxies`); process.exit(2); }

// ---- mint private jars on this host's clean IP with real Chrome (EPS scores the mint)
const mintBrowser = await chromium.launch({
  headless: false, channel: "chrome",
  args: ["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"],
});

async function mintJar(label) {
  for (let a = 1; a <= 4; a++) {
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
      await page.waitForTimeout(2000 + Math.random() * 1000);
      await page.goto(`https://www.ticketmaster.com/event/${seed}`, { waitUntil: "domcontentloaded", timeout: 45000 }).catch(() => null);
      let has = false;
      for (let w = 0; w < 30 && !has; w++) { has = (await ctx.cookies()).some((c) => c.name === "tmpt"); if (!has) await page.waitForTimeout(750); }
      await page.waitForTimeout(2500);
      const r = await ctx.request.get(facetsUrl(seed), {
        headers: { "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe", Accept: "application/json", "tmps-correlation-id": `m-${Date.now()}` },
        timeout: 25000,
      });
      if (r.status() === 200) {
        const jar = await ctx.cookies();
        await ctx.close().catch(() => {});
        say(`${label}: minted (${jar.length} cookies, validated 200)`);
        return jar;
      }
      say(`${label}: mint attempt ${a} validated ${r.status()}`);
      await ctx.close().catch(() => {});
    } catch (e) { if (ctx) await ctx.close().catch(() => {}); say(`${label}: mint attempt ${a} threw`); }
  }
  return null;
}

const jarA = await mintJar("FANOUT");
const jarB = await mintJar("DEPTH ");
const jarC = await mintJar("EXON  ");
await mintBrowser.close().catch(() => {});
if (!jarA || !jarB || !jarC) { console.error("could not mint three private jars"); process.exit(3); }

const norm = (cookies) => cookies.map((c) => ({
  name: c.name, value: c.value, domain: c.domain || ".ticketmaster.com", path: c.path || "/",
  expires: c.expires || -1, httpOnly: !!c.httpOnly, secure: c.secure !== false, sameSite: c.sameSite || "Lax",
}));

// ---- read side: prod's engine, one context per (jar, IP) pair
const browser = await Camoufox({ headless: true, humanize: false, geoip: false });

async function probe(p, jar, i) {
  const [host, port] = p.proxy.split(":");
  let ctx = null;
  try {
    ctx = await browser.newContext({
      ignoreHTTPSErrors: true, bypassCSP: true,
      proxy: { server: `http://${host}:${port}`, username: p.username, password: p.password },
    });
    await ctx.addCookies(norm(jar));
    const r = await ctx.request.get(facetsUrl(EVENTS[i % EVENTS.length]), {
      headers: { "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe", Accept: "application/json", "tmps-correlation-id": `fd-${Date.now()}` },
      timeout: 30000,
    });
    const s = r.status();
    if (s === 200) return "200";
    const b = (await r.text().catch(() => "")).slice(0, 60).replace(/\s+/g, " ");
    return /dynamic_block/.test(b) ? "403 dynamic" : /"block"/.test(b) ? "403 block" : `${s}`;
  } catch { return "CONN-FAIL"; }
  finally { if (ctx) await ctx.close().catch(() => {}); }
}

const shuffled = pool.slice().sort(() => Math.random() - 0.5);
const fanoutIps = shuffled.slice(0, N);
const depthIp = shuffled[N];

console.log(`\nFANOUT: 1 token over ${N} different IPs      DEPTH: 1 token on ${depthIp.proxy} ${N}x`);
console.log(`both arms: ${N} calls, ${(60000 / SPACING_MS).toFixed(0)} calls/min per token, interleaved\n`);
console.log("#    FANOUT ip                   verdict      | DEPTH verdict");
console.log("-".repeat(72));

const fan = [], dep = [];
for (let i = 0; i < N; i++) {
  const f = await probe(fanoutIps[i], jarA, i);
  fan.push({ ip: fanoutIps[i], v: f });
  await new Promise((r) => setTimeout(r, SPACING_MS / 2));
  const d = await probe(depthIp, jarB, i);
  dep.push(d);
  console.log(`${String(i + 1).padStart(3)}  ${fanoutIps[i].proxy.padEnd(24)} ${f.padEnd(12)} | ${d}`);
  await new Promise((r) => setTimeout(r, SPACING_MS / 2));
}

const okF = fan.filter((x) => x.v === "200").length;
const okD = dep.filter((v) => v === "200").length;
console.log("\n--- ARMS ---");
console.log(`  FANOUT (${N} IPs, 1 token): ${okF}/${N} passed`);
console.log(`  DEPTH  (1 IP,  1 token): ${okD}/${N} passed`);

// ---- Phase 2: were those IPs ever actually blocked?
const failedIps = fan.filter((x) => x.v.startsWith("403")).map((x) => x.ip);
if (failedIps.length) {
  console.log(`\n--- EXONERATION: re-probing ${Math.min(6, failedIps.length)} of the IPs FANOUT failed on, with an untouched jar ---`);
  let exOk = 0, exN = 0;
  for (const p of failedIps.slice(0, 6)) {
    await new Promise((r) => setTimeout(r, SPACING_MS));
    const v = await probe(p, jarC, exN);
    exN++; if (v === "200") exOk++;
    console.log(`  ${p.proxy.padEnd(24)} ${v}`);
  }
  console.log(`\n  ${exOk}/${exN} of the "blocked" IPs answered 200 with a clean token.`);
  if (exOk / exN > 0.6) console.log("  -> those IPs were NOT blocked. The token's fan-out was what TM rejected.");
  else if (exOk === 0) console.log("  -> those IPs are genuinely unusable right now, independent of the token.");
}

console.log("\n--- VERDICT ---");
if (okD > okF * 1.5 && okD >= N * 0.7) {
  console.log("  Spreading ONE token across MANY exit IPs is itself the trigger.");
  console.log("  Same token, same rate, same call count — only the IP count differed.");
  console.log("  => production must bind one jar to one exit IP, not share 6 jars over 60 pages.");
} else if (okF >= N * 0.7 && okD >= N * 0.7) {
  console.log("  Both arms clean: fan-out is NOT punished at this rate. Token supply can stay shared;");
  console.log("  the governing limit is per-IP request rate (see jarRateRamp.mjs).");
} else {
  console.log("  Inconclusive at this sample size — re-run with a larger N.");
}

await browser.close().catch(() => {});
await mongoose.disconnect().catch(() => {});
process.exit(0);
