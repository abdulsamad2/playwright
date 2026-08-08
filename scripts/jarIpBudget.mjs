// HOW MANY DISTINCT EXIT IPs MAY ONE TOKEN BE USED FROM BEFORE TM KILLS IT?
//
// Three independent runs all landed on the same number without being designed to:
//   proxyHealthSweep.mjs   1 token, 120 IPs -> exactly 10 × 200, then 98 × 403 block
//   jarFanoutVsDepth.mjs   1 token,  12 IPs -> 10 passed, died on the 11th TM contact
//   jarFanoutRate.mjs      1 token,  24 IPs -> IPs #1-10 passed, #11 onward 403 — and
//                                              then the FIRST TEN 403'd too, permanently
//
// That is not a rate limit (per-IP rate was ~0.3/min) and not the token wearing out
// (jarRateRamp did 275 calls and jarBurnTest 575+ on ONE IP). It is a session-hijack
// heuristic: one session appearing from too many IPs gets revoked outright.
//
// This confirms the threshold deliberately, on N freshly-minted private jars, and shows
// the revocation is permanent by re-probing IP #1 after the token dies. Each jar walks
// one call per IP, slowly, so neither per-IP nor token rate can be the cause.
//
// It matters because production shares 6-8 farm jars across ~60 pages that each sit on
// their own proxy — so every jar crosses this threshold within seconds of being handed out.
//
// Read-only w.r.t. Mongo. Usage: node scripts/jarIpBudget.mjs [jars] [maxIps]
import "dotenv/config";
import mongoose from "mongoose";
import { chromium } from "patchright";
import { Camoufox } from "camoufox-js";
import proxyArray, { loadProxies } from "../helpers/proxy.js";

const JARS = parseInt(process.argv[2], 10) || 3;
const MAX_IPS = parseInt(process.argv[3], 10) || 16;
const SPACING_MS = 4000;

const EVENTS = [
  "15006331B08D75C0", "0A006331DC273765", "0600632E29196B3E", "0800632CA3272367",
  "17006441A531D6B5", "1E00644AE8CAE766", "08006317220953D2", "0200631CB2212963",
];
const facetsUrl = (id) =>
  `https://services.ticketmaster.com/api/ismds/event/${id}/facets` +
  `?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description` +
  `&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places` +
  `&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us` +
  `&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse` +
  `&_=${Date.now()}&t=${Math.random().toString(36).slice(2)}`;

await mongoose.connect(process.env.MONGODB_URI).catch(() => {});
await loadProxies();
const pool = (proxyArray.proxies || []).filter((p) => p && p.proxy);

const mintBrowser = await chromium.launch({
  headless: false, channel: "chrome",
  args: ["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"],
});
async function mint(label) {
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
      await page.waitForTimeout(2500);
      await page.goto(`https://www.ticketmaster.com/event/${seed}`, { waitUntil: "domcontentloaded", timeout: 45000 }).catch(() => null);
      let has = false;
      for (let w = 0; w < 30 && !has; w++) { has = (await ctx.cookies()).some((c) => c.name === "tmpt"); if (!has) await page.waitForTimeout(750); }
      await page.waitForTimeout(2500);
      const r = await ctx.request.get(facetsUrl(seed), {
        headers: { "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe", Accept: "application/json", "tmps-correlation-id": `m-${Date.now()}` },
        timeout: 25000,
      });
      if (r.status() === 200) { const j = await ctx.cookies(); await ctx.close().catch(() => {}); console.log(`  ${label}: minted (${j.length} cookies)`); return j; }
      await ctx.close().catch(() => {});
    } catch { if (ctx) await ctx.close().catch(() => {}); }
  }
  return null;
}

const browser = await Camoufox({ headless: true, humanize: false, geoip: false });
const norm = (jar) => jar.map((c) => ({
  name: c.name, value: c.value, domain: c.domain || ".ticketmaster.com", path: c.path || "/",
  expires: c.expires || -1, httpOnly: !!c.httpOnly, secure: c.secure !== false, sameSite: c.sameSite || "Lax",
}));

async function hit(p, jar, ev) {
  const [host, port] = p.proxy.split(":");
  let ctx = null;
  try {
    ctx = await browser.newContext({
      ignoreHTTPSErrors: true, bypassCSP: true,
      proxy: { server: `http://${host}:${port}`, username: p.username, password: p.password },
    });
    await ctx.addCookies(norm(jar));
    const r = await ctx.request.get(facetsUrl(ev), {
      headers: { "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe", Accept: "application/json", "tmps-correlation-id": `ib-${Date.now()}` },
      timeout: 30000,
    });
    const s = r.status();
    if (s === 200) return "200";
    // 407 is the PROXY rejecting our auth — it never reached TM, so it says nothing about
    // the token. Counting it as a death once made a perfectly healthy jar look like it
    // died on its first IP. Same class as CONN-FAIL.
    if (s === 407) return "PROXY-407";
    const b = (await r.text().catch(() => "")).slice(0, 40).replace(/\s+/g, " ");
    return `${s} ${b}`;
  } catch { return "CONN-FAIL"; }
  finally { if (ctx) await ctx.close().catch(() => {}); }
}

const deaths = [];
for (let j = 0; j < JARS; j++) {
  console.log(`\n=== JAR ${j + 1} ===`);
  const jar = await mint(`jar${j + 1}`);
  if (!jar) { console.log("  mint failed, skipping"); continue; }

  const ips = pool.slice().sort(() => Math.random() - 0.5).slice(0, MAX_IPS);
  let tmVerdicts = 0, firstIp = null, diedAt = null;
  for (let i = 0; i < ips.length; i++) {
    const v = await hit(ips[i], jar, EVENTS[i % EVENTS.length]);
    if (v === "CONN-FAIL" || v === "PROXY-407") { console.log(`  ip#${String(i + 1).padStart(2)} ${ips[i].proxy.padEnd(24)} ${v} (no TM verdict, not counted)`); continue; }
    tmVerdicts++;
    if (!firstIp) firstIp = ips[i];
    console.log(`  ip#${String(i + 1).padStart(2)} (TM contact ${String(tmVerdicts).padStart(2)}) ${ips[i].proxy.padEnd(24)} ${v}`);
    if (v !== "200" && diedAt === null) { diedAt = tmVerdicts; }
    if (diedAt !== null && tmVerdicts >= diedAt + 2) break; // confirmed dead, stop burning proxies
    await new Promise((r) => setTimeout(r, SPACING_MS));
  }

  // Is the revocation permanent, and does it follow the TOKEN rather than the IP?
  if (diedAt !== null && firstIp) {
    await new Promise((r) => setTimeout(r, 5000));
    const back = await hit(firstIp, jar, EVENTS[0]);
    console.log(`  re-probe IP #1 (which passed earlier) with the SAME token: ${back}`);
    console.log(`  -> jar ${j + 1} died on distinct-IP #${diedAt}${back !== "200" ? "; revocation is permanent and follows the token" : "; recovered, so it was transient"}`);
    deaths.push(diedAt);
  } else {
    console.log(`  -> jar ${j + 1} survived all ${tmVerdicts} distinct IPs`);
  }
}

console.log("\n=== RESULT ===");
if (deaths.length) {
  // Report the median, not the minimum: one jar landing on a dud proxy would drag a
  // min-based threshold to nonsense (an early run printed "more than ~0 exit IPs").
  const sorted = deaths.slice().sort((a, b) => a - b);
  const median = sorted[Math.floor(sorted.length / 2)];
  console.log(`  jars died after ${deaths.join(", ")} distinct exit IPs (median ${median})`);
  console.log(`\n  A tmpt session is revoked once it is seen from more than ~${median - 1} exit IPs.`);
  console.log(`  Production hands each farm jar to ~60 pages that each sit on their own proxy,`);
  console.log(`  so every jar crosses this within seconds. Bind one jar to ONE exit IP.`);
} else {
  console.log("  no jar died — the distinct-IP threshold is above the range tested.");
}

await mintBrowser.close().catch(() => {});
await browser.close().catch(() => {});
await mongoose.disconnect().catch(() => {});
process.exit(0);
