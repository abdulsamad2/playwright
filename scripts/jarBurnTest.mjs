// JAR LIFETIME EXPERIMENT — what actually kills a tmpt jar?
//
// Three variables are confounded in production: total calls on a token, the RATE of
// those calls, and the token's wall-clock age. This mints PRIVATE jars (never written
// to seed_jars, so prod consumers cannot touch them) and runs each on its own arm from
// the SAME clean egress IP at the same time. Same IP + same moment for every arm →
// the only thing that differs is how the token is used.
//
//   FAST   ~60 facets/min   — high volume, high rate
//   PACED  ~20 facets/min   — prod's JAR_RATE_CAP, same clock, 3x less volume
//   IDLE   ~0.5 facets/min  — almost no volume; pure wall-clock ageing
//
// Read them together:
//   FAST dies at same COUNT as PACED (but earlier in time) -> volume/call budget is real
//   FAST dies far earlier in COUNT than PACED              -> it's a RATE limit
//   all arms die at the same MOMENT                        -> IP-level or clock-level
//   IDLE dies at ~60min with ~0 calls                      -> tmpt TTL, nothing else
//
// A NOJAR baseline (context with no cookies) runs first to prove the endpoint really
// does require the token. Every call is appended to a JSONL log for analysis.
//
// Read-only w.r.t. Mongo: nothing is written, no status flipped, no useCount touched.
//
// Usage: node scripts/jarBurnTest.mjs [minutes] [outfile]
import "dotenv/config";
import fs from "fs";
import { chromium } from "patchright";

const RUN_MIN = parseInt(process.argv[2], 10) || 80;
const OUT = process.argv[3] || `/tmp/jarburn-${Date.now()}.jsonl`;
const DEADLINE = Date.now() + RUN_MIN * 60000;

// Real events off the live events collection, rotated so no single event is hammered.
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

// ---------------------------------------------------------------- mint (real Chrome)
// EPS scores the mint with reCAPTCHA v3 — Camoufox is blocked outright, real headed
// Chrome passes. Mint on this host's own IP (no proxy) and validate with a facets 200
// before accepting the jar, exactly like the farm's recipe.
async function mint(browser, label) {
  for (let attempt = 1; attempt <= 4; attempt++) {
    let ctx = null;
    try {
      ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, ignoreHTTPSErrors: true, bypassCSP: true });
      await ctx.route("**/*", (r) => {
        const t = r.request().resourceType();
        return t === "image" || t === "media" || t === "font" ? r.abort() : r.continue();
      }).catch(() => {});
      const page = await ctx.newPage();
      const seedId = EVENTS[Math.floor(Math.random() * EVENTS.length)];
      await page.goto("https://www.ticketmaster.com/", { waitUntil: "domcontentloaded", timeout: 45000 }).catch(() => null);
      await page.waitForTimeout(2000 + Math.random() * 1500);
      await page.goto(`https://www.ticketmaster.com/event/${seedId}`, { waitUntil: "domcontentloaded", timeout: 45000 }).catch(() => null);
      const blocked = await page.evaluate(() => /paused|verified|interruption|identity/i.test(document.title)).catch(() => false);
      if (blocked) { say(`${label}: mint attempt ${attempt} hit the EPS block screen`); await ctx.close().catch(() => {}); continue; }
      let has = false;
      for (let w = 0; w < 30 && !has; w++) { has = (await ctx.cookies()).some((c) => c.name === "tmpt"); if (!has) await page.waitForTimeout(750); }
      await page.waitForTimeout(2500); // let the page's own XHRs establish the services session
      const r = await ctx.request.get(facetsUrl(seedId), {
        headers: { "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe", Accept: "application/json", "tmps-correlation-id": `mint-${Date.now()}` },
        timeout: 25000,
      });
      if (r.status() === 200) {
        const jar = await ctx.cookies();
        const tmpt = jar.find((c) => c.name === "tmpt");
        say(`${label}: minted OK (${jar.length} cookies, facets 200, tmpt ttl ${tmpt ? ((tmpt.expires * 1000 - Date.now()) / 60000).toFixed(1) : "?"}min)`);
        rec({ ev: "mint", arm: label, cookies: jar.length, tmptTtlMin: tmpt ? +((tmpt.expires * 1000 - Date.now()) / 60000).toFixed(1) : null });
        return { ctx, page, tmptAt: Date.now() };
      }
      say(`${label}: mint attempt ${attempt} validated ${r.status()} — retrying`);
      await ctx.close().catch(() => {});
    } catch (e) {
      say(`${label}: mint attempt ${attempt} threw ${e.message.slice(0, 70)}`);
      if (ctx) await ctx.close().catch(() => {});
    }
  }
  return null;
}

// ------------------------------------------------------------------------ one arm
async function runArm({ label, ctx, page, intervalMs, maxCalls }) {
  const st = { label, calls: 0, ok: 0, fail: 0, streak: 0, firstFailAt: null, firstFailCall: null, deadAt: null, byStatus: {}, tmptRotations: 0 };
  let lastTmpt = (await ctx.cookies()).find((c) => c.name === "tmpt")?.value || null;
  let i = 0;

  while (Date.now() < DEADLINE && st.calls < maxCalls) {
    const id = EVENTS[i++ % EVENTS.length];
    const t = Date.now();
    let status, note = "";
    try {
      const r = await ctx.request.get(facetsUrl(id), {
        headers: { "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe", Accept: "application/json", "tmps-correlation-id": `burn-${Date.now()}` },
        timeout: 25000,
      });
      status = r.status();
      if (status === 200) {
        const b = await r.json().catch(() => null);
        note = `${b?.facets?.length ?? 0}f`;
      } else {
        note = (await r.text().catch(() => "")).slice(0, 90).replace(/\s+/g, " ");
      }
    } catch (e) {
      status = "ERR";
      note = e.message.slice(0, 90);
    }
    const ms = Date.now() - t;
    st.calls++;
    st.byStatus[status] = (st.byStatus[status] || 0) + 1;

    // did TM hand us a refreshed tmpt on the way back?
    const now = (await ctx.cookies()).find((c) => c.name === "tmpt")?.value || null;
    let rotated = false;
    if (now && now !== lastTmpt) { rotated = true; st.tmptRotations++; lastTmpt = now; }

    rec({ ev: "call", arm: label, n: st.calls, event: id, status, ms, note, rotated });

    if (status === 200) { st.ok++; st.streak = 0; }
    else {
      st.fail++; st.streak++;
      if (st.firstFailAt === null) {
        st.firstFailAt = +el(); st.firstFailCall = st.calls;
        say(`${label}: FIRST FAILURE at call ${st.calls} (${el()}s) status=${status} ${note.slice(0, 60)}`);
      }
      if (st.streak >= 5) {
        st.deadAt = +el();
        say(`${label}: DEAD — 5 consecutive failures, ${st.ok} good calls in ${el()}s`);
        break;
      }
    }
    if (st.calls % 25 === 0) say(`${label}: ${st.calls} calls, ${st.ok} ok / ${st.fail} fail, tmpt rotations ${st.tmptRotations}`);

    const wait = intervalMs - ms;
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
  }
  if (!st.deadAt) say(`${label}: finished alive — ${st.calls} calls, ${st.ok} ok / ${st.fail} fail`);
  rec({ ev: "armEnd", ...st });
  return st;
}

// ------------------------------------------------------------------------- main
const browser = await chromium.launch({
  headless: false,
  channel: "chrome",
  args: ["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage", "--window-size=1440,900"],
});
say(`run started, deadline ${RUN_MIN}min, log ${OUT}`);

// Baseline: no cookies at all. If this 200s, the token is not what gates facets.
{
  const ctx = await browser.newContext({ ignoreHTTPSErrors: true });
  let s;
  try {
    const r = await ctx.request.get(facetsUrl(EVENTS[0]), { headers: { "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe", Accept: "application/json" }, timeout: 20000 });
    s = r.status();
  } catch (e) { s = `ERR:${e.message.slice(0, 40)}`; }
  say(`BASELINE no-cookie context -> facets ${s}`);
  rec({ ev: "baseline", status: s });
  await ctx.close().catch(() => {});
}

const ARMS = [
  { label: "FAST ", intervalMs: 1000, maxCalls: 3000 }, // ~60/min
  { label: "PACED", intervalMs: 3000, maxCalls: 1500 }, // ~20/min  (prod JAR_RATE_CAP)
  { label: "IDLE ", intervalMs: 120000, maxCalls: 60 }, // ~0.5/min (pure ageing)
];

const live = [];
for (const a of ARMS) {
  const m = await mint(browser, a.label);
  if (!m) { say(`${a.label}: could not mint a jar — arm skipped`); continue; }
  live.push({ ...a, ...m });
}
if (!live.length) { say("no jars minted — aborting"); await browser.close(); process.exit(3); }

const results = await Promise.all(live.map((a) => runArm(a)));

console.log("\n=== SUMMARY ===");
console.log("arm    rate/min  calls   ok    fail  firstFailCall  firstFailAt(s)  deadAt(s)  tmptRotations  statuses");
for (const r of results) {
  const a = live.find((x) => x.label === r.label);
  console.log(
    `${r.label}  ${String(Math.round(60000 / a.intervalMs)).padEnd(9)} ${String(r.calls).padEnd(7)} ${String(r.ok).padEnd(5)} ` +
    `${String(r.fail).padEnd(5)} ${String(r.firstFailCall ?? "-").padEnd(14)} ${String(r.firstFailAt ?? "-").padEnd(15)} ` +
    `${String(r.deadAt ?? "alive").padEnd(10)} ${String(r.tmptRotations).padEnd(14)} ${JSON.stringify(r.byStatus)}`
  );
}
console.log(`\nlog: ${OUT}`);
await browser.close().catch(() => {});
process.exit(0);
