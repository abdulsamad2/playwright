// Measure REAL proxy bandwidth of one scrape flow through the bartproxies gateway.
// Runs the exact scraper path (homepage -> event page -> tmpt -> facets) with the
// same resource blocking, summing OVER-THE-WIRE bytes (Content-Length) per URL so
// we can see MB per category. Then loads the homepage ONCE with blocking OFF to
// show how much the blocking saves. One session, ~3 page loads — a small call.
//
// Usage: node scripts/measureBandwidth.mjs --event=0D00645AC830FC14
import { Camoufox } from "camoufox-js";

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => { const [k, v] = a.replace(/^--/, "").split("="); return [k, v ?? true]; })
);
const GATEWAY = process.env.PROXY_GATEWAY || "resipro.bartproxies.com:7778";
const [host, portStr] = GATEWAY.split(":");
const USER_BASE = process.env.PROXY_USER || "B_52593_US_1080_32701_30";
const PASS = process.env.PROXY_PASS || "061t6o";
const EVENT_ID = args.event || "0D00645AC830FC14";

const BLOCKED_HOSTS = [
  "google-analytics.com","googletagmanager.com","doubleclick.net","googlesyndication.com",
  "googleadservices.com","adservice.google","scorecardresearch.com","connect.facebook.net",
  "branch.io","tealium","tiktok.com","bat.bing.com","demdex.net","omtrdc.net","krxd.net",
  "quantserve.com","nr-data.net","newrelic.com","optimizely.com","mpulse.net","adnxs.com",
  "criteo","pinterest","snapchat","clarity.ms","hotjar","segment.com","amplitude","cdn.cookielaw.org",
];
const mb = (n) => (n / 1048576).toFixed(2);

function session(i) {
  const token = `m${i}${Math.random().toString(36).slice(2, 8)}`;
  return { server: `http://${host}:${parseInt(portStr,10)}`, username: `${USER_BASE}_ss-${token}`, password: PASS };
}

async function attach(page, acc, block) {
  page.on("response", (resp) => {
    const cl = parseInt(resp.headers()["content-length"] || "0", 10);
    if (cl) { acc.bytes += cl; acc.n++; }
    else acc.missing++;
  });
  if (!block) return;
  await page.route("**/*", (route) => {
    const t = route.request().resourceType();
    if (t === "image" || t === "media" || t === "font" || t === "stylesheet") return route.abort();
    if (BLOCKED_HOSTS.some((h) => route.request().url().includes(h))) return route.abort();
    return route.continue();
  });
}

(async () => {
  console.log(`\nMeasuring via ${GATEWAY}  event=${EVENT_ID}\n`);

  // ---- Run A: real scraper flow, blocking ON ----
  let browser = await Camoufox({ headless: true, humanize: false, geoip: false, proxy: session(1) });
  let ctx = await browser.newContext({ ignoreHTTPSErrors: true, bypassCSP: true });
  const nav = { bytes: 0, n: 0, missing: 0 };
  let page = await ctx.newPage();
  await attach(page, nav, true);

  await page.goto("https://www.ticketmaster.com/", { waitUntil: "domcontentloaded", timeout: 40000 }).catch(() => null);
  const homeBlocked = nav.bytes;
  await page.waitForTimeout(400);
  const r = await page.goto(`https://www.ticketmaster.com/event/${EVENT_ID}`, { waitUntil: "domcontentloaded", timeout: 40000 }).catch(() => null);
  const eventStatus = r ? r.status() : 0;
  const navAfterEvent = nav.bytes;

  let tmpt = false;
  for (let w = 0; w < 14; w++) { if ((await ctx.cookies()).some((c) => c.name === "tmpt")) { tmpt = true; break; } await page.waitForTimeout(500); }

  // Facets call (the steady-state per-event cost)
  const facetsUrl = `https://services.ticketmaster.com/api/ismds/event/${EVENT_ID}/facets?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse`;
  let facetsCL = 0, facetsBody = 0, facetStatus = 0;
  try {
    const fr = await ctx.request.get(facetsUrl, { headers: { accept: "application/json", "x-api-key": "b462oi7fic6pehcdkzony5bxhe" }, timeout: 20000 });
    facetStatus = fr.status();
    facetsCL = parseInt(fr.headers()["content-length"] || "0", 10);
    facetsBody = (await fr.body()).length; // decompressed
  } catch (e) { facetStatus = -1; }

  await browser.close().catch(() => {});

  // ---- Run B: homepage only, blocking OFF (to show the savings) ----
  browser = await Camoufox({ headless: true, humanize: false, geoip: false, proxy: session(2) });
  ctx = await browser.newContext({ ignoreHTTPSErrors: true, bypassCSP: true });
  const raw = { bytes: 0, n: 0, missing: 0 };
  page = await ctx.newPage();
  await attach(page, raw, false);
  await page.goto("https://www.ticketmaster.com/", { waitUntil: "domcontentloaded", timeout: 40000 }).catch(() => null);
  const homeUnblocked = raw.bytes;
  await browser.close().catch(() => {});

  // ---- Report ----
  const homeSaved = homeUnblocked - homeBlocked;
  const pct = homeUnblocked ? ((homeSaved / homeUnblocked) * 100).toFixed(0) : "?";
  console.log("──────────── RESULTS (over-the-wire, Content-Length) ────────────");
  console.log(`event page status : ${eventStatus}   tmpt minted: ${tmpt ? "YES" : "NO"}   facets status: ${facetStatus}`);
  console.log("");
  console.log(`Homepage  (blocked)   : ${mb(homeBlocked)} MB`);
  console.log(`Homepage  (UNblocked) : ${mb(homeUnblocked)} MB   → blocking saves ${mb(homeSaved)} MB (${pct}%)`);
  console.log(`Event page (blocked)  : ${mb(navAfterEvent - homeBlocked)} MB`);
  console.log(`Nav TOTAL (blocked)   : ${mb(navAfterEvent)} MB   (${nav.n} responses, ${nav.missing} without CL)`);
  console.log("");
  console.log(`Facets JSON (1 call)  : ${mb(facetsCL)} MB compressed  |  ${mb(facetsBody)} MB decompressed`);
  console.log("─────────────────────────────────────────────────────────────────");
  console.log("Extrapolation to steady state (per 500-call proxy-bind cycle):");
  console.log(`  nav (once/bind)     ≈ ${mb(navAfterEvent)} MB`);
  console.log(`  facets (×500)       ≈ ${mb(facetsCL * 500)} MB compressed`);
  console.log("");
  process.exit(0);
})().catch((e) => { console.error("measure failed:", e.message); process.exit(1); });
