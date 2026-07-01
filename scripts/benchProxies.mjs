// Bench a rotating-residential proxy gateway against the REAL TM EPS flow, the
// way the scraper actually hits it: launch Camoufox -> homepage -> event page ->
// wait for the `tmpt` token to mint -> call the facets API. Reports, per exit IP:
// tmpt minted?, event-page status, facets status, and timing — so you can measure
// the IP FLAG RATE of a proxy pool before wiring it into the scraper.
//
// Usage:
//   node scripts/benchProxies.mjs --n=8 --event=0E00634DCBC16E8B
//   PROXY_GATEWAY=host:port PROXY_USER=B_52593_US_1080_32701_30 PROXY_PASS=061t6o node scripts/benchProxies.mjs
//
// Status meaning:
//   facets 200            -> GOOD (IP not flagged, full data)
//   facets 401/403        -> FLAGGED (EPS blocked this IP)
//   facets 404/410        -> event expired (inconclusive — try a live --event)
//   tmpt never minted     -> IP rejected at the challenge stage (treat as flagged)

import { Camoufox } from "camoufox-js";

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  })
);

// --- Proxy gateway config (defaults to the bartproxies credential under test) ---
const GATEWAY = process.env.PROXY_GATEWAY || "resipro.bartproxies.com:7778";
// Username WITHOUT the rotating session token. We append `_ss-<token>` per session
// (bartproxies rotates the exit IP when the `ss-` token changes — verified).
// NOTE: do NOT read process.env.USER — that's the OS login name on macOS/Linux.
const USER_BASE = process.env.PROXY_USER || "B_52593_US_1080_32701_30";
const PASS = process.env.PROXY_PASS || "061t6o";
const SESSION_PREFIX = process.env.SESSION_PREFIX || "_ss-"; // bartproxies style

const N = parseInt(args.n, 10) || 8;
const EVENT_ID = args.event || process.env.EVENT_ID || "0E00634DCBC16E8B";
const TMPT_TRIES = parseInt(args.tmptTries, 10) || 8;
const TMPT_MS = parseInt(args.tmptMs, 10) || 1200;

const [host, portStr] = GATEWAY.split(":");
const port = parseInt(portStr, 10) || 80;

function makeSession(i) {
  // Deterministic-ish unique token per slot so each session pins a distinct IP.
  const token = `bench${i}${Math.random().toString(36).slice(2, 8)}`;
  return {
    server: `http://${host}:${port}`,
    username: `${USER_BASE}${SESSION_PREFIX}${token}`,
    password: PASS,
  };
}

const facetsUrl = (id) =>
  `https://services.ticketmaster.com/api/ismds/event/${id}/facets?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse`;

async function benchOne(i) {
  const proxy = makeSession(i);
  const t0 = Date.now();
  let browser = null;
  const out = { slot: i, exitIp: "?", tmpt: false, pageStatus: 0, facetStatus: 0, ms: 0, err: null };
  try {
    browser = await Camoufox({ headless: true, humanize: false, geoip: false, proxy });
    const context = await browser.newContext({ ignoreHTTPSErrors: true, bypassCSP: true });
    // Block heavy resources — same as the scraper does now.
    await context.route("**/*", (r) =>
      ["image", "media", "font"].includes(r.request().resourceType())
        ? r.abort().catch(() => {})
        : r.continue().catch(() => {})
    );
    const page = await context.newPage();

    // Exit IP (through the browser's own stack, so it's the IP TM actually sees).
    try {
      const ipr = await context.request.get("https://api.ipify.org?format=json", { timeout: 15000 });
      out.exitIp = (await ipr.json()).ip;
    } catch {}

    // Homepage -> dwell -> event page (mirrors _createProxyPage warm-up).
    await page.goto("https://www.ticketmaster.com/", { waitUntil: "domcontentloaded", timeout: 40000 }).catch(() => null);
    await page.waitForTimeout(1500);
    const resp = await page
      .goto(`https://www.ticketmaster.com/event/${EVENT_ID}`, { waitUntil: "domcontentloaded", timeout: 40000 })
      .catch(() => null);
    out.pageStatus = resp ? resp.status() : 0;

    // Wait for tmpt to mint.
    for (let w = 0; w < TMPT_TRIES; w++) {
      const names = (await context.cookies()).map((c) => c.name);
      if (names.includes("tmpt")) { out.tmpt = true; break; }
      await page.waitForTimeout(TMPT_MS);
    }

    // The real test: does a facets call succeed on this IP?
    const fr = await context.request
      .get(facetsUrl(EVENT_ID), {
        headers: {
          accept: "application/json",
          "x-api-key": "b462oi7fic6pehcdkzony5bxhe",
          "tmps-correlation-id": "v" + Math.floor(Math.random() * 1e9),
          "x-request-id": "v" + Math.floor(Math.random() * 1e9),
        },
        timeout: 20000,
      })
      .catch(() => null);
    out.facetStatus = fr ? fr.status() : 0;
  } catch (e) {
    out.err = e.message;
  } finally {
    out.ms = Date.now() - t0;
    if (browser) await browser.close().catch(() => {});
  }
  return out;
}

(async () => {
  console.log(`\nBenching ${N} sessions on ${GATEWAY} (event ${EVENT_ID})`);
  console.log(`Base user: ${USER_BASE}${SESSION_PREFIX}<rotating>\n`);

  // Run in parallel — same as the pool's Promise.all fill.
  const t0 = Date.now();
  const results = await Promise.all(Array.from({ length: N }, (_, i) => benchOne(i)));
  const totalMs = Date.now() - t0;

  const verdict = (r) =>
    r.facetStatus === 200 ? "GOOD"
      : [401, 403].includes(r.facetStatus) ? "FLAGGED"
      : [404, 410].includes(r.facetStatus) ? "DEAD-EVENT"
      : !r.tmpt ? "NO-TMPT"
      : "OTHER";

  for (const r of results) {
    console.log(
      `  [${verdict(r).padEnd(10)}] ip=${String(r.exitIp).padEnd(16)} ` +
      `tmpt=${r.tmpt ? "Y" : "n"} page=${r.pageStatus} facets=${r.facetStatus} ` +
      `${(r.ms / 1000).toFixed(1)}s${r.err ? " err=" + r.err : ""}`
    );
  }

  const good = results.filter((r) => r.facetStatus === 200).length;
  const flagged = results.filter((r) => [401, 403].includes(r.facetStatus) || !r.tmpt).length;
  const dead = results.filter((r) => [404, 410].includes(r.facetStatus)).length;
  const avgGoodMs = (() => {
    const g = results.filter((r) => r.facetStatus === 200);
    return g.length ? (g.reduce((s, r) => s + r.ms, 0) / g.length / 1000).toFixed(1) : "n/a";
  })();

  console.log(`\n──────── SUMMARY ────────`);
  console.log(`  GOOD (facets 200): ${good}/${N} (${((good / N) * 100).toFixed(0)}%)`);
  console.log(`  FLAGGED:           ${flagged}/${N} (${((flagged / N) * 100).toFixed(0)}%)`);
  if (dead) console.log(`  DEAD-EVENT:        ${dead}/${N}  <- pass a live --event=<id> to re-test these`);
  console.log(`  avg bind time (good pages): ${avgGoodMs}s`);
  console.log(`  wall clock (parallel):      ${(totalMs / 1000).toFixed(1)}s`);
  console.log(`─────────────────────────`);
  console.log(
    dead >= N / 2
      ? `\nNOTE: mostly DEAD-EVENT — the default event id is expired. Re-run with a LIVE event:\n  node scripts/benchProxies.mjs --n=${N} --event=<liveEventId>\n`
      : good / N >= 0.7
      ? `\nVERDICT: low flag rate — this pool is good enough to cut the 4x retries & tmpt polling.\n`
      : `\nVERDICT: high flag rate — expect the same retry tax as IPRoyal on this pool.\n`
  );
  process.exit(0);
})().catch((e) => {
  console.error("bench failed:", e);
  process.exit(1);
});
