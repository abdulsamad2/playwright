import { devices, chromium, request as playwrightRequest } from "patchright";
import fs from "fs/promises";
import path from "path";
import { Camoufox } from "camoufox-js";

import { BrowserFingerprint } from "./browserFingerprint.js";
import proxyArray, { getSeedProxy } from "./helpers/proxy.js";
import mongoose from "mongoose";

// Browser engine: "camoufox" (stealth Firefox — passes EPS HEADLESS, no xvfb) or
// "chrome" (real headed Chrome via patchright — needs a display/xvfb). Camoufox is
// the default because it's the only thing that beats EPS's headless detection.
const BROWSER_ENGINE = (process.env.BROWSER_ENGINE || "camoufox").toLowerCase();
const USE_CAMOUFOX = BROWSER_ENGINE === "camoufox";
const REQUEST_CONTEXT_MODE = () => process.env.REQUEST_CONTEXT_MODE === "1";
// SEED/SCRAPE SPLIT: mint the tmpt-carrying cookie jar on clean bart RESIDENTIAL
// (the only IPs that pass reCAPTCHA/EPS at the event page), then INJECT that jar
// into pool contexts on the cheap Mongo datacenter proxies for the facets calls.
// facets validates the tmpt cookie and is IP-agnostic, so one seed serves the whole
// pool. Enable with SEED_SPLIT=1 + BART_* creds in .env (see helpers/proxy.js).
// Read at CALL time (not module-load) so .env values are picked up even though this
// module is imported before app.js calls dotenv.config().
const SEED_SPLIT = () => process.env.SEED_SPLIT === "1";
// tmpt lives 60 min (read from the cookie's own `expires`). Re-seed at 50 min for
// a safety margin; the 403-storm handler re-mints early if a session dies sooner.
const SEED_TTL_MS = () => parseInt(process.env.SEED_TTL_MS, 10) || 50 * 60 * 1000;
// SEED_FARM=1 → read ready-made jars from the shared `seed_jars` collection (kept
// fresh by the separate cookie-farm service) instead of minting on bart in-process.
// Off (default) = unchanged self-mint behaviour. See cookie-farm/README.md.
const SEED_FARM = () => process.env.SEED_FARM === "1";
// SELF_MINT=1 → when the farm has NO jar to serve, mint a tmpt in-process by
// navigating TM on the page's own proxy instead of stalling. The farm jar is still
// always preferred; this only fires when the jar lease comes back empty.
//
// OFF BY DEFAULT — measured, not assumed (scripts/testSelfMint.mjs, farm stubbed dry):
//   • Camoufox (default engine): no tmpt minted at all (39 TM cookies, tmpt=no).
//   • Real Chrome: tmpt minted and cached, but facets still 403.
//   • Either way the init path still binds the seed page, so the pool reports
//     "1 page ready" while that page 403s every request — worse than failing loudly.
// Enable only with SELF_MINT=1 after a mint path is proven to pass EPS on the IP
// in question. The default (unset/0) is the strict farm-only consumer behaviour.
const SELF_MINT = () => process.env.SELF_MINT === "1";
// --- Per-exit-IP throughput (the limit that actually governs this scraper) --------
// MEASURED 2026-08-09, scripts/jarRateRamp.mjs, one token on one IP, sliding 60s window:
//     <=29 calls/min -> 3-5% fail      30-39 -> 22% fail      40+ -> 78% fail
// and an IP driven into a block answered 200 again ~75s after the load stopped. So the
// safe operating point is ~20/min per exit IP with a ~90s rest after a 403.
// IP_RATE_PER_MIN=0 disables pacing entirely (not recommended).
const IP_RATE_PER_MIN = () => {
  const v = parseInt(process.env.IP_RATE_PER_MIN, 10);
  return Number.isFinite(v) ? v : 20;
};
const IP_COOLDOWN_MS = () => parseInt(process.env.IP_COOLDOWN_MS, 10) || 90000;
// --- ONE JAR, ONE EXIT IP — exclusive Mongo lease -------------------------------
// MEASURED 2026-08-09 (scripts/jarIpBudget.mjs, reproduced in four independent runs):
// TM revokes a tmpt session PERMANENTLY once it is used from an ELEVENTH exit IP. Every
// IP then returns 403 {"response":"block"} — including ones that answered 200 seconds
// earlier. It is a session-hijack heuristic, not a rate limit and not the token wearing
// out:
//     one token on ONE IP  -> 775+ consecutive facets calls, zero failures
//     one token on 11 IPs  -> dead on the 11th, and dead everywhere afterwards
//
// The old readFarmJar() handed the same jar round-robin to every page, and every page
// binds its OWN proxy. With 6-8 farm jars against 60 pages per machine, each jar crossed
// ten IPs within seconds of being minted and was revoked on arrival. The pool then read
// the resulting 403 storm as dead proxies, restarted the browser and discarded the jar,
// so its replacement died the same way. That loop — not blocked proxies, not call volume
// — is what held throughput down.
//
// A jar is therefore LEASED to exactly one page on exactly one proxy for its whole life.
// When nothing is free the caller gets null and does not bind that page: an unfed page
// costs nothing, whereas a shared jar kills every page holding it.
// Set JAR_LEASE=0 to fall back to the old sharing behaviour (not recommended).
const JAR_LEASE = () => process.env.JAR_LEASE !== "0";

// A lease is BOUNDED and RENEWED, not held to the jar's own expiry. ownerId embeds the
// pid, and releaseFarmJar only runs on the graceful teardown paths — so a PM2 kill (the
// 750M max_memory_restart, or any crash) stranded this instance's whole jar allocation
// for up to the jar's full 60 minutes, and the restarted process could not reclaim its
// own leases because the pid had changed. A live pool now pushes its leases forward
// every JAR_LEASE_RENEW_MS; a dead one stops, and its jars return to the pool within
// JAR_LEASE_TTL_MS. A reclaimed jar does land on a second exit IP, spending one of the
// ~10 it gets before TM revokes it — far cheaper than losing the jar outright.
const JAR_LEASE_TTL_MS = () =>
  Math.max(60000, parseInt(process.env.JAR_LEASE_TTL_MS, 10) || 5 * 60 * 1000);
const JAR_LEASE_RENEW_MS = () =>
  Math.max(15000, parseInt(process.env.JAR_LEASE_RENEW_MS, 10) || 60 * 1000);

let _leaseSeq = 0;
const newOwnerId = () => `${process.env.MACHINE_ID || "m"}:${process.pid}:${++_leaseSeq}`;

function _tmptOf(cookies) { return (cookies.find((c) => c.name === "tmpt") || {}).value; }

// Claim a jar no other page holds, freshest first so the page gets the most remaining
// life. The lease runs to the jar's own expiresAt: a jar is useless past 60 minutes
// anyway, and handing a crashed owner's jar to a DIFFERENT proxy is exactly the thing
// that revokes it. Deliberately avoids an aggregation-pipeline update (needs Mongo 4.2+)
// — a compare-and-set on one _id at a time is just as atomic and works everywhere.
// ttlMs caps how long the lease is held. Pool pages pass nothing and hold the jar for its
// whole life; short-lived callers that have no teardown hook pass a TTL so a leaked lease
// frees itself instead of stranding a good jar until expiry.
async function leaseFarmJar(ownerId, proxyKey, ttlMs) {
  try {
    const coll = mongoose.connection.db.collection("seed_jars");
    const now = new Date();
    const free = { $or: [{ leaseUntil: { $exists: false } }, { leaseUntil: { $lte: now } }] };
    const candidates = await coll
      .find({ status: "healthy", expiresAt: { $gt: now }, ...free })
      .sort({ mintedAt: -1 })
      .limit(20)
      .toArray();

    for (const c of candidates) {
      const until = ttlMs
        ? new Date(Math.min(new Date(c.expiresAt).getTime(), Date.now() + ttlMs))
        : c.expiresAt;
      const res = await coll.findOneAndUpdate(
        { _id: c._id, status: "healthy", expiresAt: { $gt: new Date() }, ...free },
        { $set: { leaseOwner: ownerId, leaseProxy: proxyKey || "direct", leaseUntil: until, leasedAt: new Date() } },
        { returnDocument: "after" }
      );
      const doc = res && (res.value !== undefined ? res.value : res);
      if (doc && doc.cookies && doc.cookies.length) {
        return { cookies: doc.cookies, jarId: doc._id, tmpt: _tmptOf(doc.cookies) };
      }
    }
    return null; // every jar is already spoken for — the farm is the bottleneck, not us
  } catch (e) {
    console.warn("[SeedFarm] lease failed:", e.message);
    return null;
  }
}

// Hand a jar back when its page dies, so a replacement page can pick it up. Only the
// holder may release it, so a stale owner cannot free someone else's lease.
async function releaseFarmJar(jarId, ownerId) {
  if (!jarId) return;
  try {
    await mongoose.connection.db.collection("seed_jars").updateOne(
      { _id: jarId, leaseOwner: ownerId },
      { $unset: { leaseOwner: "", leaseProxy: "", leaseUntil: "", leasedAt: "" } }
    );
  } catch { /* best effort — the lease expires with the jar regardless */ }
}

// Push every lease this instance still holds forward by one TTL. Matching on leaseOwner
// as well as _id means a jar we have already lost (our renew stalled long enough for
// someone else to claim it) is left alone rather than yanked onto a second exit IP.
async function renewFarmLeases(metas) {
  const until = new Date(Date.now() + JAR_LEASE_TTL_MS());
  const ops = [];
  for (const m of metas) {
    if (!m || !m.jarId || !m.jarOwner) continue;
    ops.push({
      updateOne: {
        filter: { _id: m.jarId, leaseOwner: m.jarOwner },
        update: { $set: { leaseUntil: until } },
      },
    });
  }
  if (!ops.length) return { renewed: 0, lost: 0 };
  try {
    const r = await mongoose.connection.db.collection("seed_jars").bulkWrite(ops, { ordered: false });
    const renewed = r.matchedCount || 0;
    return { renewed, lost: ops.length - renewed };
  } catch (e) {
    console.warn("[SeedFarm] lease renew failed:", e.message);
    return { renewed: 0, lost: 0 }; // a failed renew is not a lost lease; the next tick retries
  }
}

// Is there a jar this instance could actually lease right now? In strict farm-consumer
// mode a page cannot bind without one, so an instance holding none must not CLAIM events:
// it locks the stalest ones, fails every one of them inside a second, and hands them back
// marked failed — while the instances that DO hold jars are left with whatever was not
// locked. Measured on the fleet 2026-09-04: 5 of 8 instances jarless, the one healthy
// instance at 100% success, and 79 of 196 events past their 3-minute SLA.
// Cached briefly because this is consulted on every claim cycle.
let _freeJarProbe = { at: 0, free: false };
async function farmHasFreeJar(maxAgeMs = 2000) {
  if (Date.now() - _freeJarProbe.at < maxAgeMs) return _freeJarProbe.free;
  try {
    const now = new Date();
    const doc = await mongoose.connection.db.collection("seed_jars").findOne(
      {
        status: "healthy",
        expiresAt: { $gt: now },
        $or: [{ leaseUntil: { $exists: false } }, { leaseUntil: { $lte: now } }],
      },
      { projection: { _id: 1 } }
    );
    _freeJarProbe = { at: Date.now(), free: !!doc };
  } catch {
    // Can't tell → say yes. A Mongo blip must not stop the whole fleet from claiming.
    _freeJarProbe = { at: Date.now(), free: true };
  }
  return _freeJarProbe.free;
}

// Legacy shared-jar reader, used only when JAR_LEASE=0. Kept so the old behaviour is one
// env var away if the lease ever needs to be switched off in a hurry.
let _farmJars = { list: [], at: 0, rr: 0 };
async function readFarmJarShared() {
  if (!_farmJars.list.length || Date.now() - _farmJars.at > 10000) {
    try {
      const docs = await mongoose.connection.db.collection("seed_jars")
        .find({ status: "healthy", expiresAt: { $gt: new Date() } }).sort({ slot: 1 }).toArray();
      _farmJars = { list: docs.map((d) => d.cookies), at: Date.now(), rr: _farmJars.rr };
    } catch (e) {
      console.warn("[SeedFarm] read failed:", e.message);
    }
  }
  const N = _farmJars.list.length;
  if (!N) return null;
  const cookies = _farmJars.list[_farmJars.rr % N];
  _farmJars.rr = (_farmJars.rr + 1) % N;
  return { cookies, jarId: null, tmpt: _tmptOf(cookies) };
}

// --- Usage counter: OBSERVABILITY ONLY ------------------------------------------
// JAR_CALL_BUDGET (retire a jar at 400 calls) was REMOVED. It had no measured basis and
// was the largest single jar killer in production: 12 of 29 dead jars hit it, after a
// median of 14 minutes of a 60-minute life. Measured against that, one token served 775+
// consecutive facets calls on ONE IP with zero degradation (scripts/jarBurnTest.mjs), and
// tmpt's expiry does not move when it is used (scripts/tmptRefreshProbe.mjs) — so the
// only real limit is the 60-minute clock that starts at mint. useCount is still tracked
// because it is useful for spotting a starved fleet, but it no longer retires anything.
const _jarPending = new Map(); // tmpt -> calls not yet flushed
async function noteJarUsage(tmpt, n) {
  if (!tmpt || n <= 0) return;
  const pending = (_jarPending.get(tmpt) || 0) + n;
  if (pending < 15) { _jarPending.set(tmpt, pending); return; } // batch the DB writes
  _jarPending.set(tmpt, 0);
  try {
    await mongoose.connection.db.collection("seed_jars").updateOne(
      { cookies: { $elemMatch: { name: "tmpt", value: tmpt } } },
      { $inc: { useCount: pending } }
    );
  } catch { /* best effort */ }
}

// NOTE: the per-machine proxy blocklist was REMOVED. It excluded a proxy locally after
// N consecutive errors, but the state lived in-process, so each of the ~10 PM2
// instances had to rediscover the same bad IPs by burning ~5 binds apiece — and every
// one of those failed binds also voted a perfectly good farm jar toward death via
// noteFarmJarResult. Measured 2026-07-30: ~44% of the pool 403s, so the blocklist was
// destroying jars far faster than it was saving binds, and the 30-min expiry made each
// instance relearn it all over again. A proxy that 403s is simply retried.
// isBrowserCrashError() went with it — its only job was to keep a browser crash from
// being charged to the proxy, and there is no longer a proxy tally to charge.

// NOTE: protocol stability requires playwright-core@1.60.0 (matches the Camoufox 150
// build). With the matching version there are ZERO protocol errors — no swallow needed.
// Device settings
const iphone13 = devices["iPhone 13"];

// Constants
const COOKIES_FILE = "cookies.json";

// Persistent browser context for API requests (bypasses TLS fingerprinting)
let apiBrowser = null;
let apiContext = null;
let apiPage = null;
let apiContextLock = false;
const CONFIG = {
  COOKIE_REFRESH_INTERVAL: 45 * 60 * 1000, // 45 minutes
  PAGE_TIMEOUT: 90000, // 60 seconds for page operations
  MAX_RETRIES: 5, // Reduced from 5 to fail faster
  RETRY_DELAY: 8000, // Reduced from 10s to 8s
  CHALLENGE_TIMEOUT: 15000, // 15 seconds for challenge handling
  COOKIE_REFRESH_TIMEOUT: 2 * 60 * 1000, // 2 minutes timeout for cookie refresh
  MAX_REFRESH_RETRIES: 3, // Maximum retries for cookie refresh with new proxy/event
};

let browser = null;

// DIRECT_MODE: run with NO proxy — scrape straight from this host's (clean) IP.
// A clean residential IP + headed real Chrome returns 200 with full facet data; the
// proxies were the CAUSE of the blocks (their IPs were burned), not the fix. Set
// DIRECT_MODE=1 (or NO_PROXY=1) to bypass all proxy logic. NOTE: a single IP has an
// EPS rate limit — keep volume moderate (throttle / longer intervals) or it will get
// flagged like any other IP. Good for low/medium volume; high volume needs many IPs.
const DIRECT_MODE = process.env.DIRECT_MODE === "1" || process.env.NO_PROXY === "1";

// Tri-state cache for whether the real Chrome channel is usable on this host.
// null = not yet attempted, true = available, false = fall back to bundled Chromium.
let _chromeChannelOk = null;

/**
 * Launch Chromium using the real installed Google Chrome (channel:'chrome') when available.
 *
 * Ticketmaster's EPS / "iamNotaRobot" anti-bot now fingerprints and blocks patchright's
 * BUNDLED Chromium build with a hard `403 {"response":"block"}` — even from a clean
 * residential IP. The real Chrome binary clears EPS (verified: event page 200, facets 200).
 * Falls back to the bundled Chromium if Chrome isn't installed so the scraper still runs.
 *
 * Override with BROWSER_CHANNEL: "chrome" (default), "chrome-beta", or "chromium"/"none"
 * to force the bundled build.
 */
async function launchChromium(launchOptions = {}) {
  // CAMOUFOX (default): stealth Firefox that passes EPS HEADLESS (Chrome headless is
  // blocked by EPS; Camoufox isn't). Ignores Chromium args; takes its own options.
  if (USE_CAMOUFOX) {
    const opts = {
      // HEADLESS by default — Camoufox passes EPS headless (its whole advantage), and
      // production servers have no display. Set CAMOUFOX_HEADED=1 ONLY on a machine
      // with a display (local debugging) to watch the browser.
      headless: process.env.CAMOUFOX_HEADED !== "1",
      humanize: true,     // human-like cursor movement
    };
    if (launchOptions.proxy) opts.proxy = launchOptions.proxy; // {server,username,password}
    if (_chromeChannelOk === null) {
      _chromeChannelOk = true;
      console.log(`[browser] Using Camoufox (stealth Firefox, headless=${opts.headless}) — EPS-safe`);
    }
    // geoip aligns timezone/locale to the proxy IP (better stealth) — but Camoufox
    // fetches the proxy's public IP at launch to do it, and that lookup fails on
    // hosts/networks that can't reach the IP endpoints (e.g. cloud egress through a
    // residential proxy), ABORTING the whole launch. So try with geoip, and if the
    // lookup fails, fall back to launching without it. CAMOUFOX_GEOIP=0 forces off.
    if (process.env.CAMOUFOX_GEOIP === "0") {
      return await Camoufox({ ...opts, geoip: false });
    }
    try {
      return await Camoufox({ ...opts, geoip: true });
    } catch (e) {
      const msg = e?.message || "";
      if (/public proxy IP|geoip|IP address/i.test(msg)) {
        console.warn(`[browser] geoip IP-lookup failed (${msg.slice(0, 60)}) — relaunching without geoip`);
        return await Camoufox({ ...opts, geoip: false });
      }
      throw e;
    }
  }

  // CHROME engine: real installed Google Chrome (channel:'chrome'), must be HEADED on
  // a server (xvfb). EPS blocks bundled Chromium + all headless, so this is fallback.
  const channel = (process.env.BROWSER_CHANNEL ?? "chrome").trim();
  const wantChannel = channel && channel !== "chromium" && channel !== "none";

  if (wantChannel && _chromeChannelOk !== false) {
    try {
      const launched = await chromium.launch({ ...launchOptions, channel });
      if (_chromeChannelOk === null) {
        _chromeChannelOk = true;
        console.log(`[browser] Using real Chrome channel "${channel}" (EPS-safe)`);
      }
      return launched;
    } catch (err) {
      _chromeChannelOk = false;
      console.warn(
        `[browser] Chrome channel "${channel}" unavailable (${err.message.split("\n")[0]}). ` +
          `Falling back to bundled Chromium — EPS will likely block. ` +
          `Install Google Chrome on this host or set BROWSER_CHANNEL.`
      );
    }
  }

  return await chromium.launch(launchOptions);
}

/**
 * Make a TM API GET (facets / map) using the browser CONTEXT's request client.
 * This carries the context's cookies + a browser-consistent TLS, but is NOT a
 * document fetch — so it bypasses CORS entirely (required for Firefox/Camoufox,
 * which has no --disable-web-security). Works for both engines.
 * Returns { success, status, data?, error? }.
 */
async function apiGet(page, url, headers = {}) {
  try {
    const resp = await page.context().request.get(url, { headers, timeout: parseInt(process.env.API_TIMEOUT_MS, 10) || 18000 });
    const status = resp.status();
    if (status < 200 || status >= 400) {
      return { success: false, status, error: `HTTP ${status}` };
    }
    const data = await resp.json().catch(() => null);
    if (data == null) return { success: false, status, error: "Non-JSON / empty body" };
    return { success: true, status, data };
  } catch (e) {
    return { success: false, status: 0, error: e.message };
  }
}

function toStorageCookie(cookie) {
  const expires =
    typeof cookie?.expires === "number"
      ? cookie.expires
      : typeof cookie?.expiry === "number"
      ? cookie.expiry
      : -1;

  return {
    name: cookie?.name,
    value: cookie?.value,
    domain: cookie?.domain || ".ticketmaster.com",
    path: cookie?.path || "/",
    expires,
    httpOnly: !!cookie?.httpOnly,
    secure: cookie?.secure !== false,
    sameSite: cookie?.sameSite || "Lax",
  };
}

async function apiGetRequestContext(requestContext, url, headers = {}) {
  try {
    const resp = await requestContext.get(url, {
      headers,
      timeout: parseInt(process.env.API_TIMEOUT_MS, 10) || 18000,
    });
    const status = resp.status();
    if (status < 200 || status >= 400) {
      return { success: false, status, error: `HTTP ${status}` };
    }
    const data = await resp.json().catch(() => null);
    if (data == null) return { success: false, status, error: "Non-JSON / empty body" };
    return { success: true, status, data };
  } catch (e) {
    return { success: false, status: 0, error: e.message };
  }
}

async function apiGetFromWorker(worker, url, headers = {}) {
  if (worker?.requestContext) {
    return apiGetRequestContext(worker.requestContext, url, headers);
  }
  return apiGet(worker, url, headers);
}

/**
 * Gets a random location for browser fingerprinting
 */
function getRandomLocation() {
  const locations = [
    { locale: 'en-US', timezone: 'America/Los_Angeles', latitude: 34.052235, longitude: -118.243683 },
    { locale: 'en-US', timezone: 'America/New_York', latitude: 40.712776, longitude: -74.005974 },
    { locale: 'en-US', timezone: 'America/Chicago', latitude: 41.878113, longitude: -87.629799 },
    { locale: 'en-US', timezone: 'America/Denver', latitude: 39.739235, longitude: -104.990250 },
    { locale: 'en-CA', timezone: 'America/Toronto', latitude: 43.651070, longitude: -79.347015 },
    { locale: 'en-GB', timezone: 'Europe/London', latitude: 51.507351, longitude: -0.127758 },
  ];
  
  return locations[Math.floor(Math.random() * locations.length)];
}

/**
 * Generate a realistic iPhone user agent
 */
function getRealisticIphoneUserAgent() {
  // Updated to current iOS versions as of early 2026
  const iOSVersions = ['18_0', '18_1', '18_2', '18_3', '19_0', '19_1', '19_2'];
  const version = iOSVersions[Math.floor(Math.random() * iOSVersions.length)];
  return `Mozilla/5.0 (iPhone; CPU iPhone OS ${version} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/${version.split('_')[0]}.0 Mobile/15E148 Safari/604.1`;
}

/**
 * Enhance fingerprint with more browser properties
 */
function enhancedFingerprint() {
  const baseFingerprint = BrowserFingerprint.generate();
  
  // Add additional properties to make fingerprint more realistic
  return {
    ...baseFingerprint,
    webgl: {
      vendor: "Apple Inc.",
      renderer: "Apple GPU",
    },
    fonts: [
      "Arial",
      "Courier New",
      "Georgia",
      "Times New Roman",
      "Trebuchet MS",
      "Verdana"
    ],
    plugins: [
      "PDF Viewer",
      "Chrome PDF Viewer",
      "Chromium PDF Viewer",
      "Microsoft Edge PDF Viewer",
      "WebKit built-in PDF"
    ],
    screen: {
      width: 390,
      height: 844,
      availWidth: 390,
      availHeight: 844,
      colorDepth: 24,
      pixelDepth: 24
    },
    timezone: {
      offset: new Date().getTimezoneOffset()
    }
  };
}

/**
 * Simulate various mobile interactions to appear more human-like
 */
async function simulateMobileInteractions(page) {
  try {
    // Get viewport size
    const viewportSize = page.viewportSize();
    if (!viewportSize) return;
    
    // Random scroll amounts
    const scrollOptions = [
      { direction: 'down', amount: 300 },
      { direction: 'down', amount: 500 },
      { direction: 'down', amount: 800 },
      { direction: 'up', amount: 200 },
      { direction: 'up', amount: 400 }
    ];
    
    // Pick 2-3 random scroll actions
    const scrollCount = 2 + Math.floor(Math.random() * 2);
    for (let i = 0; i < scrollCount; i++) {
      const option = scrollOptions[Math.floor(Math.random() * scrollOptions.length)];
      
      // Scroll with a dynamic speed
      const scrollY = option.direction === 'down' ? option.amount : -option.amount;
      await page.evaluate((y) => {
        window.scrollBy({
          top: y,
          behavior: 'smooth'
        });
      }, scrollY);
      
      // Random pause between scrolls (500-2000ms)
      await page.waitForTimeout(500 + Math.floor(Math.random() * 1500));
    }
    
    // Simulate random taps/clicks (1-2 times)
    const tapCount = 1 + Math.floor(Math.random() * 2);
    for (let i = 0; i < tapCount; i++) {
      // Random position within viewport
      const x = 50 + Math.floor(Math.random() * (viewportSize.width - 100));
      const y = 150 + Math.floor(Math.random() * (viewportSize.height - 300));
      
      await page.mouse.click(x, y);
      await page.waitForTimeout(500 + Math.floor(Math.random() * 1000));
    }
  } catch (error) {
    console.warn("Error during mobile interaction simulation:", error.message);
  }
}

/**
 * Initialize the browser with enhanced fingerprinting
 */

async function initBrowser(proxy) {
  let context = null;
  let contextProxy = null;
  
  try {
    // Get randomized human-like properties
    const location = getRandomLocation();
    
    // For persisting browser sessions, use same browser if possible
    if (!browser || !browser.isConnected()) {
      // Launch options with enhanced stealth
      const launchOptions = {
        headless: false,
        args: [
          '--disable-blink-features=AutomationControlled',
          '--disable-features=IsolateOrigins,site-per-process',
          '--disable-web-security',
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--no-first-run',
          '--no-default-browser-check',
          '--disable-infobars',
          '--disable-notifications',
          '--disable-dev-shm-usage',
          '--disable-gpu',
          '--disable-background-timer-throttling',
          '--disable-backgrounding-occluded-windows',
          '--disable-renderer-backgrounding',
          '--disable-features=TranslateUI',
          '--disable-ipc-flooding-protection',
          '--enable-features=NetworkService,NetworkServiceInProcess',
          '--force-color-profile=srgb',
          '--metrics-recording-only',
          '--mute-audio',
          '--disable-hang-monitor',
          '--disable-prompt-on-repost',
          '--disable-sync',
          '--password-store=basic',
          '--use-mock-keychain'
        ],
        timeout: 90000,
      };

      if (proxy && typeof proxy === 'object' && proxy.proxy) {
        try {
          // Extract hostname and port from proxy string
          const proxyString = proxy.proxy;
          
          // Ensure proxyString is a string before using string methods
          if (typeof proxyString !== 'string') {
            throw new Error('Invalid proxy format: proxy.proxy must be a string, got ' + typeof proxyString);
          }
          
          // Check if proxy string is in correct format (host:port)
          if (!proxyString.includes(':')) {
            throw new Error('Invalid proxy format: ' + proxyString);
          }
          
          const [hostname, portStr] = proxyString.split(':');
          const port = parseInt(portStr) || 80;

          const parsedProxy = {
            server: `http://${hostname}:${port}`,
            username: proxy.username,
            password: proxy.password,
          };

          // Camoufox warns at launch when proxy is set without geoip. Using a
          // context-level proxy preserves routing while avoiding that warning.
          if (USE_CAMOUFOX) {
            contextProxy = parsedProxy;
          } else {
            launchOptions.proxy = parsedProxy;
          }
          
          console.log(`Configuring browser with proxy: ${hostname}:${port}`);
        } catch (error) {
          throw new Error(`Invalid proxy configuration, cannot refresh cookies without proxy: ${error.message}`);
        }
      } else if (!DIRECT_MODE) {
        throw new Error('Cannot refresh cookies without a valid proxy');
      } // DIRECT_MODE: no proxy needed — refresh straight from this host's IP.

      // Launch browser (real Chrome channel when available — EPS blocks bundled Chromium)
            browser = await launchChromium(launchOptions);
    }
    
    // Create context — clean, CONSISTENT real-Chrome desktop fingerprint.
    // Previously this emulated an iPhone *Safari* UA on the *Chrome* engine and then
    // hand-faked window.chrome / navigator.plugins — a glaring mismatch EPS flags
    // (Safari UA but Chrome-only objects present). With real Chrome (channel:'chrome')
    // + patchright, the native fingerprint is already genuine and self-consistent, so
    // we override NOTHING (no UA, no headers, no init scripts). Just locale/tz/geo.
    const res = [
      { w: 1920, h: 1080 }, { w: 1680, h: 1050 }, { w: 1536, h: 864 }, { w: 1440, h: 900 },
    ][Math.floor(Math.random() * 4)];
    context = await browser.newContext({
      locale: location.locale,
      timezoneId: location.timezone,
      geolocation: {
        latitude: location.latitude,
        longitude: location.longitude,
        accuracy: 100 * Math.random() + 50,
      },
      permissions: ["geolocation", "notifications"],
      viewport: USE_CAMOUFOX ? null : { width: res.w, height: res.h },
      javaScriptEnabled: true,
      acceptDownloads: true,
      ignoreHTTPSErrors: true,
      bypassCSP: true,
      ...(contextProxy ? { proxy: contextProxy } : {}),
    });
    
    // No manual stealth init scripts. patchright already neutralizes the automation
    // tells (navigator.webdriver etc.), and real Chrome provides genuine, consistent
    // navigator.plugins / languages / window.chrome / connection / battery / screen.
    // The old hand-rolled fakes (plugins:[1,2,3,4,5], a fabricated window.chrome —
    // which under the former Safari UA was an outright contradiction) were detectable
    // inconsistencies, so they are removed in favor of the real browser's own values.
    
    // Create a new page and simulate human behavior
    const page = await context.newPage();
    
    // Set realistic page load timeout
    page.setDefaultTimeout(CONFIG.PAGE_TIMEOUT);
    page.setDefaultNavigationTimeout(CONFIG.PAGE_TIMEOUT);
    
    // Human-like delay before any action
    await page.waitForTimeout(1500 + Math.random() * 2500);
    await simulateMobileInteractions(page);
    
    return { context, fingerprint: enhancedFingerprint(), page, browser };
  } catch (error) {
    console.error("Error initializing browser:", error.message);
    
    // Cleanup on error
    if (context) await context.close().catch(() => {});
    
    throw error;
  }
} // Added missing closing bracket for initBrowser function

/**
 * Handle Ticketmaster challenge pages (CAPTCHA, etc.)
 */
async function handleTicketmasterChallenge(page) {
  try {
    const challengePresent = await page.evaluate(() => {
      const bodyText = document.body.textContent || '';
      const titleText = document.title || '';
      
      // Check for various challenge indicators
      return bodyText.includes("Your Browsing Activity Has Been Paused") ||
             bodyText.includes("Access Denied") ||
             bodyText.includes("Security Check") ||
             bodyText.includes("Please verify you are a human") ||
             titleText.includes("Access Denied") ||
             titleText.includes("Just a moment") ||
             document.querySelector('#px-captcha') !== null ||
             document.querySelector('.g-recaptcha') !== null;
    }).catch(() => false);

    if (challengePresent) {
      console.log(" CHALLENGE DETECTED: Bot detection triggered - aborting this session");
      console.log(" This proxy/session is compromised. Will request new proxy for retry.");
      
      // Throw error to trigger proxy rotation
      throw new Error("CHALLENGE_DETECTED_ABORT_SESSION");
    }
    
    return true;
  } catch (error) {
    if (error.message === "CHALLENGE_DETECTED_ABORT_SESSION") {
      throw error; // Re-throw to propagate up
    }
    console.warn("Challenge check failed:", error.message);
    return false;
  }
}

/**
 * Check for Ticketmaster challenge page
 */
async function checkForTicketmasterChallenge(page) {
  try {
    // Check for CAPTCHA or other blocking mechanisms
    const challengeSelector = "#challenge-running"; // Example selector for CAPTCHA
    const isChallengePresent = (await page.$(challengeSelector)) !== null;

    if (isChallengePresent) {
      console.warn("Ticketmaster challenge detected");
      return true;
    }

    // Also check via text content
    const challengePresent = await page.evaluate(() => {
      return document.body.textContent.includes(
        "Your Browsing Activity Has Been Paused"
      );
    }).catch(() => false);

    return challengePresent;
  } catch (error) {
    console.error("Error checking for Ticketmaster challenge:", error);
    return false;
  }
}

/**
 * Capture cookies from the browser
 */
async function captureCookies(page, fingerprint) {
  let retryCount = 0;
  const MAX_RETRIES = 5;
  
  while (retryCount < MAX_RETRIES) {
    try {
      const challengePresent = await page.evaluate(() => {
        return document.body.textContent.includes(
          "Your Browsing Activity Has Been Paused"
        );
      }).catch(() => false);

      if (challengePresent) {
        console.log(
          `Attempt ${retryCount + 1}: Challenge detected during cookie capture`
        );

        const challengeResolved = await handleTicketmasterChallenge(page);
        if (!challengeResolved) {
          if (retryCount === MAX_RETRIES - 1) {
            console.log("Max retries reached during challenge resolution");
            return { cookies: null, fingerprint };
          }
          await page.waitForTimeout(CONFIG.RETRY_DELAY);
          retryCount++;
          continue;
        }
      }

      // Get context from page's browser context
      const context = page.context();
      if (!context) {
        throw new Error("Cannot access browser context from page");
      }

      let cookies = await context.cookies().catch(() => []);

      if (!cookies?.length) {
        console.log(`Attempt ${retryCount + 1}: No cookies captured`);
        if (retryCount === MAX_RETRIES - 1) {
          return { cookies: null, fingerprint };
        }
        await page.waitForTimeout(CONFIG.RETRY_DELAY);
        retryCount++;
        continue;
      }

      // Filter out reCAPTCHA Google cookies
      cookies = cookies.filter(cookie => !cookie.name.includes('_grecaptcha') && 
                                      !cookie.domain.includes('google.com'));

      // Check if we have enough cookies from ticketmaster.com
      const ticketmasterCookies = cookies.filter(cookie => 
        cookie.domain.includes('ticketmaster.com') || 
        cookie.domain.includes('.ticketmaster.com')
      );

      if (ticketmasterCookies.length < 3) {
        console.log(`Attempt ${retryCount + 1}: Not enough Ticketmaster cookies`);
        if (retryCount === MAX_RETRIES - 1) {
          return { cookies: null, fingerprint };
        }
        await page.waitForTimeout(CONFIG.RETRY_DELAY);
        retryCount++;
        continue;
      }

      // Check JSON size
      const cookiesJson = JSON.stringify(cookies, null, 2);
      const lineCount = cookiesJson.split('\n').length;
      
      if (lineCount < 200) {
        console.log(`Attempt ${retryCount + 1}: Cookie JSON too small (${lineCount} lines)`);
        if (retryCount === MAX_RETRIES - 1) {
          return { cookies: null, fingerprint };
        }
        await page.waitForTimeout(CONFIG.RETRY_DELAY);
        retryCount++;
        continue;
      }

      const oneHourFromNow = Date.now() + CONFIG.COOKIE_REFRESH_INTERVAL;
      cookies = cookies.map((cookie) => ({
        ...cookie,
        expires: oneHourFromNow / 1000,
        expiry: oneHourFromNow / 1000,
      }));

      // Add cookies one at a time with error handling
      for (const cookie of cookies) {
        try {
          await context.addCookies([cookie]);
        } catch (error) {
          console.warn(`Error adding cookie ${cookie.name}:`, error.message);
        }
      }

      // Save cookies to file
      await saveCookiesToFile(cookies);
      console.log(`Successfully captured cookies on attempt ${retryCount + 1}`);
      return { cookies, fingerprint };
    } catch (error) {
      console.error(`Error capturing cookies on attempt ${retryCount + 1}:`, error);
      if (retryCount === MAX_RETRIES - 1) {
        return { cookies: null, fingerprint };
      }
      await page.waitForTimeout(CONFIG.RETRY_DELAY);
      retryCount++;
    }
  }

  return { cookies: null, fingerprint };
}

/**
 * Save cookies to a file
 */
async function saveCookiesToFile(cookies) {
  try {
    // Format the cookies with updated expiration
    const cookieData = cookies.map(cookie => ({
      ...cookie,
      expires: cookie.expires || Date.now() + CONFIG.COOKIE_REFRESH_INTERVAL,
      expiry: cookie.expiry || Date.now() + CONFIG.COOKIE_REFRESH_INTERVAL
    }));

    await fs.writeFile(COOKIES_FILE, JSON.stringify(cookieData, null, 2));
    console.log(`Saved ${cookies.length} cookies to ${COOKIES_FILE}`);
    return true;
  } catch (error) {
    console.error(`Error saving cookies to file: ${error.message}`);
    return false;
  }
}

/**
 * Load cookies from file
 */
async function loadCookiesFromFile() {
  try {
    const cookiesFile = path.join(process.cwd(), COOKIES_FILE);
    
    // Check if file exists
    try {
      await fs.access(cookiesFile);
    } catch (error) {
      console.log("Cookies file does not exist");
      return null;
    }
    
    // Read and parse
    const fileData = await fs.readFile(cookiesFile, 'utf8');
    const cookies = JSON.parse(fileData);
    
    if (!Array.isArray(cookies) || cookies.length === 0) {
      console.log("Invalid or empty cookies file");
      return null;
    }
    
    console.log(`Loaded ${cookies.length} cookies from file`);
    return cookies;
  } catch (error) {
    console.error(`Error loading cookies from file: ${error.message}`);
    return null;
  }
}

/**
 * Get fresh cookies by opening a browser and navigating to Ticketmaster
 */
async function refreshCookies(eventId, proxy = null) {
  // SPLIT MODE CHOKEPOINT: every caller of refreshCookies (SessionManager,
  // CookieManager, scraperManager) funnels through here, so one guard covers them all.
  // Prefer a ready farm jar — it costs nothing and is already validated. Only when the
  // farm has nothing does behaviour diverge: SELF_MINT on falls through to the real
  // browser refresh below (mint our own tmpt rather than hand back an empty jar that
  // guarantees a 403), SELF_MINT=0 returns empty so the caller fails cleanly and waits
  // for the farm. SEED_FARM_FALLBACK=1 skips the chokepoint entirely.
  if (SEED_SPLIT() && process.env.SEED_FARM_FALLBACK !== "1") {
    // Callers of this legacy path have no teardown hook to release a lease, so take one
    // with a short TTL rather than none at all: a jar handed out here still must not end
    // up on a second exit IP while a pool page is driving it (see the lease notes above).
    const lease = SEED_FARM()
      ? (JAR_LEASE()
          ? await leaseFarmJar(newOwnerId(), proxy ? (proxy.id || proxy.proxy) : "direct",
              parseInt(process.env.REFRESH_JAR_LEASE_MS, 10) || 5 * 60 * 1000)
          : await readFarmJarShared())
      : null;
    const jar = lease && lease.cookies;
    if ((jar && jar.length) || !SELF_MINT()) {
      return { cookies: jar || [], fingerprint: BrowserFingerprint.generate(), lastRefresh: Date.now() };
    }
    console.log('[Cookies] farm dry — self-minting a fresh jar in-process');
  }
  if ((!proxy || !proxy.proxy) && !DIRECT_MODE) {
    throw new Error('Cannot refresh cookies without a valid proxy');
  }
  let retryCount = 0;
  let lastError = null;
  
  while (retryCount <= CONFIG.MAX_REFRESH_RETRIES) {
    let localContext = null;
    let page = null;
    let browserInstance = null;
    let timeoutId = null;
    
    try {
      console.log(`Refreshing cookies using event ${eventId} (attempt ${retryCount + 1}/${CONFIG.MAX_REFRESH_RETRIES + 1})`);

      // Try to load existing cookies first (only on first attempt)
      if (retryCount === 0) {
        const existingCookies = await loadCookiesFromFile();
        if (existingCookies && existingCookies.length >= 3) {
          const cookieAge = existingCookies[0]?.expiry ? 
            (existingCookies[0].expiry * 1000 - Date.now()) : 0;
          
          if (cookieAge > 0 && cookieAge < CONFIG.COOKIE_REFRESH_INTERVAL) {
            console.log(`Using existing cookies (age: ${Math.floor(cookieAge/1000/60)} minutes)`);
            return {
              cookies: existingCookies,
              fingerprint: BrowserFingerprint.generate(),
              lastRefresh: Date.now()
            };
          }
        }
      }
      
      // Create a promise that will be resolved/rejected based on timeout
      const refreshPromise = new Promise(async (resolve, reject) => {
        // Set up timeout
        timeoutId = setTimeout(() => {
          reject(new Error(`Cookie refresh timeout after ${CONFIG.COOKIE_REFRESH_TIMEOUT / 1000} seconds`));
        }, CONFIG.COOKIE_REFRESH_TIMEOUT);
        
        try {

          // Initialize browser with improved error handling
          let initAttempts = 0;
          let initSuccess = false;
          let initError = null;
          
          while (initAttempts < 3 && !initSuccess) {
            try {
              const result = await initBrowser(proxy);
              if (!result || !result.context || !result.fingerprint) {
                throw new Error("Failed to initialize browser or generate fingerprint");
              }
              
              browserInstance = result.browser;
              localContext = result.context;
              page = result.page;
              
              initSuccess = true;
            } catch (error) {
              initAttempts++;
              initError = error;
              console.error(`Browser init attempt ${initAttempts} failed:`, error.message);
              await new Promise(resolve => setTimeout(resolve, 1000 * initAttempts));
            }
          }
          
          if (!initSuccess) {
            console.error("All browser initialization attempts failed");
            throw initError || new Error("Failed to initialize browser");
          }

          // STEP 1 — land on the HOMEPAGE first (like a real visitor), pick up the
          // initial seed cookies, dwell + interact, THEN navigate to the event page
          // carrying those cookies. Going straight to /event cold looks more bot-like
          // and skips the cookies TM sets on the landing page.
          console.log('Seeding cookies from homepage first...');
          try {
            await page.goto('https://www.ticketmaster.com/', {
              waitUntil: 'domcontentloaded',
              timeout: CONFIG.PAGE_TIMEOUT,
            });
            await page.waitForTimeout(2000 + Math.random() * 2500);
            await simulateMobileInteractions(page);
            const homeCookies = (await page.context().cookies()).filter(c => c.domain.includes('ticketmaster'));
            console.log(`Homepage seeded ${homeCookies.length} cookies: [${homeCookies.map(c => c.name).join(', ')}]`);
          } catch (e) {
            console.warn(`Homepage seed nav failed (continuing to event): ${e.message}`);
          }

          // STEP 2 — now navigate to the event page (carries the homepage cookies)
          const url = `https://www.ticketmaster.com/event/${eventId}`;
          console.log(`Navigating to ${url}`);

          await page.goto(url, {
            waitUntil: "domcontentloaded",
            timeout: CONFIG.PAGE_TIMEOUT
          });
          
          // Check if the page loaded properly
          const currentUrl = page.url();
          const pageLoadSuccessful = currentUrl.includes(`/event/${eventId}`);
          
          if (!pageLoadSuccessful) {
            console.warn(`Failed to load event page, URL: ${currentUrl}`);
            
            // Try refreshing the page
            console.log("Attempting to reload the page...");
            await page.reload({ waitUntil: "domcontentloaded", timeout: CONFIG.PAGE_TIMEOUT });
            
            const newUrl = page.url();
            const reloadSuccessful = newUrl.includes(`/event/${eventId}`);
            
            if (!reloadSuccessful) {
              console.warn(`Reload failed, URL: ${newUrl}`);
              throw new Error("Failed to load Ticketmaster event page");
            }
          }
          
          console.log(`Successfully loaded page for event ${eventId}`);
          
          // Check for Ticketmaster challenge
          const isChallengePresent = await checkForTicketmasterChallenge(page);
          if (isChallengePresent) {
            console.warn("Detected Ticketmaster challenge page, attempting to resolve...");
            await handleTicketmasterChallenge(page);
          }
          
          // Simulate human behavior
          await simulateMobileInteractions(page);
          
          // Wait for cookies to be set
          await page.waitForTimeout(2000);
          
          // Capture cookies
          const fingerprint = BrowserFingerprint.generate();
          const { cookies } = await captureCookies(page, fingerprint);
          
          if (!cookies || cookies.length === 0) {
            throw new Error("Failed to capture cookies");
          }
          
          // Clear timeout and resolve with success
          clearTimeout(timeoutId);
          resolve({
            cookies,
            fingerprint,
            lastRefresh: Date.now()
          });
        } catch (error) {
          clearTimeout(timeoutId);
          reject(error);
        }
      });
      
      // Wait for the refresh promise to complete
      const result = await refreshPromise;
      return result;
    } catch (error) {
      lastError = error;
      console.error(`Cookie refresh attempt ${retryCount + 1} failed: ${error.message}`);
      
      // Check if this was a timeout error
      const isTimeout = error.message.includes('timeout');
      
      if (isTimeout && retryCount < CONFIG.MAX_REFRESH_RETRIES) {
        console.log(`Cookie refresh timed out, will retry with new proxy and event ID`);
        
        // Generate a new event ID for retry (use a different event from the same venue/artist)
        const newEventId = await generateAlternativeEventId(eventId);
        if (newEventId && newEventId !== eventId) {
          console.log(`Using alternative event ID for retry: ${newEventId}`);
          eventId = newEventId;
        }
        
        // Get a new proxy for retry
        if (proxy) {
          const newProxy = await getAlternativeProxy(proxy);
          if (newProxy) {
            console.log(`Using alternative proxy for retry: ${newProxy.host}:${newProxy.port}`);
            proxy = newProxy;
          }
        }
      }
      
      retryCount++;
      
      // If we've exhausted all retries, throw the last error
      if (retryCount > CONFIG.MAX_REFRESH_RETRIES) {
        console.error(`All cookie refresh attempts failed after ${CONFIG.MAX_REFRESH_RETRIES + 1} tries`);
        throw lastError;
      }
      
      // Wait before retrying
      await new Promise(resolve => setTimeout(resolve, CONFIG.RETRY_DELAY * retryCount));
      
    } finally {
      // Close page and context but keep browser open for reuse
      if (page) {
        try {
          await page.close().catch(e => console.error("Error closing page:", e));
        } catch (e) {
          console.error("Error closing page in finally block:", e);
        }
      }
      
      if (localContext) {
        try {
          await localContext.close().catch(e => console.error("Error closing context:", e));
        } catch (e) {
          console.error("Error closing context in finally block:", e);
        }
      }
    }
  }
  
  // This should never be reached, but just in case
  throw lastError || new Error('Cookie refresh failed after all retries');
}

/**
 * Generate an alternative event ID for retry attempts
 * This function attempts to find a similar event or generates a fallback
 */
async function generateAlternativeEventId(originalEventId) {
  try {
    // For now, we'll generate a simple variation of the original event ID
    // In a production environment, this could query a database for similar events
    const timestamp = Date.now().toString().slice(-6);
    const randomSuffix = Math.random().toString(36).substring(2, 8);
    
    // Create a variation that's likely to be a valid event ID format
    const alternativeId = originalEventId.replace(/\d+$/, timestamp + randomSuffix);
    
    console.log(`Generated alternative event ID: ${alternativeId} from original: ${originalEventId}`);
    return alternativeId;
  } catch (error) {
    console.warn(`Failed to generate alternative event ID: ${error.message}`);
    return originalEventId; // Fallback to original
  }
}

/**
 * Get an alternative proxy for retry attempts
 * This function should integrate with your proxy management system
 */

/**
 * Initialize persistent browser context for API requests
 * This bypasses TLS fingerprinting by using real browser requests
 */
async function initApiBrowserContext(proxy = null, cookies = null) {
  // If context already exists and is valid, return it
  if (apiContext && apiBrowser && apiBrowser.isConnected()) {
    // Verify the page is still alive by checking if it's not closed
    try {
      if (apiPage && !apiPage.isClosed()) {
        // Update cookies if provided
        if (cookies && cookies.length > 0) {
          try {
            await apiContext.clearCookies();
            const browserCookies = cookies.map(c => ({
              name: c.name,
              value: c.value,
              domain: c.domain || '.ticketmaster.com',
              path: c.path || '/',
              expires: c.expires || c.expiry || -1,
              httpOnly: c.httpOnly || false,
              secure: c.secure || true,
              sameSite: c.sameSite || 'Lax'
            }));
            await apiContext.addCookies(browserCookies);
          } catch (e) {
            console.warn('Error updating API context cookies:', e.message);
          }
        }
        return { browser: apiBrowser, context: apiContext, page: apiPage };
      }
    } catch (e) {
      // Context is broken, fall through to recreate
      console.warn('Cached API context is broken, recreating:', e.message);
    }
    // Clean up the dead context
    await cleanupApiBrowser();
  }

  // Wait if context is being created
  if (apiContextLock) {
    await new Promise(resolve => setTimeout(resolve, 1000));
    if (apiContext && apiBrowser && apiBrowser.isConnected()) {
      return { browser: apiBrowser, context: apiContext, page: apiPage };
    }
  }

  apiContextLock = true;

  try {
    const location = getRandomLocation();
    const fingerprint = BrowserFingerprint.generate('desktop');
    let contextProxy = null;

    const launchOptions = {
      // EPS blocks HEADLESS Chrome (event page 403, cookies never mint) even with
      // channel:'chrome'. The cookie-refresh path runs headed and works; the scrape
      // API context MUST run headed too. On a headless server, run under a virtual
      // display (xvfb-run). Verified: headed=200/1352, headless=403 on the same IP.
      headless: false,
      args: [
        '--disable-blink-features=AutomationControlled',
        '--disable-features=IsolateOrigins,site-per-process',
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas',
        '--disable-gpu',
        '--window-size=1920,1080',
        '--disable-web-security',
        '--disable-features=IsolateOrigins',
        '--disable-site-isolation-trials',
        // Memory optimizations for 30 PM2 instances on 160GB
        '--js-flags=--max-old-space-size=256',
        '--disable-extensions',
        '--disable-background-networking',
        '--disable-default-apps',
        '--disable-sync',
        '--disable-translate',
        '--metrics-recording-only',
        '--no-first-run',
        '--disable-hang-monitor',
        '--disable-popup-blocking',
        '--disable-prompt-on-repost',
        '--disable-renderer-backgrounding',
        '--disable-backgrounding-occluded-windows',
        '--disable-ipc-flooding-protection',
      ],
    };

    // Configure proxy if provided
    if (proxy) {
      try {
        let proxyString = proxy.proxy || proxy;
        if (typeof proxyString !== 'string') {
          throw new Error('Invalid proxy format');
        }
        
        const [hostname, portStr] = proxyString.split(':');
        const port = parseInt(portStr) || 80;

        const parsedProxy = {
          server: `http://${hostname}:${port}`,
          username: proxy.username,
          password: proxy.password,
        };

        // Camoufox warning fix: apply proxy on context instead of launch.
        if (USE_CAMOUFOX) {
          contextProxy = parsedProxy;
        } else {
          launchOptions.proxy = parsedProxy;
        }
      } catch (error) {
        console.warn('Invalid proxy for API context:', error.message);
      }
    }

    // Launch browser for API requests (real Chrome channel when available — EPS blocks bundled Chromium)
    apiBrowser = await launchChromium(launchOptions);
    
    // Create desktop context (better for API requests).
    // NO userAgent override: let real Chrome present its native UA + matching
    // Sec-CH-UA client hints + navigator.platform. A hardcoded Windows/Chrome-133
    // string on a Mac/Linux/Chrome-149 binary is a fingerprint mismatch EPS detects.
    apiContext = await apiBrowser.newContext({
      locale: location.locale,
      timezoneId: location.timezone,
      viewport: USE_CAMOUFOX ? null : { width: 1920, height: 1080 },
      // deviceScaleFactor/hasTouch/isMobile are invalid with viewport:null (Firefox);
      // Camoufox manages its own window + device metrics, so omit them there.
      ...(USE_CAMOUFOX ? {} : { deviceScaleFactor: 1, hasTouch: false, isMobile: false }),
      javaScriptEnabled: true,
      ignoreHTTPSErrors: true,
      bypassCSP: true,
      // Let Camoufox/Firefox send its own native headers; the explicit Accept:json
      // here would wrongly tag page navigations. Only set for Chrome.
      ...(USE_CAMOUFOX ? {} : { extraHTTPHeaders: {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
      } })
      ,
      ...(contextProxy ? { proxy: contextProxy } : {})
    });

    // NO manual stealth init scripts here. patchright already masks automation
    // (webdriver, etc.) natively, and real Chrome supplies real plugins/chrome/
    // connection objects. The old fakes (plugins:[1,2,3,4,5], a hand-rolled
    // window.chrome, webdriver override) are detectable INCONSISTENCIES against a
    // real headed Chrome — removing them makes the fingerprint genuinely real.

    // Add cookies if provided
    if (cookies && cookies.length > 0) {
      const browserCookies = cookies.map(c => ({
        name: c.name,
        value: c.value,
        domain: c.domain || '.ticketmaster.com',
        path: c.path || '/',
        expires: c.expires || c.expiry || -1,
        httpOnly: c.httpOnly || false,
        secure: c.secure || true,
        sameSite: c.sameSite || 'Lax'
      }));
      await apiContext.addCookies(browserCookies);
    }

    // Create a page for requests
    apiPage = await apiContext.newPage();
    
    // Navigate to ticketmaster initially to establish session. SKIP in split mode:
    // the per-page injected farm jar supplies the session, so touching TM on this
    // datacenter init proxy only risks a self-mint / a 30s hang for no benefit.
    try {
      if (SEED_SPLIT()) {
        console.log('[API] split mode — skipping establish-session nav (jar injected per page)');
      } else {
      await apiPage.goto('https://www.ticketmaster.com/', {
        waitUntil: 'domcontentloaded',
        timeout: 30000
      });
      await new Promise(r => setTimeout(r, 1000));
      }
    } catch (e) {
      // If browser crashed (Target closed), this is fatal — don't return dead context
      if (e.message?.includes('Target') && e.message?.includes('closed')) {
        console.error('Browser crashed during initial navigation:', e.message);
        await cleanupApiBrowser();
        throw new Error(`Browser crashed during init: ${e.message}`);
      }
      console.warn('Initial TM navigation warning:', e.message);
    }

    // Verify browser is still alive before returning. Use ?. — a crash handler can
    // null `apiBrowser` between launch and here, and `null.isConnected()` would throw
    // a TypeError that escapes as an un-descriptive init failure (and crash-loops).
    if (!apiBrowser?.isConnected()) {
      console.error('Browser disconnected after init');
      await cleanupApiBrowser();
      throw new Error('Browser disconnected immediately after launch');
    }

    console.log('API browser context initialized successfully');
    return { browser: apiBrowser, context: apiContext, page: apiPage };
    
  } catch (error) {
    console.error('Failed to initialize API browser context:', error.message);
    await cleanupApiBrowser();
    throw error;
  } finally {
    apiContextLock = false;
  }
}

/**
 * Make an API request through the browser context (bypasses TLS fingerprinting)
 * @param {string} url - The URL to fetch
 * @param {object} headers - Request headers
 * @param {object} proxy - Proxy configuration
 * @param {array} cookies - Cookies to use
 * @returns {Promise<object>} Response data
 */
async function browserApiRequest(url, headers = {}, proxy = null, cookies = null) {
  try {
    // Initialize or get existing API context
    const { page, context } = await initApiBrowserContext(proxy, cookies);
    
    if (!page || !context) {
      throw new Error('Failed to get API browser context');
    }

    // Make request via the context request client (bypasses CORS; carries cookies).
    const result = await apiGet(page, url, headers);

    if (!result.success) {
      const error = new Error(result.error || `Request failed with status ${result.status}`);
      error.statusCode = result.status;
      throw error;
    }

    return result.data;
    
  } catch (error) {
    // If browser context fails, try to reinitialize
    if (error.message?.includes('Target closed') || error.message?.includes('Browser')) {
      await cleanupApiBrowser();
    }
    throw error;
  }
}

/**
 * Clean up API browser resources
 */
async function cleanupApiBrowser() {
  try {
    if (apiPage) {
      await apiPage.close().catch(() => {});
      apiPage = null;
    }
    if (apiContext) {
      await apiContext.close().catch(() => {});
      apiContext = null;
    }
    if (apiBrowser) {
      await apiBrowser.close().catch(() => {});
      apiBrowser = null;
    }
  } catch (error) {
    console.warn('Error cleaning up API browser:', error.message);
  }
}

/**
 * Check if API browser context is available
 */
function isApiBrowserAvailable() {
  return apiBrowser && apiBrowser.isConnected() && apiContext && apiPage;
}

// ====================================================
// RequestBatcher: Groups requests from MANY events into mega-batches.
// Instead of 1 page per event (2 fetches), 1 page handles 20 events
// (40 fetches) in a single page.evaluate(Promise.all(...)) call.
// 8 pages × 20 events = 160 events per cycle ≈ 40-50 events/sec.
// ====================================================
class RequestBatcher {
  constructor(pool, maxEventsPerBatch = 10, flushIntervalMs = 50) {
    this.pool = pool;
    this.maxEventsPerBatch = maxEventsPerBatch;
    this.flushIntervalMs = flushIntervalMs;
    this.queue = []; // [{requests: [{url,headers}], resolve, reject}]
    this._timer = null;
    this._activeFlushes = 0;
  }

  /**
   * Submit requests for one event. Returns Promise<results[]>.
   * Requests are automatically batched with other events for throughput.
   */
  submit(requests) {
    return new Promise((resolve, reject) => {
      this.queue.push({ requests, resolve, reject });
      this._scheduleFlush();
    });
  }

  _scheduleFlush() {
    // Immediate flush when batch is full
    if (this.queue.length >= this.maxEventsPerBatch) {
      this._tryFlush();
    }
    // Always ensure a timer is running while items are queued
    if (this.queue.length > 0 && !this._timer) {
      this._timer = setTimeout(() => {
        this._timer = null;
        this._tryFlush();
      }, this.flushIntervalMs);
    }
  }

  _tryFlush() {
    if (this.queue.length === 0) return;

    // Limit parallel flushes to number of pool pages
    if (this._activeFlushes >= this.pool.pages.length) {
      // All pages busy — schedule retry
      if (!this._timer) {
        this._timer = setTimeout(() => {
          this._timer = null;
          this._tryFlush();
        }, 50);
      }
      return;
    }

    const batch = this.queue.splice(0, this.maxEventsPerBatch);
    if (batch.length === 0) return;

    this._activeFlushes++;
    this._executeBatch(batch).finally(() => {
      this._activeFlushes--;
      // Flush more if queued
      if (this.queue.length > 0) {
        setImmediate(() => this._tryFlush());
      }
    });

    // If still more in queue, try another flush (will use a different page)
    if (this.queue.length > 0) {
      setImmediate(() => this._tryFlush());
    }
  }

  async _executeBatch(batch) {
    // Build flat request array with index tracking
    const allRequests = [];
    const indexMap = []; // maps flat index → {batchIdx, reqIdx}

    for (let i = 0; i < batch.length; i++) {
      for (let j = 0; j < batch[i].requests.length; j++) {
        indexMap.push({ bi: i, ri: j });
        allRequests.push(batch[i].requests[j]);
      }
    }

    let worker;
    try {
      worker = await this.pool.acquire(20000); // 20s timeout for worker acquisition
    } catch (err) {
      for (const item of batch) item.reject(err);
      return;
    }

    // Fail fast if a page worker was closed between acquire and evaluate
    if (!worker?.requestContext && worker?.isClosed && worker.isClosed()) {
      this.pool._removePage(worker);
      for (const item of batch) item.reject(new Error('Acquired page was already closed'));
      return;
    }

    const meta = this.pool._pageMeta.get(worker);

    try {
      // PACE THE EXIT IP BEFORE ANYTHING GOES OUT.
      // Measured 2026-08-09 (scripts/jarRateRamp.mjs) on a single exit IP, sliding 60s
      // window: <=29 calls/min fails 3-5%, 30-39 fails 22%, 40+ fails 78%. Firing a whole
      // batch at once put 12 requests down one IP in a single burst — far past that knee —
      // and the 403s it caused were then misread as dead jars and dead proxies.
      await this.pool.reserveIpSlots(meta, allRequests.length);

      // Fetch all requests via either a page context client or lightweight request context.
      const results = await Promise.all(
        allRequests.map(({ url, headers }) => apiGetFromWorker(worker, url, headers))
      );

      this.pool.release(worker);

      // A 403 here means this IP is over its window, and it clears on its own in ~75s
      // (measured: a hard-blocked IP answered 200 again 75s after the load stopped). It
      // does NOT mean the jar is dead and does NOT mean the proxy is burned — so rest
      // this IP briefly instead of restarting the browser and throwing the jar away.
      const blocked = results.filter((r) => r.status === 403).length;
      if (blocked) this.pool.coolProxy(meta, blocked);
      else if (results.some((r) => r.status >= 200 && r.status < 400)) this.pool.trackError(200);

      // Usage counter for observability only — nothing retires on it any more.
      const tmpt = meta && meta.tmpt;
      if (tmpt) {
        const facetsCount = allRequests.reduce((a, r) => a + (/\/ismds\/|facets\?/.test(r.url) ? 1 : 0), 0);
        if (facetsCount) noteJarUsage(tmpt, facetsCount).catch(() => {});
      }

      // Distribute results back to each event's promise
      const eventResults = batch.map(() => []);
      for (let i = 0; i < results.length; i++) {
        eventResults[indexMap[i].bi][indexMap[i].ri] = results[i];
      }
      for (let i = 0; i < batch.length; i++) {
        batch[i].resolve(eventResults[i]);
      }

      // Batch completion logged at debug level only
    } catch (error) {
      // Handle dead pages
      if (error.message?.includes('Target closed') ||
          error.message?.includes('Protocol error') ||
          error.message?.includes('crashed') ||
          error.message?.includes('Execution context')) {
        this.pool._removePage(worker);
      } else {
        this.pool.release(worker);
      }
      // Reject all events in this batch
      for (const item of batch) item.reject(error);
    }
  }

  cleanup() {
    if (this._timer) {
      clearTimeout(this._timer);
      this._timer = null;
    }
    // Reject everything still queued
    for (const item of this.queue) {
      item.reject(new Error('Batcher cleanup'));
    }
    this.queue = [];
  }
}

// ====================================================
// BrowserPagePool: Pool of browser pages for parallel API requests
// All pages share one browser context (same cookies, proxy, real TLS).
// The RequestBatcher multiplexes many events onto each page.
// ====================================================
class BrowserPagePool {
  constructor(size = 3) {
    this.size = size;
    this.pages = [];
    this.available = [];
    this.waiting = [];
    this.initialized = false;
    this._initPromise = null;
    this._context = null;
    this._browser = null;
    this._batcher = null;
    // Cookie refresh via full browser restart
    this._lastCookieRefresh = Date.now();
    // 8 min default — well before 10-15m cookie expiry. Tunable via COOKIE_REFRESH_MIN
    // (raise it to cut re-mint churn, which is what burns proxies; keep < ~10m expiry).
    this._cookieRefreshInterval = (parseInt(process.env.COOKIE_REFRESH_MIN, 10) || 8) * 60 * 1000;
    // Add random jitter so multiple instances don't refresh at the exact same time.
    this._cookieRefreshJitterMs = (parseInt(process.env.COOKIE_REFRESH_JITTER_SEC, 10) || 90) * 1000;
    this._isRestarting = false;
    this._isRefreshing = false; // rolling refresh in progress (browser stays alive)
    this._refreshTimer = null;
    this._consecutiveErrors = 0;
    // Proxy rotation after N requests
    this._requestsSinceRotation = 0;
    this._proxyRotationThreshold = 500; // rotate proxy every 500 event calls — avoids restart thrashing
    // Store init params for restart
    this._initProxy = null;
    this._initCookies = null;
    this._initEventId = null;
    // Deferred requests queue (filled during restart)
    this._deferredQueue = [];
    // Per-page proxy isolation: each pool page lives in its OWN context with its
    // OWN proxy, so a batch's fetches spread across N IPs instead of hammering one.
    this._contexts = [];                 // all contexts (for cleanup)
    this._pageMeta = new Map();          // page -> { context, proxy, tmpt, jarId, jarOwner }
    this._usedProxies = new Set();       // proxy strings currently assigned to a page
    this._ipBuckets = new Map();         // proxyKey -> { tokens, last, coolUntil }
    // Seed/scrape split: shared cookie jar minted on bart, injected into scrape pages.
    this._seedJar = null;                // Array<cookie> last minted on bart
    this._seedJarAt = 0;                 // ms timestamp of the jar
    this._seedInFlight = null;           // dedupe concurrent mints
    this._refreshCursor = 0;             // round-robin target index for rolling refresh
    this._leaseTimer = null;             // renews this instance's jar leases (JAR_LEASE_TTL_MS)
  }

  // Acquire the jar for ONE page on ONE proxy. Returns { cookies, jarId, ownerId } or
  // null. The lease is what keeps a token off a second exit IP; `proxyKey` is recorded on
  // the lease so a jar's IP is visible in the DB when something looks wrong.
  async _acquireSeedJar(eventId, proxyKey) {
    if (!SEED_SPLIT()) return null;
    if (SEED_FARM() && JAR_LEASE()) {
      const ownerId = newOwnerId();
      const lease = await leaseFarmJar(ownerId, proxyKey, JAR_LEASE_TTL_MS());
      if (lease) return { ...lease, ownerId };
      // Every jar is already leased. Do NOT fall back to a shared one — that is the
      // failure this lease exists to prevent. The page simply does not bind and retries;
      // the fix for "no jar available" is more mint throughput, not more sharing.
      if (process.env.SEED_FARM_FALLBACK !== "1") {
        if (SELF_MINT() && this._seedJar && Date.now() - this._seedJarAt < SEED_TTL_MS()) {
          return { cookies: this._seedJar, jarId: null, ownerId: null };
        }
        return null;
      }
    }
    const cookies = await this._ensureSeedJar(eventId);
    return cookies && cookies.length ? { cookies, jarId: null, ownerId: null } : null;
  }

  // Ensure a fresh seed cookie jar exists (minted on bart). Returns the jar or null.
  // TTL-cached; concurrent callers share one in-flight mint.
  async _ensureSeedJar(eventId) {
    if (!SEED_SPLIT()) return null;
    // FARM MODE: read a ready jar from the shared store (minted by cookie-farm) —
    // this instance never touches bart.
    if (SEED_FARM()) {
      const shared = await readFarmJarShared();
      const farmJar = shared && shared.cookies;
      if (farmJar && farmJar.length) return farmJar;
      // FLEET SAFETY: with the farm on, do NOT fall back to minting on bart in-process.
      // 100 instances all stampeding bart when the farm briefly runs dry would flood
      // and flag the residential range. Return null (page won't bind → retries) and
      // wait for the farm to supply a jar. SEED_FARM_FALLBACK=1 re-enables local mint
      // (single-instance bootstrap only).
      if (process.env.SEED_FARM_FALLBACK !== "1") {
        // SELF_MINT: the farm is dry, but a sibling page may have already minted a
        // jar on its own proxy this cycle — reuse it (tmpt is IP-agnostic once valid)
        // so only ONE page pays the navigate-TM cost per TTL. Null → the caller
        // self-mints and calls _cacheSelfMintedJar() to populate this for the rest.
        if (SELF_MINT() && this._seedJar && Date.now() - this._seedJarAt < SEED_TTL_MS()) {
          return this._seedJar;
        }
        return null;
      }
    }
    if (this._seedJar && Date.now() - this._seedJarAt < SEED_TTL_MS()) return this._seedJar;
    if (this._seedInFlight) return this._seedInFlight;
    this._seedInFlight = this._mintSeedJar(eventId).finally(() => { this._seedInFlight = null; });
    return this._seedInFlight;
  }

  // Remember a jar we minted ourselves (SELF_MINT) so every other page in this pool
  // injects it instead of navigating TM too. Only stores jars that actually carry a
  // `tmpt` — a tmpt-less jar is worthless to facets and would poison the cache for a
  // full TTL, keeping siblings from making their own (possibly successful) attempt.
  _cacheSelfMintedJar(cookies) {
    const jar = (cookies || []).filter((c) => String(c.domain || '').includes('ticketmaster'));
    if (!jar.some((c) => c.name === 'tmpt')) return false;
    this._seedJar = jar;
    this._seedJarAt = Date.now();
    console.log(`[Seed] self-minted jar cached (${jar.length} cookies) — siblings will inject it`);
    return true;
  }

  // Mint a tmpt-carrying cookie jar on clean bart residential using the FULL recipe
  // (homepage → event → wait tmpt → settle → validate facets 200). Retries fresh
  // bart sticky sessions until one passes (~1/3 hit rate). Returns the jar (or the
  // previous stale jar if all attempts fail this round).
  async _mintSeedJar(eventId) {
    const seedId = eventId || this._initEventId;
    if (!seedId) return this._seedJar;
    if (!getSeedProxy()) { console.warn('[Seed] bart not configured (set BART_* in .env)'); return this._seedJar; }
    const attempts = parseInt(process.env.BART_SEED_ATTEMPTS, 10) || 6;
    const parallel = Math.max(1, parseInt(process.env.SEED_PARALLEL, 10) || 4);
    const vu = `https://services.ticketmaster.com/api/ismds/event/${seedId}/facets?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse`;
    // Bart exits pass only ~1/3 of the time, so run attempts in PARALLEL batches —
    // sequential retries stall pool init for minutes. First facets-200 jar wins.
    const t0 = Date.now();
    let tried = 0;
    while (tried < attempts) {
      const batch = Math.min(parallel, attempts - tried);
      const jars = await Promise.all(Array.from({ length: batch }, () => this._seedAttempt(seedId, vu)));
      tried += batch;
      const jar = jars.find((j) => j && j.length);
      if (jar) {
        this._seedJar = jar;
        this._seedJarAt = Date.now();
        console.log(`[Seed] minted jar on bart (${jar.length} cookies) — facets 200 ✓ in ${Math.round((Date.now() - t0) / 1000)}s (${tried} sessions tried)`);
        return this._seedJar;
      }
      console.log(`[Seed] batch of ${batch} bart sessions all flagged (${tried}/${attempts}) — retrying`);
    }
    console.warn(`[Seed] could not mint a fresh jar in ${attempts} attempts; using stale jar if present`);
    return this._seedJar;
  }

  // One bart seed attempt on a fresh sticky session: homepage → event → wait tmpt
  // → settle → validate facets. Returns the cookie jar (Array) on facets 200, else
  // null. Owns its own context lifecycle so batches can run concurrently.
  async _seedAttempt(seedId, vu) {
    const bart = getSeedProxy();
    if (!bart) return null;
    let ctx = null;
    try {
      ctx = await this._browser.newContext({ viewport: USE_CAMOUFOX ? null : { width: 1920, height: 1080 }, ignoreHTTPSErrors: true, bypassCSP: true, proxy: bart });
      // BANDWIDTH: bart is metered by the GB. The reCAPTCHA challenge that mints tmpt
      // is JS-based, so abort heavy assets (images/media/fonts) — tmpt still mints,
      // page weight drops sharply. Keep CSS/scripts (safer for the reCAPTCHA score).
      // Only the SEED touches bart; facets run on the cheap Mongo pool. SEED_BLOCK_ASSETS=0 disables.
      if (process.env.SEED_BLOCK_ASSETS !== '0') {
        await ctx.route('**/*', (route) => {
          const t = route.request().resourceType();
          if (t === 'image' || t === 'media' || t === 'font') return route.abort();
          return route.continue();
        }).catch(() => {});
      }
      const page = await ctx.newPage();
      await page.goto('https://www.ticketmaster.com/', { waitUntil: 'domcontentloaded', timeout: 40000 }).catch(() => null);
      await page.waitForTimeout(2000 + Math.random() * 1500);
      for (let r = 0; r < 2; r++) {
        const resp = await page.goto(`https://www.ticketmaster.com/event/${seedId}`, { waitUntil: 'domcontentloaded', timeout: 40000 }).catch(() => null);
        if ((resp ? resp.status() : 0) === 200) break;
        await new Promise((t) => setTimeout(t, 1200));
      }
      // Fast-fail a flagged exit sitting on the EPS block screen.
      const onBlockScreen = await page.evaluate(() => /paused|verified|interruption|identity/i.test(document.title)).catch(() => false);
      if (onBlockScreen) { await ctx.close().catch(() => {}); return null; }
      let tmpt = false;
      for (let w = 0; w < 30 && !tmpt; w++) { tmpt = (await ctx.cookies()).some((c) => c.name === 'tmpt'); if (!tmpt) await new Promise((t) => setTimeout(t, 750)); }
      await page.waitForTimeout(2500); // let the page's own XHRs establish the services session
      const vr = await apiGet(page, vu, { accept: 'application/json', 'x-api-key': 'b462oi7fic6pehcdkzony5bxhe', 'tmps-correlation-id': 'v' + Math.floor(Math.random() * 1e9), 'x-request-id': 'v' + Math.floor(Math.random() * 1e9) });
      if ((vr.status || 0) === 200) {
        const jar = await ctx.cookies();
        await ctx.close().catch(() => {});
        return jar;
      }
      await ctx.close().catch(() => {});
      return null;
    } catch (e) {
      if (ctx) await ctx.close().catch(() => {});
      return null;
    }
  }

  // Pick a proxy not already assigned to another pool page (falls back to any).
  _pickUnusedProxy() {
    const all = (proxyArray && proxyArray.proxies) || [];
    if (!all.length) return null;
    // Key on a unique id when present (IPRoyal sticky sessions all share one
    // host:port but differ by session id); fall back to host:port for static lists.
    const keyOf = (p) => p.id || p.proxy;
    const base = all.filter((p) => p && p.proxy);
    const free = base.filter((p) => !this._usedProxies.has(keyOf(p)));
    const pool = free.length ? free : base;
    return pool.length ? pool[Math.floor(Math.random() * pool.length)] : null;
  }

  // Create one lightweight request-context slot bound to one proxy.
  async _createRequestContextSlot(eventId, attempts = 4) {
    const seedId = eventId || this._initEventId;
    const tries = DIRECT_MODE ? 1 : attempts;
    for (let a = 0; a < tries; a++) {
      const proxy = DIRECT_MODE ? null : this._pickUnusedProxy();
      if (!DIRECT_MODE && !proxy) return null;

      let requestContext = null;
      let lease = null; // one jar, one proxy — released below if this slot fails to bind
      try {
        const proxyKey = proxy ? (proxy.id || proxy.proxy) : 'direct';
        lease = SEED_SPLIT() ? await this._acquireSeedJar(seedId, proxyKey) : null;
        const jar = lease && lease.cookies;
        if (SEED_SPLIT() && (!jar || !jar.length)) {
          // No self-mint escape hatch here even with SELF_MINT=1: a request-context has
          // no page to run the reCAPTCHA challenge that mints tmpt. Skip and retry —
          // set REQUEST_CONTEXT_MODE=0 (page pool) if you need self-minting.
          console.log('[RequestPool] no farm jar available — skipping proxy (request contexts cannot mint)');
          continue;
        }

        const rcOpts = {
          ignoreHTTPSErrors: true,
          extraHTTPHeaders: {
            accept: 'application/json',
            'x-api-key': 'b462oi7fic6pehcdkzony5bxhe',
          },
        };

        if (proxy) {
          const [host, portStr] = String(proxy.proxy).split(':');
          rcOpts.proxy = {
            server: `http://${host}:${parseInt(portStr, 10) || 80}`,
            username: proxy.username,
            password: proxy.password,
          };
        }

        if (jar && jar.length) {
          rcOpts.storageState = { cookies: jar.map(toStorageCookie), origins: [] };
        }

        requestContext = await playwrightRequest.newContext(rcOpts);

        let facetStatus = 0;
        if (seedId) {
          const vu = `https://services.ticketmaster.com/api/ismds/event/${seedId}/facets?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse`;
          const vr = await apiGetRequestContext(requestContext, vu, {
            accept: 'application/json',
            'x-api-key': 'b462oi7fic6pehcdkzony5bxhe',
            'tmps-correlation-id': 'v' + Math.floor(Math.random() * 1e9),
            'x-request-id': 'v' + Math.floor(Math.random() * 1e9),
          });
          facetStatus = vr.status || 0;
          // No jar health-feedback here: a bind 403 is far more likely the exit IP than
          // the token, and blaming the token retired 94% good jars. See the note above.
        }

        const label = proxy ? (proxy.id || proxy.proxy) : 'direct (no proxy)';
        if (facetStatus === 200 || !seedId) {
          if (proxy) this._usedProxies.add(proxy.id || proxy.proxy);

          const worker = {
            type: 'request-context',
            requestContext,
            isClosed: () => false,
            close: () => requestContext.dispose(),
          };

          this._pageMeta.set(worker, {
            requestContext,
            proxy,
            tmpt: jar ? _tmptOf(jar) : undefined,
            jarId: lease && lease.jarId,
            jarOwner: lease && lease.ownerId,
          });

          console.log(`[RequestPool] slot bound to ${label} (facets=${facetStatus}) ✓`);
          return { page: worker, context: null, proxy };
        }

        console.log(`[RequestPool] ${label} blocked (facets=${facetStatus}) — trying another`);
        if (lease && lease.jarId) await releaseFarmJar(lease.jarId, lease.ownerId);
        await requestContext.dispose().catch(() => {});
      } catch (e) {
        console.warn(`[RequestPool] ${proxy ? proxy.proxy : 'direct'} setup failed: ${e.message}`);
        if (lease && lease.jarId) await releaseFarmJar(lease.jarId, lease.ownerId);
        if (requestContext) await requestContext.dispose().catch(() => {});
      }
    }
    return null;
  }

  // Create one pool page in its OWN context bound to its OWN proxy, seeded on an
  // event page and validated with a real facets call. Returns {page,context,proxy}
  // or null if no working proxy could be found within `attempts`.
  async _createProxyPage(eventId, attempts = 4) {
    if (REQUEST_CONTEXT_MODE()) {
      return this._createRequestContextSlot(eventId, attempts);
    }

    const seedId = eventId || this._initEventId;
    const tries = DIRECT_MODE ? 1 : attempts;
    for (let a = 0; a < tries; a++) {
      // DIRECT_MODE: no proxy — page goes straight out this host's IP.
      const proxy = DIRECT_MODE ? null : this._pickUnusedProxy();
      if (!DIRECT_MODE && !proxy) return null;
      let context = null;
      let lease = null; // declared out here so the catch below can release it
      try {
        const ctxOpts = {
          viewport: USE_CAMOUFOX ? null : { width: 1920, height: 1080 },
          ignoreHTTPSErrors: true,
          bypassCSP: true,
        };
        if (proxy) {
          const [host, portStr] = String(proxy.proxy).split(':');
          ctxOpts.proxy = { server: `http://${host}:${parseInt(portStr, 10) || 80}`, username: proxy.username, password: proxy.password };
        }
        context = await this._browser.newContext(ctxOpts);
        const page = await context.newPage();
        let status = 0;
        // SEED/SCRAPE SPLIT: this context runs on a cheap Mongo datacenter proxy that
        // CANNOT mint tmpt (reCAPTCHA/EPS blocks datacenter IPs at the event page).
        // Inject the bart-minted jar (tmpt + session cookies) so facets validates
        // without any reCAPTCHA seed on this IP. facets is IP-agnostic once tmpt is
        // valid, so one bart seed serves every scrape proxy.
        const proxyKey = proxy ? (proxy.id || proxy.proxy) : 'direct';
        lease = SEED_SPLIT() ? await this._acquireSeedJar(seedId, proxyKey) : null;
        const jar = lease && lease.cookies;
        // Did we mint this page's session ourselves? If so, and facets validates it,
        // cache the jar so the rest of the pool injects instead of navigating too.
        let selfMinted = false;
        if (jar && jar.length) {
          await context.addCookies(jar).catch((e) => console.warn('[PagePool] inject seed jar failed:', e.message));
          status = 200; // session comes from the injected jar; page nav is unnecessary
        } else if (SEED_SPLIT() && !SELF_MINT()) {
          // Strict farm-consumer mode (SELF_MINT=0). The farm is momentarily dry — skip
          // this proxy and retry later rather than navigate TM to mint a tmpt on a
          // datacenter IP (EPS 403s it, burns the attempt, and pollutes the pool).
          console.log('[PagePool] no farm jar available — skipping proxy (not self-minting)');
          await context.close().catch(() => {});
          continue;
        } else {
          // Farm dry + SELF_MINT on (or non-split standalone): self-seed on this
          // proxy — homepage first (seed cookies), then the event page carrying them.
          // The facets check below is the real verdict: a proxy that can't mint just
          // fails validation and we move to the next one, exactly as a blocked IP does.
          selfMinted = SEED_SPLIT();
          if (selfMinted) console.log('[PagePool] no farm jar — self-minting tmpt on this proxy');
          await page.goto('https://www.ticketmaster.com/', { waitUntil: 'domcontentloaded', timeout: 40000 }).catch(() => null);
          await page.waitForTimeout(1500 + Math.random() * 1500);
          const url = seedId ? `https://www.ticketmaster.com/event/${seedId}` : 'https://www.ticketmaster.com/';
          for (let r = 0; r < 2; r++) {
            const resp = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 40000 }).catch(() => null);
            status = resp ? resp.status() : 0;
            if (status === 200) break;
            await new Promise((t) => setTimeout(t, 1500));
          }
          // Wait for the EPS `tmpt` token to mint BEFORE validating. Through a
          // residential proxy the challenge JS needs time (~6-18s); without tmpt the
          // facets call always 403s — and the event PAGE may stay 401 even when the
          // facets API will succeed, so tmpt (not page status) is the real signal.
          if (seedId) {
            for (let w = 0; w < 8; w++) {
              const names = (await context.cookies()).map((c) => c.name);
              if (names.includes('tmpt')) break;
              await new Promise((t) => setTimeout(t, 1500));
            }
          }
        }
        // True validation: does a real facets call succeed? Use the FULL header set
        // the scraper sends — minimal headers return 400 even on a GOOD session, which
        // would wrongly reject working IPs. tmps-correlation-id + x-request-id flip it.
        let facetStatus = 0;
        if (seedId) {
          const vu = `https://services.ticketmaster.com/api/ismds/event/${seedId}/facets?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse`;
          const vr = await apiGet(page, vu, { accept: 'application/json', 'x-api-key': 'b462oi7fic6pehcdkzony5bxhe', 'tmps-correlation-id': 'v' + Math.floor(Math.random() * 1e9), 'x-request-id': 'v' + Math.floor(Math.random() * 1e9) });
          facetStatus = vr.status || 0;
          // No jar health-feedback here: a bind 403 is far more likely the exit IP than
          // the token, and blaming the token retired 94% good jars. See the note above.
        }
        const label = proxy ? (proxy.id || proxy.proxy) : 'direct (no proxy)';
        if (facetStatus === 200 || (!seedId && status === 200)) {
          if (proxy) this._usedProxies.add(proxy.id || proxy.proxy);
          this._contexts.push(context);
          // A self-minted session that just passed facets is proven good — share it
          // with the remaining pages so they skip the nav entirely.
          let ownTmpt;
          if (selfMinted) {
            const own = await context.cookies().catch(() => []);
            if (this._cacheSelfMintedJar(own)) ownTmpt = _tmptOf(this._seedJar);
          }
          this._pageMeta.set(page, {
            context, proxy, tmpt: jar ? _tmptOf(jar) : ownTmpt,
            jarId: lease && lease.jarId, jarOwner: lease && lease.ownerId,
          });
          console.log(`[PagePool] page bound to ${label} (page=${status}, facets=${facetStatus}) ✓`);
          return { page, context, proxy };
        }
        console.log(`[PagePool] ${label} blocked (page=${status}, facets=${facetStatus}) — ${DIRECT_MODE ? 'retrying' : 'trying another'}`);
        // Hand the jar back before trying another proxy. A leaked lease would strand a
        // perfectly good jar until it expired, and jar supply is the scarce resource.
        if (lease && lease.jarId) await releaseFarmJar(lease.jarId, lease.ownerId);
        await context.close().catch(() => {});
      } catch (e) {
        console.warn(`[PagePool] ${proxy ? proxy.proxy : 'direct'} setup failed: ${e.message}`);
        if (lease && lease.jarId) await releaseFarmJar(lease.jarId, lease.ownerId);
        if (context) await context.close().catch(() => {});
      }
    }
    return null;
  }

  // Attach a crash-recovery handler to a launched browser. If it dies OUTSIDE a
  // controlled restart, immediately mark the pool busy (_isRestarting=true) so
  // racing init()/submit calls can't spawn dueling _doInit's that close each
  // other's browser and burn the pool; then trigger ONE restart — unless an init
  // is already handling recovery (_initPromise set) or the pool already recovered.
  _attachCrashHandler(browser) {
    browser.on('disconnected', () => {
      if (this._isRestarting) return; // a controlled restart/refresh already owns this
      this._isRestarting = true;      // block init()/submit/other restarts right now
      console.error('[PagePool] Browser disconnected unexpectedly — scheduling auto-restart');
      this.initialized = false;
      this.pages = [];
      this.available = [];
      setTimeout(() => {
        this._isRestarting = false; // release the guard so the restart can acquire it
        if (this.initialized || this._initPromise) return; // already recovered / an init is on it
        this._restartBrowser('browser-crash').catch((err) => {
          console.error(`[PagePool] Auto-restart after crash failed: ${err.message}`);
        });
      }, 2000);
    });
  }

  async init(proxy = null, cookies = null, eventId = null) {
    // Already initialized and browser alive — nothing to do
    if (this.initialized && (REQUEST_CONTEXT_MODE() || this._browser?.isConnected())) return;

    // Another caller is already initializing — piggyback on their promise
    if (this._initPromise) return this._initPromise;

    // A restart / crash-recovery is in flight — do NOT start a competing _doInit.
    // Racing them spawns concurrent inits that cleanup() (close) each other's
    // browser → 0 pages → frantic pool-wide rebind that burns every proxy (the
    // 8-min-restart collapse). Wait for it to finish, then re-check.
    if (this._isRestarting) {
      for (let i = 0; i < 200 && this._isRestarting; i++) {
        await new Promise((r) => setTimeout(r, 200));
      }
      if (this.initialized && (REQUEST_CONTEXT_MODE() || this._browser?.isConnected())) return;
      if (this._initPromise) return this._initPromise;
    }

    this._initPromise = this._doInit(proxy, cookies, eventId);
    try {
      await this._initPromise;
    } finally {
      this._initPromise = null; // always clear so future re-inits aren't blocked by a stale promise
    }
  }

  async _doInit(proxy, cookies, eventId = null) {
    // Serialize inits. A slow seed (~60-75s) let ~12 events each start their own
    // _doInit, whose async fills piled pages into ONE array (the 15/3 over-spawn +
    // extra datacenter hammering). Only one runs; the rest wait and reuse its pool.
    if (this._isIniting) {
      for (let i = 0; i < 400 && this._isIniting; i++) await new Promise((r) => setTimeout(r, 200));
      if (this.initialized && (REQUEST_CONTEXT_MODE() || this._browser?.isConnected())) return;
    }
    this._isIniting = true;
    try {
    // Honor POOL_SIZE from .env — read HERE (after dotenv has loaded); the
    // constructor runs at import, before app.js calls dotenv.config(), so it can't.
    this.size = parseInt(process.env.POOL_SIZE, 10) || this.size;
    // Only cleanup if there's something to clean up
    if (this.pages.length > 0 || this._browser || this._pageMeta.size > 0) {
      await this.cleanup();
    }

    // DIRECT_MODE: ignore any passed proxy — seed + all pages go out this host's IP.
    if (DIRECT_MODE) proxy = null;

    // Save init params for browser restart
    this._initProxy = proxy;
    this._initCookies = cookies;
    this._initEventId = eventId;

    // Reset per-page proxy tracking for this (re)init
    this._releaseAllLeases();
    this._contexts = [];
    this._pageMeta = new Map();
    this._usedProxies = new Set();

    if (REQUEST_CONTEXT_MODE()) {
      // Lightweight client-library mode: use request contexts only (no browser pages).
      const fillResults = await Promise.all(
        Array.from({ length: this.size }, () => this._createRequestContextSlot(eventId))
      );

      for (const made of fillResults) {
        if (made) {
          this.pages.push(made.page);
          this.available.push(made.page);
        } else {
          console.warn('[RequestPool] Could not bind a working proxy for a slot');
        }
      }

      console.log(`[RequestPool] ${this.pages.length}/${this.size} slots ready (${this._usedProxies.size} distinct proxies)`);

      const batchSize = parseInt(process.env.REQUEST_BATCH_SIZE, 10) || 6;
      const batchFlushMs = parseInt(process.env.REQUEST_BATCH_FLUSH_MS, 10) || 150;
      this._batcher = new RequestBatcher(this, batchSize, batchFlushMs);

      this._lastCookieRefresh = Date.now();
      this._consecutiveErrors = 0;
      this._isRestarting = false;

      this._startRestartTimer();
      this.initialized = true;
      this._initPromise = null;

      console.log(`[RequestPool] Ready: ${this.pages.length} slot(s) — refresh every ${this._cookieRefreshInterval / 60000}min`);
      return;
    }

    console.log(`[PagePool] Initial proxy: ${DIRECT_MODE ? 'DIRECT (no proxy)' : (proxy?.proxy || 'none')}`);

    // initApiBrowserContext launches browser, creates context with stealth scripts,
    // creates an apiPage, and navigates it to ticketmaster.com — this seeds initial
    // cookies into the shared context.
    const { browser, context } = await initApiBrowserContext(proxy, cookies);
    this._browser = browser;
    this._context = context;

    // Auto-recover if browser process crashes unexpectedly
    this._attachCrashHandler(browser);

    // Navigate one seed page to a real event page to get full cookies
    const eventUrl = eventId
      ? `https://www.ticketmaster.com/event/${eventId}`
      : 'https://www.ticketmaster.com/';

    console.log(`[PagePool] Seeding cookies via: ${eventUrl}`);
    const seedPage = await context.newPage();
    let seedTmpt;
    let seedLease = null;
    try {
      seedLease = SEED_SPLIT()
        ? await this._acquireSeedJar(eventId, proxy ? (proxy.id || proxy.proxy) : 'direct')
        : null;
      const jar = seedLease && seedLease.cookies;
      if (jar && jar.length) {
        seedTmpt = _tmptOf(jar);
        // Split mode: this shared context is on a datacenter proxy that can't mint
        // tmpt — inject the bart-minted jar so page #1 scrapes like the rest.
        await context.addCookies(jar).catch((e) => console.warn('[PagePool] seed inject failed:', e.message));
        console.log(`[PagePool] seed page #1 using injected bart jar (${jar.length} cookies)`);
      } else if (SEED_SPLIT() && !SELF_MINT()) {
        // Strict farm-consumer mode (SELF_MINT=0): farm is dry, so fail pool init
        // (caller retries) rather than navigate TM on a datacenter IP for our own tmpt.
        throw new Error('no farm jar available — not self-minting (SELF_MINT=0)');
      } else {
        if (SEED_SPLIT()) {
          // Split mode skipped the establish-session nav in initApiBrowserContext (it
          // assumed an injected jar), so do the homepage hop here first — the proven
          // mint recipe is homepage → event page, not a cold jump to the event.
          console.log('[PagePool] no farm jar — self-minting tmpt on the init proxy');
          await seedPage.goto('https://www.ticketmaster.com/', { waitUntil: 'domcontentloaded', timeout: 40000 }).catch(() => null);
          await seedPage.waitForTimeout(1500 + Math.random() * 1500);
        }
        await seedPage.goto(eventUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });
        // Wait for the EPS `tmpt` token to mint (IP-bound; residential hop needs time).
        let hasTmpt = false;
        for (let w = 0; w < 12; w++) {
          const names = (await context.cookies()).map((c) => c.name);
          if (names.includes('tmpt')) { hasTmpt = true; break; }
          await new Promise((t) => setTimeout(t, 1500));
        }

        const allCookies = await context.cookies();
        const tmCookies = allCookies.filter(c => c.domain.includes('ticketmaster'));
        console.log(`[PagePool] ${tmCookies.length} TM cookies seeded (tmpt=${hasTmpt ? 'YES' : 'no'})`);

        if (tmCookies.length === 0) {
          console.warn('[PagePool] WARNING: No TM cookies found after page load!');
        }
        // Share a successful self-mint with the pages filled below, so they inject
        // this jar instead of each navigating TM on their own proxy.
        if (hasTmpt && this._cacheSelfMintedJar(allCookies)) seedTmpt = _tmptOf(this._seedJar);
      }
    } catch (e) {
      console.error(`[PagePool] Seed page load failed: ${e.message}`);
      await seedPage.close().catch(() => {});
      throw e;
    }

    // The seed page becomes pool page #1 (on the init proxy / context #1)
    this.pages.push(seedPage);
    this.available.push(seedPage);
    this._contexts.push(context);
    this._pageMeta.set(seedPage, { context, proxy, tmpt: seedTmpt, jarId: seedLease && seedLease.jarId, jarOwner: seedLease && seedLease.ownerId });
    if (proxy) this._usedProxies.add(proxy.id || proxy.proxy);

    // Create remaining pool pages — EACH in its OWN context bound to its OWN
    // proxy, so the batcher spreads a batch's fetches across many IPs instead of
    // hammering one (EPS rate-flags a single hammered IP). Each candidate proxy
    // is validated with a real facets call before the page joins the pool.
    // Validate candidate pages in PARALLEL — with rotating residential sessions
    // ~60% are flagged, so sequential validation (each waiting on tmpt + facets)
    // makes init take minutes. Filling concurrently cuts that to one round.
    const fillResults = await Promise.all(
      Array.from({ length: this.size - 1 }, () => this._createProxyPage(eventId))
    );
    for (const made of fillResults) {
      if (made) {
        this.pages.push(made.page);
        this.available.push(made.page);
      } else {
        console.warn(`[PagePool] Could not bind a working proxy for a page`);
      }
    }
    console.log(`[PagePool] ${this.pages.length}/${this.size} pages ready, each on its own proxy (${this._usedProxies.size} distinct IPs)`);

    // Smaller batches reduce per-IP burst pressure and avoid EPS rate spikes.
    const batchSize = parseInt(process.env.REQUEST_BATCH_SIZE, 10) || 6;
    const batchFlushMs = parseInt(process.env.REQUEST_BATCH_FLUSH_MS, 10) || 150;
    this._batcher = new RequestBatcher(this, batchSize, batchFlushMs);

    this._lastCookieRefresh = Date.now();
    this._consecutiveErrors = 0;
    this._isRestarting = false;

    // Start periodic browser restart timer
    this._startRestartTimer();

    this.initialized = true;
    this._initPromise = null;
    console.log(`[PagePool] Ready: ${this.pages.length} page(s) — restart every ${this._cookieRefreshInterval / 60000}min`);
    } finally {
      this._isIniting = false;
    }
  }

  /**
   * Start a background timer that restarts the browser every 8 minutes
   * to get completely fresh cookies and prevent stale session issues.
   */
  // Renew every jar this pool holds while the process is alive. Deliberately a timer and
  // not a piggyback on the request path: a page that is alive but idle still holds its
  // jar, and must not lose it just because no events were routed to it this minute.
  _startLeaseHeartbeat() {
    if (this._leaseTimer || !SEED_FARM() || !JAR_LEASE()) return;
    this._leaseTimer = setInterval(() => {
      renewFarmLeases([...this._pageMeta.values()])
        .then(({ lost }) => {
          if (lost) console.warn(`[PagePool] ${lost} jar lease(s) no longer held by this instance`);
        })
        .catch(() => {});
    }, JAR_LEASE_RENEW_MS());
    this._leaseTimer.unref?.();
  }

  _startRestartTimer() {
    this._startLeaseHeartbeat();
    if (this._refreshTimer) clearTimeout(this._refreshTimer);

    const nextRegularDelay = () => {
      const jitter = Math.round((Math.random() * 2 - 1) * this._cookieRefreshJitterMs);
      return Math.max(60000, this._cookieRefreshInterval + jitter);
    };

    const scheduleAfter = (delayMs) => {
      if (!this.initialized) return;
      this._refreshTimer = setTimeout(() => {
        this._refreshTimer = null;
        runTick().catch((err) => {
          console.warn(`[PagePool] Refresh scheduler error: ${err.message}`);
          if (this.initialized) scheduleAfter(30000);
        });
      }, delayMs);
      this._refreshTimer.unref?.();
    };

    const runTick = async () => {
      if (!this.initialized) return;
      if (this._isRestarting || this._isRefreshing) {
        scheduleAfter(15000);
        return;
      }

      const queueDepth = this._batcher?.queue?.length || 0;
      const activeFlushes = this._batcher?._activeFlushes || 0;
      const waitingDepth = this.waiting.length || 0;
      if (queueDepth > 0 || activeFlushes > 0 || waitingDepth > 0) {
        const deferMs = Math.max(
          15000,
          parseInt(process.env.ROLLING_REFRESH_BUSY_DEFER_MS, 10) || 45000
        );
        console.log(
          `[PagePool] Deferring rolling refresh (queue=${queueDepth}, activeFlushes=${activeFlushes}, waiting=${waitingDepth})`
        );
        scheduleAfter(deferMs);
        return;
      }

      console.log(`[PagePool] Scheduled rolling refresh (cookies age: ${Math.round((Date.now() - this._lastCookieRefresh) / 60000)}min)`);
      await this._rollingRefresh();
      scheduleAfter(nextRegularDelay());
    };

    scheduleAfter(nextRegularDelay());
  }

  /**
   * Rolling refresh — the burn-safe, crash-safe alternative to a full restart.
   * Keeps the ONE warmed-up browser alive (a just-launched Camoufox is unstable
   * while spinning up several contexts — that's what crashed the full restart)
   * and swaps pages ONE AT A TIME on fresh proxies: build a new page, add it,
   * retire the old one. The pool keeps serving throughout (no downtime, no
   * deferred-queue stampede), and if a slot can't bind a clean proxy the old
   * page is kept rather than lost.
   */
  async _rollingRefresh() {
    if (this._isRestarting || this._isRefreshing) return;
    if (!REQUEST_CONTEXT_MODE() && !this._browser?.isConnected()) return; // dead browser → crash handler owns recovery
    this._isRefreshing = true;
    const start = Date.now();
    console.log('[PagePool] Rolling refresh starting…');
    try {
      // Seed event for minting/validation (same source the full restart used)
      let seedEventId = this._initEventId || null;
      try {
        const { Event } = await import('./models/index.js');
        const sample = await Event.aggregate([
          { $match: { Skip_Scraping: { $ne: true } } },
          { $sample: { size: 1 } },
          { $project: { Event_ID: 1 } },
        ]);
        if (sample?.length > 0) seedEventId = sample[0].Event_ID;
      } catch (e) { /* fall back to _initEventId/homepage */ }

      // Reset the used-proxy set but keep proxies of pages we're NOT replacing yet,
      // so _pickUnusedProxy hands out genuinely fresh IPs.
      this._usedProxies = new Set();
      for (const meta of this._pageMeta.values()) {
        if (meta?.proxy) this._usedProxies.add(meta.proxy.id || meta.proxy.proxy);
      }

      const oldPages = [...this.pages];
      if (!oldPages.length) return;

      const maxPagesPerCycle = Math.max(
        1,
        parseInt(process.env.ROLLING_REFRESH_MAX_PAGES, 10) || oldPages.length
      );
      const refreshAttempts = Math.max(
        1,
        parseInt(process.env.ROLLING_REFRESH_ATTEMPTS, 10) || 2
      );

      const targetCount = Math.min(oldPages.length, maxPagesPerCycle);
      const startIndex = this._refreshCursor % oldPages.length;
      const pagesToRefresh = [];
      for (let i = 0; i < targetCount; i++) {
        pagesToRefresh.push(oldPages[(startIndex + i) % oldPages.length]);
      }
      this._refreshCursor = (startIndex + targetCount) % oldPages.length;

      let refreshed = 0;
      for (const oldPage of pagesToRefresh) {
        if (!this._browser?.isConnected()) break; // bail if the browser dies mid-refresh
        const made = await this._createProxyPage(seedEventId, refreshAttempts);
        if (!made) {
          console.warn('[PagePool] Rolling refresh: no fresh proxy for a slot — keeping current page');
          continue;
        }
        this.pages.push(made.page);
        this.available.push(made.page);
        this._retirePage(oldPage); // remove + close the stale page/context
        refreshed++;
        await new Promise((r) => setTimeout(r, 500));
      }

      this._lastCookieRefresh = Date.now();
      this._consecutiveErrors = 0;
      console.log(`[PagePool] Rolling refresh complete in ${Date.now() - start}ms — refreshed ${refreshed}/${pagesToRefresh.length} (total pages=${oldPages.length}), ${this.pages.length} page(s) live`);
    } catch (e) {
      console.error(`[PagePool] Rolling refresh error: ${e.message}`);
    } finally {
      this._isRefreshing = false;
    }
  }

  /** Remove a page from the pool and close its page + context. */
  _retirePage(page) {
    const ai = this.available.indexOf(page);
    if (ai !== -1) this.available.splice(ai, 1);
    const pi = this.pages.indexOf(page);
    if (pi !== -1) this.pages.splice(pi, 1);
    const meta = this._pageMeta.get(page);
    this._pageMeta.delete(page);
    const proxyKey = meta?.proxy ? (meta.proxy.id || meta.proxy.proxy) : null;
    if (proxyKey) this._usedProxies.delete(proxyKey);
    if (page?.close) page.close().catch(() => {});
    if (meta?.requestContext) {
      meta.requestContext.dispose().catch(() => {});
    }
    const ctx = meta?.context;
    if (ctx) {
      const ci = this._contexts.indexOf(ctx);
      if (ci !== -1) this._contexts.splice(ci, 1);
      ctx.close().catch(() => {});
    }
  }

  /**
   * Full browser restart for cookie refresh.
   * 1. Pause new requests (queue them in _deferredQueue)
   * 2. Wait for in-flight batches to finish
   * 3. Close browser + all pages
   * 4. Relaunch with fresh cookies
   * 5. Drain deferred queue
   */
  async _restartBrowser(reason = 'scheduled') {
    if (REQUEST_CONTEXT_MODE()) {
      return this._restartRequestPool(reason);
    }

    if (this._isRestarting) return;
    this._isRestarting = true;
    const restartStart = Date.now();

    console.log(`[PagePool] Browser restart starting (${reason})...`);

    try {
      // 1. Stop the old batcher — no new flushes; items already in-flight will finish
      const oldBatcher = this._batcher;
      this._batcher = null;
      this.initialized = false; // submitRequests will queue to _deferredQueue

      // 2. Wait for in-flight batch flushes to complete (max 10s)
      if (oldBatcher) {
        const waitStart = Date.now();
        while (oldBatcher._activeFlushes > 0 && Date.now() - waitStart < 10000) {
          await new Promise(r => setTimeout(r, 100));
        }
        // Reject anything still queued in old batcher
        oldBatcher.cleanup();
      }

      // 3. Reject all waiting page acquirers
      for (const w of this.waiting) {
        clearTimeout(w.timer);
        w.resolve = null; // prevent double-resolve
      }
      this.waiting = [];

      // 4. Close all pages
      await Promise.allSettled(this.pages.map(p => p.close().catch(() => {})));
      this.pages = [];
      this.available = [];

      // 5. Close old browser
      if (this._browser) {
        try {
          await this._browser.close();
        } catch (e) {
          // Browser may already be dead
        }
        this._browser = null;
        this._context = null;
      }

      // Also clean up the global apiContext references
      apiBrowser = null;
      apiContext = null;
      apiPage = null;

      // 6. Reset per-page proxy tracking + pick a fresh proxy for context #1
      this._releaseAllLeases();
      this._contexts = [];
      this._pageMeta = new Map();
      this._usedProxies = new Set();
      const allProxies = proxyArray.proxies;
      const newProxy = DIRECT_MODE
        ? null
        : (allProxies.length > 0
            ? allProxies[Math.floor(Math.random() * allProxies.length)]
            : this._initProxy);
      this._initProxy = newProxy;
      console.log(`[PagePool] Rotated context #1 proxy to ${DIRECT_MODE ? 'DIRECT (no proxy)' : (newProxy?.proxy || 'none')}`);

      // 7. Relaunch browser + context #1
      const { browser, context } = await initApiBrowserContext(newProxy, this._initCookies);
      this._browser = browser;
      this._context = context;
      this._attachCrashHandler(browser); // keep crash-recovery alive after a restart too

      // Determine a seed event (for cookie minting + per-proxy validation)
      let seedEventId = this._initEventId || null;
      try {
        const { Event } = await import('./models/index.js');
        const randomEvents = await Event.aggregate([
          { $match: { Skip_Scraping: { $ne: true } } },
          { $sample: { size: 1 } },
          { $project: { Event_ID: 1 } }
        ]);
        if (randomEvents?.length > 0) seedEventId = randomEvents[0].Event_ID;
      } catch (e) { /* fall back to homepage */ }
      const seedUrl = seedEventId
        ? `https://www.ticketmaster.com/event/${seedEventId}`
        : 'https://www.ticketmaster.com/';

      // Seed cookies on page #1 (context #1 / newProxy). In split mode inject the
      // bart-minted jar instead of self-seeding on the datacenter proxy.
      const seedPage = await context.newPage();
      const rlease = SEED_SPLIT()
        ? await this._acquireSeedJar(seedEventId, newProxy ? (newProxy.id || newProxy.proxy) : 'direct')
        : null;
      const rjar = rlease && rlease.cookies;
      if (rjar && rjar.length) {
        await context.addCookies(rjar).catch((e) => console.warn('[PagePool] Restart seed inject failed:', e.message));
        console.log(`[PagePool] Restart: seed page #1 using injected bart jar (${rjar.length} cookies)`);
      } else if (SEED_SPLIT() && !SELF_MINT()) {
        // Strict farm-consumer mode (SELF_MINT=0): farm is dry, so abort the restart
        // (next submit re-inits) instead of navigating TM to mint our own tmpt.
        await seedPage.close().catch(() => {});
        throw new Error('no farm jar available — not self-minting (SELF_MINT=0)');
      } else {
        if (SEED_SPLIT()) {
          // Homepage first, then the event page — same mint recipe as init/_createProxyPage.
          console.log('[PagePool] Restart: no farm jar — self-minting tmpt on the restart proxy');
          await seedPage.goto('https://www.ticketmaster.com/', { waitUntil: 'domcontentloaded', timeout: 40000 }).catch(() => null);
          await seedPage.waitForTimeout(1500 + Math.random() * 1500);
        }
        await seedPage.goto(seedUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });
        // Wait for tmpt rather than a flat 2s: the token is what makes the jar useful,
        // and a restart that seeds without it puts a dead page #1 back in the pool.
        for (let w = 0; w < 8; w++) {
          const names = (await context.cookies()).map((c) => c.name);
          if (names.includes('tmpt')) break;
          await new Promise((t) => setTimeout(t, 1500));
        }
        const all = await context.cookies();
        const tmCookies = all.filter(c => c.domain.includes('ticketmaster'));
        console.log(`[PagePool] Restart: ${tmCookies.length} TM cookies after seed (tmpt=${all.some(c => c.name === 'tmpt') ? 'YES' : 'no'})`);
        if (SEED_SPLIT()) this._cacheSelfMintedJar(all);
      }

      this.pages.push(seedPage);
      this.available.push(seedPage);
      this._contexts.push(context);
      this._pageMeta.set(seedPage, { context, proxy: newProxy, tmpt: rjar ? _tmptOf(rjar) : _tmptOf(await context.cookies().catch(() => [])), jarId: rlease && rlease.jarId, jarOwner: rlease && rlease.ownerId });
      if (newProxy?.proxy) this._usedProxies.add(newProxy.proxy);

      // Remaining pool pages — each on its OWN validated proxy
      for (let i = 1; i < this.size; i++) {
        const made = await this._createProxyPage(seedEventId);
        if (made) {
          this.pages.push(made.page);
          this.available.push(made.page);
        } else {
          console.warn(`[PagePool] Restart: could not bind a working proxy for page ${i + 1}`);
        }
      }

      // 7. Create new batcher and mark ready
      const batchSize = parseInt(process.env.REQUEST_BATCH_SIZE, 10) || 6;
      const batchFlushMs = parseInt(process.env.REQUEST_BATCH_FLUSH_MS, 10) || 150;
      this._batcher = new RequestBatcher(this, batchSize, batchFlushMs);
      this._lastCookieRefresh = Date.now();
      this._consecutiveErrors = 0;
      this._requestsSinceRotation = 0;
      this.initialized = true;

      const restartMs = Date.now() - restartStart;
      console.log(`[PagePool] Browser restart complete in ${restartMs}ms — ${this.pages.length} page(s) ready`);

      // 8. Drain deferred queue — resubmit requests that came in during restart
      if (this._deferredQueue.length > 0) {
        const deferred = this._deferredQueue.splice(0);
        console.log(`[PagePool] Draining ${deferred.length} deferred request(s)`);
        for (const { requests, resolve, reject, timer } of deferred) {
          if (timer) clearTimeout(timer);
          this._batcher.submit(requests).then(resolve).catch(reject);
        }
      }
    } catch (error) {
      console.error(`[PagePool] Browser restart FAILED: ${error.message}`);
      // Mark as not initialized so next submitRequests triggers re-init
      this.initialized = false;
      this._initPromise = null;
      // Reject all deferred
      for (const { reject, timer } of this._deferredQueue) {
        if (timer) clearTimeout(timer);
        reject(new Error(`Browser restart failed: ${error.message}`));
      }
      this._deferredQueue = [];
    } finally {
      this._isRestarting = false;
    }
  }

  async _restartRequestPool(reason = 'scheduled') {
    if (this._isRestarting) return;
    this._isRestarting = true;
    const restartStart = Date.now();

    console.log(`[RequestPool] Restart starting (${reason})...`);

    try {
      const oldBatcher = this._batcher;
      this._batcher = null;
      this.initialized = false;

      if (oldBatcher) {
        const waitStart = Date.now();
        while (oldBatcher._activeFlushes > 0 && Date.now() - waitStart < 10000) {
          await new Promise((r) => setTimeout(r, 100));
        }
        oldBatcher.cleanup();
      }

      for (const w of this.waiting) {
        clearTimeout(w.timer);
        w.resolve = null;
      }
      this.waiting = [];

      const workers = [...this.pages];
      this.pages = [];
      this.available = [];

      await Promise.allSettled(
        workers.map(async (worker) => {
          const meta = this._pageMeta.get(worker);
          this._pageMeta.delete(worker);
          if (meta?.jarId) releaseFarmJar(meta.jarId, meta.jarOwner).catch(() => {});
          if (meta?.requestContext) {
            await meta.requestContext.dispose().catch(() => {});
            return;
          }
          if (worker?.close) await worker.close().catch(() => {});
        })
      );

      this._contexts = [];
      this._usedProxies = new Set();

      const refreshAttempts = Math.max(
        1,
        parseInt(process.env.ROLLING_REFRESH_ATTEMPTS, 10) || 2
      );

      const fillResults = await Promise.all(
        Array.from({ length: this.size }, () =>
          this._createRequestContextSlot(this._initEventId, refreshAttempts)
        )
      );

      for (const made of fillResults) {
        if (made) {
          this.pages.push(made.page);
          this.available.push(made.page);
        }
      }

      const batchSize = parseInt(process.env.REQUEST_BATCH_SIZE, 10) || 6;
      const batchFlushMs = parseInt(process.env.REQUEST_BATCH_FLUSH_MS, 10) || 150;
      this._batcher = new RequestBatcher(this, batchSize, batchFlushMs);

      this._lastCookieRefresh = Date.now();
      this._consecutiveErrors = 0;
      this._requestsSinceRotation = 0;
      this.initialized = true;

      const restartMs = Date.now() - restartStart;
      console.log(`[RequestPool] Restart complete in ${restartMs}ms — ${this.pages.length} slot(s) ready`);

      if (this._deferredQueue.length > 0) {
        const deferred = this._deferredQueue.splice(0);
        console.log(`[RequestPool] Draining ${deferred.length} deferred request(s)`);
        for (const { requests, resolve, reject, timer } of deferred) {
          if (timer) clearTimeout(timer);
          this._batcher.submit(requests).then(resolve).catch(reject);
        }
      }
    } catch (error) {
      console.error(`[RequestPool] Restart FAILED: ${error.message}`);
      this.initialized = false;
      this._initPromise = null;
      for (const { reject, timer } of this._deferredQueue) {
        if (timer) clearTimeout(timer);
        reject(new Error(`Request pool restart failed: ${error.message}`));
      }
      this._deferredQueue = [];
    } finally {
      this._isRestarting = false;
    }
  }

  /**
   * Track errors from batch results. If too many 403s pile up,
   * trigger an early browser restart.
   */
  // A 403 no longer restarts anything.
  //
  // It used to: five consecutive 403s tore down the whole browser and threw the seed jar
  // away. Measured 2026-08-09, that reasoning was wrong on both counts. A 403 is a
  // per-exit-IP rate trip that clears by itself in ~75s (scripts/jarRateRamp.mjs), and
  // the batcher fired up to 12 requests down one IP at once — so ONE over-sized burst
  // produced five 403s instantly and took down all six pages plus their jars. The
  // replacement pages then repeated the burst. That loop is what capped throughput.
  //
  // Rate is now controlled before the fact by reserveIpSlots(), and a 403 that still gets
  // through rests just the offending IP via coolProxy(). Genuine browser death is already
  // handled where it actually shows up: Target closed / Protocol error / crashed.
  trackError(status) {
    if (status >= 200 && status < 400) this._consecutiveErrors = 0;
    else if (status === 403) this._consecutiveErrors++;
  }

  /**
   * Submit requests for ONE event. The batcher groups many events
   * into mega-batches across pool pages automatically.
   * During browser restart, requests are deferred and replayed after restart.
   * Returns Promise<Array<{success, data?, error?, status}>>.
   */
  async submitRequests(requests) {
    // During restart — queue for replay after browser is back (with 30s timeout)
    if (this._isRestarting || !this.initialized || !this._batcher) {
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          const idx = this._deferredQueue.findIndex(d => d.resolve === resolve);
          if (idx !== -1) this._deferredQueue.splice(idx, 1);
          reject(new Error('Deferred queue timeout — browser restart took too long'));
        }, 30000);
        this._deferredQueue.push({ requests, resolve, reject, timer });
      });
    }

    // Track calls and trigger proxy rotation when threshold reached
    this._requestsSinceRotation++;
    if (this._requestsSinceRotation >= this._proxyRotationThreshold && !this._isRestarting) {
      console.log(`[PagePool] ${this._requestsSinceRotation} requests since last rotation — rotating proxy`);
      this._requestsSinceRotation = 0;
      this._restartBrowser('proxy-rotation').catch(() => {});
      // Queue this request for replay after restart (with 30s timeout)
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          const idx = this._deferredQueue.findIndex(d => d.resolve === resolve);
          if (idx !== -1) this._deferredQueue.splice(idx, 1);
          reject(new Error('Deferred queue timeout — browser restart took too long'));
        }, 30000);
        this._deferredQueue.push({ requests, resolve, reject, timer });
      });
    }

    return this._batcher.submit(requests);
  }

  // --- Per-exit-IP pacing and cooldown -------------------------------------------
  // The governing limit is requests per minute per EXIT IP, measured at a knee of ~30/min
  // with a ~75s recovery (scripts/jarRateRamp.mjs). The old controls were both aimed at
  // the wrong resource: JAR_RATE_CAP counted per TOKEN, and it lived in-process, so ten
  // PM2 instances multiplied whatever cap you set by ten. Both are replaced by one token
  // bucket per proxy, which is also per-jar now that a jar is leased to a single proxy.
  _proxyKeyOf(page) {
    const m = this._pageMeta.get(page);
    return m && m.proxy ? (m.proxy.id || m.proxy.proxy) : 'direct';
  }

  // Hand every held jar back before the page map is wiped. Without this a restart strands
  // the instance's whole jar allocation until it expires — and with the farm minting only
  // a handful an hour, jars are the scarce resource, not pages.
  _releaseAllLeases() {
    for (const m of this._pageMeta.values()) {
      if (m && m.jarId) releaseFarmJar(m.jarId, m.jarOwner).catch(() => {});
    }
  }

  _ipState(meta) {
    const key = meta && meta.proxy ? (meta.proxy.id || meta.proxy.proxy) : 'direct';
    let s = this._ipBuckets.get(key);
    if (!s) {
      s = { tokens: IP_RATE_PER_MIN(), last: Date.now(), coolUntil: 0 };
      this._ipBuckets.set(key, s);
    }
    return s;
  }

  // Block until this IP can afford `n` more requests, and until any cooldown has passed.
  async reserveIpSlots(meta, n) {
    const rate = IP_RATE_PER_MIN();
    if (!rate) return;
    const s = this._ipState(meta);
    for (let guard = 0; guard < 600; guard++) {
      const now = Date.now();
      if (now < s.coolUntil) {
        await new Promise((r) => setTimeout(r, Math.min(2000, s.coolUntil - now)));
        continue;
      }
      s.tokens = Math.min(rate, s.tokens + ((now - s.last) / 60000) * rate);
      s.last = now;
      if (s.tokens >= n) { s.tokens -= n; return; }
      await new Promise((r) => setTimeout(r, Math.min(2000, ((n - s.tokens) / rate) * 60000)));
    }
  }

  // Rest an IP that just came back 403. Never touches the jar: a 403 cannot distinguish a
  // bad token from an over-driven IP, and retiring jars on 403 previously threw away 94%
  // good tokens (see the notes on the removed JAR_DEAD_THRESHOLD).
  coolProxy(meta, blockedCount) {
    const s = this._ipState(meta);
    const ms = IP_COOLDOWN_MS();
    s.coolUntil = Math.max(s.coolUntil, Date.now() + ms);
    s.tokens = 0;
    const key = meta && meta.proxy ? (meta.proxy.id || meta.proxy.proxy) : 'direct';
    console.log(`[PagePool] ${key} returned ${blockedCount}×403 — resting it ${Math.round(ms / 1000)}s (jar kept)`);
  }

  async acquire(timeoutMs = 20000) {
    if (!this.initialized || (!REQUEST_CONTEXT_MODE() && !this._browser?.isConnected())) {
      throw new Error('Pool not initialized or browser disconnected');
    }

    if (this.available.length > 0) {
      // Prefer a page whose IP is not resting; fall back to any rather than stall the
      // queue, since reserveIpSlots() will wait out the remaining cooldown anyway.
      const now = Date.now();
      const idx = this.available.findIndex((p) => {
        const s = this._ipBuckets.get(this._proxyKeyOf(p));
        return !s || now >= s.coolUntil;
      });
      return idx === -1 ? this.available.pop() : this.available.splice(idx, 1)[0];
    }

    // Wait for a page to be released
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const idx = this.waiting.findIndex(w => w.resolve === resolve);
        if (idx !== -1) this.waiting.splice(idx, 1);
        reject(new Error('Page pool acquisition timeout'));
      }, timeoutMs);
      this.waiting.push({ resolve, timer });
    });
  }

  release(page) {
    if (!this.pages.includes(page)) return;
    if (this.waiting.length > 0) {
      const { resolve, timer } = this.waiting.shift();
      clearTimeout(timer);
      resolve(page);
    } else {
      this.available.push(page);
    }
  }

  _removePage(page) {
    let idx = this.pages.indexOf(page);
    if (idx !== -1) this.pages.splice(idx, 1);
    idx = this.available.indexOf(page);
    if (idx !== -1) this.available.splice(idx, 1);

    // Tear down this page's OWN context and free its proxy
    const meta = this._pageMeta.get(page);
    if (meta) {
      this._pageMeta.delete(page);
      const proxyKey = meta.proxy ? (meta.proxy.id || meta.proxy.proxy) : null;
      if (proxyKey) this._usedProxies.delete(proxyKey);
      // Give the jar back so a replacement page can use it — on whatever proxy it lands
      // on. The jar is still good: pages die for browser reasons far more often than
      // token reasons, and a jar has no per-call budget to have used up.
      if (meta.jarId) releaseFarmJar(meta.jarId, meta.jarOwner).catch(() => {});
      if (meta.requestContext) {
        meta.requestContext.dispose().catch(() => {});
      }
      const ci = this._contexts.indexOf(meta.context);
      if (ci !== -1) this._contexts.splice(ci, 1);
      meta.context?.close().catch(() => {});
    }

    // Replace it with a fresh page on a NEW validated proxy (best-effort)
    const canRebuild = REQUEST_CONTEXT_MODE() || this._browser?.isConnected();
    if (canRebuild && !this._isRestarting && this.pages.length < this.size) {
      this._createProxyPage(this._initEventId).then((made) => {
        if (made) {
          this.pages.push(made.page);
          this.release(made.page);
          console.log(`[PagePool] Replaced dead page on fresh proxy ${made.proxy.proxy}, pool: ${this.pages.length}`);
        }
      }).catch(() => {});
    }
  }

  async cleanup() {
    // Stop browser restart timer
    if (this._refreshTimer) {
      clearTimeout(this._refreshTimer);
      this._refreshTimer = null;
    }
    if (this._leaseTimer) {
      clearInterval(this._leaseTimer);
      this._leaseTimer = null;
    }

    if (this._batcher) {
      this._batcher.cleanup();
      this._batcher = null;
    }
    for (const w of this.waiting) clearTimeout(w.timer);
    this.waiting = [];

    // Reject deferred requests
    for (const { reject, timer } of this._deferredQueue) {
      if (timer) clearTimeout(timer);
      reject(new Error('Pool cleanup'));
    }
    this._deferredQueue = [];

    const requestContexts = Array.from(this._pageMeta.values())
      .map((m) => m?.requestContext)
      .filter(Boolean);
    await Promise.allSettled(requestContexts.map((rc) => rc.dispose().catch(() => {})));

    await Promise.allSettled(this.pages.map(p => p.close().catch(() => {})));
    await Promise.allSettled(this._contexts.map(c => c.close().catch(() => {})));
    this.pages = [];
    this.available = [];
    this._contexts = [];
    this._releaseAllLeases();
    this._pageMeta = new Map();
    this._usedProxies = new Set();
    this.initialized = false;
    this._initPromise = null;
    this._isRestarting = false;
  }

  get stats() {
    const cookieAgeMin = Math.round((Date.now() - this._lastCookieRefresh) / 60000);
    return {
      total: this.pages.length,
      available: this.available.length,
      inUse: this.pages.length - this.available.length,
      waiting: this.waiting.length,
      batcherQueue: this._batcher?.queue.length || 0,
      activeFlushes: this._batcher?._activeFlushes || 0,
      initialized: this.initialized,
      cookieAgeMinutes: cookieAgeMin,
      consecutiveErrors: this._consecutiveErrors,
      isRestarting: this._isRestarting,
      deferredQueue: this._deferredQueue.length
    };
  }
}

// Global page pool instance — each page = its own context + its own proxy, so
// more pages = more proxies scraping in parallel = higher throughput. Default 3;
// raise on big boxes with rotating proxies via POOL_SIZE (e.g. POOL_SIZE=10).
const browserPagePool = new BrowserPagePool(parseInt(process.env.POOL_SIZE, 10) || 3);

/**
 * Clean up browser resources
 */
async function cleanup() {
  // Clean up page pool first
  await browserPagePool.cleanup();

  // Clean up main browser
  if (browser) {
    try {
      await browser.close();
      browser = null;
    } catch (error) {
      console.warn("Error closing browser:", error.message);
    }
  }
  
  // Clean up API browser
  await cleanupApiBrowser();
}

export {
  initBrowser,
  captureCookies,
  refreshCookies,
  loadCookiesFromFile,
  saveCookiesToFile,
  cleanup,
  handleTicketmasterChallenge,
  checkForTicketmasterChallenge,
  enhancedFingerprint,
  getRandomLocation,
  getRealisticIphoneUserAgent,
  generateAlternativeEventId,
  simulateMobileInteractions,
  // Browser-based API functions (bypass TLS fingerprinting)
  initApiBrowserContext,
  browserApiRequest,
  cleanupApiBrowser,
  isApiBrowserAvailable,
  // Page pool for high-throughput parallel requests
  browserPagePool,
  // Jar leasing — exported so the cookie-farm and scripts/ diagnostics can honour the
  // same one-jar-one-IP rule instead of re-introducing the fan-out that revokes tokens.
  leaseFarmJar,
  releaseFarmJar,
  renewFarmLeases,
  farmHasFreeJar,
  newOwnerId
};