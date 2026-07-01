// SEED-then-RAW test: mint cookies once with a real browser, then hit the TM APIs
// WITHOUT a browser using `impit` (browser-TLS-impersonating fetch), reusing the
// SAME proxy IP + the minted cookies.
//
// Why this matters: EPS blocks a cold cookie-less request (proven: facets 403
// {"response":"block"}). The open question is whether EPS actually needs the live
// Chromium/Firefox process, or just (a) the `tmpt` cookie it minted and (b) a
// browser-shaped TLS/HTTP2 fingerprint on the SAME IP. If impit (Firefox JA3/JA4)
// + the minted cookie clears facets, we can seed ONCE per IP and then fire
// thousands of cheap cookie-carrying calls with NO browser in the hot path —
// killing the per-event browser memory the scraper spends today.
//
// Flow (ONE browser launch, closed immediately after seeding):
//   1. Camoufox(proxy) -> ticketmaster.com -> /event/<id> -> wait for `tmpt`
//   2. read cookies, close browser
//   3. impit({browser:'firefox', proxyUrl: SAME proxy}) -> facets + map, Cookie attached
//
// Usage:
//   node scripts/seedThenRaw.mjs --event=1500647995125B72
//   node scripts/seedThenRaw.mjs --event=<liveId> --proxyIndex=0

import { Camoufox } from "camoufox-js";
import { Impit } from "impit";

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  })
);

const EVENT_ID = args.event || process.env.EVENT_ID || "1500647995125B72";

// One IPRoyal sticky session (fixed US exit IP for 30m). Seed + raw MUST share it.
const PROXIES = [
  "geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-MTz93l9k_lifetime-30m",
  "geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-oLExzbRi_lifetime-30m",
  "geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-CJSBZ7RG_lifetime-30m",
];
const p = PROXIES[parseInt(args.proxyIndex, 10) || 0].split(":");
const PROXY = { host: p[0], port: p[1], user: p[2], pass: p.slice(3).join(":") };
const proxyUrl = `http://${PROXY.user}:${PROXY.pass}@${PROXY.host}:${PROXY.port}`;
const camoufoxProxy = { server: `http://${PROXY.host}:${PROXY.port}`, username: PROXY.user, password: PROXY.pass };

const facetsUrl = (id) =>
  `https://services.ticketmaster.com/api/ismds/event/${id}/facets?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse`;
const mapUrl = (id) =>
  `https://mapsapi.tmol.io/maps/geometry/3/event/${id}/placeDetailNoKeys?useHostGrids=true&app=CCP&sectionLevel=true&systemId=HOST`;

async function seedCookies() {
  console.log(`[seed] launching Camoufox via session ${PROXY.pass.match(/session-([^_]+)/)?.[1]}...`);
  const browser = await Camoufox({ headless: true, humanize: false, geoip: false, proxy: camoufoxProxy });
  try {
    const context = await browser.newContext({ ignoreHTTPSErrors: true, bypassCSP: true });
    // block heavy assets — we only need the EPS challenge JS to run + tmpt to mint
    await context.route("**/*", (r) =>
      ["image", "media", "font"].includes(r.request().resourceType())
        ? r.abort().catch(() => {})
        : r.continue().catch(() => {})
    );
    const page = await context.newPage();

    let exitIp = "?";
    try {
      const ipr = await context.request.get("https://api.ipify.org?format=json", { timeout: 15000 });
      exitIp = (await ipr.json()).ip;
    } catch {}
    console.log(`[seed] exit IP: ${exitIp}`);

    await page.goto("https://www.ticketmaster.com/", { waitUntil: "domcontentloaded", timeout: 40000 }).catch(() => null);
    await page.waitForTimeout(1500);
    const resp = await page
      .goto(`https://www.ticketmaster.com/event/${EVENT_ID}`, { waitUntil: "domcontentloaded", timeout: 40000 })
      .catch(() => null);
    const pageStatus = resp ? resp.status() : 0;
    console.log(`[seed] event page status: ${pageStatus}`);

    let tmpt = false;
    for (let w = 0; w < 8; w++) {
      const names = (await context.cookies()).map((c) => c.name);
      if (names.includes("tmpt")) { tmpt = true; break; }
      await page.waitForTimeout(1200);
    }
    const cookies = (await context.cookies()).filter((c) => c.domain.includes("ticketmaster"));
    console.log(`[seed] tmpt minted: ${tmpt ? "YES" : "no"} | ${cookies.length} TM cookies: [${cookies.map((c) => c.name).join(", ")}]`);
    return { cookies, exitIp, tmpt, pageStatus };
  } finally {
    await browser.close().catch(() => {});
    console.log(`[seed] browser closed`);
  }
}

async function rawCall(impit, url, label) {
  try {
    const res = await impit.fetch(url, {
      headers: {
        accept: "application/json, text/plain, */*",
        "x-api-key": "b462oi7fic6pehcdkzony5bxhe",
        origin: "https://www.ticketmaster.com",
        referer: "https://www.ticketmaster.com/",
      },
    });
    const body = await res.text();
    return { status: res.status, len: body.length, snippet: body.slice(0, 160).replace(/\s+/g, " ") };
  } catch (e) {
    return { status: 0, err: e.message };
  }
}

(async () => {
  console.log(`\nSEED-then-RAW — event ${EVENT_ID}\n`);
  const { cookies, exitIp, tmpt } = await seedCookies();
  if (!cookies.length) {
    console.log(`\nNo cookies minted — cannot proceed. IP may be blocked at the challenge stage.\n`);
    process.exit(1);
  }

  const cookieHeader = cookies.map((c) => `${c.name}=${c.value}`).join("; ");
  console.log(`\n[raw] impit(firefox TLS) via SAME proxy, ${cookies.length} cookies attached...`);

  // browser:'firefox' to match the Camoufox (Firefox) engine that minted tmpt.
  const impit = new Impit({
    browser: "firefox",
    proxyUrl,
    ignoreTlsErrors: true,
    timeout: 20000,
    headers: { cookie: cookieHeader },
  });

  const facet = await rawCall(impit, facetsUrl(EVENT_ID), "facet");
  const map = await rawCall(impit, mapUrl(EVENT_ID), "map");

  console.log(`\n──────── RESULT ────────`);
  console.log(`  seed exit IP:   ${exitIp}  (tmpt minted: ${tmpt ? "YES" : "no"})`);
  console.log(`  RAW facets:     ${facet.status} ${facet.err ? "err=" + facet.err : `(${facet.len}B)`}`);
  if (facet.snippet) console.log(`    body: ${facet.snippet}`);
  console.log(`  RAW map:        ${map.status} ${map.err ? "err=" + map.err : `(${map.len}B)`}`);
  console.log(`────────────────────────`);
  console.log(
    facet.status === 200
      ? `\n✅ WORKS: browser-minted cookie + impit Firefox-TLS on the same IP clears facets.\n   → seed ONCE per IP, then fire cheap impit calls with NO browser in the hot path.\n`
      : `\n❌ facets still ${facet.status} — EPS needs the live browser process, not just cookie+TLS.\n   The browser-minted tmpt alone (over impit's TLS) is not sufficient.\n`
  );
  process.exit(0);
})().catch((e) => {
  console.error("failed:", e);
  process.exit(1);
});
