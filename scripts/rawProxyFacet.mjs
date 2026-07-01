// RAW test: hit TM's facets + map APIs DIRECTLY through each IPRoyal proxy —
// NO browser, NO cookies, NO tmpt token, NO EPS challenge. Just a plain HTTPS GET
// tunneled through the proxy (CONNECT). This answers one question only:
//
//   "Do these residential IPs return facet/map JSON on a cold, cookie-less request?"
//
// Almost certainly NO for facets (EPS requires the browser-minted `tmpt` cookie +
// real TLS fingerprint — a raw node request has neither), but this proves it per-IP
// and shows exactly what status/body TM hands back. The map API (mapsapi.tmol.io) is
// often open and MAY return geometry without cookies — worth confirming.
//
// Usage:
//   node scripts/rawProxyFacet.mjs                       # default event, all proxies from stdin/file
//   node scripts/rawProxyFacet.mjs --event=1500647995125B72 --n=10
//   PROXIES_FILE=debug/iproyal-seed.txt node scripts/rawProxyFacet.mjs
//
// Proxy input: one `host:port:user:pass` per line, read from --file / PROXIES_FILE,
// else from the built-in list below (paste your proxies there).

import pkg from "https-proxy-agent";
const { HttpsProxyAgent } = pkg;
import https from "https";
import fs from "fs";

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  })
);

const EVENT_ID = args.event || process.env.EVENT_ID || "1500647995125B72";
const LIMIT = parseInt(args.n, 10) || Infinity;

// --- Proxies: host:port:user:pass per line ---
const BUILTIN = `
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-MTz93l9k_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-ZgH3nTEJ_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-oLExzbRi_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-CJSBZ7RG_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-2KM2NUvq_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-joHKMcGg_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-jIq1JFnR_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-OsZvD1on_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-p4MWaay8_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-SbAUQy6v_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-z6jesKQ6_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-WWEqNX3K_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-VPaLjtrj_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-1bqB3z5U_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-PCRQRNsi_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-zfTDvpaW_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-32sipHtV_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-Q4u9eIHg_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-FsJ86qso_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-up8EshPq_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-3b0lE3vo_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-I7cWYYdS_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-7XCbbh66_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-TPbFeTX7_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-TFceefrN_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-sHCwTL5n_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-jMI3TT3h_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-YeqNgG1W_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-3fDu3oI5_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-FyIgUbi6_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-RLN6X83v_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-O6r8REv7_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-CBNBqIck_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-6dR4xhQz_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-hhGBtbrP_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-KFhGoy3x_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-MWwUYDyt_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-jB6iVFe3_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-X53lSf7l_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-5Q0LRF9V_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-414tctq3_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-NxwHIbZE_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-ePDN4d4s_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-lBvPIxUp_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-6VG9U680_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-zTwW9KEv_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-xXf134oP_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-7eor2LAH_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-ZTEelpfz_lifetime-30m
geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-ELy2OPYO_lifetime-30m
`.trim();

function loadProxies() {
  const file = args.file || process.env.PROXIES_FILE;
  const raw = file ? fs.readFileSync(file, "utf8") : BUILTIN;
  return raw
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l && !l.startsWith("#"))
    .map((line) => {
      // host:port:user:pass  (pass may itself contain no colons for IPRoyal)
      const parts = line.split(":");
      const [host, port, user, ...passParts] = parts;
      return { host, port, user, pass: passParts.join(":"), raw: line };
    })
    .filter((p) => p.host && p.port);
}

const facetsUrl = (id) =>
  `https://services.ticketmaster.com/api/ismds/event/${id}/facets?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse`;

const mapUrl = (id) =>
  `https://mapsapi.tmol.io/maps/geometry/3/event/${id}/placeDetailNoKeys?useHostGrids=true&app=CCP&sectionLevel=true&systemId=HOST`;

// One raw GET through a proxy. Returns {status, len, ct, snippet, err}.
function rawGet(url, agent, extraHeaders = {}) {
  return new Promise((resolve) => {
    const req = https.get(
      url,
      {
        agent,
        timeout: 20000,
        headers: {
          accept: "application/json, text/plain, */*",
          "accept-language": "en-US,en;q=0.9",
          "user-agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
          ...extraHeaders,
        },
      },
      (res) => {
        let body = "";
        res.on("data", (c) => {
          if (body.length < 400) body += c.toString();
        });
        res.on("end", () =>
          resolve({
            status: res.statusCode,
            len: res.headers["content-length"] || "?",
            ct: (res.headers["content-type"] || "").split(";")[0],
            snippet: body.slice(0, 160).replace(/\s+/g, " "),
          })
        );
      }
    );
    req.on("timeout", () => {
      req.destroy();
      resolve({ status: 0, err: "timeout" });
    });
    req.on("error", (e) => resolve({ status: 0, err: e.message }));
  });
}

async function testProxy(p) {
  const proxyUrl = `http://${encodeURIComponent(p.user)}:${encodeURIComponent(p.pass)}@${p.host}:${p.port}`;
  const agent = new HttpsProxyAgent(proxyUrl);
  const out = { session: (p.pass.match(/session-([^_]+)/) || [, "?"])[1], ip: "?" };

  // 1) exit IP (proves the tunnel works + shows the residential IP TM would see)
  const ipr = await rawGet("https://api.ipify.org?format=json", agent).catch(() => null);
  if (ipr?.snippet) {
    try { out.ip = JSON.parse(ipr.snippet).ip; } catch {}
  }
  if (ipr?.err) out.ipErr = ipr.err;

  // 2) facets — the real gate (expected to fail without tmpt cookie)
  out.facet = await rawGet(facetsUrl(EVENT_ID), agent, {
    "x-api-key": "b462oi7fic6pehcdkzony5bxhe",
    origin: "https://www.ticketmaster.com",
    referer: "https://www.ticketmaster.com/",
  });

  // 3) map geometry — often cookie-free
  out.map = await rawGet(mapUrl(EVENT_ID), agent, {
    origin: "https://www.ticketmaster.com",
    referer: "https://www.ticketmaster.com/",
  });

  return out;
}

(async () => {
  const proxies = loadProxies().slice(0, LIMIT);
  console.log(`\nRAW cookie-less test — ${proxies.length} proxies, event ${EVENT_ID}`);
  console.log(`facets: services.ticketmaster.com/api/ismds/...  |  map: mapsapi.tmol.io/...\n`);

  const results = await Promise.all(proxies.map(testProxy));

  const verdict = (r) =>
    r.facet?.status === 200 ? "FACET-200" :
    r.map?.status === 200 ? "MAP-ONLY" :
    r.ip === "?" ? "PROXY-DEAD" : "BLOCKED";

  let facetOk = 0, mapOk = 0, proxyDead = 0;
  for (const r of results) {
    if (r.facet?.status === 200) facetOk++;
    if (r.map?.status === 200) mapOk++;
    if (r.ip === "?") proxyDead++;
    const f = r.facet || {}, m = r.map || {};
    console.log(
      `[${verdict(r).padEnd(10)}] sess=${String(r.session).padEnd(9)} ip=${String(r.ip).padEnd(16)} ` +
      `facet=${String(f.status).padEnd(3)}(${f.ct || f.err || ""}) map=${String(m.status).padEnd(3)}(${m.ct || m.err || ""})`
    );
    if (f.status && f.status !== 200 && f.snippet) console.log(`             facet body: ${f.snippet}`);
  }

  console.log(`\n──────── SUMMARY ────────`);
  console.log(`  proxies alive:      ${proxies.length - proxyDead}/${proxies.length}`);
  console.log(`  FACET 200 (cookie-less): ${facetOk}/${proxies.length}`);
  console.log(`  MAP   200 (cookie-less): ${mapOk}/${proxies.length}`);
  console.log(`─────────────────────────`);
  console.log(
    facetOk > 0
      ? `\nRESULT: facets ARE reachable cookie-less on ${facetOk} IP(s) — you can skip the whole browser/tmpt flow for those.\n`
      : `\nRESULT: 0 facets cookie-less — EPS needs the browser-minted tmpt cookie + real TLS.` +
        (mapOk ? ` But the MAP api returned 200 on ${mapOk} IP(s) cookie-less.` : ``) + `\n`
  );
  process.exit(0);
})().catch((e) => {
  console.error("test failed:", e);
  process.exit(1);
});
