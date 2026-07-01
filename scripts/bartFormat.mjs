// Diagnose the correct bartproxies username format: which directive actually
// rotates the exit IP, and whether geo-targeting (US/zip) is honored.
import { createRequire } from "module";
const require = createRequire(import.meta.url);
const { HttpsProxyAgent } = require("https-proxy-agent");

const GATEWAY = process.env.PROXY_GATEWAY || "resipro.bartproxies.com:7778";
const PASS = process.env.PROXY_PASS || "061t6o";

// Candidate usernames to probe. Each labeled with what it's testing.
const candidates = [
  ["base-only",            "B_52593_US_1080_32701_30"],
  ["orig-token",           "B_52593_US_1080_32701_30_ss-P9E0l8O4v5V1"],
  ["ss-dash A",            "B_52593_US_1080_32701_30_ss-AAAA1111"],
  ["ss-dash B",            "B_52593_US_1080_32701_30_ss-BBBB2222"],
  ["ss-underscore A",      "B_52593_US_1080_32701_30_ss_AAAA1111"],
  ["session-kw A",         "B_52593_US_1080_32701_30_session-AAAA1111"],
  ["sessid-kw A",          "B_52593_US_1080_32701_30_sessid-AAAA1111"],
  ["no-geo base",          "B_52593_30"],
  ["no-geo ss A",          "B_52593_30_ss-AAAA1111"],
];

async function probe(user) {
  const url = `http://${encodeURIComponent(user)}:${encodeURIComponent(PASS)}@${GATEWAY}`;
  const agent = new HttpsProxyAgent(url, { timeout: 30000 });
  const t0 = Date.now();
  try {
    const res = await fetch("http://ip-api.com/json/?fields=query,country,regionName,city,isp", {
      agent,
      signal: AbortSignal.timeout(20000),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const j = await res.json();
    return { ip: j.query, loc: `${j.city || "?"},${j.regionName || "?"},${j.country || "?"}`, isp: j.isp, ms: Date.now() - t0 };
  } catch (e) {
    return { ip: null, err: e.message, ms: Date.now() - t0 };
  }
}

(async () => {
  console.log(`\nProbing username formats on ${GATEWAY} (sequential)\n`);
  for (const [label, user] of candidates) {
    const r = await probe(user);
    if (r.ip) console.log(`  ${label.padEnd(18)} → ${r.ip.padEnd(16)} ${String(r.loc).padEnd(28)} ${r.isp} (${r.ms}ms)`);
    else console.log(`  ${label.padEnd(18)} → ERR ${r.err} (${r.ms}ms)`);
  }
  console.log("");
  process.exit(0);
})();
