// Quick check: open N sticky sessions on the bartproxies gateway and confirm
// each `_ss-<token>` pins a DISTINCT exit IP (proves multi-session works before
// the heavier EPS bench). No browser — raw HTTPS through the proxy to ipify.
//
// Usage: node scripts/bartSessions.mjs --n=8
import { createRequire } from "module";
const require = createRequire(import.meta.url);
const { HttpsProxyAgent } = require("https-proxy-agent");

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  })
);

const GATEWAY = process.env.PROXY_GATEWAY || "resipro.bartproxies.com:7778";
const USER_BASE = process.env.PROXY_USER || "B_52593_US_1080_32701_30";
const PASS = process.env.PROXY_PASS || "061t6o";
const N = parseInt(args.n, 10) || 8;

async function oneSession(i) {
  const token = `s${i}${Math.random().toString(36).slice(2, 8)}`;
  const user = `${USER_BASE}_ss-${token}`;
  const url = `http://${encodeURIComponent(user)}:${encodeURIComponent(PASS)}@${GATEWAY}`;
  const agent = new HttpsProxyAgent(url, { timeout: 30000 });
  const t0 = Date.now();
  try {
    const res = await fetch("https://api.ipify.org?format=json", {
      agent,
      signal: AbortSignal.timeout(20000),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const { ip } = await res.json();
    return { slot: i, token, ip, ms: Date.now() - t0 };
  } catch (e) {
    return { slot: i, token, ip: null, ms: Date.now() - t0, err: e.message };
  }
}

(async () => {
  console.log(`\nOpening ${N} sticky sessions on ${GATEWAY}`);
  console.log(`user pattern: ${USER_BASE}_ss-<token>\n`);
  const results = await Promise.all(Array.from({ length: N }, (_, i) => oneSession(i)));
  for (const r of results) {
    if (r.ip) console.log(`  ✓ slot ${r.slot}  ss-${r.token.padEnd(10)} → ${r.ip.padEnd(16)} (${r.ms}ms)`);
    else console.log(`  ✗ slot ${r.slot}  ss-${r.token.padEnd(10)} → ${r.err} (${r.ms}ms)`);
  }
  const ips = results.filter((r) => r.ip).map((r) => r.ip);
  const unique = new Set(ips);
  console.log(`\n${ips.length}/${N} sessions connected — ${unique.size} distinct exit IPs`);
  if (ips.length && unique.size === ips.length) console.log("✓ each session pinned its OWN IP — multi-session works.");
  else if (ips.length) console.log("⚠ some sessions shared an IP — gateway may be reusing exits under load.");
  process.exit(0);
})();
