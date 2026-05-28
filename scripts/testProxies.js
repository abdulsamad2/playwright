// Verify proxies load from MongoDB and test connectivity through a sample.
// Usage: node scripts/testProxies.js [--all] [--sample=N]

import dotenv from "dotenv";
import { createRequire } from "module";
import connectDB, { closeConnections } from "../config/db.js";
import { loadProxies } from "../helpers/proxy.js";
import proxyArray from "../helpers/proxy.js";

dotenv.config();

const require = createRequire(import.meta.url);
const { HttpsProxyAgent } = require("https-proxy-agent");

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  })
);

const sampleSize = args.all ? Infinity : parseInt(args.sample, 10) || 10;

async function testProxy(p, timeoutMs = 10000) {
  const url = `http://${encodeURIComponent(p.username)}:${encodeURIComponent(p.password)}@${p.proxy}`;
  const agent = new HttpsProxyAgent(url);
  const start = Date.now();
  try {
    const res = await fetch("https://api.ipify.org?format=json", {
      agent,
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const body = await res.json();
    return { proxy: p.proxy, ok: true, ms: Date.now() - start, exitIp: body.ip };
  } catch (e) {
    return { proxy: p.proxy, ok: false, ms: Date.now() - start, err: e.message };
  }
}

(async () => {
  await connectDB();
  await loadProxies();
  console.log(`\n✓ loadProxies() returned ${proxyArray.proxies.length} proxies`);
  if (proxyArray.proxies.length === 0) {
    console.error("No proxies loaded — check CLIENT_ID env var and the proxies collection.");
    await closeConnections();
    process.exit(1);
  }

  const toTest = proxyArray.proxies.slice(0, sampleSize === Infinity ? undefined : sampleSize);
  console.log(`Testing ${toTest.length} proxies against https://api.ipify.org (parallel, 10s timeout each)...\n`);

  const t0 = Date.now();
  const results = await Promise.all(toTest.map((p) => testProxy(p)));
  const totalMs = Date.now() - t0;

  for (const r of results) {
    if (r.ok) console.log(`  ✓ ${r.proxy.padEnd(24)} → ${String(r.exitIp).padEnd(16)} (${r.ms}ms)`);
    else console.log(`  ✗ ${r.proxy.padEnd(24)} → ${r.err} (${r.ms}ms)`);
  }

  const ok = results.filter((r) => r.ok).length;
  const fail = results.length - ok;
  console.log(`\nSummary: ${ok}/${results.length} working (${((ok / results.length) * 100).toFixed(0)}%) in ${totalMs}ms`);
  if (fail > 0) {
    const errCounts = {};
    for (const r of results.filter((r) => !r.ok)) {
      const key = (r.err || "unknown").split(":")[0].slice(0, 60);
      errCounts[key] = (errCounts[key] || 0) + 1;
    }
    console.log("Failure breakdown:", errCounts);
  }

  await closeConnections();
  process.exit(0);
})().catch(async (err) => {
  console.error("Test failed:", err);
  try { await closeConnections(); } catch {}
  process.exit(1);
});
