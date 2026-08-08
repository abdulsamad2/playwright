// Does the per-exit-IP token bucket actually hold the line?
//
// The measured knee is ~30 facets/min per exit IP, with a ~75s recovery after a block
// (scripts/jarRateRamp.mjs). Before this, nothing paced an IP at all: the batcher fired a
// whole batch — up to 12 requests — down one IP simultaneously, which is roughly 700/min
// instantaneous, and the 403s that came back were then misread as dead jars and dead
// proxies. reserveIpSlots() is what keeps that from happening, so it needs a test.
//
// Pure in-process logic: no network, no browser, no DB. Runs in a few seconds by using a
// fast rate (60/min = 1/s) and a short cooldown.
//
// Usage: node scripts/testIpPacing.mjs
process.env.IP_RATE_PER_MIN = "60";   // 1 request/sec, so the test finishes quickly
process.env.IP_COOLDOWN_MS = "1500";

import "dotenv/config";
import { browserPagePool as pool } from "../browser-cookies.js";

let pass = 0, fail = 0;
const check = (name, ok, detail = "") => {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${name}${detail ? ` — ${detail}` : ""}`);
  ok ? pass++ : fail++;
};
const near = (actual, expected, tol) => Math.abs(actual - expected) <= tol;

const metaA = { proxy: { proxy: "203.0.113.10:9000" } };
const metaB = { proxy: { proxy: "203.0.113.11:9000" } };

// A full bucket absorbs one burst without waiting — that is the point of a bucket, and it
// keeps normal batches fast.
let t = Date.now();
await pool.reserveIpSlots(metaA, 60);
const burstMs = Date.now() - t;
check("a full bucket absorbs one burst immediately", burstMs < 300, `${burstMs}ms`);

// Once drained, further requests are paced at the configured rate rather than fired.
t = Date.now();
await pool.reserveIpSlots(metaA, 2);
const pacedMs = Date.now() - t;
check("a drained bucket paces the next requests", near(pacedMs, 2000, 900), `${pacedMs}ms for 2 slots at 60/min`);

// Pacing is PER IP: a second exit IP must not be slowed by the first one's spending.
t = Date.now();
await pool.reserveIpSlots(metaB, 30);
const otherMs = Date.now() - t;
check("a different exit IP has its own budget", otherMs < 300, `${otherMs}ms`);

// A 403 rests that IP, and the rest is observed before anything else goes out.
pool.coolProxy(metaB, 4);
t = Date.now();
await pool.reserveIpSlots(metaB, 1);
const cooledMs = Date.now() - t;
check("a 403 rests that IP for the cooldown", cooledMs >= 1400, `waited ${cooledMs}ms for a 1500ms cooldown`);

// ...and resting one IP must not stall a healthy one.
t = Date.now();
await pool.reserveIpSlots(metaA, 1);
const unaffectedMs = Date.now() - t;
check("resting one IP does not stall another", unaffectedMs < 1200, `${unaffectedMs}ms`);

// The escape hatch has to actually disable pacing, or a bad night can't be rescued.
process.env.IP_RATE_PER_MIN = "0";
t = Date.now();
await pool.reserveIpSlots(metaA, 5000);
const offMs = Date.now() - t;
check("IP_RATE_PER_MIN=0 disables pacing", offMs < 100, `${offMs}ms`);

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
