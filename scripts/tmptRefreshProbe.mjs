// DOES TM PUSH THE tmpt EXPIRY FORWARD ON A SUCCESSFUL CALL?
//
// jarBurnTest.mjs watches whether the tmpt VALUE changes and has seen 0 rotations in 400+
// calls. But a session can be extended without the value changing — Set-Cookie can resend
// the same token with a later `expires`. That distinction decides how long a jar is good
// for:
//
//   expiry pushed forward -> a jar in continuous use never ages out; 60min is an IDLE
//                            timeout and the farm only has to cover COLD starts.
//   expiry fixed at mint  -> every jar is dead 60min after minting no matter what, and
//                            the farm must sustain (target rate / per-jar rate) mints/hr.
//
// Takes one jar, records tmpt's expiry, makes a few spaced facets calls, and re-reads the
// expiry after each. Read-only, ~6 calls total.
//
// Usage: node scripts/tmptRefreshProbe.mjs [calls] [proxy|direct]
import "dotenv/config";
import mongoose from "mongoose";
import { Camoufox } from "camoufox-js";
import proxyArray, { loadProxies } from "../helpers/proxy.js";

const CALLS = parseInt(process.argv[2], 10) || 6;
const WHERE = process.argv[3] || "auto";

const EVENTS = ["15006331B08D75C0", "0A006331DC273765", "0600632E29196B3E", "0800632CA3272367"];
const facetsUrl = (id) =>
  `https://services.ticketmaster.com/api/ismds/event/${id}/facets` +
  `?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description` +
  `&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places` +
  `&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us` +
  `&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse` +
  `&_=${Date.now()}&t=${Math.random().toString(36).slice(2)}`;

await mongoose.connect(process.env.MONGODB_URI);
await loadProxies();
const [doc] = await mongoose.connection.db.collection("seed_jars")
  .find({ status: "healthy", expiresAt: { $gt: new Date() } }).sort({ mintedAt: -1 }).limit(1).toArray();
if (!doc) { console.error("no healthy jar"); process.exit(2); }

const mintedAt = new Date(doc.mintedAt || doc.createdAt);
console.log(`jar slot=${doc.slot} useCount=${doc.useCount ?? 0} minted ${((Date.now() - mintedAt) / 60000).toFixed(1)}min ago`);

let proxy = null;
if (WHERE !== "direct") {
  const pool = (proxyArray.proxies || []).filter((p) => p && p.proxy);
  proxy = WHERE === "auto" ? pool[Math.floor(Math.random() * pool.length)] : pool.find((p) => p.proxy === WHERE);
}
console.log(`egress: ${proxy ? proxy.proxy : "DIRECT"}\n`);

const browser = await Camoufox({ headless: true, humanize: false, geoip: false });
const ctxOpts = { ignoreHTTPSErrors: true, bypassCSP: true };
if (proxy) {
  const [host, port] = proxy.proxy.split(":");
  ctxOpts.proxy = { server: `http://${host}:${port}`, username: proxy.username, password: proxy.password };
}
const ctx = await browser.newContext(ctxOpts);
await ctx.addCookies(doc.cookies.map((c) => ({
  name: c.name, value: c.value, domain: c.domain || ".ticketmaster.com", path: c.path || "/",
  expires: c.expires || -1, httpOnly: !!c.httpOnly, secure: c.secure !== false, sameSite: c.sameSite || "Lax",
})));

const tmptOf = async () => (await ctx.cookies()).find((c) => c.name === "tmpt");
const fmt = (t) => t?.expires ? `${((t.expires * 1000 - Date.now()) / 60000).toFixed(2)}min left` : "no expiry";

let prev = await tmptOf();
console.log(`before any call : ${fmt(prev)}  value ${prev?.value?.slice(0, 18)}…`);
const baselineExpiry = prev?.expires;

for (let i = 0; i < CALLS; i++) {
  const r = await ctx.request.get(facetsUrl(EVENTS[i % EVENTS.length]), {
    headers: { "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe", Accept: "application/json", "tmps-correlation-id": `rf-${Date.now()}` },
    timeout: 25000,
  }).catch((e) => ({ status: () => `ERR ${e.message.slice(0, 30)}` }));
  const now = await tmptOf();
  const moved = now?.expires && baselineExpiry ? (now.expires - baselineExpiry) : 0;
  console.log(
    `call ${String(i + 1).padStart(2)} -> ${String(r.status())}  ${fmt(now)}  ` +
    `expiry moved ${moved >= 0 ? "+" : ""}${(moved / 60).toFixed(2)}min  ` +
    `value ${now?.value === prev?.value ? "same" : "CHANGED"}`
  );
  prev = now;
  await new Promise((res) => setTimeout(res, 5000));
}

const final = await tmptOf();
const drift = final?.expires && baselineExpiry ? (final.expires - baselineExpiry) / 60 : 0;
console.log(
  Math.abs(drift) < 0.05
    ? `\n-> tmpt expiry did NOT move (${drift.toFixed(2)}min). The 60-minute clock starts at MINT and calls do not extend it.`
    : `\n-> tmpt expiry moved ${drift.toFixed(2)}min across ${CALLS} calls — the session IS being extended in use.`
);

await browser.close().catch(() => {});
await mongoose.disconnect().catch(() => {});
process.exit(0);
