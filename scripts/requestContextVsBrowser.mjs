// CAN THE READ PATH DROP THE BROWSER ENTIRELY?
//
// Reads currently run through Camoufox pages: ~150-280MB each, POOL_SIZE=6 per instance,
// 10 instances per machine — roughly 9-16GB of browser just to issue HTTP GETs. The repo
// already has REQUEST_CONTEXT_MODE=1 (playwright's bare request context, no browser), but
// it is off, and the open question is whether TM accepts its TLS/JA3 fingerprint the way
// it accepts Camoufox's. If it does, the entire browser fleet on the read side is dead
// weight and worker count stops being bounded by RAM.
//
// Same jar, same exit IP, alternating transports so neither can be blamed for the other's
// conditions. Only ONE exit IP is used, because a tmpt session is revoked after ~10
// distinct IPs (jarIpBudget.mjs) and a two-transport comparison must not spend that budget.
//
// Read-only. Usage: node scripts/requestContextVsBrowser.mjs [rounds] [proxy|direct]
import "dotenv/config";
import mongoose from "mongoose";
import { request as playwrightRequest } from "patchright";
import { Camoufox } from "camoufox-js";
import proxyArray, { loadProxies } from "../helpers/proxy.js";

const ROUNDS = parseInt(process.argv[2], 10) || 6;
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

let proxy = null;
if (WHERE !== "direct") {
  const pool = (proxyArray.proxies || []).filter((p) => p && p.proxy);
  proxy = WHERE === "auto" ? pool[Math.floor(Math.random() * pool.length)] : pool.find((p) => p.proxy === WHERE);
}
console.log(`jar slot=${doc.slot} age=${((Date.now() - new Date(doc.mintedAt)) / 60000).toFixed(1)}min`);
console.log(`single egress: ${proxy ? proxy.proxy : "DIRECT"}   ${ROUNDS} rounds, alternating transports\n`);

const cookieHeader = doc.cookies
  .filter((c) => String(c.domain || "").includes("ticketmaster"))
  .map((c) => `${c.name}=${c.value}`).join("; ");

// --- transport A: bare request context (no browser)
const rcOpts = {
  ignoreHTTPSErrors: true,
  extraHTTPHeaders: { accept: "application/json", "x-api-key": "b462oi7fic6pehcdkzony5bxhe", cookie: cookieHeader },
};
if (proxy) {
  const [host, port] = proxy.proxy.split(":");
  rcOpts.proxy = { server: `http://${host}:${port}`, username: proxy.username, password: proxy.password };
}
const rc = await playwrightRequest.newContext(rcOpts);

// --- transport B: Camoufox page context (what production uses today)
const browser = await Camoufox({ headless: true, humanize: false, geoip: false });
const ctxOpts = { ignoreHTTPSErrors: true, bypassCSP: true };
if (proxy) {
  const [host, port] = proxy.proxy.split(":");
  ctxOpts.proxy = { server: `http://${host}:${port}`, username: proxy.username, password: proxy.password };
}
const bctx = await browser.newContext(ctxOpts);
await bctx.addCookies(doc.cookies.map((c) => ({
  name: c.name, value: c.value, domain: c.domain || ".ticketmaster.com", path: c.path || "/",
  expires: c.expires || -1, httpOnly: !!c.httpOnly, secure: c.secure !== false, sameSite: c.sameSite || "Lax",
})));

async function go(kind, i) {
  const url = facetsUrl(EVENTS[i % EVENTS.length]);
  const headers = { "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe", Accept: "application/json", "tmps-correlation-id": `rcb-${Date.now()}` };
  const t = Date.now();
  try {
    const r = kind === "requestctx"
      ? await rc.get(url, { headers, timeout: 25000 })
      : await bctx.request.get(url, { headers, timeout: 25000 });
    const s = r.status();
    const detail = s === 200 ? `${(await r.json().catch(() => ({})))?.facets?.length ?? 0} facets` : (await r.text().catch(() => "")).slice(0, 40).replace(/\s+/g, " ");
    return { s, ms: Date.now() - t, detail };
  } catch (e) { return { s: "ERR", ms: Date.now() - t, detail: e.message.slice(0, 40) }; }
}

const tally = { requestctx: { ok: 0, n: 0, ms: 0 }, camoufox: { ok: 0, n: 0, ms: 0 } };
console.log("round  transport    status  ms     detail");
for (let i = 0; i < ROUNDS; i++) {
  for (const kind of ["requestctx", "camoufox"]) {
    const { s, ms, detail } = await go(kind, i);
    tally[kind].n++; tally[kind].ms += ms;
    if (s === 200) tally[kind].ok++;
    console.log(`${String(i + 1).padStart(5)}  ${kind.padEnd(12)} ${String(s).padEnd(7)} ${String(ms).padEnd(6)} ${detail}`);
    await new Promise((r) => setTimeout(r, 3000)); // stay well under the ~30/min per-IP knee
  }
}

console.log("\n--- RESULT ---");
for (const k of ["requestctx", "camoufox"]) {
  const t = tally[k];
  console.log(`  ${k.padEnd(12)} ${t.ok}/${t.n} passed, mean ${(t.ms / t.n).toFixed(0)}ms`);
}
const rok = tally.requestctx.ok / tally.requestctx.n, cok = tally.camoufox.ok / tally.camoufox.n;
console.log(
  rok >= 0.8 && cok >= 0.8 ? "\n  -> TM accepts the bare request context as readily as Camoufox. The read path does not\n     need a browser at all: drop ~200MB/worker and stop sizing the fleet by RAM."
  : rok < 0.5 && cok >= 0.8 ? "\n  -> Only the browser transport is accepted. Camoufox stays on the read path; savings must\n     come from fewer workers, not a lighter transport."
  : "\n  -> Neither transport is clean right now (jar or IP problem) — re-run before concluding."
);

await rc.dispose().catch(() => {});
await browser.close().catch(() => {});
await mongoose.disconnect().catch(() => {});
process.exit(0);
