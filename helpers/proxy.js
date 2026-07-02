import { Proxy } from "../models/proxyModel.js";

// Proxies come ONLY from MongoDB (the `proxies` collection, filtered by clientId
// + enabled). No hardcoded list, no IPRoyal mode, no env overrides — the database
// is the single source of truth. app.js calls loadProxies() at startup and
// startProxyRefresh() keeps the list in sync.

const CLIENT_ID = process.env.CLIENT_ID || "default";
const REFRESH_INTERVAL_MS =
  parseInt(process.env.PROXY_REFRESH_INTERVAL_MS, 10) || 5 * 60 * 1000;
let refreshTimer = null;

// Live array — mutated IN PLACE so existing consumers (`proxyArray.proxies`)
// always see the latest list. Starts empty; loadProxies() fills it from Mongo.
const proxies = [];

// Map one MongoDB proxy row → one pool entry. `id` (host:port) is the unique key
// the page pool uses to tell proxies apart.
function toPoolEntry(d) {
  const host = `${d.ip}:${d.port}`;
  return { id: host, proxy: host, username: d.username, password: d.password };
}

// Load enabled proxies for this client from MongoDB and REPLACE the live list.
// On empty/error the existing list is left untouched (so a transient DB blip
// doesn't wipe a working pool).
export async function loadProxies() {
  try {
    const docs = await Proxy.find({ clientId: CLIENT_ID, enabled: true }).lean();
    if (!docs.length) {
      console.warn(
        `[PROXY] MongoDB returned 0 enabled proxies (clientId=${CLIENT_ID}); keeping current ${proxies.length}`
      );
      return proxies;
    }
    proxies.splice(0, proxies.length, ...docs.map(toPoolEntry));
    console.log(
      `[PROXY] Loaded ${proxies.length} proxies from MongoDB (clientId=${CLIENT_ID})`
    );
    return proxies;
  } catch (err) {
    console.error(
      `[PROXY] loadProxies failed (${err.message}); keeping current ${proxies.length}`
    );
    return proxies;
  }
}

export function startProxyRefresh() {
  if (refreshTimer) return;
  refreshTimer = setInterval(() => {
    loadProxies().catch((err) =>
      console.error("[PROXY] Refresh failed:", err.message)
    );
  }, REFRESH_INTERVAL_MS);
  refreshTimer.unref?.();
}

export function stopProxyRefresh() {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
}

export default {
  proxies: proxies,
};
