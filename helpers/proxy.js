import { Proxy } from "../models/proxyModel.js";

// Live array — mutated in place so existing consumers (`proxyArray.proxies`)
// always see the latest list without re-importing.
const proxies = [];

const CLIENT_ID = process.env.CLIENT_ID || "default";
const REFRESH_INTERVAL_MS = parseInt(process.env.PROXY_REFRESH_INTERVAL_MS, 10) || 5 * 60 * 1000;

let refreshTimer = null;

export async function loadProxies() {
  const docs = await Proxy.find({ clientId: CLIENT_ID, enabled: true }).lean();
  const next = docs.map((d) => ({
    proxy: `${d.ip}:${d.port}`,
    username: d.username,
    password: d.password,
  }));
  proxies.splice(0, proxies.length, ...next);
  console.log(`[PROXY] Loaded ${proxies.length} proxies from MongoDB (clientId=${CLIENT_ID})`);
  return proxies;
}

export function startProxyRefresh() {
  if (refreshTimer) return;
  refreshTimer = setInterval(() => {
    loadProxies().catch((err) => console.error("[PROXY] Refresh failed:", err.message));
  }, REFRESH_INTERVAL_MS);
  refreshTimer.unref?.();
}

export function stopProxyRefresh() {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
}

export default { proxies };
