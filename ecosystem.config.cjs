// PM2 config for the scraper fleet.
//   pm2 start ecosystem.config.cjs
//   pm2 logs scraper        # watch
//   pm2 restart scraper     # after a code change / .env change
//   pm2 delete scraper
//
// Fixes the two common "works with npm run but not PM2" gotchas:
//   1. args:'--start-scraper' passes the flag to the app (PM2 would otherwise eat it).
//   2. cwd:__dirname makes dotenv.config() find THIS folder's .env.
//
// Runs 10 instances on THIS machine (×10 machines = your 100). Fork mode = 10
// independent workers (not Node cluster). Each instance stays lean (POOL_SIZE=2 etc.
// from .env). Set FLEET_INSTANCES to change the per-machine count.
module.exports = {
  apps: [
    {
      name: "scraper",
      script: "app.js",
      args: "--start-scraper",
      cwd: __dirname,
      instances: parseInt(process.env.FLEET_INSTANCES, 10) || 10,
      exec_mode: "fork",
      autorestart: true,
      // recycle a leaky/hung instance instead of letting it drag; each Camoufox
      // context sits ~150-280MB and POOL_SIZE=6 runs several per instance, so 750M
      // leaves headroom (raised from 500M when POOL_SIZE went 4→6 for ~480 events).
      max_memory_restart: "750M",
      // don't hammer-restart a crash-looping instance (e.g. bad Redis/Mongo config)
      min_uptime: "20s",
      max_restarts: 10,
      restart_delay: 3000,
      kill_timeout: 15000, // give graceful shutdown time to close browsers
      merge_logs: true,
      time: true,
      env: {
        NODE_ENV: "production",
        // Split + farm mode MUST be on for the fleet: every instance reads a ready
        // tmpt jar from the shared `seed_jars` collection and injects it, instead of
        // self-seeding on a datacenter proxy (which EPS 403s). Baked in here so a
        // missed `.env` edit or PM2's cached-env trap can't silently drop them —
        // that's what makes instances fall back to self-seed and fail wholesale.
        SEED_SPLIT: "1",
        SEED_FARM: "1",
        SEED_FARM_FALLBACK: "0", // never stampede bart in-process when the farm runs dry
        // Self-minting stays OFF. Tested with the farm stubbed dry
        // (scripts/testSelfMint.mjs): Camoufox mints no tmpt at all, and real Chrome
        // mints one that facets still 403s — and the page binds anyway, so the pool
        // looks healthy while every request fails. Stalling at 0/N is the better
        // failure: it's visible and it recovers the moment the farm has a jar.
        SELF_MINT: "0",
      },
    },
  ],
};
