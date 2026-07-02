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
      // instance sits ~150-280MB, so 500M leaves headroom.
      max_memory_restart: "500M",
      // don't hammer-restart a crash-looping instance (e.g. bad Redis/Mongo config)
      min_uptime: "20s",
      max_restarts: 10,
      restart_delay: 3000,
      kill_timeout: 15000, // give graceful shutdown time to close browsers
      merge_logs: true,
      time: true,
      env: {
        NODE_ENV: "production",
      },
    },
  ],
};
