// app.js
import "dotenv/config";
import express from "express";
import mongoose from "mongoose";
import morgan from "morgan";
import cors from "cors";

// Route imports
import scraperRoutes from "./routes/scraperRoutes.js";
import eventRoutes from "./routes/eventRoutes.js";
import statsRoutes from "./routes/statsRoutes.js";
import healthRoutes from "./routes/healthRoutes.js";
import cookieRefreshRoutes from "./routes/cookieRefreshRoutes.js";

// Import global setup
import setupGlobals from "./setup.js";
import scraperManager from "./scraperManager.js"; // Added for command-line start and graceful shutdown
import { cleanup as cleanupBrowsers, cleanupApiBrowser } from "./browser-cookies.js";

// Redis imports
import { connectRedis, closeRedis, isRedisReady } from "./config/redis.js";
import redisLiveStore from "./helpers/RedisLiveStore.js";

// Proxy loader (MongoDB-backed, replaces hardcoded list)
import { loadProxies, startProxyRefresh, stopProxyRefresh } from "./helpers/proxy.js";

const app = express();
const initialPort = parseInt(process.env.PORT, 10) || 3000; // Renamed and parsed
let serverInstance; // To hold the server instance for graceful shutdown
// MongoDB URI now handled in config/db.js

// Middleware
const allowedOrigins = [
  "https://americanwebgeek.com",
  "http://3.81.42.229", // Production", // Production
  "http://localhost:5173", // Local development
];

app.use(
  cors({
    origin: function (origin, callback) {
      if (!origin || allowedOrigins.includes(origin)) {
        callback(null, true);
      } else {
        callback(new Error("Not allowed by CORS"));
      }
    },
    methods: "GET,POST,PUT,DELETE",
    allowedHeaders: "Content-Type,Authorization",
  })
);

app.use(express.json());
app.use(morgan("dev"));


// Import database configuration
import connectDB, { closeConnections } from "./config/db.js";

// Database + Redis initialization → then start server
const SKIP_REDIS = process.env.SKIP_REDIS === "true";

(async () => {
  try {
    // 1. MongoDB first (RedisLiveStore hydration reads from MongoDB)
    await connectDB();

    // 1b. Load proxies from MongoDB BEFORE setupGlobals (ProxyManager reads them at construction)
    await loadProxies();
    startProxyRefresh();

    // 1c. Initialize global components (ProxyManager now sees the loaded list)
    setupGlobals();

    if (SKIP_REDIS) {
      console.log("[Startup] SKIP_REDIS=true — running in MongoDB-only mode (no Redis)");
    } else {
      // 2. Redis connection
      await connectRedis();
      // connectRedis() returns null (doesn't throw) on failure, so verify it's
      // actually ready — otherwise hydrate() silently skips and the event queue
      // stays empty (scraper idles at 0 events with no obvious error).
      if (!isRedisReady()) {
        throw new Error(
          "Redis did NOT connect — the scraper's event queue requires Redis. " +
          "Set REDIS_HOST / REDIS_PORT / REDIS_PASSWORD in .env (they're currently " +
          "unset → defaulting to localhost:6379), or run with SKIP_REDIS=true for " +
          "MongoDB-only mode."
        );
      }
      console.log("Redis connected successfully.");

      // 3. Hydrate Redis from MongoDB (distributed-lock-protected, runs once across all instances)
      await redisLiveStore.hydrate();
      console.log("Redis live store hydrated.");

      // 4. Start the write batcher (flushes Redis writes → MongoDB every 5s)
      redisLiveStore.startWriteBatcher();

      // 5. Instance heartbeat every 30s so other instances know we're alive
      setInterval(() => redisLiveStore.heartbeat().catch(() => {}), 30000);
      redisLiveStore.heartbeat().catch(() => {}); // immediate first beat
    }

    // 6. NOW start the HTTP server
    startServerWithPortFallback(initialPort);
  } catch (err) {
    console.error("Startup initialization failed:", err);
    process.exit(1);
  }
})();

// Routes
app.use("/api/health", healthRoutes);
app.use("/api/scraper", scraperRoutes);
app.use("/api/events", eventRoutes);
app.use("/api/stats", statsRoutes);
app.use("/api/cookies", cookieRefreshRoutes);

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({
    status: "error",
    message: "Internal server error",
    error: err.message,
  });
});

// Start server
function startServerWithPortFallback(currentPort, attempt = 0, maxAttempts = 20) {
  if (attempt >= maxAttempts) {
    console.error(`Failed to bind to a port after ${maxAttempts} attempts. Exiting.`);
    process.exit(1);
    return;
  }

  const server = app.listen(currentPort, "0.0.0.0");

  server.on('listening', () => {
    serverInstance = server; // Assign to the higher-scoped variable
    global.serverInstance = server; // Make it globally accessible for restart logic
    console.log(`Server running on port ${currentPort}`);

    // Check for --start-scraper argument
    if (process.argv.includes('--start-scraper')) {
      console.log('Command-line argument --start-scraper detected. Attempting to start scraper...');
      if (scraperManager && scraperManager.isRunning) {
        console.log("Scraper is already running.");
      } else if (scraperManager && typeof scraperManager.startContinuousScraping === 'function') {
        scraperManager.startContinuousScraping().catch((error) => {
          console.error("Error starting scraper from command line:", error);
          if (scraperManager) scraperManager.isRunning = false;
        });
        console.log("Scraper initiated via command line.");
      } else {
        console.error("Scraper manager or startContinuousScraping method is not available.");
      }
    }
  });

  server.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
      console.warn(`Port ${currentPort} is in use. Trying port ${currentPort + 1}...`);
      server.close(); // Close the server that failed to listen
      startServerWithPortFallback(currentPort + 1, attempt + 1, maxAttempts);
    } else {
      console.error("Failed to start server:", err);
      process.exit(1);
    }
  });
}

// Graceful shutdown handler - shared between SIGTERM and SIGINT
let isShuttingDown = false;

async function gracefulShutdown(signal) {
  if (isShuttingDown) {
    console.log(`${signal}: Shutdown already in progress, skipping...`);
    return;
  }
  isShuttingDown = true;
  console.log(`${signal} received. Starting graceful shutdown...`);

  // 1. Stop the scraper first (prevents new browser launches)
  if (scraperManager && typeof scraperManager.stopContinuousScraping === 'function') {
    try {
      console.log("Stopping scraper...");
      await scraperManager.stopContinuousScraping();
      console.log("Scraper stopped successfully.");
    } catch (error) {
      console.error("Error during scraper stop:", error);
    }
  }

  // 2. Close all open browser instances (main + API browsers)
  try {
    console.log("Closing all browser instances...");
    await Promise.race([
      cleanupBrowsers(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Browser cleanup timeout')), 10000)),
    ]);
    console.log("All browser instances closed successfully.");
  } catch (error) {
    console.error("Error closing browsers (force killing):", error.message);
    // Force kill any remaining chromium processes on Windows
    try {
      const { execSync } = await import('child_process');
      execSync('taskkill /F /IM chromium.exe /T 2>nul', { stdio: 'ignore' });
      execSync('taskkill /F /IM chrome.exe /T 2>nul', { stdio: 'ignore' });
      console.log("Force killed remaining browser processes.");
    } catch {
      // No processes to kill, that's fine
    }
  }

  // 2b. Stop proxy refresh timer
  stopProxyRefresh();

  // 3. Flush pending Redis writes to MongoDB & close Redis
  if (!SKIP_REDIS) {
    try {
      console.log("Flushing Redis write buffer to MongoDB...");
      await redisLiveStore.stopWriteBatcher();
      console.log("Write buffer flushed. Closing Redis...");
      await closeRedis();
      console.log("Redis closed successfully.");
    } catch (error) {
      console.error("Error during Redis shutdown:", error.message);
    }
  }

  // 4. Close HTTP server
  const closeDbAndExit = async (serverError) => {
    console.log("Closing database connections...");
    try {
      await closeConnections();
      console.log("Database connections closed successfully.");
      process.exit(serverError ? 1 : 0);
    } catch (dbErr) {
      console.error("Error closing database connections:", dbErr);
      process.exit(1);
    }
  };

  if (serverInstance && serverInstance.listening) {
    console.log("Closing HTTP server...");
    serverInstance.close(async (serverCloseErr) => {
      if (serverCloseErr) {
        console.error("Error closing HTTP server:", serverCloseErr);
      } else {
        console.log("HTTP server closed successfully.");
      }
      await closeDbAndExit(serverCloseErr);
    });
  } else {
    console.log("HTTP server was not started or already closed. Proceeding to close database.");
    await closeDbAndExit(null);
  }

  // Safety: force exit after 15 seconds if graceful shutdown hangs
  setTimeout(() => {
    console.error("Graceful shutdown timed out after 15s. Force exiting.");
    process.exit(1);
  }, 15000).unref();
}

// PM2 sends SIGINT on restart, SIGTERM on stop
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

// Safety net: clean up browsers on uncaught errors before crashing
process.on("uncaughtException", async (error) => {
  console.error("Uncaught exception:", error);
  try {
    await cleanupBrowsers();
  } catch {
    // Best effort
  }
  process.exit(1);
});

process.on("unhandledRejection", (reason) => {
  console.error("Unhandled rejection:", reason);
});

export default app;
