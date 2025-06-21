// app.js
import express from "express";
import mongoose from "mongoose";
import morgan from "morgan";
import cors from "cors";
import dotenv from "dotenv";

// Route imports
import scraperRoutes from "./routes/scraperRoutes.js";
import eventRoutes from "./routes/eventRoutes.js";
import statsRoutes from "./routes/statsRoutes.js";
import healthRoutes from "./routes/healthRoutes.js";
import cookieRefreshRoutes from "./routes/cookieRefreshRoutes.js";
import adminRoutes from "./routes/adminRoutes.js";

// Import global setup
import setupGlobals from "./setup.js";
import scraperManager from "./scraperManager.js"; // Added for command-line start and graceful shutdown

dotenv.config();

// Initialize global components (including ProxyManager)
setupGlobals();

const app = express();
const initialPort = parseInt(process.env.PORT, 10) || 3000; // Renamed and parsed
let serverInstance; // To hold the server instance for graceful shutdown
const mongoUri =
  process.env.DATABASE_URL || "mongodb://localhost:27017/ticketScraper";

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


// Database connection
mongoose
  .connect(mongoUri)
  .then(() => console.log("Connected to MongoDB"))
  .catch((err) => console.error("MongoDB connection error:", err));

// Routes
app.use("/api/health", healthRoutes);
app.use("/api/scraper", scraperRoutes);
app.use("/api/events", eventRoutes);
app.use("/api/stats", statsRoutes);
app.use("/api/cookies", cookieRefreshRoutes);
app.use("/api/admin", adminRoutes);

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

// Start server
startServerWithPortFallback(initialPort);

// Graceful shutdown
process.on("SIGTERM", async () => {
  console.log("SIGTERM received. Starting graceful shutdown...");

  // Stop the scraper
  if (scraperManager && typeof scraperManager.stop === 'function') {
    try {
      console.log("Attempting to stop scraper gracefully...");
      await scraperManager.stop();
      console.log("Scraper stopped successfully.");
    } catch (error) {
      console.error("Error during scraper stop:", error);
    }
  } else {
    console.log("Scraper manager not found or stop method is unavailable. Skipping scraper stop.");
  }

  const closeDbAndExit = (serverError) => {
    console.log("Closing MongoDB connection...");
    mongoose.connection.close(false, (dbErr) => {
      if (dbErr) {
        console.error("Error closing MongoDB connection:", dbErr);
      } else {
        console.log("MongoDB connection closed successfully.");
      }
      process.exit(serverError || dbErr ? 1 : 0);
    });
  };

  if (serverInstance && serverInstance.listening) {
    console.log("Closing HTTP server...");
    serverInstance.close((serverCloseErr) => {
      if (serverCloseErr) {
        console.error("Error closing HTTP server:", serverCloseErr);
      } else {
        console.log("HTTP server closed successfully.");
      }
      closeDbAndExit(serverCloseErr);
    });
  } else {
    console.log("HTTP server was not started or already closed. Proceeding to close database.");
    closeDbAndExit(null);
  }
});

export default app;
