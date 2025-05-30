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
import inventoryRoutes from "./routes/inventoryRoutes.js";
import cookieRefreshRoutes from "./routes/cookieRefreshRoutes.js";

// Import global setup
import setupGlobals from "./setup.js";
import scraperManager from "./scraperManager.js"; // Added for command-line start and graceful shutdown

dotenv.config();

// Initialize global components (including ProxyManager)
setupGlobals();

const app = express();
const port = process.env.PORT || 3000;
const mongoUri =
  process.env.DATABASE_URL || "mongodb://localhost:27017/ticketScraper";

// Middleware
const allowedOrigins = [
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
app.use("/api/inventory", inventoryRoutes);
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
const server = app.listen(port, "0.0.0.0", () => {
  console.log(`Server running on port ${port}`);

  // Check for --start-scraper argument to start scraper automatically
  if (process.argv.includes('--start-scraper')) {
    console.log('Command-line argument --start-scraper detected. Attempting to start scraper...');
    if (scraperManager && scraperManager.isRunning) {
      console.log("Scraper is already running.");
    } else if (scraperManager && typeof scraperManager.startContinuousScraping === 'function') {
      scraperManager.startContinuousScraping().catch((error) => {
        console.error("Error starting scraper from command line:", error);
        if (scraperManager) {
          scraperManager.isRunning = false; // Ensure state is updated on error
        }
      });
      console.log("Scraper initiated via command line.");
    } else {
      console.error("Scraper manager or startContinuousScraping method is not available.");
    }
  }
});

// Graceful shutdown
process.on("SIGTERM", async () => {
  console.log("SIGTERM received. Starting graceful shutdown...");

  // Stop the scraper
  if (scraperManager && typeof scraperManager.stop === 'function') {
    try {
      console.log("Attempting to stop scraper gracefully...");
      await scraperManager.stop(); // Assuming stop() is async and returns a Promise
      console.log("Scraper stopped successfully.");
    } catch (error) {
      console.error("Error during scraper stop:", error);
    }
  } else {
    console.log("Scraper manager not found or stop method is unavailable. Skipping scraper stop.");
  }

  // Close server
  console.log("Closing HTTP server...");
  server.close((err) => {
    if (err) {
      console.error("Error closing HTTP server:", err);
    } else {
      console.log("HTTP server closed successfully.");
    }

    // Close database connection
    console.log("Closing MongoDB connection...");
    mongoose.connection.close(false, (dbErr) => {
      if (dbErr) {
        console.error("Error closing MongoDB connection:", dbErr);
      } else {
        console.log("MongoDB connection closed successfully.");
      }
      process.exit(err || dbErr ? 1 : 0); // Exit with 1 if any error occurred
    });
  });
});

export default app;
