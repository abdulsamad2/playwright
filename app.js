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

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;
const mongoUri =
  process.env.DATABASE_URL || "mongodb://localhost:27017/ticketScraper";

// Middleware
app.use(
  cors({
    origin: "http://18.234.29.129", 
    methods: "GET,POST,PUT,DELETE",
    allowedHeaders: "Content-Type,Authorization",
  })
);app.use(express.json());
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
app.listen(port, "0.0.0.0", () => {
  console.log(`Server running on port ${port}`);
});

// Graceful shutdown
process.on("SIGTERM", async () => {
  console.log("SIGTERM received. Starting graceful shutdown...");

  // Stop the scraper
  if (scraperManager.isRunning) {
    scraperManager.stop();
  }

  // Close server
  server.close(() => {
    console.log("HTTP server closed");

    // Close database connection
    mongoose.connection.close(false, () => {
      console.log("MongoDB connection closed");
      process.exit(0);
    });
  });
});

export default app;
