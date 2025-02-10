
import express from "express";
import mongoose from "mongoose";
import { ScrapingManager } from "./scraperManager.js";
import seatService from "./services/seatService.js";
import morgan from "morgan";
import dotenv from "dotenv";
import cors from "cors";

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;
const corsOptions = {
  origin: process.env.ALLOWED_ORIGINS
    ? process.env.ALLOWED_ORIGINS.split(",")
    : ["http://localhost:3000", "http://localhost:5173"], // Default origins
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
  credentials: true,
  maxAge: 86400, // 24 hours
};
const mongoUri =
  process.env.DATABASE_URL || "mongodb://localhost:27017/ticketScraper";


mongoose
  .connect(mongoUri)
  .then(() => console.log("Connected to MongoDB"))
  .catch((err) => console.error("MongoDB connection error:", err));

const scrapingManager = new ScrapingManager();

app.use(cors(corsOptions));
app.use(express.json());
app.use(morgan("dev"));

// CORS preflight handler
app.options("*", cors(corsOptions));

// Routes
app.post("/scrape", async (req, res) => {
  try {
    const { url } = req.body;
    if (!url) {
      return res.status(400).json({
        error: "Missing URL",
        status: "error",
      });
    }

    const result = await scrapingManager.startScraping(url);
    res.json(result);
  } catch (error) {
    res.status(400).json({
      error: error.message,
      status: "error",
    });
  }
});

app.delete("/scrape", async (req, res) => {
  try {
    const { url } = req.body;
    if (!url) {
      return res.status(400).json({
        error: "Missing URL",
        status: "error",
      });
    }
    const result = await scrapingManager.stopScraping(url);
    res.json(result);
  } catch (error) {
    res.status(404).json({
      error: error.message,
      status: "error",
    });
  }
});

app.get("/status", async (req, res) => {
  try {
    const { url } = req.query;
    const status = await scrapingManager.getStatus(url);
    res.json(status);
  } catch (error) {
    res.status(404).json({
      error: error.message,
      status: "error",
    });
  }
});

app.get("/seats/:eventId", async (req, res) => {
  try {
    const { eventId } = req.params;
    const data = await seatService.getEventData(eventId);
    res.json(data);
  } catch (error) {
    res.status(404).json({
      error: error.message,
      status: "error",
    });
  }
});

app.get("/events", async (req, res) => {
  try {
    const events = await mongoose
      .model("Event")
      .find()
      .sort({ lastUpdated: -1 });
    res.json(events);
  } catch (error) {
    res.status(500).json({
      error: error.message,
      status: "error",
    });
  }
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({
    error: "Something broke!",
    status: "error",
    message: err.message,
  });
});

// Start server
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});

export default app;