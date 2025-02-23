import express from "express";
import mongoose from "mongoose";
import morgan from "morgan";
import cors from "cors";
import dotenv from "dotenv";
import ScraperManager from "./scraperManager.js";
import { Event, ConsecutiveGroup, ErrorLog } from "./models/index.js";

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;
const mongoUri =
  process.env.DATABASE_URL || "mongodb://localhost:27017/ticketScraper";

// Initialize scraper manager
const scraperManager = new ScraperManager();

// Middleware
app.use(cors());
app.use(express.json());
app.use(morgan("dev"));

// Database connection
mongoose
  .connect(mongoUri)
  .then(() => console.log("Connected to MongoDB"))
  .catch((err) => console.error("MongoDB connection error:", err));

// Health check endpoint
app.get("/health", (req, res) => {
  res.json({
    status: "healthy",
    scraperRunning: scraperManager.isRunning,
    mongoConnection: mongoose.connection.readyState === 1,
  });
});

// Start scraping
app.post("/api/scraper/start", async (req, res) => {
  try {
    if (scraperManager.isRunning) {
      return res.status(400).json({
        status: "error",
        message: "Scraper is already running",
      });
    }

    scraperManager.startContinuousScraping().catch((error) => {
      console.error("Scraper error:", error);
      scraperManager.isRunning = false;
    });

    res.json({
      status: "success",
      message: "Scraper started successfully",
    });
  } catch (error) {
    res.status(500).json({
      status: "error",
      message: error.message,
    });
  }
});

// Stop scraping
app.post("/api/scraper/stop", (req, res) => {
  try {
    if (!scraperManager.isRunning) {
      return res.status(400).json({
        status: "error",
        message: "Scraper is not running",
      });
    }

    scraperManager.stop();

    res.json({
      status: "success",
      message: "Scraper stopped successfully",
    });
  } catch (error) {
    res.status(500).json({
      status: "error",
      message: error.message,
    });
  }
});

// Get scraper status
app.get("/api/scraper/status", (req, res) => {
  res.json({
    isRunning: scraperManager.isRunning,
    activeJobs: Array.from(scraperManager.activeJobs.keys()),
    successCount: scraperManager.successCount,
    failedEvents: Array.from(scraperManager.failedEvents),
    retryQueueSize: scraperManager.retryQueue.length,
  });
});

// Add new event to scrape
app.post("/api/events", async (req, res) => {
  try {
    const {
      Event_ID,
      Event_Name,
      Event_DateTime,
      Venue,
      URL,
      Available_Seats,
      Skip_Scraping = true,
      Zone,
    } = req.body;

    if (!Event_Name || !Event_DateTime || !URL) {
      return res.status(400).json({
        status: "error",
        message: "Missing required fields",
      });
    }

    const existingEvent = await Event.findOne({ URL });
    if (existingEvent) {
      return res.status(400).json({
        status: "error",
        message: "Event with this URL already exists",
      });
    }

    const event = new Event({
      Event_ID,
      Event_Name,
      Event_DateTime,
      Venue,
      URL,
      Zone: Zone || "none",
      Available_Seats: Available_Seats || 0,
      Skip_Scraping,
      metadata: {
        iterationNumber: 0,
      },
    });

    await event.save();

    res.status(201).json({
      status: "success",
      data: event,
    });
  } catch (error) {
    res.status(500).json({
      status: "error",
      message: error.message,
    });
  }
});

// Start scraping for a single event
app.post("/api/events/:eventId/start", async (req, res) => {
  try {
    const { eventId } = req.params;

    // Find and update the event
    const event = await Event.findOneAndUpdate(
      { Event_ID: eventId },
      { Skip_Scraping: false },
      { new: true }
    );

    if (!event) {
      return res.status(404).json({
        status: "error",
        message: "Event not found",
      });
    }

    // Check if the event is already being scraped
    if (scraperManager.activeJobs.has(eventId)) {
      return res.status(400).json({
        status: "error",
        message: "Scraping is already running for this event",
      });
    }

    // Start scraping for this specific event
    await scraperManager.scrapeEvent(eventId);

    res.json({
      status: "success",
      message: `Scraping started for event ${eventId}`,
      data: event,
    });
  } catch (error) {
    res.status(500).json({
      status: "error",
      message: error.message,
    });
  }
});

// Stop scraping for a single event
app.post("/api/events/:eventId/stop", async (req, res) => {
  try {
    const { eventId } = req.params;

    // Update the event to skip scraping
    const event = await Event.findOneAndUpdate(
      { Event_ID: eventId },
      { Skip_Scraping: true },
      { new: true }
    );

    if (!event) {
      return res.status(404).json({
        status: "error",
        message: "Event not found",
      });
    }

    // Remove from active jobs if it's currently being scraped
    if (scraperManager.activeJobs.has(eventId)) {
      scraperManager.activeJobs.delete(eventId);
    }

    // Remove from retry queue if present
    scraperManager.retryQueue = scraperManager.retryQueue.filter(
      (item) => item.eventId !== eventId
    );

    res.json({
      status: "success",
      message: `Scraping stopped for event ${eventId}`,
      data: event,
    });
  } catch (error) {
    res.status(500).json({
      status: "error",
      message: error.message,
    });
  }
});

// Get all events
app.get("/api/events", async (req, res) => {
  try {
    const events = await Event.find().sort({ Last_Updated: -1 });

    // Add active status to each event
    const eventsWithStatus = events.map((event) => ({
      ...event.toObject(),
      isActive: scraperManager.activeJobs.has(event.Event_ID),
    }));

    res.json({
      status: "success",
      data: eventsWithStatus,
    });
  } catch (error) {
    res.status(500).json({
      status: "error",
      message: error.message,
    });
  }
});

// Get single event details
app.get("/api/events/:eventId", async (req, res) => {
  try {
    const event = await Event.findOne({ Event_ID: req.params.eventId });
    if (!event) {
      return res.status(404).json({
        status: "error",
        message: "Event not found",
      });
    }

    const seatGroups = await ConsecutiveGroup.find({
      eventId: req.params.eventId,
    });

    const isActive = scraperManager.activeJobs.has(req.params.eventId);

    res.json({
      status: "success",
      data: {
        ...event.toObject(),
        isActive,
        seatGroups,
      },
    });
  } catch (error) {
    console.error("Error:", error);
    res.status(500).json({
      status: "error",
      message: error.message,
    });
  }
});

// Get scraping statistics
app.get("/api/stats", async (req, res) => {
  try {
    const totalEvents = await Event.countDocuments();
    const activeEvents = await Event.countDocuments({ Skip_Scraping: false });
    const totalErrors = await ErrorLog.countDocuments();
    const recentErrors = await ErrorLog.find().sort({ createdAt: -1 }).limit(5);

    const eventsWithChanges = await Event.find({
      "metadata.ticketStats.ticketCountChange": { $ne: 0 },
    })
      .sort({ Last_Updated: -1 })
      .limit(5);

    res.json({
      status: "success",
      data: {
        totalEvents,
        activeEvents,
        totalErrors,
        recentErrors,
        eventsWithChanges,
        scraperStatus: {
          isRunning: scraperManager.isRunning,
          successCount: scraperManager.successCount,
          failedCount: scraperManager.failedEvents.size,
          activeJobs: Array.from(scraperManager.activeJobs.keys()),
          retryQueueSize: scraperManager.retryQueue.length,
        },
      },
    });
  } catch (error) {
    res.status(500).json({
      status: "error",
      message: error.message,
    });
  }
});

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
const server = app.listen(port, () => {
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
