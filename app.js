
import express from "express";
import mongoose from "mongoose";
import { ScrapingManager } from "./scraperManager.js";
import morgan from "morgan";
import dotenv from "dotenv";
import cors from "cors";
import fs from "fs";

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

// Get list of events with basic info and last update times
// Updated API endpoint
app.get("/api/events/summary", async (req, res) => {
  try {
    // Define the Event model properly
    const Event = mongoose.model("Event");

    const events = await Event.find(
      {}, // Query criteria
      { 
        title: 1,
        dateTime: 1,
        availableSeats: 1,
        lastUpdated: 1,
        'metadata.lastUpdate': 1,
        'metadata.ticketStats': 1,
        _id: 1 // Explicitly include _id
      }
    )
    .lean() // Convert to plain JavaScript objects (better performance)
    .sort({ lastUpdated: -1 });

    // Check if events were found
    if (!events) {
      return res.status(404).json({
        status: "error",
        message: "No events found"
      });
    }

    // Send response
    return res.status(200).json({
      status: "success",
      data: events
    });

  } catch (error) {
    console.error('Error fetching events:', error);
    return res.status(500).json({
      status: "error",
      message: "Internal server error",
      error: error.message
    });
  }
});

// Get single event with full details including all seat groups
app.get("/api/events/:eventId/details", async (req, res) => {
  try {
    const { eventId } = req.params;
    
    const event = await mongoose.model("Event").findById(eventId);
    if (!event) {
      return res.status(404).json({
        error: "Event not found",
        status: "error"
      });
    }

    const seatGroups = await mongoose.model("ConsecutiveGroup")
      .find({ eventId: event._id })
      .sort({ section: 1, row: 1 });

    const eventDetails = {
      ...event.toObject(),
      seatGroups
    };

    res.json(eventDetails);
  } catch (error) {
    res.status(500).json({
      error: error.message,
      status: "error"
    });
  }
});

// Get events by date range
app.get("/api/events/range", async (req, res) => {
  try {
    const { startDate, endDate } = req.query;
    const query = {};
    
    if (startDate || endDate) {
      query.dateTime = {};
      if (startDate) query.dateTime.$gte = new Date(startDate);
      if (endDate) query.dateTime.$lte = new Date(endDate);
    }

    const events = await mongoose.model("Event")
      .find(query)
      .sort({ dateTime: 1 });
    
    res.json(events);
  } catch (error) {
    res.status(500).json({
      error: error.message,
      status: "error"
    });
  }
});

// Get event stats
app.get("/api/events/stats", async (req, res) => {
  try {
    const totalEvents = await mongoose.model("Event").countDocuments();
    const totalSeats = await mongoose.model("Event").aggregate([
      {
        $group: {
          _id: null,
          totalSeats: { $sum: "$availableSeats" }
        }
      }
    ]);

    const latestUpdates = await mongoose.model("Event")
      .find({}, {
        title: 1,
        lastUpdated: 1,
        'metadata.ticketStats': 1
      })
      .sort({ lastUpdated: -1 })
      .limit(5);

    res.json({
      totalEvents,
      totalAvailableSeats: totalSeats[0]?.totalSeats || 0,
      latestUpdates
    });
  } catch (error) {
    res.status(500).json({
      error: error.message,
      status: "error"
    });
  }
});

// Get seat availability history for an event
app.get("/api/events/:eventId/history", async (req, res) => {
  try {
    const { eventId } = req.params;
    const event = await mongoose.model("Event").findById(eventId);
    
    if (!event) {
      return res.status(404).json({
        error: "Event not found",
        status: "error"
      });
    }

    const seatGroups = await mongoose.model("ConsecutiveGroup")
      .find({ 
        eventId: event._id 
      })
      .sort({ createdAt: -1 });

    // Group seat data by timestamp
    const history = seatGroups.reduce((acc, group) => {
      const timestamp = group.createdAt;
      if (!acc[timestamp]) {
        acc[timestamp] = {
          timestamp,
          totalSeats: 0,
          sections: {}
        };
      }
      
      if (!acc[timestamp].sections[group.section]) {
        acc[timestamp].sections[group.section] = 0;
      }
      
      acc[timestamp].sections[group.section] += group.seatCount;
      acc[timestamp].totalSeats += group.seatCount;
      
      return acc;
    }, {});

    res.json(Object.values(history));
  } catch (error) {
    res.status(500).json({
      error: error.message,
      status: "error"
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