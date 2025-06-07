import express from "express";
import mongoose from "mongoose";
import morgan from "morgan";
import cors from "cors";
import amqp from "amqplib";
import { Event } from "../models/index.js";

// Route imports
import scraperRoutes from "../routes/scraperRoutes.js";
import eventRoutes from "../routes/eventRoutes.js";
import statsRoutes from "../routes/statsRoutes.js";
import healthRoutes from "../routes/healthRoutes.js";
import inventoryRoutes from "../routes/inventoryRoutes.js";
import cookieRefreshRoutes from "../routes/cookieRefreshRoutes.js";
import adminRoutes from "../routes/adminRoutes.js";

// RabbitMQ connection
let channel;
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://localhost:5672";
const QUEUE_NAME = "event_queue";

// Connect to RabbitMQ
async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect(RABBITMQ_URL);
    channel = await connection.createChannel();
    
    // Create queue with durable option for persistence
    await channel.assertQueue(QUEUE_NAME, { durable: true });
    
    console.log("Connected to RabbitMQ");
    
    // Set up event scheduler
    startEventScheduler();
  } catch (error) {
    console.error("RabbitMQ connection error:", error);
    setTimeout(connectRabbitMQ, 5000); // Retry after 5 seconds
  }
}

// Schedule events for processing
async function startEventScheduler() {
  setInterval(async () => {
    try {
      // Find events that need processing
      const events = await Event.find({
        $or: [
          { Skip_Scraping: { $ne: true } },
          { Skip_Scraping: { $exists: false } },
          { Skip_Scraping: null },
          { Skip_Scraping: false }
        ]
      })
        .select("Event_ID url Last_Updated")
        .sort({ Last_Updated: 1 })
        .limit(500) // Process in batches
        .lean();

      if (events.length > 0) {
        console.log(`Scheduling ${events.length} events for processing`);
        
        // Send events to queue
        for (const event of events) {
          channel.sendToQueue(
            QUEUE_NAME,
            Buffer.from(JSON.stringify({
              eventId: event.Event_ID,
              url: event.url,
              timestamp: Date.now()
            })),
            { persistent: true } // Make message persistent
          );
          
          // Update Last_Updated to prevent immediate reprocessing
          await Event.updateOne(
            { Event_ID: event.Event_ID },
            { $set: { Last_Updated: new Date() } }
          );
        }
      }
    } catch (error) {
      console.error("Error scheduling events:", error);
    }
  }, 10000); // Check every 10 seconds
}

// Express app setup
const app = express();
const initialPort = parseInt(process.env.PORT, 10) || 3000;

// Middleware
const allowedOrigins = [
  "https://americanwebgeek.com",
  "http://3.81.42.229",
  "http://localhost:5173",
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
const mongoUri = process.env.DATABASE_URL || "mongodb://localhost:27017/ticketScraper";
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
app.use("/api/admin", adminRoutes);

// Add queue status endpoint
app.get("/api/queue/status", async (req, res) => {
  try {
    const queueInfo = await channel.assertQueue(QUEUE_NAME);
    res.json({
      queue: QUEUE_NAME,
      messageCount: queueInfo.messageCount,
      consumerCount: queueInfo.consumerCount,
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
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
function startServerWithPortFallback(currentPort, attempt = 0, maxAttempts = 20) {
  if (attempt >= maxAttempts) {
    console.error(`Failed to bind to a port after ${maxAttempts} attempts. Exiting.`);
    process.exit(1);
    return;
  }

  const server = app.listen(currentPort, "0.0.0.0", () => {
    console.log(`API server listening on port ${currentPort}`);
    // Connect to RabbitMQ after server starts
    connectRabbitMQ();
  });

  server.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
      console.warn(`Port ${currentPort} is in use. Trying port ${currentPort + 1}...`);
      server.close();
      startServerWithPortFallback(currentPort + 1, attempt + 1, maxAttempts);
    } else {
      console.error("Failed to start server:", err);
      process.exit(1);
    }
  });

  // Graceful shutdown
  process.on("SIGTERM", async () => {
    console.log("SIGTERM received. Starting graceful shutdown...");
    server.close(() => {
      console.log("HTTP server closed");
      mongoose.connection.close(false, () => {
        console.log("MongoDB connection closed");
        process.exit(0);
      });
    });
  });
}

// Start server
startServerWithPortFallback(initialPort);