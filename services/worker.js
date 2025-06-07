import mongoose from "mongoose";
import amqp from "amqplib";
import { setTimeout } from "timers/promises";
import { ScrapeEvent } from "../scraper.js";
import { Event } from "../models/index.js";
import ProxyManager from "../helpers/ProxyManager.js";
import SessionManager from "../helpers/SessionManager.js";
import { v4 as uuidv4 } from 'uuid';
import Redis from "ioredis";

// Configuration
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://localhost:5672";
const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";
const QUEUE_NAME = "event_queue";
const WORKER_ID = uuidv4();
const PREFETCH_COUNT = 5; // Number of messages to prefetch

// Initialize components
const proxyManager = new ProxyManager();
const sessionManager = new SessionManager();
const redis = new Redis(REDIS_URL);

// Connect to MongoDB
const mongoUri = process.env.DATABASE_URL || "mongodb://localhost:27017/ticketScraper";
mongoose
  .connect(mongoUri)
  .then(() => console.log("Worker connected to MongoDB"))
  .catch((err) => console.error("Worker MongoDB connection error:", err));

// Connect to RabbitMQ and start consuming messages
async function start() {
  try {
    // Connect to RabbitMQ
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();
    
    // Create queue with durable option
    await channel.assertQueue(QUEUE_NAME, { durable: true });
    
    // Set prefetch count
    channel.prefetch(PREFETCH_COUNT);
    
    console.log(`Worker ${WORKER_ID} connected to RabbitMQ, waiting for messages...`);
    
    // Consume messages
    channel.consume(QUEUE_NAME, async (msg) => {
      if (msg) {
        try {
          const event = JSON.parse(msg.content.toString());
          console.log(`Worker ${WORKER_ID} processing event: ${event.eventId}`);
          
          // Try to acquire a distributed lock
          const lockKey = `lock:event:${event.eventId}`;
          const lockAcquired = await redis.set(lockKey, WORKER_ID, 'NX', 'EX', 60); // 60 second lock
          
          if (!lockAcquired) {
            // Another worker is processing this event, requeue
            channel.nack(msg, false, true);
            return;
          }
          
          // Process the event
          await processEvent(event);
          
          // Acknowledge the message
          channel.ack(msg);
          
          // Release the lock
          await redis.del(lockKey);
        } catch (error) {
          console.error(`Error processing message: ${error.message}`);
          
          // Negative acknowledge and requeue
          channel.nack(msg, false, true);
        }
      }
    });
    
    // Handle connection close
    connection.on("close", () => {
      console.error("RabbitMQ connection closed, attempting to reconnect...");
      setTimeout(5000).then(start); // Retry after 5 seconds
    });
  } catch (error) {
    console.error("Error connecting to RabbitMQ:", error);
    setTimeout(5000).then(start); // Retry after 5 seconds
  }
}

// Process a single event
async function processEvent(event) {
  const { eventId } = event;
  let retryCount = 0;
  let proxyAgent = null;
  let proxy = null;
  
  try {
    // Get a proxy for this event
    try {
      const proxyData = proxyManager.getProxyForEvent(eventId);
      if (proxyData) {
        const proxyAgentData = proxyManager.createProxyAgent(proxyData);
        proxyAgent = proxyAgentData.proxyAgent;
        proxy = proxyAgentData.proxy;
        proxyManager.assignProxyToEvent(eventId, proxy.proxy);
      }
    } catch (proxyError) {
      console.warn(`Error getting proxy for ${eventId}: ${proxyError.message}`);
    }
    
    // Get headers for this event
    const headers = await refreshEventHeaders(eventId);
    
    if (!headers) {
      throw new Error("Failed to obtain headers");
    }
    
    // Add unique identifiers to headers
    if (headers.headers) {
      headers.headers["X-Event-ID"] = eventId;
      headers.headers["X-Session-ID"] = `session-${eventId}-${Date.now()}`;
      headers.headers["X-Worker-ID"] = WORKER_ID;
    }
    
    // Create event object with session data
    const eventWithSession = {
      eventId: eventId,
      headers: headers,
      sessionId: `worker-${WORKER_ID}-${eventId}-${Date.now()}`,
      proxyId: proxy?.proxy || 'default',
      retryCount: retryCount
    };
    
    // Scrape the event
    const result = await ScrapeEvent(eventWithSession, proxyAgent, proxy);
    
    if (!result || !Array.isArray(result) || result.length === 0) {
      throw new Error("Invalid scrape result");
    }
    
    // Update event in database
    await updateEventInDatabase(eventId, result);
    
    console.log(`Worker ${WORKER_ID} successfully processed event ${eventId}`);
    
    // Release proxy
    if (proxy) {
      proxyManager.releaseProxy(eventId, true);
    }
    
    return true;
  } catch (error) {
    console.error(`Worker ${WORKER_ID} error processing event ${eventId}: ${error.message}`);
    
    // Release proxy on error
    if (proxy) {
      proxyManager.releaseProxy(eventId, false, error);
    }
    
    throw error; // Rethrow to trigger nack
  }
}

// Helper function to refresh headers
async function refreshEventHeaders(eventId) {
  try {
    // Implementation based on your existing refreshEventHeaders function
    // This is a simplified version
    const headers = {
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
      }
    };
    
    return headers;
  } catch (error) {
    console.error(`Error refreshing headers for ${eventId}: ${error.message}`);
    return null;
  }
}

// Helper function to update event in database
async function updateEventInDatabase(eventId, result) {
  try {
    const currentTicketCount = result.length;
    
    // Update basic info
    await Event.updateOne(
      { Event_ID: eventId },
      {
        $set: {
          Available_Seats: currentTicketCount,
          Last_Updated: new Date(),
          "metadata.lastUpdate": new Date(),
          "metadata.ticketCount": currentTicketCount
        },
      }
    );
    
    // Additional processing as needed
    // This would include your existing updateEventMetadata logic
    
    return true;
  } catch (error) {
    console.error(`Error updating event ${eventId} in database: ${error.message}`);
    throw error;
  }
}

// Start the worker
start();