import { Event, ConsecutiveGroup, ErrorLog } from "../models/index.js";
import scraperManager from "../scraperManager.js";

// Initialize scraper manager

export const getAllEvents = async (req, res) => {
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
};

export const getEventById = async (req, res) => {
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
};

export const createEvent = async (req, res) => {
  try {
    const {
      Event_ID,
      Event_Name,
      Event_DateTime,
      Venue,
      URL,
      inHandDate,
      Available_Seats = 0,
      Skip_Scraping = true,
      Zone,
    } = req.body;

    if (!Event_Name || !inHandDate || !Event_DateTime || !URL) {
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
      inHandDate,
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
    console.log(error);
    res.status(500).json({
      status: "error",
      message: error.message,
    });
  }
};

export const startEventScraping = async (req, res) => {
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
};

export const stopEventScraping = async (req, res) => {
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
};


export const deleteEvent = async (req, res) => {
  try {
    const { eventId } = req.params;

    // Find and delete the event
    const event = await Event.findOneAndDelete({ Event_ID: eventId });

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
      message: `Event ${eventId} deleted successfully`,
    });
  } catch (error) {
    res.status(500).json({
      status: "error",
      message: error.message,
    });
  }
};
