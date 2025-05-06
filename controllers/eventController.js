import { Event, ConsecutiveGroup, ErrorLog } from "../models/index.js";
import scraperManager from "../scraperManager.js";
import fs from 'fs';
import path from 'path';

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
      priceIncreasePercentage = 25, // Default 25% if not provided
      mapping_ID,
    } = req.body;

    if (!Event_Name || !inHandDate || !Event_DateTime || !URL || !mapping_ID) {
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
      priceIncreasePercentage,
      metadata: {
        iterationNumber: 0,
        mapping_ID,
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

export const downloadEventCsv = async (req, res) => {
  try {
    console.log('==== START DOWNLOAD EVENT CSV ====');
    const { eventId } = req.params;
    console.log(`Attempting to download CSV for event ID: ${eventId}`);
    
    // Define the data directory where CSVs are stored
    const dataDir = path.join(process.cwd(), 'data');
    console.log(`Looking for files in directory: ${dataDir}`);
    
    // Check if data directory exists
    try {
      await fs.promises.access(dataDir);
      console.log('Data directory exists');
    } catch (err) {
      console.error(`Data directory doesn't exist: ${dataDir}`);
      return res.status(404).json({
        status: "error",
        message: "Data directory not found. No inventory data available."
      });
    }
    
    // Find all CSV files for this event
    let files;
    try {
      files = await fs.promises.readdir(dataDir);
      console.log(`Found ${files.length} files in data directory`);
    } catch (err) {
      console.error(`Failed to read data directory: ${err.message}`);
      return res.status(500).json({
        status: "error",
        message: "Failed to access inventory data"
      });
    }
    
    // Check if there are any CSV files for this event
    const eventFiles = files.filter(file => 
      file.startsWith(`event_${eventId}_`) && file.endsWith('.csv')
    );
    console.log(`Found ${eventFiles.length} CSV files for event ${eventId}:`);
    eventFiles.forEach(file => console.log(` - ${file}`));
    
    // Check if we have the inventory controller to look for processed events
    let inventoryController;
    try {
      inventoryController = (await import('../controllers/inventoryController.js')).default;
      
      // Check if this event has been processed
      if (inventoryController && !eventFiles.length) {
        const isProcessed = !inventoryController.isFirstScrape(eventId);
        if (!isProcessed) {
          return res.status(404).json({
            status: "error",
            message: "This event hasn't been processed yet. No inventory CSV available."
          });
        }
      }
    } catch (err) {
      console.log('Could not load inventory controller:', err.message);
    }
    
    if (eventFiles.length > 0) {
      // Sort files by timestamp (newest first)
      eventFiles.sort().reverse();
      const latestFile = path.join(dataDir, eventFiles[0]);
      
      // Check if file exists and is readable
      try {
        await fs.promises.access(latestFile, fs.constants.R_OK);
      } catch (err) {
        return res.status(404).json({
          status: "error",
          message: "Could not access inventory CSV file"
        });
      }
      
      // Log successful access
      console.log(`Serving CSV file: ${latestFile}`);
      
      // Set headers for download
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', `attachment; filename=event_${eventId}_inventory.csv`);
      
      // Stream the file
      const fileStream = fs.createReadStream(latestFile);
      fileStream.on('error', (err) => {
        console.error(`Error streaming file: ${err.message}`);
        if (!res.headersSent) {
          res.status(500).json({
            status: "error",
            message: "Error while streaming file"
          });
        }
      });
      
      fileStream.pipe(res);
    } else {
      // If no CSV exists for this event
      return res.status(404).json({
        status: "error",
        message: "No CSV inventory found for this event"
      });
    }
  } catch (error) {
    console.error(`Error in downloadEventCsv: ${error.message}`);
    console.error(error.stack);
    res.status(500).json({
      status: "error",
      message: `Server error: ${error.message}`
    });
  }
};
