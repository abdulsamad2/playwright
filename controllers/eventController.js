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
    const event = await Event.findOne({ 
      $or: [
        { Event_ID: req.params.eventId },
        { mapping_id: req.params.eventId }
      ]
    });
    if (!event) {
      return res.status(404).json({
        status: "error",
        message: "Event not found",
      });
    }

    const seatGroups = await ConsecutiveGroup.find({
      eventId: event.Event_ID,
    });

    const isActive = scraperManager.activeJobs.has(event.Event_ID);

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
      mapping_id,
    } = req.body;

    // Use mapping_id directly
    const finalMappingId = mapping_id;

    if (!Event_Name || !inHandDate || !Event_DateTime || !URL || !finalMappingId) {
      return res.status(400).json({
        status: "error",
        message: "Missing required fields",
      });
    }

    // Check for existing event with same URL or mapping_id
    const existingEvent = await Event.findOne({
      $or: [
        { URL },
        { mapping_id: finalMappingId }
      ]
    });
    
    if (existingEvent) {
      return res.status(400).json({
        status: "error",
        message: existingEvent.URL === URL 
          ? "Event with this URL already exists"
          : "Event with this mapping_id already exists",
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
      mapping_id: finalMappingId,
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

    // Find the event by either Event_ID or mapping_id
    const event = await Event.findOne({
      $or: [
        { Event_ID: eventId },
        { mapping_id: eventId }
      ]
    });

    if (!event) {
      return res.status(404).json({
        status: "error",
        message: "Event not found",
      });
    }

    // Update the event to skip scraping
    const updatedEvent = await Event.findOneAndUpdate(
      { _id: event._id },
      { Skip_Scraping: true },
      { new: true }
    );

    // Remove from active jobs if it's currently being scraped
    if (scraperManager.activeJobs.has(event.Event_ID)) {
      scraperManager.activeJobs.delete(event.Event_ID);
      scraperManager.logWithTime(`Stopped scraping for event ${event.Event_ID}`, "info");
    }

    // Remove from retry queue if present
    scraperManager.retryQueue = scraperManager.retryQueue.filter(
      (item) => item.eventId !== event.Event_ID
    );

    // Clear any failure counts or cooldowns
    scraperManager.eventFailureCounts.delete(event.Event_ID);
    scraperManager.eventFailureTimes.delete(event.Event_ID);
    scraperManager.cooldownEvents.delete(event.Event_ID);

    res.json({
      status: "success",
      message: `Scraping stopped for event ${event.Event_ID}`,
      data: updatedEvent,
    });
  } catch (error) {
    console.error("Error stopping event scraping:", error);
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
    
    // Create data directory if it doesn't exist
    try {
      await fs.promises.mkdir(dataDir, { recursive: true });
      console.log('Data directory created/verified');
    } catch (err) {
      console.error(`Error creating data directory: ${err.message}`);
      return res.status(500).json({
        status: "error",
        message: "Failed to create data directory"
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
    
    // If no CSV files found, try to generate one from the database
    if (eventFiles.length === 0) {
      try {
        // Import the inventory controller
        const inventoryController = (await import('../controllers/inventoryController.js')).default;
        
        // Get the event data
        const event = await Event.findOne({ Event_ID: eventId })
          .select('Event_Name Venue Event_DateTime priceIncreasePercentage inHandDate')
          .lean();
          
        if (!event) {
          return res.status(404).json({
            status: "error",
            message: "Event not found"
          });
        }
        
        // Get the consecutive groups for this event
        const groups = await ConsecutiveGroup.find({ eventId })
          .select('section row seats inventory')
          .lean();
          
        if (!groups || groups.length === 0) {
          return res.status(404).json({
            status: "error",
            message: "No inventory data found for this event"
          });
        }
        
        // Format the groups as inventory records
        const inventoryRecords = groups.map(group => ({
          inventory_id: group._id.toString(),
          event_name: event.Event_Name || `Event ${eventId}`,
          venue_name: event.Venue || "Unknown Venue",
          event_date: event.Event_DateTime?.toISOString() || new Date().toISOString(),
          event_id: group.mapping_id || '',
          quantity: group.inventory.quantity,
          section: group.section,
          row: group.row,
          seats: group.seats.map(s => s.number).join(","),
          barcodes: "",
          internal_notes: group.inventory.notes || `These are internal notes. @sec[${group.section}]`,
          public_notes: group.inventory.publicNotes || `These are ${group.row} Row`,
          tags: group.inventory.tags || "",
          list_price: group.inventory.listPrice,
          face_price: group.inventory.tickets?.[0]?.faceValue || "",
          taxed_cost: group.inventory.tickets?.[0]?.taxedCost || "",
          cost: group.inventory.tickets?.[0]?.cost || "",
          hide_seats: "Y",
          in_hand: "N",
          in_hand_date: event.inHandDate?.toISOString() || new Date().toISOString(),
          instant_transfer: "N",
          files_available: "Y",
          split_type: group.inventory.splitType || "NEVERLEAVEONE",
          custom_split: group.inventory.customSplit || `${Math.ceil(group.inventory.quantity/2)},${group.inventory.quantity}`,
          stock_type: group.inventory.stockType || "NEVERLEAVEONE",
          zone: "N",
          shown_quantity: String(Math.ceil(group.inventory.quantity/2)),
          passthrough: "",
          mapping_id: group.mapping_id || ""
        }));
        
        // Generate a new CSV file
        const timestamp = new Date().toISOString().replace(/:/g, '-');
        const newFilePath = path.join(dataDir, `event_${eventId}_${timestamp}.csv`);
        
        // Add the records in bulk
        const result = await inventoryController.addBulkInventory(inventoryRecords, eventId);
        
        if (!result.success) {
          throw new Error(result.message);
        }
        
        // Use the newly generated file
        eventFiles.push(`event_${eventId}_${timestamp}.csv`);
      } catch (err) {
        console.error(`Error generating CSV: ${err.message}`);
        return res.status(500).json({
          status: "error",
          message: "Failed to generate CSV from database data"
        });
      }
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
      
      // Stream the file with error handling
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
      
      // Handle client disconnect
      req.on('close', () => {
        fileStream.destroy();
      });
      
      // Pipe the file to the response
      fileStream.pipe(res);
    } else {
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
