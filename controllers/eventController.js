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
    const { eventId } = req.params;
    
    // Get event data with mapping_id
    const event = await Event.findOne({ Event_ID: eventId })
      .select('Event_Name Venue Event_DateTime mapping_id inHandDate')
      .lean();
      
    if (!event) {
      return res.status(404).json({ success: false, message: 'Event not found' });
    }
    
    // Validate mapping_id
    if (!event.mapping_id) {
      return res.status(400).json({ success: false, message: 'No mapping_id found for this event' });
    }
    
    const mapping_id = event.mapping_id;
    
    // Get all groups for this event
    const groups = await ConsecutiveGroup.find({ event_id: eventId })
      .select('section row seats inventory')
      .lean();
      
    if (!groups || groups.length === 0) {
      return res.status(404).json({ success: false, message: 'No groups found for this event' });
    }
    
    // Import the inventory controller
    const inventoryController = (await import('./inventoryController.js')).default;
    
    // Delete old CSV files before generating new ones
    inventoryController.deleteOldCsvFiles(eventId);
    
    // Create a Set to track unique combinations
    const uniqueKeys = new Set();
    const uniqueGroups = [];
    
    // Filter out duplicates based on section, row, and seats
    groups.forEach(group => {
      const uniqueKey = `${group.section}-${group.row}-${group.seats.map(s => s.number).join(",")}`;
      if (!uniqueKeys.has(uniqueKey)) {
        uniqueKeys.add(uniqueKey);
        uniqueGroups.push(group);
      }
    });
    
    // Format the unique groups as inventory records
    const inventoryRecords = uniqueGroups.map(group => ({
      inventory_id: group._id.toString(),
      event_name: event.Event_Name || `Event ${eventId}`,
      venue_name: event.Venue || "Unknown Venue",
      event_date: event.Event_DateTime?.toISOString() || new Date().toISOString(),
      event_id: mapping_id || '',
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
      stock_type: group.inventory.stockType || "custom",
      zone: "N",
      shown_quantity: String(Math.ceil(group.inventory.quantity/2)),
      passthrough: "",
      mapping_id: mapping_id || ""
    }));
    
    // Add the records in bulk
    const result = await inventoryController.addBulkInventory(inventoryRecords, eventId);
    
    if (!result.success) {
      throw new Error(result.message);
    }
    
    // Send the file as a download
    const filePath = path.join(process.cwd(), 'data', `event_${eventId}.csv`);
    res.download(filePath, `event_${eventId}.csv`, (err) => {
      if (err) {
        res.status(500).json({ success: false, message: 'Error sending file' });
      }
    });
  } catch (error) {
    console.error(`Error downloading event CSV: ${error.message}`);
    res.status(500).json({ success: false, message: error.message });
  }
};
