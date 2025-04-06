import moment from "moment";
import { Event } from "../models/index.js";
import config from "../config/scraperConfig.js";

/**
 * Manages event scheduling and prioritization
 */
class EventScheduler {
  constructor(logger) {
    this.logger = logger;
    this.eventUpdateSchedule = new Map(); // Tracks when each event needs to be updated next
    this.eventUpdateTimestamps = new Map(); // Tracks when events were last updated
    this.priorityQueue = new Set(); // High priority events
  }

  /**
   * Get the eventUpdateTimestamps map
   */
  getUpdateTimestamps() {
    return this.eventUpdateTimestamps;
  }

  /**
   * Set the next update time for an event
   */
  scheduleNextUpdate(eventId) {
    this.eventUpdateSchedule.set(
      eventId, 
      moment().add(config.MAX_UPDATE_INTERVAL, 'milliseconds')
    );
    this.eventUpdateTimestamps.set(eventId, moment());
    return this.eventUpdateSchedule.get(eventId);
  }

  /**
   * Record an event update
   */
  recordEventUpdate(eventId) {
    this.eventUpdateTimestamps.set(eventId, moment());
    return this.scheduleNextUpdate(eventId);
  }

  /**
   * Mark an event as high priority
   */
  markHighPriority(eventId) {
    this.priorityQueue.add(eventId);
  }

  /**
   * Check if an event has missed its deadline
   */
  hasEventMissedDeadline(eventId) {
    if (!this.eventUpdateSchedule.has(eventId)) {
      return false;
    }
    
    const deadline = this.eventUpdateSchedule.get(eventId);
    return moment().isAfter(deadline);
  }

  /**
   * Get high priority events that need to be processed ASAP
   */
  getHighPriorityEvents() {
    return [...this.priorityQueue];
  }

  /**
   * Clear an event from high priority queue
   */
  clearHighPriority(eventId) {
    this.priorityQueue.delete(eventId);
  }

  /**
   * Fetch and prioritize events that need to be processed
   */
  async getEventsToProcess(processingEvents) {
    const now = moment();
    
    // First, identify events approaching their 2-minute deadline
    const urgentEvents = [];
    const nearDeadlineEvents = [];
    const regularEvents = [];
    
    // Get all events that need updating
    const allEvents = await Event.find({
      Skip_Scraping: { $ne: true },
      Event_ID: { $nin: [...processingEvents] },
    })
      .sort({ Last_Updated: 1 })
      .select("Event_ID Last_Updated")
      .lean();
    
    // Process based on deadline proximity
    for (const event of allEvents) {
      // If we don't have a scheduled update time, set one based on last update
      if (!this.eventUpdateSchedule.has(event.Event_ID)) {
        const lastUpdate = moment(event.Last_Updated);
        this.eventUpdateSchedule.set(
          event.Event_ID, 
          lastUpdate.add(config.MAX_UPDATE_INTERVAL, 'milliseconds')
        );
      }
      
      const deadline = this.eventUpdateSchedule.get(event.Event_ID);
      const timeToDeadline = deadline.diff(now);
      
      // Past deadline or within 10 seconds of deadline
      if (timeToDeadline <= 0) {
        urgentEvents.push(event.Event_ID);
      } 
      // Within threshold of deadline
      else if (timeToDeadline <= config.URGENT_THRESHOLD) {
        nearDeadlineEvents.push(event.Event_ID);
      }
      // All other events
      else {
        regularEvents.push(event.Event_ID);
      }
    }
    
    // Add high priority events to urgent list if they're not already there
    const highPriorityEvents = this.getHighPriorityEvents()
      .filter(id => !urgentEvents.includes(id) && !processingEvents.has(id));
    
    urgentEvents.push(...highPriorityEvents);
    
    // Log urgency metrics
    if (urgentEvents.length > 0) {
      this.logger.logWithTime(`URGENT: ${urgentEvents.length} events past deadline`, "warning");
    }
    
    // Prioritize urgent events first, then near deadline, then some regular events
    const urgentBatchSize = Math.min(urgentEvents.length, config.BATCH_SIZE);
    const remainingCapacity = config.BATCH_SIZE - urgentBatchSize;
    
    let result = urgentEvents.slice(0, urgentBatchSize);
    
    if (remainingCapacity > 0) {
      const nearDeadlineBatchSize = Math.min(
        nearDeadlineEvents.length, 
        Math.floor(remainingCapacity * 0.7)
      );
      result = [...result, ...nearDeadlineEvents.slice(0, nearDeadlineBatchSize)];
      
      const regularBatchSize = remainingCapacity - nearDeadlineBatchSize;
      if (regularBatchSize > 0) {
        result = [...result, ...regularEvents.slice(0, regularBatchSize)];
      }
    }
    
    // Clear processed events from priority queue
    for (const eventId of result) {
      this.clearHighPriority(eventId);
    }
    
    return result;
  }

  /**
   * Check for events that missed their deadlines and add them to priority queue
   */
  checkForMissedDeadlines(processingEvents) {
    const now = moment();
    let missedDeadlines = 0;
    
    for (const [eventId, deadline] of this.eventUpdateSchedule.entries()) {
      if (now.isAfter(deadline) && !processingEvents.has(eventId)) {
        missedDeadlines++;
        // Force immediate processing of missed events
        this.markHighPriority(eventId);
      }
    }
    
    return missedDeadlines;
  }
}

export default EventScheduler; 