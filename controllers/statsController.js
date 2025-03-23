import { Event, ErrorLog } from "../models/index.js";
import scraperManager from "../scraperManager.js";

// Initialize scraper manager

export const getStats = async (req, res) => {
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
};
