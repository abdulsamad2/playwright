import scraperManager from "../scraperManager.js";

// Initialize scraper manager


export const getScraperStatus = (req, res) => {
  res.json({
    isRunning: scraperManager.isRunning,
    activeJobs: Array.from(scraperManager.activeJobs.keys()),
    successCount: scraperManager.successCount,
    failedEvents: Array.from(scraperManager.failedEvents),
    retryQueueSize: scraperManager.retryQueue.length,
  });
};

export const startScraper = async (req, res) => {
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
};

export const stopScraper = (req, res) => {
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
};
