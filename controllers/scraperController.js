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

    scraperManager.stopContinuousScraping();

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

// Auto-restart monitor endpoints
export const getAutoRestartStatus = (req, res) => {
  try {
    const status = scraperManager.getAutoRestartStatus();
    res.json({
      status: "success",
      data: status
    });
  } catch (error) {
    res.status(500).json({
      status: "error",
      message: error.message,
    });
  }
};

export const enableAutoRestart = (req, res) => {
  try {
    const config = req.body.config || {};
    scraperManager.enableAutoRestart(config);
    
    res.json({
      status: "success",
      message: "Auto-restart monitoring enabled",
      data: scraperManager.getAutoRestartStatus()
    });
  } catch (error) {
    res.status(500).json({
      status: "error",
      message: error.message,
    });
  }
};

export const disableAutoRestart = (req, res) => {
  try {
    scraperManager.disableAutoRestart();
    
    res.json({
      status: "success",
      message: "Auto-restart monitoring disabled",
      data: scraperManager.getAutoRestartStatus()
    });
  } catch (error) {
    res.status(500).json({
      status: "error",
      message: error.message,
    });
  }
};

export const updateAutoRestartConfig = (req, res) => {
  try {
    const config = req.body.config;
    
    if (!config) {
      return res.status(400).json({
        status: "error",
        message: "Configuration object is required",
      });
    }
    
    const success = scraperManager.updateAutoRestartConfig(config);
    
    if (success) {
      res.json({
        status: "success",
        message: "Auto-restart configuration updated",
        data: scraperManager.getAutoRestartStatus()
      });
    } else {
      res.status(500).json({
        status: "error",
        message: "Failed to update auto-restart configuration",
      });
    }
  } catch (error) {
    res.status(500).json({
      status: "error",
      message: error.message,
    });
  }
};
