import express from "express";
import {
  getScraperStatus,
  startScraper,
  stopScraper,
  getAutoRestartStatus,
  enableAutoRestart,
  disableAutoRestart,
  updateAutoRestartConfig,
} from "../controllers/scraperController.js";

const router = express.Router();

router.get("/status", getScraperStatus);
router.post("/start", startScraper);
router.post("/stop", stopScraper);

// Auto-restart monitor routes
router.get("/auto-restart/status", getAutoRestartStatus);
router.post("/auto-restart/enable", enableAutoRestart);
router.post("/auto-restart/disable", disableAutoRestart);
router.put("/auto-restart/config", updateAutoRestartConfig);

// Add this route to the existing routes:

// Force complete session reset
router.post('/force-session-reset', async (req, res) => {
  try {
    const success = await scraperManager.forceCompleteSessionReset();
    
    if (success) {
      res.json({
        success: true,
        message: 'Complete session reset completed successfully',
        timestamp: new Date().toISOString()
      });
    } else {
      res.status(500).json({
        success: false,
        message: 'Session reset failed',
        timestamp: new Date().toISOString()
      });
    }
  } catch (error) {
    res.status(500).json({
      success: false,
      message: `Session reset error: ${error.message}`,
      timestamp: new Date().toISOString()
    });
  }
});

// Get session reset status
router.get('/session-reset-status', async (req, res) => {
  try {
    const status = scraperManager.getSessionResetStatus();
    res.json({
      success: true,
      status,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: `Error getting session reset status: ${error.message}`,
      timestamp: new Date().toISOString()
    });
  }
});

export default router;
