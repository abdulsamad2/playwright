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

export default router;
