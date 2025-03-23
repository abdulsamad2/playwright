import express from "express";
import {
  getScraperStatus,
  startScraper,
  stopScraper,
} from "../controllers/scraperController.js";

const router = express.Router();

router.get("/status", getScraperStatus);
router.post("/start", startScraper);
router.post("/stop", stopScraper);

export default router;
