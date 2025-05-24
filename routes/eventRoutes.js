import express from "express";
import {
  getAllEvents,
  getEventById,
  createEvent,
  startEventScraping,
  stopEventScraping,
  deleteEvent,
  downloadEventCsv,
  startEventBroadcast,
  stopEventBroadcast,
} from "../controllers/eventController.js";

const router = express.Router();

router.get("/", getAllEvents);
router.get("/:eventId", getEventById);
router.post("/", createEvent);
router.post("/:eventId/start", startEventScraping);
router.post("/:eventId/stop", stopEventScraping);
router.delete("/:eventId", deleteEvent);

// Broadcast routes
router.post("/:eventId/broadcast/start", startEventBroadcast);
router.post("/:eventId/broadcast/stop", stopEventBroadcast);

// Add CSV download route
router.get("/:eventId/inventory/csv", downloadEventCsv);

export default router;
