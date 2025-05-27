import express from "express";
import { getStats } from "../controllers/statsController.js";
import { forceCleanupTracking } from "../scraperManager.js";

const router = express.Router();

router.get("/", getStats);

// Force cleanup of tracking accumulation
router.post("/cleanup", async (req, res) => {
  try {
    const result = await forceCleanupTracking();
    res.json({
      success: true,
      message: `Cleaned up ${result.cleaned} stale tracked events`,
      details: result
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: "Cleanup failed",
      error: error.message
    });
  }
});

export default router;
