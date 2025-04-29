import express from 'express';
import {
  getCookieRefreshStats,
  getRecentRefreshes,
  triggerRefresh,
  getRefreshDetails
} from '../controllers/cookieRefreshController.js';

const router = express.Router();

// Get cookie refresh statistics
router.get('/stats', getCookieRefreshStats);

// Get list of recent cookie refresh operations
router.get('/history', getRecentRefreshes);

// Trigger a manual cookie refresh
router.post('/trigger', triggerRefresh);

// Get details of a specific cookie refresh
router.get('/:refreshId', getRefreshDetails);

export default router; 