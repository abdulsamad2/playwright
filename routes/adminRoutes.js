import express from 'express';
import { restartServer } from '../controllers/adminController.js';

const router = express.Router();

// POST /admin/restart
router.post('/restart', restartServer);

// IMPORTANT: Secure this route in a real application!
// Example: router.post('/restart', ensureAdminAuthenticated, restartServer);

export default router;
