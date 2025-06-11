import express from 'express';
import { coordinationController } from '../controllers/coordinationController.js';

const router = express.Router();

// Instance registration and management
router.post('/register', coordinationController.registerInstance);
router.post('/heartbeat', coordinationController.updateHeartbeat);
router.get('/status', coordinationController.getCoordinationStatus);
router.get('/instances', coordinationController.getInstances);

// Event assignment and management
router.get('/events', coordinationController.getEventsForInstance);
router.post('/events/status', coordinationController.updateEventStatus);
router.post('/events/redistribute', coordinationController.redistributeEvents);

// Cookie and session sharing
router.get('/cookies', coordinationController.getCookies);
router.get('/sessions', coordinationController.getSessions);
router.post('/cookies/refresh', coordinationController.refreshCookies);

// Health and monitoring
router.get('/health', coordinationController.getHealth);
router.post('/instances/:instanceId/stop', coordinationController.stopInstance);

export default router;