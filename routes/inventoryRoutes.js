import express from 'express';
import inventoryController from '../controllers/inventoryController.js';
import multer from 'multer';
import path from 'path';
import fs from 'fs';

// Set up storage for uploaded files
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    const uploadDir = path.join(process.cwd(), 'uploads');
    
    // Create the directory if it doesn't exist
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir, { recursive: true });
    }
    
    cb(null, uploadDir);
  },
  filename: function (req, file, cb) {
    // Generate a unique filename
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
    cb(null, file.fieldname + '-' + uniqueSuffix + path.extname(file.originalname));
  }
});

const upload = multer({ storage: storage });

// Create router
const router = express.Router();

// Get all inventory
router.get('/', (req, res) => {
  const inventory = inventoryController.getAllInventory();
  res.json({ success: true, data: inventory });
});

// Get inventory by ID
router.get('/:id', (req, res) => {
  const inventory = inventoryController.getInventoryById(req.params.id);
  
  if (!inventory) {
    return res.status(404).json({ success: false, message: 'Inventory not found' });
  }
  
  res.json({ success: true, data: inventory });
});

// Add new inventory
router.post('/', (req, res) => {
  const isNewEvent = req.body.isNewEvent === true || req.body.isNewEvent === 'true';
  const result = inventoryController.addInventory(req.body, isNewEvent);
  
  if (!result.success) {
    return res.status(400).json(result);
  }
  
  res.status(201).json(result);
});

// Add bulk inventory for an event (first scrape)
router.post('/bulk/:eventId', (req, res) => {
  const { eventId } = req.params;
  
  if (!eventId) {
    return res.status(400).json({ success: false, message: 'Event ID is required' });
  }
  
  if (!Array.isArray(req.body)) {
    return res.status(400).json({ success: false, message: 'Request body must be an array of inventory records' });
  }
  
  const result = inventoryController.addBulkInventory(req.body, eventId);
  
  if (!result.success) {
    return res.status(400).json(result);
  }
  
  res.status(201).json(result);
});

// Update inventory
router.put('/:id', (req, res) => {
  const result = inventoryController.updateInventory(req.params.id, req.body);
  
  if (!result.success) {
    return res.status(404).json(result);
  }
  
  res.json(result);
});

// Delete inventory
router.delete('/:id', (req, res) => {
  const result = inventoryController.deleteInventory(req.params.id);
  
  if (!result.success) {
    return res.status(404).json(result);
  }
  
  res.json(result);
});

// Check and fix seats
router.post('/:id/fix-seats', (req, res) => {
  const result = inventoryController.checkAndFixSeats(req.params.id);
  
  if (!result.success) {
    return res.status(404).json(result);
  }
  
  res.json(result);
});

// Check if event has been processed before
router.get('/event/:eventId/status', (req, res) => {
  const { eventId } = req.params;
  
  if (!eventId) {
    return res.status(400).json({ success: false, message: 'Event ID is required' });
  }
  
  const isFirstScrape = inventoryController.isFirstScrape(eventId);
  
  res.json({ 
    success: true, 
    eventId,
    isFirstScrape,
    hasGeneratedCSV: !isFirstScrape
  });
});

// Export inventory to CSV
router.get('/export/csv', (req, res) => {
  const exportDir = path.join(process.cwd(), 'exports');
  
  // Create the directory if it doesn't exist
  if (!fs.existsSync(exportDir)) {
    fs.mkdirSync(exportDir, { recursive: true });
  }
  
  const timestamp = new Date().toISOString().replace(/:/g, '-');
  const filePath = path.join(exportDir, `inventory-${timestamp}.csv`);
  
  const result = inventoryController.exportInventory(filePath);
  
  if (!result.success) {
    return res.status(500).json(result);
  }
  
  // Send the file as a download
  res.download(filePath, `inventory-${timestamp}.csv`, (err) => {
    if (err) {
      res.status(500).json({ success: false, message: 'Error sending file' });
    }
  });
});

// Import inventory from CSV
router.post('/import/csv', upload.single('file'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ success: false, message: 'No file uploaded' });
  }
  
  const replaceExisting = req.body.replace === 'true';
  const result = inventoryController.importInventory(req.file.path, replaceExisting);
  
  // Clean up the uploaded file
  fs.unlinkSync(req.file.path);
  
  if (!result.success) {
    return res.status(400).json(result);
  }
  
  res.json(result);
});

export default router; 