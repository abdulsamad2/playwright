import dotenv from "dotenv";
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import setupGlobals from "./setup.js";

// Load environment variables
dotenv.config();

// Initialize global components
setupGlobals();

// Determine service type from environment
const serviceType = process.env.SERVICE_TYPE || 'api';

if (serviceType === 'api') {
  // Start API service
  console.log('Starting API service...');
  import('./services/api.js');
} else if (serviceType === 'worker') {
  // Start worker service
  console.log('Starting worker service...');
  import('./services/worker.js');
} else {
  console.error(`Unknown service type: ${serviceType}`);
  process.exit(1);
}