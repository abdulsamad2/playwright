#!/usr/bin/env node

/**
 * Script to start multiple scraper instances for horizontal scaling
 * Usage:
 *   node scripts/start-instances.js --primary --secondary 4
 *   This will start 1 primary instance and 4 secondary instances
 */

import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = path.join(__dirname, '..');

// Parse command line arguments
const args = process.argv.slice(2);
let startPrimary = false;
let secondaryCount = 0;
let primaryPort = 3000;
let baseSecondaryPort = 3001;

for (let i = 0; i < args.length; i++) {
  switch (args[i]) {
    case '--primary':
      startPrimary = true;
      break;
    case '--secondary':
      secondaryCount = parseInt(args[i + 1]) || 1;
      i++; // Skip next argument as it's the count
      break;
    case '--primary-port':
      primaryPort = parseInt(args[i + 1]) || 3000;
      i++;
      break;
    case '--secondary-port-base':
      baseSecondaryPort = parseInt(args[i + 1]) || 3001;
      i++;
      break;
    case '--help':
    case '-h':
      showHelp();
      process.exit(0);
      break;
  }
}

function showHelp() {
  console.log(`
Horizontal Scaling Startup Script

Usage: node scripts/start-instances.js [options]

Options:
  --primary                 Start a primary instance
  --secondary <count>       Start <count> secondary instances
  --primary-port <port>     Port for primary instance (default: 3000)
  --secondary-port-base <port> Base port for secondary instances (default: 3001)
  --help, -h               Show this help message

Examples:
  # Start 1 primary and 4 secondary instances
  node scripts/start-instances.js --primary --secondary 4
  
  # Start only 3 secondary instances (primary running elsewhere)
  node scripts/start-instances.js --secondary 3
  
  # Start primary on port 4000 and 2 secondaries starting from port 4001
  node scripts/start-instances.js --primary --secondary 2 --primary-port 4000 --secondary-port-base 4001
`);
}

if (!startPrimary && secondaryCount === 0) {
  console.error('Error: Must specify --primary and/or --secondary <count>');
  showHelp();
  process.exit(1);
}

const instances = [];

/**
 * Start a single instance
 */
function startInstance(type, port, instanceIndex = 0) {
  const env = {
    ...process.env,
    INSTANCE_TYPE: type,
    PORT: port.toString()
  };
  
  // Set PRIMARY_URL for secondary instances
  if (type === 'secondary') {
    env.PRIMARY_URL = `http://127.0.0.1:${primaryPort}`;
  }
  
  console.log(`Starting ${type} instance on port ${port}...`);
  
  const child = spawn('node', ['app.js', '--start-scraper'], {
    cwd: projectRoot,
    env: env,
    stdio: ['inherit', 'pipe', 'pipe']
  });
  
  // Add instance identifier to logs
  const instanceId = type === 'primary' ? 'PRIMARY' : `SECONDARY-${instanceIndex + 1}`;
  
  child.stdout.on('data', (data) => {
    const lines = data.toString().split('\n').filter(line => line.trim());
    lines.forEach(line => {
      console.log(`[${instanceId}:${port}] ${line}`);
    });
  });
  
  child.stderr.on('data', (data) => {
    const lines = data.toString().split('\n').filter(line => line.trim());
    lines.forEach(line => {
      console.error(`[${instanceId}:${port}] ERROR: ${line}`);
    });
  });
  
  child.on('close', (code) => {
    console.log(`[${instanceId}:${port}] Process exited with code ${code}`);
    
    // Remove from instances array
    const index = instances.findIndex(inst => inst.child.pid === child.pid);
    if (index !== -1) {
      instances.splice(index, 1);
    }
    
    // If primary instance dies, we might want to restart it
    if (type === 'primary' && code !== 0) {
      console.log(`[${instanceId}:${port}] Primary instance crashed, consider restarting`);
    }
  });
  
  child.on('error', (error) => {
    console.error(`[${instanceId}:${port}] Failed to start: ${error.message}`);
  });
  
  return {
    type,
    port,
    instanceIndex,
    child,
    instanceId
  };
}

/**
 * Start all instances
 */
async function startAllInstances() {
  console.log('\n🚀 Starting Horizontal Scaling Setup...');
  console.log(`Primary instances: ${startPrimary ? 1 : 0}`);
  console.log(`Secondary instances: ${secondaryCount}`);
  console.log('\n' + '='.repeat(50) + '\n');
  
  // Start primary instance first
  if (startPrimary) {
    const primaryInstance = startInstance('primary', primaryPort);
    instances.push(primaryInstance);
    
    // Wait a bit for primary to start before starting secondaries
    console.log('Waiting 10 seconds for primary instance to initialize...');
    await new Promise(resolve => setTimeout(resolve, 10000));
  }
  
  // Start secondary instances
  for (let i = 0; i < secondaryCount; i++) {
    const port = baseSecondaryPort + i;
    const secondaryInstance = startInstance('secondary', port, i);
    instances.push(secondaryInstance);
    
    // Stagger secondary instance starts
    if (i < secondaryCount - 1) {
      await new Promise(resolve => setTimeout(resolve, 3000));
    }
  }
  
  console.log(`\n✅ All instances started successfully!`);
  console.log(`\nInstance Summary:`);
  instances.forEach(instance => {
    console.log(`  - ${instance.instanceId}: http://localhost:${instance.port}`);
  });
  
  if (startPrimary) {
    console.log(`\n🎯 Primary Dashboard: http://localhost:${primaryPort}`);
    console.log(`📊 Coordination Status: http://localhost:${primaryPort}/api/coordination/status`);
  }
  
  console.log(`\n💡 Tips:`);
  console.log(`  - Monitor all instances: curl http://localhost:${primaryPort}/api/coordination/status`);
  console.log(`  - Stop gracefully: Ctrl+C`);
  console.log(`  - Check health: curl http://localhost:<port>/api/health`);
}

/**
 * Graceful shutdown
 */
function gracefulShutdown() {
  console.log('\n🛑 Shutting down all instances...');
  
  instances.forEach(instance => {
    console.log(`Stopping ${instance.instanceId}...`);
    instance.child.kill('SIGTERM');
  });
  
  // Force kill after 30 seconds
  setTimeout(() => {
    instances.forEach(instance => {
      if (!instance.child.killed) {
        console.log(`Force killing ${instance.instanceId}...`);
        instance.child.kill('SIGKILL');
      }
    });
    process.exit(0);
  }, 30000);
  
  // Exit when all instances are stopped
  const checkInterval = setInterval(() => {
    if (instances.length === 0) {
      clearInterval(checkInterval);
      console.log('✅ All instances stopped successfully');
      process.exit(0);
    }
  }, 1000);
}

// Handle shutdown signals
process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);

// Start all instances
startAllInstances().catch(error => {
  console.error('Failed to start instances:', error);
  process.exit(1);
});