#!/usr/bin/env node

import fs from 'fs';
import path from 'path';
import { Command } from 'commander';
import chalk from 'chalk';
import { 
  processInventoryCSV, 
  validateConsecutiveSeats, 
  readInventoryFromCSV, 
  saveInventoryToCSV 
} from './helpers/csvInventoryHelper.js';
import {
  startScrapeCycle,
  markEventScraped,
  generateCombinedEventsCSV
} from './controllers/inventoryController.js';

// Define the CLI program
const program = new Command();

program
  .name('inventory-processor')
  .description('CLI to validate and fix consecutive seats in inventory CSV files')
  .version('1.0.0');

// Command to check a single seat string
program
  .command('check-seats')
  .description('Check if a seat string has consecutive seats')
  .argument('<seats>', 'Comma separated seat numbers to check')
  .action((seats) => {
    console.log(chalk.blue('Checking seat sequence:'), seats);
    
    const result = validateConsecutiveSeats(seats);
    
    if (result.valid) {
      console.log(chalk.green('✓ Seats are consecutive'));
    } else {
      console.log(chalk.red('✗ Seats are not consecutive'));
      console.log(chalk.yellow('Suggested fix:'), result.fixedSeats);
    }
  });

// Command to process a full CSV file
program
  .command('process')
  .description('Process a full inventory CSV file')
  .argument('<input>', 'Input CSV file path')
  .option('-o, --output <output>', 'Output file path (default: input-fixed.csv)')
  .action((input, options) => {
    // Validate input file exists
    if (!fs.existsSync(input)) {
      console.error(chalk.red(`Error: Input file not found: ${input}`));
      process.exit(1);
    }
    
    // Set default output file if not provided
    const outputFile = options.output || 
      path.join(
        path.dirname(input), 
        `${path.basename(input, path.extname(input))}-fixed${path.extname(input)}`
      );
    
    console.log(chalk.blue('Processing inventory file:'), input);
    console.log(chalk.blue('Output will be saved to:'), outputFile);
    
    // Process the file
    const stats = processInventoryCSV(input, outputFile);
    
    // Report results
    console.log('\nProcessing complete:');
    console.log(chalk.blue(`Total records: ${stats.total}`));
    console.log(chalk.yellow(`Records fixed: ${stats.fixed}`));
    console.log(chalk.red(`Errors: ${stats.errors}`));
    
    if (stats.fixed > 0) {
      console.log(chalk.green(`\nFixed file saved to: ${outputFile}`));
    } else if (stats.total > 0 && stats.errors === 0) {
      console.log(chalk.green('\nAll records were already valid!'));
    }
  });

// Command to extract a single record for examination
program
  .command('extract')
  .description('Extract a specific record from a CSV file')
  .argument('<input>', 'Input CSV file path')
  .option('-i, --id <id>', 'Inventory ID to extract')
  .option('-o, --output <output>', 'Output file path for the extracted record')
  .action((input, options) => {
    if (!fs.existsSync(input)) {
      console.error(chalk.red(`Error: Input file not found: ${input}`));
      process.exit(1);
    }
    
    if (!options.id) {
      console.error(chalk.red('Error: Please provide an inventory ID to extract using --id option'));
      process.exit(1);
    }
    
    // Read the inventory data
    const records = readInventoryFromCSV(input);
    
    // Find the record with the matching ID
    const record = records.find(r => r.inventory_id === options.id);
    
    if (!record) {
      console.error(chalk.red(`Error: No record found with inventory_id: ${options.id}`));
      process.exit(1);
    }
    
    // Display the record
    console.log(chalk.green('Record found:'));
    console.table(record);
    
    // Save to file if output option provided
    if (options.output) {
      saveInventoryToCSV([record], options.output);
      console.log(chalk.green(`Record saved to: ${options.output}`));
    }
  });

// Command to start a new scrape cycle
program
  .command('start-cycle')
  .description('Start a new scrape cycle for the given event IDs')
  .argument('<eventIds>', 'Comma-separated list of event IDs to include in this cycle')
  .action((eventIdsArg) => {
    const eventIds = eventIdsArg.split(',').map(id => id.trim());
    
    if (eventIds.length === 0) {
      console.error(chalk.red('Error: Please provide at least one event ID'));
      process.exit(1);
    }
    
    console.log(chalk.blue('Starting new scrape cycle for events:'), eventIds.join(', '));
    
    const result = startScrapeCycle(eventIds);
    
    if (result.success) {
      console.log(chalk.green(result.message));
    } else {
      console.error(chalk.red(`Error: ${result.message}`));
      process.exit(1);
    }
  });

// Command to mark an event as scraped in the current cycle
program
  .command('mark-scraped')
  .description('Mark an event as scraped in the current cycle')
  .argument('<eventId>', 'Event ID that has been scraped')
  .action((eventId) => {
    console.log(chalk.blue('Marking event as scraped:'), eventId);
    
    const result = markEventScraped(eventId);
    
    if (result.success) {
      console.log(chalk.green(result.message));
      
      if (result.cycleComplete) {
        console.log(chalk.green('🎉 All events in the cycle have been scraped!'));
        console.log(chalk.green('The combined CSV has been automatically generated.'));
      }
    } else {
      console.error(chalk.red(`Error: ${result.message}`));
      process.exit(1);
    }
  });

// Command to manually generate the combined CSV
program
  .command('generate-combined')
  .description('Manually generate a combined CSV file with all events data')
  .option('-n, --new-cycle', 'Treat this as a new cycle (deletes previous combined file)')
  .action((options) => {
    console.log(chalk.blue('Generating combined CSV for all events'));
    
    const result = generateCombinedEventsCSV(options.newCycle || false);
    
    if (result.success) {
      console.log(chalk.green(result.message));
      console.log(chalk.green(`File saved to: ${result.filePath}`));
    } else {
      console.error(chalk.red(`Error: ${result.message}`));
      process.exit(1);
    }
  });

// Execute the program
program.parse(process.argv); 