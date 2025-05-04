/**
 * Skybox Sync Integration Example
 * 
 * This file demonstrates how to integrate the skyboxSync module with
 * ScraperManager's updateEventMetadata function.
 */

import skyboxSync from './skyboxSync.js';

/**
 * Example integration into ScraperManager.updateEventMetadata
 * 
 * This function shows how you would modify the updateEventMetadata 
 * function in ScraperManager to add Skybox synchronization.
 */
export function integrateWithScraperManager() {
  // This code represents a snippet of how to modify ScraperManager.updateEventMetadata
  // Copy and adapt this code into the ScraperManager.js file
  
  /* 
  // Inside ScraperManager.js
  
  // First import the skyboxSync module at the top of your file
  import skyboxSync from './skyboxSync.js';
  
  // Initialize skyboxSync during ScraperManager initialization
  // Add this to the constructor or another initialization method
  skyboxSync.initializeSkyboxClient({
    apiToken: process.env.SKYBOX_API_TOKEN || "your-default-api-token",
    accountId: process.env.SKYBOX_ACCOUNT_ID || "your-default-account-id",
    vendorId: process.env.SKYBOX_VENDOR_ID || "your-default-vendor-id",
    applicationToken: process.env.SKYBOX_APP_TOKEN || "your-default-app-token",
  });
  
  // Then modify the updateEventMetadata method to include the syncSeatChangesToSkybox call
  // You need to add this code in the appropriate location within updateEventMetadata
  
  async updateEventMetadata(eventId, scrapeResult) {
    const startTime = performance.now();
    const session = await Event.startSession();

    try {
      await session.withTransaction(async () => {
        const event = await Event.findOne({ Event_ID: eventId }).session(session);
        if (!event) {
          throw new Error(`Event ${eventId} not found in database for metadata update`);
        }

        // Filter scrape results to include only groups with at least 2 seats
        const validScrapeResult = scrapeResult.filter(group => 
          group.seats && group.seats.length >= 2 && group.inventory && group.inventory.quantity >= 2
        );

        // Check if seats have actually changed before performing database operations
        const currentGroups = await ConsecutiveGroup.find(
          { eventId },
          { section: 1, row: 1, seats: 1, "inventory.listPrice": 1, "inventory.mapping_id": 1 }
        ).lean().session(session);

        // Create hash sets for efficient comparison
        const currentSeatsHash = new Set(
          currentGroups.flatMap((g) =>
            g.seats.map((s) => `${g.section}-${g.row}-${s.number}-${s.price}`)
          )
        );

        const newSeatsHash = new Set(
          validScrapeResult.flatMap((g) =>
            g.seats.map(
              (s) => `${g.section}-${g.row}-${s}-${g.inventory.listPrice}`
            )
          )
        );

        // Check if there are differences in the seats
        const hasChanges =
          currentSeatsHash.size !== newSeatsHash.size ||
          [...currentSeatsHash].some((s) => !newSeatsHash.has(s)) ||
          [...newSeatsHash].some((s) => !currentSeatsHash.has(s));

        if (hasChanges) {
          // Only log at appropriate log level
          if (LOG_LEVEL >= 2) {
            this.logWithTime(`Seat changes detected for event ${eventId}, updating database`, "info");
          }
          
          // *** HERE IS WHERE YOU ADD SKYBOX SYNC ***
          try {
            // Sync changes to Skybox
            const syncResult = await skyboxSync.syncSeatChangesToSkybox(
              eventId, 
              currentGroups, 
              validScrapeResult,
              { logLevel: LOG_LEVEL }
            );
            
            if (syncResult.success) {
              if (LOG_LEVEL >= 1) {
                const changedCount = 
                  syncResult.changes.updated.length + 
                  syncResult.changes.added.length + 
                  syncResult.changes.removed.length;
                
                this.logWithTime(
                  `Successfully synced ${changedCount} inventory changes to Skybox for event ${eventId}`,
                  "success"
                );
              }
            } else if (syncResult.errors.length > 0) {
              this.logWithTime(
                `Errors occurred during Skybox sync for event ${eventId}: ${syncResult.errors.length} errors`,
                "error"
              );
            }
          } catch (error) {
            // Log error but continue with database update
            this.logWithTime(
              `Error syncing with Skybox for event ${eventId}: ${error.message}`,
              "error"
            );
            
            // Optionally log to error database
            await this.logError(eventId, "SKYBOX_SYNC_ERROR", error, {
              message: "Failed to sync seat changes with Skybox"
            });
          }
          
          // Continue with your existing database update logic...
          // Bulk delete and insert for better performance
          await ConsecutiveGroup.deleteMany({ eventId }).session(session);
          
          // Your existing code to prepare and insert groups...
          // ...
        }
      });
      
      // Rest of your function...
    } catch (error) {
      // Error handling...
    }
  }
  */
}

/**
 * Example of manually testing the synchronization
 * This can be run independently to test the Skybox sync functionality
 */
export async function testSkyboxSync() {
  // Initialize Skybox client
  skyboxSync.initializeSkyboxClient({
    apiToken: process.env.SKYBOX_API_TOKEN || "18123373-a111-400b-84ef-6651fb424d60",
    accountId: process.env.SKYBOX_ACCOUNT_ID || "2058",
    vendorId: process.env.SKYBOX_VENDOR_ID || "684306",
    applicationToken: process.env.SKYBOX_APP_TOKEN || "44ba3709-9cb4-4d81-a7c0-e1b5247fa60b",
  });
  
  // Example event ID
  const eventId = '5526836';
  
  // Example old groups (before changes)
  const oldGroups = [
    {
      section: "MEZC",
      row: "F",
      seatCount: 2,
      seatRange: "110-111",
      seats: [
        { number: "110", price: 131.85, inHandDate: "2025-05-13" },
        { number: "111", price: 131.85, inHandDate: "2025-05-13" }
      ],
      inventory: {
        quantity: 2,
        listPrice: 181.37,
        mapping_id: "716791739",
        customSplit: "2"
      }
    }
  ];
  
  // Example new groups (after changes)
  const newGroups = [
    {
      section: "MEZCX", // Section changed
      row: "FX",       // Row changed
      seatCount: 2,
      seatRange: "110-111",
      seats: [
        { number: "110", price: 150.00, inHandDate: "2025-05-13" }, // Price changed
        { number: "111", price: 150.00, inHandDate: "2025-05-13" }  // Price changed
      ],
      inventory: {
        quantity: 2,
        listPrice: 200.00, // List price changed
        mapping_id: "716791739",
        customSplit: "2"
      }
    }
  ];
  
  // Test the sync function
  try {
    console.log(`Testing Skybox sync for event ${eventId}...`);
    const result = await skyboxSync.syncSeatChangesToSkybox(eventId, oldGroups, newGroups, { logLevel: 2 });
    
    console.log("Sync result:", JSON.stringify(result, null, 2));
    
    if (result.success) {
      console.log("✅ Skybox sync test successful!");
    } else {
      console.log("❌ Skybox sync test failed with errors:", result.errors);
    }
  } catch (error) {
    console.error("❌ Exception during Skybox sync test:", error);
  }
}

// Export a function to run the test
export function runTest() {
  testSkyboxSync().catch(console.error);
}

// Export default object with all functions
export default {
  integrateWithScraperManager,
  testSkyboxSync,
  runTest
}; 