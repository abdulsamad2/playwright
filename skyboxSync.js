/**
 * Skybox Synchronization Module
 * 
 * This module handles synchronization of inventory data with Skybox API
 * when consecutive seat changes are detected during scraping.
 */

import { SkyboxClient } from './skyboxapi.js';
import moment from 'moment';

// Configure default Skybox client
let skyboxClient = null;

/**
 * Initialize the Skybox client with credentials
 * @param {Object} config - Configuration object containing API credentials
 */
function initializeSkyboxClient(config) {
  skyboxClient = new SkyboxClient(config);
  return skyboxClient;
}

/**
 * Get the existing Skybox client or create one with default credentials
 * @returns {SkyboxClient} The Skybox client instance
 */
function getSkyboxClient() {
  if (!skyboxClient) {
    // Default credentials - should be overridden by calling initializeSkyboxClient
    skyboxClient = new SkyboxClient({
      apiToken: process.env.SKYBOX_API_TOKEN || "18123373-a111-400b-84ef-6651fb424d60",
      accountId: process.env.SKYBOX_ACCOUNT_ID || "2058",
      vendorId: process.env.SKYBOX_VENDOR_ID || "684306",
      applicationToken: process.env.SKYBOX_APP_TOKEN || "44ba3709-9cb4-4d81-a7c0-e1b5247fa60b",
    });
  }
  return skyboxClient;
}

/**
 * Check if consecutive seat changes have occurred by comparing old and new seat data
 * @param {Array} oldGroups - Previous consecutive group data
 * @param {Array} newGroups - New consecutive group data
 * @returns {Object} Object with changes and affected skybox inventory IDs
 */
function detectSeatChanges(oldGroups, newGroups) {
  const changes = {
    hasChanges: false,
    changedInventoryIds: new Set(),
    addedGroups: [],
    removedGroups: [],
    updatedGroups: []
  };

  // Create maps for efficient comparison
  const oldGroupMap = new Map();
  oldGroups.forEach(group => {
    // Use section-row-range as key
    const key = `${group.section}-${group.row}-${group.seatRange}`;
    oldGroupMap.set(key, group);
  });

  const newGroupMap = new Map();
  newGroups.forEach(group => {
    const key = `${group.section}-${group.row}-${group.seatRange}`;
    newGroupMap.set(key, group);
  });

  // Find added groups (in new but not in old)
  for (const [key, group] of newGroupMap.entries()) {
    if (!oldGroupMap.has(key)) {
      changes.addedGroups.push(group);
      // If the group has a mapping_id, it's a Skybox inventory ID
      if (group.inventory && group.inventory.mapping_id) {
        changes.changedInventoryIds.add(group.inventory.mapping_id);
      }
    } else {
      // Group exists in both - check for inventory changes
      const oldGroup = oldGroupMap.get(key);
      
      // Deep compare relevant inventory details
      const inventoryChanged = 
        group.seatCount !== oldGroup.seatCount ||
        JSON.stringify(group.seats) !== JSON.stringify(oldGroup.seats) ||
        (group.inventory?.quantity !== oldGroup.inventory?.quantity) ||
        (group.inventory?.listPrice !== oldGroup.inventory?.listPrice);
      
      if (inventoryChanged) {
        changes.updatedGroups.push({
          old: oldGroup,
          new: group
        });
        
        // If the group has a mapping_id, it's a Skybox inventory ID
        if (group.inventory && group.inventory.mapping_id) {
          changes.changedInventoryIds.add(group.inventory.mapping_id);
        }
      }
    }
  }

  // Find removed groups (in old but not in new)
  for (const [key, group] of oldGroupMap.entries()) {
    if (!newGroupMap.has(key)) {
      changes.removedGroups.push(group);
      // If the group has a mapping_id, it's a Skybox inventory ID
      if (group.inventory && group.inventory.mapping_id) {
        changes.changedInventoryIds.add(group.inventory.mapping_id);
      }
    }
  }

  // Set hasChanges if any changes were detected
  changes.hasChanges = 
    changes.addedGroups.length > 0 || 
    changes.removedGroups.length > 0 || 
    changes.updatedGroups.length > 0;

  return changes;
}

/**
 * Creates a new inventory entry based on the provided inventory data
 * @param {Object} client - SkyboxClient instance
 * @param {Object} inventoryData - The inventory data to create
 * @returns {Promise<Object>} The created inventory
 */
async function createNewInventory(client, inventoryData) {
  try {
    // Format the inventory data for creation
    const newInventory = [
      {
        accountId: inventoryData.accountId,
        eventId: inventoryData.eventId,
        quantity: inventoryData.quantity,
        section: inventoryData.section,
        row: inventoryData.row,
        lowSeat: inventoryData.lowSeat,
        highSeat: inventoryData.highSeat,
        notes: inventoryData.notes || "",
        publicNotes: inventoryData.publicNotes || "",
        inHandDate: inventoryData.inHandDate,
        stockType: inventoryData.stockType || "MOBILE_TRANSFER",
        splitType: inventoryData.splitType || "CUSTOM",
        customSplit: inventoryData.customSplit || "2",
        listPrice: inventoryData.listPrice,
        hideSeatNumbers: inventoryData.hideSeatNumbers === undefined ? true : inventoryData.hideSeatNumbers,
        tickets: inventoryData.tickets || []
      }
    ];

    // Create new inventory
    const result = await client.inventoryUpdates(newInventory);
    return result;
  } catch (error) {
    console.error(`Error creating new inventory: ${error.message}`);
    throw error;
  }
}

/**
 * Special method to handle the cancellation workflow:
 * 1. Update section/row by adding "X" suffix
 * 2. Create new inventory with original data
 * 3. Cancel the modified inventory (with X suffix)
 * 
 * @param {Object} client - SkyboxClient instance
 * @param {Object} inventory - The original inventory data
 * @param {Object} matchingGroup - The matching group data from scraping
 * @param {Object} options - Additional options
 * @returns {Promise<Object>} Result of the cancellation process
 */
async function handleInventoryCancellationWorkflow(client, inventory, matchingGroup, options = {}) {
  const LOG_LEVEL = options.logLevel || 1;
  const inventoryId = inventory.id;
  
  try {
    // Step 1: Update the original inventory with "X" suffix in section and row
    if (LOG_LEVEL >= 1) {
      console.log(`[SkyboxSync] Initiating cancellation workflow for inventory ${inventoryId}`);
      console.log(`[SkyboxSync] Step 1: Updating section/row with X suffix`);
    }
    
    const modifiedInventory = {
      id: inventoryId,
      accountId: inventory.accountId,
      eventId: inventory.eventId,
      quantity: inventory.quantity,
      section: `${inventory.section}X`,
      row: `${inventory.row}X`,
      lowSeat: inventory.lowSeat,
      highSeat: inventory.highSeat,
      notes: inventory.notes,
      publicNotes: inventory.publicNotes,
      inHandDate: inventory.inHandDate,
      stockType: inventory.stockType,
      splitType: inventory.splitType,
      customSplit: inventory.customSplit,
      listPrice: inventory.listPrice,
      hideSeatNumbers: inventory.hideSeatNumbers
    };
    
    // Update the inventory with X suffix
    await client.inventoryUpdate(inventoryId, modifiedInventory);
    
    // Step 2: Create new inventory with original data and updated values from matching group
    if (LOG_LEVEL >= 1) {
      console.log(`[SkyboxSync] Step 2: Creating new inventory with original data`);
    }
    
    const newInventoryData = {
      accountId: inventory.accountId,
      eventId: inventory.eventId,
      quantity: matchingGroup ? matchingGroup.seatCount : inventory.quantity,
      section: matchingGroup ? matchingGroup.section : inventory.section,
      row: matchingGroup ? matchingGroup.row : inventory.row,
      lowSeat: matchingGroup ? parseInt(matchingGroup.seats[0].number) : inventory.lowSeat,
      highSeat: matchingGroup ? parseInt(matchingGroup.seats[matchingGroup.seats.length - 1].number) : inventory.highSeat,
      notes: inventory.notes,
      publicNotes: inventory.publicNotes,
      inHandDate: matchingGroup && matchingGroup.seats[0].inHandDate ? 
        moment(matchingGroup.seats[0].inHandDate).format('YYYY-MM-DD') : inventory.inHandDate,
      stockType: inventory.stockType,
      splitType: inventory.splitType,
      customSplit: matchingGroup && matchingGroup.inventory?.customSplit ? 
        matchingGroup.inventory.customSplit : inventory.customSplit,
      listPrice: matchingGroup && matchingGroup.inventory?.listPrice ? 
        matchingGroup.inventory.listPrice : inventory.listPrice,
      hideSeatNumbers: inventory.hideSeatNumbers,
      tickets: inventory.tickets || []
    };
    
    // Create the new inventory
    const createResult = await createNewInventory(client, newInventoryData);
    
    if (createResult && createResult.id) {
      result.changes.added.push({
        inventoryId: createResult.id,
        section: group.section,
        row: group.row,
        seatRange: `${group.seats[0].number}-${group.seats[group.seats.length - 1].number}`,
        quantity: group.seatCount,
        price: group.inventory?.listPrice
      });
      
      if (LOG_LEVEL >= 2) {
        console.log(`[SkyboxSync] Created new inventory ${createResult.id} for ${group.section} ${group.row}`);
      }
      
      // Update the group with the new mapping ID
      group.inventory = {
        ...group.inventory,
        mapping_id: createResult.id.toString()
      };
    }
    
    // Step 3: Cancel the modified inventory (with X suffix)
    if (LOG_LEVEL >= 1) {
      console.log(`[SkyboxSync] Step 3: Cancelling modified inventory ${inventoryId}`);
    }
    
    // Send cancellation request
    await client.cancelInventory([{
      id: parseInt(inventoryId),
      accountId: parseInt(inventory.accountId),
      eventId: parseInt(inventory.eventId),
      reason: "INVALID_LISTING",
      notes: "Automated cancellation after X suffix update"
    }]);
    
    return {
      success: true,
      originalId: inventoryId,
      newInventory: createResult,
      message: "Successfully completed cancellation workflow"
    };
  } catch (error) {
    console.error(`[SkyboxSync] Error in cancellation workflow for inventory ${inventoryId}: ${error.message}`);
    return {
      success: false,
      originalId: inventoryId,
      error: error.message
    };
  }
}

/**
 * Sync consecutive seat changes with Skybox
 * @param {string} eventId - Event ID
 * @param {Array} oldGroups - Previous consecutive group data
 * @param {Array} newGroups - New consecutive group data
 * @param {Object} options - Additional options like log level
 * @returns {Object} Result of sync operation
 */
async function syncSeatChangesToSkybox(eventId, oldGroups, newGroups, options = {}) {
  const LOG_LEVEL = options.logLevel || 1;
  const result = {
    eventId,
    success: false,
    syncedAt: new Date(),
    changes: {
      updated: [],
      added: [],
      removed: []
    },
    errors: []
  };

  try {
    // Get Skybox client
    const client = getSkyboxClient();
    
    // Detect changes
    const changes = detectSeatChanges(oldGroups, newGroups);
    
    // If no changes, return early
    if (!changes.hasChanges) {
      if (LOG_LEVEL >= 2) {
        console.log(`[SkyboxSync] No changes detected for event ${eventId}`);
      }
      result.success = true;
      return result;
    }
    
    if (LOG_LEVEL >= 1) {
      console.log(`[SkyboxSync] Detected changes for event ${eventId}: ${changes.changedInventoryIds.size} affected inventories`);
    }
    
    // Process each changed inventory ID
    for (const inventoryId of changes.changedInventoryIds) {
      try {
        // Get current inventory details from Skybox
        const skyboxInventory = await client.getInventory(inventoryId);
        
        if (!skyboxInventory) {
          throw new Error(`Could not find Skybox inventory with ID ${inventoryId}`);
        }
        
        // Find the corresponding consecutive group
        let matchingGroup = null;
        
        // Try to find in updated groups first
        const updatedMatch = changes.updatedGroups.find(
          group => group.new.inventory?.mapping_id === inventoryId
        );
        
        if (updatedMatch) {
          matchingGroup = updatedMatch.new;
        } else {
          // Try to find in added groups
          matchingGroup = changes.addedGroups.find(
            group => group.inventory?.mapping_id === inventoryId
          );
        }
        
        // If no matching group found, this inventory might have been removed
        if (!matchingGroup) {
          const removedMatch = changes.removedGroups.find(
            group => group.inventory?.mapping_id === inventoryId
          );
          
          if (removedMatch) {
            // Handle removed inventory using our special workflow
            if (LOG_LEVEL >= 1) {
              console.log(`[SkyboxSync] Inventory ${inventoryId} removed, initiating cancellation workflow`);
            }
            
            // Execute the cancellation workflow
            const cancellationResult = await handleInventoryCancellationWorkflow(
              client, 
              skyboxInventory,
              null, // No matching group for removed inventory
              options
            );
            
            if (cancellationResult.success) {
              result.changes.removed.push({
                inventoryId,
                section: removedMatch.section,
                row: removedMatch.row,
                seatRange: removedMatch.seatRange,
                workflowCompleted: true
              });
            } else {
              throw new Error(`Cancellation workflow failed: ${cancellationResult.error}`);
            }
            
            continue;
          }
        }
        
        // If we have a matching group, update the inventory
        if (matchingGroup) {
          // Check if this is a significant change (section/row/seats) requiring cancellation workflow
          const requiresCancellation = 
            matchingGroup.section !== skyboxInventory.section || 
            matchingGroup.row !== skyboxInventory.row ||
            parseInt(matchingGroup.seats[0].number) !== skyboxInventory.lowSeat ||
            parseInt(matchingGroup.seats[matchingGroup.seats.length - 1].number) !== skyboxInventory.highSeat;
          
          if (requiresCancellation) {
            if (LOG_LEVEL >= 1) {
              console.log(`[SkyboxSync] Significant changes detected for inventory ${inventoryId}, using cancellation workflow`);
            }
            
            // Execute the cancellation workflow
            const cancellationResult = await handleInventoryCancellationWorkflow(
              client,
              skyboxInventory,
              matchingGroup,
              options
            );
            
            if (cancellationResult.success) {
              result.changes.updated.push({
                inventoryId,
                section: matchingGroup.section,
                row: matchingGroup.row,
                seatRange: matchingGroup.seatRange,
                quantity: matchingGroup.seatCount,
                price: matchingGroup.inventory?.listPrice,
                usedCancellationWorkflow: true
              });
            } else {
              throw new Error(`Cancellation workflow failed: ${cancellationResult.error}`);
            }
          } else {
            // Only minor changes (price, etc.) - use regular update
            if (LOG_LEVEL >= 2) {
              console.log(`[SkyboxSync] Minor changes detected for inventory ${inventoryId}, using standard update`);
            }
            
            // Prepare update data
            const updateData = {
              id: parseInt(inventoryId),
              accountId: parseInt(skyboxInventory.accountId),
              eventId: parseInt(skyboxInventory.eventId),
              quantity: matchingGroup.seatCount,
              section: matchingGroup.section,
              row: matchingGroup.row,
              lowSeat: parseInt(matchingGroup.seats[0].number),
              highSeat: parseInt(matchingGroup.seats[matchingGroup.seats.length - 1].number),
              notes: skyboxInventory.notes,
              publicNotes: skyboxInventory.publicNotes,
              inHandDate: moment(matchingGroup.seats[0].inHandDate).format('YYYY-MM-DD'),
              stockType: skyboxInventory.stockType,
              splitType: skyboxInventory.splitType,
              hideSeatNumbers: skyboxInventory.hideSeatNumbers
            };
            
            // Only update price if it's changed
            if (matchingGroup.inventory?.listPrice && 
                matchingGroup.inventory.listPrice !== skyboxInventory.listPrice) {
              updateData.listPrice = matchingGroup.inventory.listPrice;
            }
            
            // Update custom split if needed
            if (matchingGroup.inventory?.customSplit) {
              updateData.customSplit = matchingGroup.inventory.customSplit;
            }
            
            // Make the update request
            await client.inventoryUpdate(inventoryId, updateData);
            
            result.changes.updated.push({
              inventoryId,
              section: matchingGroup.section,
              row: matchingGroup.row,
              seatRange: matchingGroup.seatRange,
              quantity: matchingGroup.seatCount,
              price: matchingGroup.inventory?.listPrice,
              usedCancellationWorkflow: false
            });
          }
        }
      } catch (error) {
        console.error(`[SkyboxSync] Error syncing inventory ${inventoryId}: ${error.message}`);
        result.errors.push({
          inventoryId,
          message: error.message,
          stack: error.stack
        });
      }
    }
    
    // Check for new groups that don't have mapping_id (need to be created in Skybox)
    const newGroupsWithoutMappingId = changes.addedGroups.filter(
      group => !group.inventory?.mapping_id
    );
    
    if (newGroupsWithoutMappingId.length > 0 && LOG_LEVEL >= 1) {
      console.log(`[SkyboxSync] Found ${newGroupsWithoutMappingId.length} new groups without mapping ID, skipping creation`);
      // Note: Creation of new inventories would go here if implemented
      
      // Implement creation of new inventories in Skybox
      if (options.createNewInventories) {
        if (LOG_LEVEL >= 1) {
          console.log(`[SkyboxSync] Creating ${newGroupsWithoutMappingId.length} new inventories in Skybox`);
        }
        
        for (const group of newGroupsWithoutMappingId) {
          try {
            // Prepare inventory data for creation
            const newInventoryData = {
              accountId: options.accountId || process.env.SKYBOX_ACCOUNT_ID,
              eventId: eventId,
              quantity: group.seatCount,
              section: group.section,
              row: group.row,
              lowSeat: parseInt(group.seats[0].number),
              highSeat: parseInt(group.seats[group.seats.length - 1].number),
              notes: group.inventory?.notes || `Auto-created inventory @sec[${group.section}]`,
              publicNotes: group.inventory?.publicNotes || `${group.row} Row`,
              inHandDate: moment(group.seats[0].inHandDate).format('YYYY-MM-DD'),
              stockType: group.inventory?.stockType || "MOBILE_TRANSFER",
              splitType: group.inventory?.splitType || "CUSTOM",
              customSplit: group.inventory?.customSplit || `${Math.ceil(group.seatCount/2)},${group.seatCount}`,
              listPrice: group.inventory?.listPrice || 500.00,
              hideSeatNumbers: group.inventory?.hideSeatNumbers || true
            };
            
            // Create the new inventory
            const createResult = await createNewInventory(client, newInventoryData);
            
            if (createResult && createResult.id) {
              result.changes.added.push({
                inventoryId: createResult.id,
                section: group.section,
                row: group.row,
                seatRange: `${group.seats[0].number}-${group.seats[group.seats.length - 1].number}`,
                quantity: group.seatCount,
                price: group.inventory?.listPrice
              });
              
              if (LOG_LEVEL >= 2) {
                console.log(`[SkyboxSync] Created new inventory ${createResult.id} for ${group.section} ${group.row}`);
              }
              
              // Update the group with the new mapping ID
              group.inventory = {
                ...group.inventory,
                mapping_id: createResult.id.toString()
              };
            }
          } catch (error) {
            console.error(`[SkyboxSync] Error creating new inventory for ${group.section} ${group.row}: ${error.message}`);
            result.errors.push({
              section: group.section,
              row: group.row,
              message: error.message,
              stack: error.stack
            });
          }
        }
      }
    }
    
    // Set success based on whether there were errors
    result.success = result.errors.length === 0;
    
    return result;
  } catch (error) {
    console.error(`[SkyboxSync] Error during sync for event ${eventId}: ${error.message}`);
    result.errors.push({
      global: true,
      message: error.message,
      stack: error.stack
    });
    return result;
  }
}

/**
 * This function should be integrated with ScraperManager.updateEventMetadata
 * Example integration:
 * 
 * // Inside updateEventMetadata after detecting changes
 * if (hasChanges) {
 *   try {
 *     await syncSeatChangesToSkybox(eventId, currentGroups, validScrapeResult);
 *   } catch (error) {
 *     console.error(`Error syncing with Skybox: ${error.message}`);
 *   }
 * }
 */

/**
 * Initializes an event by fetching all its data from Skybox, including inventory IDs and mapping IDs
 * @param {string} eventId - Event ID to initialize
 * @param {string} skyboxEventId - The event ID in Skybox (if known)
 * @param {Object} options - Additional options
 * @returns {Promise<Object>} Result of initialization with all event and inventory data
 */
async function initializeEventFromSkybox(eventId, skyboxEventId, options = {}) {
  const LOG_LEVEL = options.logLevel || 1;
  const result = {
    eventId,
    skyboxEventId,
    success: false,
    eventData: null,
    inventories: [],
    errors: []
  };

  try {
    // Get Skybox client
    const client = getSkyboxClient();
    
    if (LOG_LEVEL >= 1) {
      console.log(`[SkyboxSync] Initializing event ${eventId} from Skybox`);
    }
    
    // If skyboxEventId is not provided, try to find the event in Skybox
    if (!skyboxEventId) {
      if (LOG_LEVEL >= 1) {
        console.log(`[SkyboxSync] No Skybox event ID provided, searching for event ${eventId}`);
      }
      
      // Use the search API to find the event
      // This will depend on how your SkyboxClient is implemented
      // Assuming there's a searchEvents method that can search by external ID or name
      const searchResults = await client.searchEvents(eventId);
      
      if (searchResults && searchResults.length > 0) {
        // Use the first matching event
        skyboxEventId = searchResults[0].id;
        if (LOG_LEVEL >= 1) {
          console.log(`[SkyboxSync] Found matching Skybox event: ${skyboxEventId}`);
        }
      } else {
        throw new Error(`Could not find event ${eventId} in Skybox`);
      }
    }
    
    // Get event details from Skybox
    const eventDetails = await client.getEvent(skyboxEventId);
    
    if (!eventDetails) {
      throw new Error(`Could not retrieve event details for Skybox event ID ${skyboxEventId}`);
    }
    
    result.eventData = eventDetails;
    
    // Get all inventories for this event
    const inventories = await client.getInventoriesByEvent(skyboxEventId);
    
    if (inventories && inventories.length > 0) {
      if (LOG_LEVEL >= 1) {
        console.log(`[SkyboxSync] Found ${inventories.length} inventories for event ${eventId} in Skybox`);
      }
      
      // Process each inventory to create a mapping structure
      for (const inventory of inventories) {
        result.inventories.push({
          inventoryId: inventory.id,
          mapping_id: inventory.id, // Use inventory ID as mapping ID
          section: inventory.section,
          row: inventory.row,
          lowSeat: inventory.lowSeat,
          highSeat: inventory.highSeat,
          quantity: inventory.quantity,
          listPrice: inventory.listPrice,
          notes: inventory.notes,
          publicNotes: inventory.publicNotes,
          stockType: inventory.stockType,
          splitType: inventory.splitType,
          customSplit: inventory.customSplit,
          inHandDate: inventory.inHandDate,
          hideSeatNumbers: inventory.hideSeatNumbers
        });
      }
    } else {
      if (LOG_LEVEL >= 1) {
        console.log(`[SkyboxSync] No existing inventories found for event ${eventId} in Skybox`);
      }
    }
    
    result.success = true;
    return result;
  } catch (error) {
    console.error(`[SkyboxSync] Error initializing event ${eventId} from Skybox: ${error.message}`);
    result.errors.push({
      message: error.message,
      stack: error.stack
    });
    return result;
  }
}

export default {
  initializeSkyboxClient,
  getSkyboxClient,
  detectSeatChanges,
  syncSeatChangesToSkybox,
  handleInventoryCancellationWorkflow,
  initializeEventFromSkybox
}; 