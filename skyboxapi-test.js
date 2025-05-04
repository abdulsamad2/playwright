/**
 * Example test for SkyboxAPI to get listed events
 * 
 * Usage:
 * 1. Make sure you have valid API credentials in env variables or update them below
 * 2. Run: node skyboxapi-test.js [operation]
 *    Valid operations: update, cancel, bulk-update, get-inventory, update-by-id
 */

import { SkyboxClient } from './skyboxapi.js';

// Configure API client with your credentials
const skyboxConfig = {
  apiToken:
    process.env.SKYBOX_API_TOKEN || "18123373-a111-400b-84ef-6651fb424d60",
  accountId: process.env.SKYBOX_ACCOUNT_ID || "2058",
  vendorId: process.env.SKYBOX_VENDOR_ID || "684306",
  applicationToken: process.env.SKYBOX_APP_TOKEN || "44ba3709-9cb4-4d81-a7c0-e1b5247fa60b",
  // Optional: override base URL if needed
  // baseUrl: 'https://skybox.vividseats.com/services'
};

// Initialize the SkyboxClient
const skyboxClient = new SkyboxClient(skyboxConfig);

// Example inventory ID to retrieve
const inventoryId = "716699635"; // Sample inventory ID

// Example inventory update data - Standard update format
const inventoryUpdates = [
  {
    id: 716791739,
    accountId: 2058,
    eventId: 5526836,
    quantity: 3,
    notes: "-tnow -tmplus Updated",
    section: "MEZCX",
    row: "FX",
    lowSeat: 110,
    highSeat: 116,
    cost: 393.06,
    taxedCost: 393.06,
    taxedCostAverage: 131.02,
    faceValue: 393.06,
    listPrice: 190.0, // Updated price
    stockType: "MOBILE_TRANSFER",
    splitType: "CUSTOM",
    customSplit: "1,2,3",
    publicNotes: "AbdulSamad Update",
    inHandDate: "2025-05-13",
  },
];

// Example data for single inventory update by ID
const singleInventoryUpdate = {
  accountId: 2058,
  eventId: 5526836,
  quantity: 2,
  notes: "-tnow -tmplus AbdulSamad Update",
  section: "MEZCX",
  row: "FX",
  lowSeat: 110,
  highSeat: 111,
  cost: 263.7,
  taxedCost: 263.7,
  taxedCostAverage: 131.85,
  faceValue: 263.7,
  listPrice: 200.0, // Updated price
  stockType: "MOBILE_TRANSFER",
  splitType: "CUSTOM",
  customSplit: "2",
  publicNotes: "Updated using the AbdulSamad Update",
  status: "AVAILABLE",
  inHandDate: "2025-05-13",
  broadcast: false,
  zoneSeating: false,
  electronicTransfer: true,
  optOutAutoPrice: false,
  hideSeatNumbers: true,
  vsrOption: "ALL",
  tickets: [
    {
      id: 2660431492,
      seatNumber: 110,
      inventoryId: 716791739,
      section: "MEZC",
      row: "F",
      notes: "-tnow -tmplus",
      cost: 131.85,
      faceValue: 131.85,
      taxedCost: 131.85,
      sellPrice: 95.0, // Updated individual ticket price
      stockType: "MOBILE_TRANSFER",
      eventId: 5526836,
      accountId: 2058,
      status: "AVAILABLE"
    },
    {
      id: 2660431493,
      seatNumber: 111,
      inventoryId: 716791739,
      section: "MEZC",
      row: "F",
      notes: "-tnow -tmplus",
      cost: 131.85,
      faceValue: 131.85,
      taxedCost: 131.85,
      sellPrice: 105.0, // Updated individual ticket price
      stockType: "MOBILE_TRANSFER",
      eventId: 5526836,
      accountId: 2058,
      status: "AVAILABLE"
    }
  ]
};

// Example inventory cancellation data
const inventoryCancellations = [
  {
    id: 123, // ID of the inventory to cancel
    accountId: 456,
    eventId: 789,
    reason: "SOLD_ELSEWHERE", // Reason for cancellation
    notes: "Inventory sold through another platform"
  }
];

// Example bulk inventory update data
const bulkInventoryUpdateData = {
  inventory_ids: ["716221976"], // IDs of inventory items to update
  public_notes_to_replace: "New public notes for all items",
  internal_notes_to_add: "Additional internal information",
  split_type: "MULTIPLES_OF_2",
  stock_type: "ELECTRONIC",
  in_hand_date: "2025-06-15", // YYYY-MM-DD format
  section: "MEZCX",
  row: "FX",
  face_value: 85.5,
  broadcast: false,
  hide_seat_numbers: true,
};

// Full JSON Format Example for Inventory Update
// 

// Make inventory update request
async function updateInventory() {
  try {
    console.log('Sending inventory update request...');
    const result = await skyboxClient.inventoryUpdates(inventoryUpdates);
    console.log('Inventory update successful:', result);
    return result;
  } catch (error) {
    console.error('Error updating inventory:', error);
    throw error;
  }
}

// Make single inventory update by ID request
async function updateInventoryById(id = inventoryId, allowSeatChange = false) {
  try {
    console.log(`Sending inventory update by ID request for inventory #${id}...`);
    console.log(`Allow seat number changes: ${allowSeatChange}`);
    await skyboxClient.inventoryUpdate(id, singleInventoryUpdate, allowSeatChange);
    console.log('Inventory update by ID successful');
    return true;
  } catch (error) {
    console.error(`Error updating inventory #${id}:`, error);
    throw error;
  }
}

// Make inventory cancellation request
async function cancelInventory() {
  try {
    console.log('Sending inventory cancellation request...');
    const result = await skyboxClient.cancelInventory(inventoryCancellations);
    console.log('Inventory cancellation successful:', result);
    return result;
  } catch (error) {
    console.error('Error cancelling inventory:', error);
    throw error;
  }
}

// Make bulk inventory update request
async function bulkUpdateInventory() {
  try {
    console.log('Sending bulk inventory update request...');
    const result = await skyboxClient.bulkInventoryUpdate(bulkInventoryUpdateData);
    console.log('Bulk inventory update successful:', result);
    return result;
  } catch (error) {
    console.error('Error performing bulk inventory update:', error);
    throw error;
  }
}

// Get inventory by ID
async function getInventoryById(id = inventoryId) {
  try {
    console.log(`Fetching inventory details for ID: ${id}...`);
    const result = await skyboxClient.getInventory(id);
    console.log('Inventory details:', JSON.stringify(result, null, 2));
    return result;
  } catch (error) {
    console.error(`Error fetching inventory with ID ${id}:`, error);
    throw error;
  }
}

// Choose which operation to perform
const operation = process.argv[2] || 'update'; // Default to update if no argument provided
// Get the optional ID parameter for get-inventory operation
const customId = process.argv[3];
// Get the optional changeSeatNumbers parameter for update-by-id operation
const changeSeatNumbers = process.argv[4] === 'true';

switch (operation) {
  case 'update':
    updateInventory()
      .then(() => console.log('Inventory update process completed'))
      .catch(err => console.error('Inventory update process failed:', err));
    break;
  case 'update-by-id':
    updateInventoryById(customId || inventoryId, changeSeatNumbers)
      .then(() => console.log('Inventory update by ID process completed'))
      .catch(err => console.error('Inventory update by ID process failed:', err));
    break;
  case 'cancel':
    cancelInventory()
      .then(() => console.log('Inventory cancellation process completed'))
      .catch(err => console.error('Inventory cancellation process failed:', err));
    break;
  case 'bulk-update':
    bulkUpdateInventory()
      .then(() => console.log('Bulk inventory update process completed'))
      .catch(err => console.error('Bulk inventory update process failed:', err));
    break;
  case 'get-inventory':
    getInventoryById(customId)
      .then(() => console.log('Inventory fetch process completed'))
      .catch(err => console.error('Inventory fetch process failed:', err));
    break;
  default:
    console.log('Invalid operation. Use "update", "cancel", "bulk-update", "get-inventory", or "update-by-id"');
}

