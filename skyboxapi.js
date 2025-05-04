import axios from "axios";

/**
 * SkyBox API Client for Node.js
 * Based on the OpenAPI specification from https://github.com/Evizent/skybox_openapi_client
 */
export class SkyboxClient {
  /**
   * Create a new Skybox API client
   * @param {Object} config - Configuration object
   * @param {string} config.apiToken - Authorization token (X-Api-Token)
   * @param {string} config.accountId - Account ID (X-Account)
   * @param {string} config.vendorId - Vendor ID (X-Vendor-ID)
   * @param {string} [config.applicationToken] - Application token (X-Application-Token)
   * @param {string} [config.baseUrl='https://skybox.vividseats.com/services'] - API base URL
   */
  constructor(config) {
    this.apiToken = config.apiToken;
    this.accountId = config.accountId;
    this.vendorId = config.vendorId;
    this.applicationToken = config.applicationToken;
    this.baseUrl = config.baseUrl || "https://skybox.vividseats.com/services";

    // Initialize axios instance with default configuration
    this.apiClient = axios.create({
      baseURL: this.baseUrl,
      headers: {
        "X-Api-Token": this.apiToken,
        "X-Account": this.accountId.toString(),
        "Content-Type": "application/json",
      },
    });

    // Add application token to headers if provided
    if (this.applicationToken) {
      this.apiClient.defaults.headers["X-Application-Token"] = this.applicationToken;
    }

    // Add vendor ID to headers if provided
    if (this.vendorId) {
      this.apiClient.defaults.headers["X-Vendor-ID"] = this.vendorId.toString();
    }

    // Add response interceptor for error handling
    this.apiClient.interceptors.response.use(
      (response) => response,
      (error) => {
        const errorResponse = error.response || {};
        const errorDetails = {
          status: errorResponse.status,
          statusText: errorResponse.statusText,
          data: errorResponse.data,
          message: error.message,
        };

        console.error("API request failed:", errorDetails);
        return Promise.reject(errorDetails);
      }
    );
    
    // Initialize rate limiter properties
    this.lastRequestTime = 0;
    this.requestQueue = [];
    this.isProcessingQueue = false;
  }
  
  /**
   * Rate limiter to ensure requests are sent at a maximum rate of 1 per second
   * @returns {Promise<void>} - Resolves when it's safe to make a request
   */
  async rateLimiter() {
    const now = Date.now();
    const timeSinceLastRequest = now - this.lastRequestTime;
    
    // If less than 1 second has passed since the last request, we need to wait
    if (this.lastRequestTime > 0 && timeSinceLastRequest < 1000) {
      const timeToWait = 1000 - timeSinceLastRequest;
      console.log(`Rate limiting: Waiting ${timeToWait}ms before next Skybox API request`);
      await new Promise(resolve => setTimeout(resolve, timeToWait));
    }
    
    // Update the last request time
    this.lastRequestTime = Date.now();
  }
  
  /**
   * Add a request to the queue and process it respecting rate limits
   * @param {Function} requestFn - Function that will make the actual request
   * @returns {Promise<any>} - Promise that resolves with the result of the request
   */
  async queueRequest(requestFn) {
    return new Promise((resolve, reject) => {
      // Add the request to the queue
      this.requestQueue.push({ requestFn, resolve, reject });
      
      // Start processing the queue if not already processing
      if (!this.isProcessingQueue) {
        this.processQueue();
      }
    });
  }
  
  /**
   * Process the queue of requests respecting rate limits
   */
  async processQueue() {
    if (this.requestQueue.length === 0) {
      this.isProcessingQueue = false;
      return;
    }
    
    this.isProcessingQueue = true;
    
    // Get the next request from the queue
    const { requestFn, resolve, reject } = this.requestQueue.shift();
    
    try {
      // Wait for rate limiter
      await this.rateLimiter();
      
      // Execute the request
      const result = await requestFn();
      resolve(result);
    } catch (error) {
      reject(error);
    }
    
    // Process the next request in the queue
    setTimeout(() => this.processQueue(), 0);
  }

  /**
   * Make a request to the API
   * @param {string} method - HTTP method (GET, POST, PUT, DELETE)
   * @param {string} endpoint - API endpoint
   * @param {Object} [data=null] - Request body for POST/PUT requests
   * @param {Object} [params={}] - Query parameters
   * @param {Object} [headers={}] - Additional headers
   * @returns {Promise<Object>} - API response
   */
  async request(method, endpoint, data = null, params = {}, headers = {}) {
    // Queue the request with rate limiting
    return this.queueRequest(async () => {
      try {
        const config = {
          method: method.toLowerCase(),
          url: endpoint,
          params,
          headers,
        };

        if (
          data &&
          (method.toLowerCase() === "post" || method.toLowerCase() === "put")
        ) {
          // For inventory updates, ensure prices are rounded to 2 decimal places
          if (endpoint.includes("/inventory")) {
            config.data = this.formatInventoryData(data);
          } else {
            config.data = data;
          }
        }

        const response = await this.apiClient(config);
        return response.data;
      } catch (error) {
        console.error(`Error making ${method} request to ${endpoint}:`, error);
        throw error;
      }
    });
  }
  
  /**
   * Format inventory data to ensure prices are rounded to 2 decimal places
   * @param {Object|Array} data - The inventory data to format
   * @returns {Object|Array} - Formatted inventory data
   */
  formatInventoryData(data) {
    if (Array.isArray(data)) {
      return data.map(item => this.formatInventoryItem(item));
    } else {
      return this.formatInventoryItem(data);
    }
  }
  
  /**
   * Format a single inventory item to ensure prices are rounded to 2 decimal places
   * @param {Object} item - The inventory item to format
   * @returns {Object} - Formatted inventory item
   */
  formatInventoryItem(item) {
    const formattedItem = { ...item };
    
    // Round listPrice to 2 decimal places if it exists
    if (formattedItem.listPrice !== undefined) {
      formattedItem.listPrice = parseFloat(parseFloat(formattedItem.listPrice).toFixed(2));
    }
    
    // If tickets array exists, round prices in tickets
    if (formattedItem.tickets && Array.isArray(formattedItem.tickets)) {
      formattedItem.tickets = formattedItem.tickets.map(ticket => {
        const formattedTicket = { ...ticket };
        
        // Round monetary values to 2 decimal places
        if (formattedTicket.cost !== undefined) {
          formattedTicket.cost = parseFloat(parseFloat(formattedTicket.cost).toFixed(2));
        }
        if (formattedTicket.faceValue !== undefined) {
          formattedTicket.faceValue = parseFloat(parseFloat(formattedTicket.faceValue).toFixed(2));
        }
        if (formattedTicket.taxedCost !== undefined) {
          formattedTicket.taxedCost = parseFloat(parseFloat(formattedTicket.taxedCost).toFixed(2));
        }
        if (formattedTicket.sellPrice !== undefined) {
          formattedTicket.sellPrice = parseFloat(parseFloat(formattedTicket.sellPrice).toFixed(2));
        }
        
        return formattedTicket;
      });
    }
    
    return formattedItem;
  }

  /**
   * Get account information
   * @returns {Promise<Object>} - Account information
   */
  async getAccount() {
    return this.request("GET", "/account");
  }

  /**
   * Get account notifications
   * @returns {Promise<Object>} - Account notifications
   */
  async getNotifications() {
    return this.request("GET", "/account/notifications");
  }

  /**
   * Search for events
   * @param {Object} params - Query parameters
   * @returns {Promise<Object>} - Events
   */
  async searchEvents(params = {}) {
    return this.request("GET", "/events", null, params);
  }

  /**
   * Get event details by ID
   * @param {string} eventId - Event ID
   * @returns {Promise<Object>} - Event details
   */
  async getEvent(eventId) {
    return this.request("GET", `/events/${eventId}`);
  }

  /**
   * Search for inventory
   * @param {Object} params - Query parameters
   * @returns {Promise<Object>} - Inventory
   */
  async searchInventory(params = {}) {
    return this.request("GET", "/inventory", null, params);
  }

  /**
   * Get inventory details by ID
   * @param {string} inventoryId - Inventory ID
   * @returns {Promise<Object>} - Inventory details
   */
  async getInventory(inventoryId) {
    return this.request("GET", `/inventory/${inventoryId}`);
  }

  /**
   * Update inventory pricing in bulk
   * @param {Array} updates - Array of inventory price updates
   * @returns {Promise<Object>} - Update result
   */
  async bulkUpdatePrice(updates) {
    return this.request("PUT", "/inventory", updates);
  }

  /**
   * Update inventory items
   * @param {Array} inventoryUpdates - Array of inventory updates
   * @returns {Promise<Object>} - Update result
   */
  async inventoryUpdates(inventoryUpdates) {
    return this.request("PUT", "/inventory", inventoryUpdates);
  }

  /**
   * Update a specific inventory by ID
   * @param {number|string} inventoryId - The ID of the inventory to update
   * @param {Object} inventory - Inventory update data
   * @param {boolean} [changeSeatNumbers=false] - Whether to allow changing seat numbers
   * @returns {Promise<void>} - Empty response on success
   */
  async inventoryUpdate(inventoryId, inventory, changeSeatNumbers = false) {
    const params = changeSeatNumbers ? { change_seat_numbers: true } : {};
    return this.request("PUT", `/inventory/${inventoryId}`, inventory, params);
  }

  /**
   * Cancel inventory items
   * @param {Array} inventoryCancellations - Array of inventory cancellations with IDs
   * @returns {Promise<Object>} - Cancellation result
   */
  async cancelInventory(inventoryCancellations) {
    return this.request("DELETE", "/inventory", inventoryCancellations);
  }

  /**
   * Bulk update inventory with specified properties
   * @param {Object} bulkUpdateRequest - Bulk inventory update request
   * @param {Array<number>} bulkUpdateRequest.inventory_ids - Inventory id's which will be updated
   * @param {string} [bulkUpdateRequest.public_notes_to_replace] - New public notes
   * @param {string} [bulkUpdateRequest.public_notes_to_add] - Public notes to add to existing notes
   * @param {string} [bulkUpdateRequest.internal_notes_to_replace] - New internal notes
   * @param {string} [bulkUpdateRequest.internal_notes_to_add] - Internal notes to add to existing notes
   * @param {string} [bulkUpdateRequest.split_type] - Split type to update
   * @param {string} [bulkUpdateRequest.custom_split] - Valid split quantities
   * @param {string} [bulkUpdateRequest.seat_type] - Seat type to update
   * @param {number} [bulkUpdateRequest.in_hand_days_before_event] - In-hand days before event to update
   * @param {string} [bulkUpdateRequest.stock_type] - Stock type to update
   * @param {boolean} [bulkUpdateRequest.zone_seating] - Zone seating
   * @param {boolean} [bulkUpdateRequest.hide_seat_numbers] - Hide seat numbers
   * @param {string} [bulkUpdateRequest.friendly_section] - Friendly Section
   * @param {string} [bulkUpdateRequest.section] - Section
   * @param {string} [bulkUpdateRequest.friendly_row] - Friendly Row
   * @param {string} [bulkUpdateRequest.row] - Row
   * @param {boolean} [bulkUpdateRequest.remove_barcode] - Barcodes should be removed
   * @param {boolean} [bulkUpdateRequest.remove_pdf] - Whether pdfs should be removed
   * @param {boolean} [bulkUpdateRequest.remove_external_ticket_id] - Whether external ticket IDs should be removed
   * @param {string} [bulkUpdateRequest.ticket_disclosure] - Ticket disclosure to update
   * @param {string} [bulkUpdateRequest.inventory_attribute] - Inventory attribute to update
   * @param {number} [bulkUpdateRequest.face_value] - Amount to set to each ticket's face value
   * @param {boolean} [bulkUpdateRequest.opt_out_auto_price] - Opt-out auto-price
   * @param {boolean} [bulkUpdateRequest.broadcast] - Broadcast to exchanges
   * @param {boolean} [bulkUpdateRequest.electronic_transfer] - Electronic transfer to set
   * @param {boolean} [bulkUpdateRequest.no_split] - Whether split type is full quantity (no splits)
   * @param {boolean} [bulkUpdateRequest.received] - Received status to be set to the purchases of the inventories
   * @param {string} [bulkUpdateRequest.vsr_option] - VSR option
   * @param {number} [bulkUpdateRequest.replenishment_group_id] - Replenishment group ID
   * @param {boolean} [bulkUpdateRequest.clear_disclosures] - Clear disclosures
   * @param {boolean} [bulkUpdateRequest.clear_attributes] - Clear attributes
   * @param {number} [bulkUpdateRequest.shown_quantity] - Shown quantity
   * @param {boolean} [bulkUpdateRequest.force] - Force update
   * @param {boolean} [bulkUpdateRequest.instant_transfer] - Received instant transfer value to be set to the inventories
   * @param {boolean} [bulkUpdateRequest.instant_transfer_opted_out] - Received instant transfer opted out value to be set to the inventories
   * @param {boolean} [bulkUpdateRequest.integrated_listing] - Integrated listing
   * @param {string} [bulkUpdateRequest.in_hand_date] - In-hand date to update (YYYY-MM-DD)
   * @returns {Promise<Object>} - Update result
   */
  async bulkInventoryUpdate(bulkUpdateRequest) {
    return this.request("PUT", "/inventory/bulk-update", bulkUpdateRequest);
  }

  /**
   * Get inventory price history by ID
   * @param {string} inventoryId - Inventory ID
   * @returns {Promise<Object>} - Price history
   */
  async getInventoryPriceHistory(inventoryId) {
    return this.request("GET", `/inventory/${inventoryId}/price-history`);
  }

  /**
   * Search for venues
   * @param {Object} params - Query parameters
   * @returns {Promise<Object>} - Venues
   */
  async searchVenues(params = {}) {
    return this.request("GET", "/venues", null, params);
  }

  /**
   * Get venue details by ID
   * @param {string} venueId - Venue ID
   * @returns {Promise<Object>} - Venue details
   */
  async getVenue(venueId) {
    return this.request("GET", `/venues/${venueId}`);
  }

  /**
   * Search for performers
   * @param {Object} params - Query parameters
   * @returns {Promise<Object>} - Performers
   */
  async getPerformers(params = {}) {
    return this.request("GET", "/events/performers", null, params);
  }

  /**
   * Get performer details by ID
   * @param {string} performerId - Performer ID
   * @returns {Promise<Object>} - Performer details
   */
  async getPerformer(performerId) {
    return this.request("GET", `/events/performers/${performerId}`);
  }

  /**
   * Search for invoices
   * @param {Object} params - Query parameters
   * @returns {Promise<Object>} - Invoices
   */
  async searchInvoices(params = {}) {
    return this.request("GET", "/invoices", null, params);
  }

  /**
   * Get invoice details by ID
   * @param {string} invoiceId - Invoice ID
   * @returns {Promise<Object>} - Invoice details
   */
  async getInvoice(invoiceId) {
    return this.request("GET", `/invoices/${invoiceId}`);
  }

  /**
   * Search for purchases
   * @param {Object} params - Query parameters
   * @returns {Promise<Object>} - Purchases
   */
  async searchPurchases(params = {}) {
    return this.request("GET", "/purchases", null, params);
  }

  /**
   * Get purchase details by ID
   * @param {string} purchaseId - Purchase ID
   * @returns {Promise<Object>} - Purchase details
   */
  async getPurchase(purchaseId) {
    return this.request("GET", `/purchases/${purchaseId}`);
  }

  /**
   * Create a new purchase
   * @param {Object} purchaseData - Purchase data
   * @returns {Promise<Object>} - Created purchase
   */
  async createPurchase(purchaseData) {
    return this.request("POST", "/purchases", purchaseData);
  }

  /**
   * Update a purchase
   * @param {string} purchaseId - Purchase ID
   * @param {Object} purchaseData - Purchase data
   * @returns {Promise<Object>} - Updated purchase
   */
  async updatePurchase(purchaseId, purchaseData) {
    return this.request("PUT", `/purchases/${purchaseId}`, purchaseData);
  }

  /**
   * Get inventory with standard admission tickets only
   * @param {Object} params - Query parameters
   * @returns {Promise<Object>} - Standard admission tickets
   */
  async getStandardAdmissionTickets(params = {}) {
    try {
      const inventory = await this.searchInventory(params);

      // Filter for standard admission tickets
      const standardTickets = inventory.filter((item) => {
        if (!item || !item.offers) return false;

        const offer = item.offers.find((o) => o.offerId === item.offerId);
        if (!offer) return false;

        const offerTypeMatch = offer.offerType
          ?.toLowerCase()
          ?.includes("standard");
        const offerNameMatch = offer.name?.toLowerCase()?.includes("standard");

        return offerTypeMatch || offerNameMatch;
      });

      return standardTickets;
    } catch (error) {
      console.error("Error fetching standard admission tickets:", error);
      throw error;
    }
  }
}

// Example usage:
/*
const skybox = new SkyboxClient({
  apiToken: "18123373-a111-400b-84ef-6651fb424d60",
  accountId: "2058",
  vendorId: "684306"
});

// Get account information
skybox.getAccount()
  .then(account => console.log('Account:', account))
  .catch(error => console.error('Error:', error));

// Search for events
skybox.searchEvents({ performer: "Concert" })
  .then(events => console.log('Events:', events))
  .catch(error => console.error('Error:', error));

// Get standard admission tickets
skybox.getStandardAdmissionTickets({ eventId: "12345" })
  .then(tickets => console.log('Standard Tickets:', tickets))
  .catch(error => console.error('Error:', error));
*/
