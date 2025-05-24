/**
 * BroadcastManager handles the tracking of events that are being broadcasted
 * Events that are broadcasted will be included in the combined CSV file
 */
class BroadcastManager {
  constructor() {
    this.broadcastingEvents = new Set();
  }

  /**
   * Start broadcasting an event
   * @param {string} eventId - The ID of the event to broadcast
   * @returns {boolean} - Whether the operation was successful
   */
  startBroadcast(eventId) {
    if (!eventId) return false;
    
    this.broadcastingEvents.add(eventId);
    console.log(`Started broadcasting event: ${eventId}`);
    return true;
  }

  /**
   * Stop broadcasting an event
   * @param {string} eventId - The ID of the event to stop broadcasting
   * @returns {boolean} - Whether the operation was successful
   */
  stopBroadcast(eventId) {
    if (!eventId) return false;
    
    const result = this.broadcastingEvents.delete(eventId);
    if (result) {
      console.log(`Stopped broadcasting event: ${eventId}`);
    }
    return result;
  }

  /**
   * Check if an event is currently being broadcasted
   * @param {string} eventId - The ID of the event to check
   * @returns {boolean} - Whether the event is being broadcasted
   */
  isBroadcasting(eventId) {
    return this.broadcastingEvents.has(eventId);
  }

  /**
   * Get all broadcasting event IDs
   * @returns {string[]} - Array of event IDs that are being broadcasted
   */
  getBroadcastingEvents() {
    return Array.from(this.broadcastingEvents);
  }
  
  /**
   * Sync the broadcast status with the database
   * @param {Object} event - The event object with broadcasting status
   */
  syncEventStatus(event) {
    if (event && event.Event_ID) {
      if (event.broadcasting) {
        this.startBroadcast(event.Event_ID);
      } else {
        this.stopBroadcast(event.Event_ID);
      }
    }
  }
}

// Create and export a singleton instance
const broadcastManager = new BroadcastManager();
export default broadcastManager;
