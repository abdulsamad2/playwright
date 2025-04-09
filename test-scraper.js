import { ScrapeEvent } from './scraper.js';

// Test event ID
const eventId = '0400619496250E05'; // This is the event ID that was failing

async function runTest() {
  console.log('Testing ScrapeEvent with incomplete headers...');
  
  // Test case 1: Event with incomplete headers
  const eventWithIncompleteHeaders = {
    eventId,
    headers: {
      // Missing Cookie and User-Agent
    }
  };
  
  try {
    console.log('Running test with incomplete headers...');
    const result1 = await ScrapeEvent(eventWithIncompleteHeaders);
    console.log('Test with incomplete headers result:', result1 ? 'Success' : 'Failed');
  } catch (error) {
    console.error('Test with incomplete headers error:', error.message);
  }
  
  // Test case 2: Just event ID (standard flow)
  try {
    console.log('\nRunning test with just event ID...');
    const result2 = await ScrapeEvent(eventId);
    console.log('Test with just event ID result:', result2 ? 'Success' : 'Failed');
  } catch (error) {
    console.error('Test with just event ID error:', error.message);
  }
}

runTest().catch(console.error); 