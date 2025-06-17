import mongoose from 'mongoose';
import dotenv from 'dotenv';
import { Event, ConsecutiveGroup } from './models/index.js';

dotenv.config();

const mongoUri = process.env.DATABASE_URL || 'mongodb://localhost:27017/ticketScraper';

/**
 * Clean up consecutive seats for stopped events
 * @param {boolean} dryRun - If true, only show what would be deleted without actually deleting
 */
async function cleanupConsecutiveSeats(dryRun = false) {
  try {
    await mongoose.connect(mongoUri);
    console.log('🔗 Connected to MongoDB');
    
    // Find all events that are stopped (Skip_Scraping: true)
    const stoppedEvents = await Event.find(
      { Skip_Scraping: true },
      { Event_ID: 1, Event_Name: 1, Skip_Scraping: 1 }
    );
    
    if (stoppedEvents.length === 0) {
      console.log('📊 No stopped events found');
      return;
    }
    
    console.log(`🔍 Found ${stoppedEvents.length} stopped events`);
    
    const stoppedEventIds = stoppedEvents.map(event => event.Event_ID);
    
    // Count consecutive seat groups for stopped events
    const consecutiveSeatsCount = await ConsecutiveGroup.countDocuments({
      eventId: { $in: stoppedEventIds }
    });
    
    if (consecutiveSeatsCount === 0) {
      console.log('✅ No consecutive seat groups found for stopped events');
      return;
    }
    
    console.log(`🎫 Found ${consecutiveSeatsCount} consecutive seat groups for stopped events`);
    
    if (dryRun) {
      console.log('\n🔍 DRY RUN - Would delete the following:');
      
      // Group by event for better reporting
      const groupsByEvent = await ConsecutiveGroup.aggregate([
        { $match: { eventId: { $in: stoppedEventIds } } },
        { 
          $group: {
            _id: '$eventId',
            count: { $sum: 1 },
            sections: { $addToSet: '$section' },
            totalSeats: { $sum: '$seatCount' }
          }
        }
      ]);
      
      for (const group of groupsByEvent) {
        const event = stoppedEvents.find(e => e.Event_ID === group._id);
        console.log(`  📍 Event: ${event?.Event_Name || group._id}`);
        console.log(`     - ${group.count} consecutive seat groups`);
        console.log(`     - ${group.totalSeats} total seats`);
        console.log(`     - Sections: ${group.sections.join(', ')}`);
      }
      
      console.log(`\n💡 Run with --execute flag to perform actual cleanup`);
    } else {
      console.log('\n🧹 Performing cleanup...');
      
      const deleteResult = await ConsecutiveGroup.deleteMany({
        eventId: { $in: stoppedEventIds }
      });
      
      console.log(`✅ Successfully deleted ${deleteResult.deletedCount} consecutive seat groups`);
      console.log(`🎯 Cleaned up data for ${stoppedEvents.length} stopped events`);
    }
    
  } catch (error) {
    console.error(`❌ Error during cleanup: ${error.message}`);
    throw error;
  } finally {
    await mongoose.disconnect();
    console.log('🔌 Disconnected from MongoDB');
  }
}

/**
 * Clean up consecutive seats for a specific event
 * @param {string} eventId - The specific event ID to clean up
 * @param {boolean} dryRun - If true, only show what would be deleted
 */
async function cleanupEventConsecutiveSeats(eventId, dryRun = false) {
  try {
    await mongoose.connect(mongoUri);
    console.log('🔗 Connected to MongoDB');
    
    // Check if event exists and is stopped
    const event = await Event.findOne(
      { Event_ID: eventId },
      { Event_ID: 1, Event_Name: 1, Skip_Scraping: 1 }
    );
    
    if (!event) {
      console.log(`❌ Event ${eventId} not found`);
      return;
    }
    
    if (!event.Skip_Scraping) {
      console.log(`⚠️ Event ${eventId} (${event.Event_Name}) is not stopped (Skip_Scraping: false)`);
      console.log('   Only stopped events should have their consecutive seats cleaned up');
      return;
    }
    
    // Count consecutive seat groups for this event
    const consecutiveSeatsCount = await ConsecutiveGroup.countDocuments({ eventId });
    
    if (consecutiveSeatsCount === 0) {
      console.log(`✅ No consecutive seat groups found for event ${eventId}`);
      return;
    }
    
    console.log(`🎫 Found ${consecutiveSeatsCount} consecutive seat groups for event ${eventId} (${event.Event_Name})`);
    
    if (dryRun) {
      console.log('\n🔍 DRY RUN - Would delete the following:');
      
      const groups = await ConsecutiveGroup.find(
        { eventId },
        { section: 1, row: 1, seatCount: 1, seatRange: 1 }
      ).limit(10);
      
      groups.forEach((group, index) => {
        console.log(`  ${index + 1}. Section: ${group.section}, Row: ${group.row}, Seats: ${group.seatRange} (${group.seatCount} seats)`);
      });
      
      if (consecutiveSeatsCount > 10) {
        console.log(`  ... and ${consecutiveSeatsCount - 10} more groups`);
      }
      
      console.log(`\n💡 Run with --execute flag to perform actual cleanup`);
    } else {
      console.log('\n🧹 Performing cleanup...');
      
      const deleteResult = await ConsecutiveGroup.deleteMany({ eventId });
      
      console.log(`✅ Successfully deleted ${deleteResult.deletedCount} consecutive seat groups for event ${eventId}`);
    }
    
  } catch (error) {
    console.error(`❌ Error during cleanup: ${error.message}`);
    throw error;
  } finally {
    await mongoose.disconnect();
    console.log('🔌 Disconnected from MongoDB');
  }
}

// Command line interface
if (import.meta.url === `file://${process.argv[1]}`) {
  const args = process.argv.slice(2);
  const dryRun = !args.includes('--execute');
  const eventId = args.find(arg => arg.startsWith('--event='))?.split('=')[1];
  
  console.log('🧹 Consecutive Seats Cleanup Utility');
  console.log('=====================================\n');
  
  if (eventId) {
    console.log(`🎯 Cleaning up specific event: ${eventId}`);
    if (dryRun) {
      console.log('🔍 DRY RUN MODE - No actual changes will be made\n');
    }
    cleanupEventConsecutiveSeats(eventId, dryRun);
  } else {
    console.log('🌐 Cleaning up all stopped events');
    if (dryRun) {
      console.log('🔍 DRY RUN MODE - No actual changes will be made\n');
    }
    cleanupConsecutiveSeats(dryRun);
  }
}

export { cleanupConsecutiveSeats, cleanupEventConsecutiveSeats };