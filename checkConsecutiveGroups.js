import mongoose from 'mongoose';
import dotenv from 'dotenv';
import { ConsecutiveGroup } from './models/index.js';

dotenv.config();

const mongoUri = process.env.DATABASE_URL || 'mongodb://localhost:27017/ticketScraper';

async function checkConsecutiveGroups() {
  try {
    await mongoose.connect(mongoUri);
    console.log('Connected to MongoDB');
    
    // Count documents
    const count = await ConsecutiveGroup.countDocuments();
    console.log(`Total ConsecutiveGroup documents: ${count}`);
    
    if (count > 0) {
      // Get sample documents
      const docs = await ConsecutiveGroup.find().limit(2);
      console.log('\nSample documents:');
      console.log(JSON.stringify(docs, null, 2));
      
      // Check for inHandDate field
      console.log('\nChecking inHandDate field:');
      const docsWithInHandDate = await ConsecutiveGroup.find({ inHandDate: { $exists: true } }).countDocuments();
      console.log(`Documents with inHandDate field: ${docsWithInHandDate}`);
      
      // Check for specific event
      if (docs.length > 0) {
        const eventId = docs[0].eventId;
        console.log(`\nChecking event ${eventId}:`);
        const eventDocs = await ConsecutiveGroup.find({ eventId }).limit(5);
        
        // Compare event_date and inHandDate
        eventDocs.forEach(doc => {
          const eventDate = doc.event_date ? new Date(doc.event_date) : null;
          const inHandDate = doc.inHandDate ? new Date(doc.inHandDate) : null;
          
          console.log(`\nDocument ID: ${doc._id}`);
          console.log(`Event Date: ${eventDate ? eventDate.toISOString() : 'Not set'}`);
          console.log(`In Hand Date: ${inHandDate ? inHandDate.toISOString() : 'Not set'}`);
          
          if (eventDate && inHandDate) {
            // Calculate difference in days
            const diffTime = eventDate.getTime() - inHandDate.getTime();
            const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
            console.log(`Difference: ${diffDays} day(s)`);
          }
        });
      }
    } else {
      console.log('No ConsecutiveGroup documents found in the database.');
    }
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await mongoose.disconnect();
    console.log('\nDisconnected from MongoDB');
  }
}

checkConsecutiveGroups();