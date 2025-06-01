import { MongoClient } from 'mongodb';
import fs from 'fs';
import path from 'path';

// --- Configuration ---
// IMPORTANT: Replace this with your actual SOURCE MongoDB connection URI.
const SOURCE_MONGO_URI = 'mongodb+srv://abdulsamadlaghari1:baAx0cXLpZCVmFhN@learning-with-mongoose.hrwr1gx.mongodb.net/scraper?retryWrites=true&w=majority&appName=Learning-with-mongoose'; // REPLACE THIS
const OUTPUT_DIRECTORY = path.join(process.cwd(), 'db_export');
// ---------------------

async function exportDatabaseToJson() {
  let client;
  console.log(`Starting database export from: ${SOURCE_MONGO_URI}`);
  console.log(`Output directory: ${OUTPUT_DIRECTORY}`);

  if (SOURCE_MONGO_URI.includes('yourSourceDatabaseName')) {
    console.warn('\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
    console.warn('WARNING: You are using a default/placeholder SOURCE_MONGO_URI.');
    console.warn('Please update SOURCE_MONGO_URI in exportDbToJson.js or set EXPORT_SOURCE_MONGO_URI environment variable.');
    console.warn('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n');
    // return; // Uncomment this line if you want to prevent running with placeholder URI
  }

  try {
    client = new MongoClient(SOURCE_MONGO_URI);
    await client.connect();
    const db = client.db(); // Gets the default database from the URI
    console.log(`Successfully connected to source database: ${db.databaseName}`);

    if (!fs.existsSync(OUTPUT_DIRECTORY)) {
      fs.mkdirSync(OUTPUT_DIRECTORY, { recursive: true });
      console.log(`Created output directory: ${OUTPUT_DIRECTORY}`);
    }

    const collections = await db.listCollections().toArray();
    console.log(`Found ${collections.length} collections: ${collections.map(c => c.name).join(', ')}`);

    for (const collectionInfo of collections) {
      const collectionName = collectionInfo.name;
      if (collectionName.startsWith('system.')) {
        console.log(`Skipping system collection: ${collectionName}`);
        continue;
      }

      console.log(`Exporting collection: ${collectionName}...`);
      const collection = db.collection(collectionName);
      const documents = await collection.find({}).toArray();
      
      const filePath = path.join(OUTPUT_DIRECTORY, `${collectionName}.json`);
      fs.writeFileSync(filePath, JSON.stringify(documents, null, 2));
      console.log(`Successfully exported ${documents.length} documents from '${collectionName}' to ${filePath}`);
    }

    console.log('\nDatabase export to JSON completed successfully!');

  } catch (error) {
    console.error('Error during database export:', error);
  } finally {
    if (client) {
      await client.close();
      console.log('Source MongoDB connection closed.');
    }
  }
}

exportDatabaseToJson();
