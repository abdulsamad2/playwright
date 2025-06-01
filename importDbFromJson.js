import { MongoClient } from 'mongodb';
import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';

// Load environment variables from .env file
dotenv.config();

// --- Configuration ---
const TARGET_MONGO_URI = process.env.DATABASE_URL;
const INPUT_DIRECTORY = path.join(process.cwd(), 'db_export');
// ---------------------

async function importDatabaseFromJson() {
  let client;

  if (!TARGET_MONGO_URI) {
    console.error('Error: DATABASE_URL is not defined in your .env file or environment variables.');
    console.error('Please ensure DATABASE_URL is set correctly.');
    return;
  }

  console.log(`Starting database import to: ${TARGET_MONGO_URI.split('@')[1] || TARGET_MONGO_URI}`); // Mask credentials
  console.log(`Input directory: ${INPUT_DIRECTORY}`);

  if (!fs.existsSync(INPUT_DIRECTORY)) {
    console.error(`Error: Input directory ${INPUT_DIRECTORY} does not exist. Please run the export script first.`);
    return;
  }

  try {
    client = new MongoClient(TARGET_MONGO_URI);
    await client.connect();
    const db = client.db(); // Gets the default database from the URI
    console.log(`Successfully connected to target database: ${db.databaseName}`);

    const files = fs.readdirSync(INPUT_DIRECTORY).filter(file => file.endsWith('.json'));
    console.log(`Found ${files.length} JSON files in ${INPUT_DIRECTORY}: ${files.join(', ')}`);

    for (const file of files) {
      const collectionName = path.basename(file, '.json');
      const filePath = path.join(INPUT_DIRECTORY, file);

      console.log(`\nImporting data for collection: ${collectionName} from ${file}...`);
      
      const fileContent = fs.readFileSync(filePath, 'utf-8');
      let documents;
      try {
        documents = JSON.parse(fileContent);
      } catch (parseError) {
        console.error(`Error parsing JSON from file ${file}: ${parseError.message}`);
        console.log(`Skipping file ${file} due to parsing error.`);
        continue;
      }

      if (!Array.isArray(documents)) {
        console.error(`Error: Content of ${file} is not a JSON array. Skipping.`);
        continue;
      }

      if (documents.length === 0) {
        console.log(`No documents found in ${file}. Nothing to import for collection '${collectionName}'.`);
        continue;
      }

      const targetCollection = db.collection(collectionName);
      
      // Optional: Clear existing collection before import
      // try {
      //   await targetCollection.deleteMany({});
      //   console.log(`Cleared existing documents from target collection '${collectionName}'.`);
      // } catch (deleteError) {
      //   console.warn(`Could not clear target collection ${collectionName}: ${deleteError.message}`);
      // }

      try {
        const insertResult = await targetCollection.insertMany(documents, { ordered: false });
        console.log(`Successfully inserted ${insertResult.insertedCount} documents into target collection '${collectionName}'.`);
      } catch (insertError) {
        console.error(`Error inserting documents into '${collectionName}': ${insertError.message}`);
        if (insertError.writeErrors) {
          insertError.writeErrors.forEach(err => console.error(`  - Write Error Index ${err.index}: ${err.errmsg}`));
        }
      }
    }

    console.log('\nDatabase import from JSON completed!');

  } catch (error) {
    console.error('Error during database import:', error);
  } finally {
    if (client) {
      await client.close();
      console.log('Target MongoDB connection closed.');
    }
  }
}

importDatabaseFromJson();
