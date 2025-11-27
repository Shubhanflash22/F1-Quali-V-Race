// Using ES modules
import fetch from 'node-fetch';
import pg from 'pg';
import xlsx from 'xlsx';

const { Pool } = pg;

// Configure PostgreSQL connection
const pool = new Pool({
  user: 'Shubhan',
  host: 'postgres',
  database: 'f1-db',
  password: 'Rishita@28',
  port: 5432,
});

// GitHub repository information
const GITHUB_REPO = 'Shubhanflash22/F1-Quali-V-Race';
const GITHUB_RAW_URL = `https://raw.githubusercontent.com/${GITHUB_REPO}/refs/heads/main/Files/`;

// Function to fetch file from GitHub
async function fetchFileFromGithub(filename) {
  const url = GITHUB_RAW_URL + filename;
  console.log('Fetching URL:', url);
  
  try {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to fetch ${filename}: ${response.status}`);
    }
    
    // Get the file as array buffer
    const buffer = await response.arrayBuffer();
    return buffer;
  } catch (error) {
    console.error(`Error fetching ${filename}:`, error);
    throw error;
  }
}

// Function to read Excel file
async function readFile(filename) {
    try {
        const buffer = await fetchFileFromGithub(filename);
        const workbook = xlsx.read(buffer, { type: 'array' });
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        return xlsx.utils.sheet_to_json(worksheet);
    } catch (error) {
        console.error(`Error reading file ${filename}:`, error);
        throw error;
    }
}

// Function to extract year from filename
function extractYearFromFilename(filename) {
  const match = filename.match(/Formula1_(\d{4})/);
  console.log('Year', match);
  return match ? parseInt(match[1]) : null;
}

// Function to process qualifying data
function processQualifyingData(data, year, mappings) {
  // Create lookup maps for faster access
  const driverMap = new Map(mappings.driverMap.map(item => [item['Driver Name'], item['Unique Code']]));
  const constructorMap = new Map(mappings.constructorMap.map(item => [item['Constructor Name'], item['Unique Code']]));
  const trackMap = new Map(mappings.trackMap.map(item => [item['Track Name'], item['Unique Code']]));
  
  return data.map(row => {
    const trackCode = trackMap.get(row.Track) || 'UNKNOWN';
    const driverCode = driverMap.get(row.Driver) || 'UNKNOWN';
    const constructorCode = constructorMap.get(row.Team) || 'UNKNOWN';
    
    return {
      year,
      track_code: trackCode,
      driver_code: driverCode,
      constructor_code: constructorCode,
      position: row.Position,
    };
  });
}

// Function to process race data
function processRaceData(data, year, mappings) {
  // Create lookup maps for faster access
  const driverMap = new Map(mappings.driverMap.map(item => [item['Driver Name'], item['Unique Code']]));
  const constructorMap = new Map(mappings.constructorMap.map(item => [item['Constructor Name'], item['Unique Code']]));
  const trackMap = new Map(mappings.trackMap.map(item => [item['Track Name'], item['Unique Code']]));
  
  return data.map(row => {
    const trackCode = trackMap.get(row.Track) || 'UNKNOWN';
    const driverCode = driverMap.get(row.Driver) || 'UNKNOWN';
    const constructorCode = constructorMap.get(row.Team) || 'UNKNOWN';
    
    return {
      year,
      track_code: trackCode,
      driver_code: driverCode,
      constructor_code: constructorCode,
      position: row.Position,
      starting_grid: row['Starting Grid'],
    };
  });
}

// Function to insert data into database
async function insertData(table, data) {
  if (!data || data.length === 0) {
    console.log(`No data to insert into ${table}`);
    return;
  }
  
  // Get column names from the first object
  const columns = Object.keys(data[0]);
  
  for (const item of data) {
    const values = columns.map(col => item[col]);
    const placeholders = columns.map((_, i) => `$${i+1}`).join(', ');
    
    const query = {
      text: `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${placeholders}) ON CONFLICT DO NOTHING`,
      values: values,
    };
    
    try {
      await pool.query(query);
    } catch (error) {
      console.error(`Error inserting into ${table}:`, error);
      console.error('Query:', query);
    }
  }
  
  console.log(`Inserted ${data.length} rows into ${table}`);
}

// Function to create database tables if they don't exist
async function setupDatabase() {
  const createTablesQueries = [
    `
    CREATE TABLE IF NOT EXISTS qualifying_results (
      id SERIAL PRIMARY KEY,
      year INTEGER,
      track_code TEXT,
      driver_code TEXT,
      constructor_code TEXT,
      position TEXT
    )
    `,
    `
    CREATE TABLE IF NOT EXISTS race_results (
      id SERIAL PRIMARY KEY,
      year INTEGER,
      track_code TEXT,  
      driver_code TEXT,
      constructor_code TEXT,
      position TEXT,
      starting_grid INTEGER
    )
    `,
    `
    CREATE TABLE IF NOT EXISTS constructors (
      constructor_id TEXT PRIMARY KEY,
      constructor_name TEXT NOT NULL
    )
    `,
    `
    CREATE TABLE IF NOT EXISTS drivers (
      driver_id TEXT PRIMARY KEY,
      driver_name TEXT NOT NULL
    )
    `,
    `
    CREATE TABLE IF NOT EXISTS tracks (
      track_id TEXT PRIMARY KEY,
      track_name TEXT NOT NULL
    )
    `,
    `
    CREATE TABLE IF NOT EXISTS race_results_raw (
      race_id SERIAL PRIMARY KEY,
      meeting_key TEXT NOT NULL,
      circuit_key TEXT NOT NULL,
      date TEXT NOT NULL,
      year INTEGER NOT NULL,
      quali_positions TEXT NOT NULL,
      race_positions TEXT NOT NULL
    )
    `
  ];

  for (const query of createTablesQueries) {
    try {
      await pool.query(query);
    } catch (error) {
      console.error('Error creating table:', error);
      throw error;
    }
  }
  
  console.log('Database tables setup complete');
}

// Wait for PostgreSQL to be ready before starting
async function waitForPostgres() {
  let retries = 10;
  while (retries > 0) {
    try {
      const client = await pool.connect();
      client.release();
      console.log('Connected to PostgreSQL!');
      return true;
    } catch (err) {
      console.log('Waiting for PostgreSQL to start...', retries);
      retries--;
      await new Promise(resolve => setTimeout(resolve, 3000));
    }
  }
  return false;
}

// Main function to process F1 data
async function processF1Data() {
  try {
    // Load mapping data first
    console.log('Loading mapping data...');
    const driverMap = await readFile('Unique codes Drivers.csv');
    const constructorMap = await readFile('Unique codes Constructors.csv');
    const trackMap = await readFile('Unique codes Tracks.csv');

    const mappings = {
      driverMap: driverMap,
      constructorMap: constructorMap,
      trackMap: trackMap
    };
       
    // Files to process - matching the ones in your GitHub image
    const qualifyingFiles = [
      'Formula1_2022season_qualifyingResults.csv',
      'Formula1_2023season_qualifyingResults.csv',
      'Formula1_2024season_qualifyingResults.csv',
      'Formula1_2025Season_QualifyingResults.csv'
    ];
    
    const raceFiles = [
      'Formula1_2022season_raceResults.csv',
      'Formula1_2023season_raceResults.csv',
      'Formula1_2024season_raceResults.csv',
      'Formula1_2025Season_RaceResults.csv'
    ];
    
    // Process qualifying files
    for (const file of qualifyingFiles) {
      console.log(`Processing qualifying file: ${file}`);
      const year = extractYearFromFilename(file);
      const data = await readFile(file);
      const processedData = processQualifyingData(data, year, mappings);
      await insertData('qualifying_results', processedData);
    }
    
    // Process race files
    for (const file of raceFiles) {
      console.log(`Processing race file: ${file}`);
      const year = extractYearFromFilename(file);
      const data = await readFile(file);
      const processedData = processRaceData(data, year, mappings);
      await insertData('race_results', processedData);
    }

    // Create the Maps correctly based on the data structure
    const driverData = await readFile('Unique codes Drivers.csv');
    const constructorData = await readFile('Unique codes Constructors.csv');
    const trackData = await readFile('Unique codes Tracks.csv');
    const driverMap_topush = new Map(driverData.map(row => [row['Driver Name'], row['Unique Code']]));
    const constructorMap_topush = new Map(constructorData.map(row => [row['Constructor Name'], row['Unique Code']]));
    const trackMap_topush = new Map(trackData.map(row => [row['Track Name'], row['Unique Code']]));
    const mappings_topush = {
      driverMap: driverMap_topush,
      constructorMap: constructorMap_topush,
      trackMap: trackMap_topush
    };
    
    // Optionally process and insert data into constructors, drivers, and tracks tables
    await insertData('constructors', Array.from(mappings_topush.constructorMap).map(([constructor_name, constructor_id]) => ({ constructor_id: constructor_id, constructor_name: constructor_name })));
    await insertData('drivers', Array.from(mappings_topush.driverMap).map(([driver_name, driver_id]) => ({ driver_id: driver_id, driver_name: driver_name })));
    await insertData('tracks', Array.from(mappings_topush.trackMap).map(([track_name, track_id]) => ({ track_id: track_id, track_name: track_name })));
    console.log('F1 data processing complete!');
  } catch (error) {
    console.error('Error processing F1 data:', error);
  }
}

// Main execution
async function main() {
  try {
    const isPostgresReady = await waitForPostgres();
    
    if (!isPostgresReady) {
      console.error('Failed to connect to PostgreSQL after multiple attempts. Exiting.');
      process.exit(1);
    }
    
    // Setup database tables
    await setupDatabase();
    
    // Process F1 data
    await processF1Data();
  } catch (error) {
    console.error('Fatal error:', error);
  } finally {
    await pool.end();
  }
}

main();