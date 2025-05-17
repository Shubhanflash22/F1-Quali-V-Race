// F1 Database Structure and API Data Collection

// Define database schema
const dbSchema = {
  // Constructor table
  constructors: {
    constructor_id: "INTEGER PRIMARY KEY",
    constructor_name: "TEXT NOT NULL"
  },
  
  // Driver table
  drivers: {
    driver_id: "INTEGER PRIMARY KEY", // Using driver_number as the ID
    driver_name: "TEXT NOT NULL",
    team_name: "TEXT",
    country_code: "TEXT"
  },
  
  // Track table
  tracks: {
    track_id: "INTEGER PRIMARY KEY",
    track_name: "TEXT NOT NULL",
    circuit_key: "TEXT NOT NULL",
    country_name: "TEXT",
    location: "TEXT"
  },
  
  // Race results table
  race_results: {
    race_id: "INTEGER PRIMARY KEY",
    meeting_key: "TEXT NOT NULL",
    circuit_key: "TEXT NOT NULL",
    date: "TEXT NOT NULL",
    year: "INTEGER NOT NULL",
    
    // Store qualifying and race positions as JSON
    quali_positions: "TEXT NOT NULL", // JSON string of position data
    race_positions: "TEXT NOT NULL"   // JSON string of position data
  }
};

// Function to fetch data from OpenF1 API
async function fetchOpenF1Data(endpoint, params = {}) {
  const baseUrl = "https://api.openf1.org/v1/";
  const url = new URL(baseUrl + endpoint);
  
  // Add query parameters
  Object.keys(params).forEach(key => {
    if (params[key] !== undefined) {
      url.searchParams.append(key, params[key]);
    }
  });
  
  try {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`API request failed with status: ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.error(`Error fetching ${endpoint}:`, error);
    return [];
  }
}

// Function to get constructor data
// Note: OpenF1 doesn't have a direct constructor endpoint, so we'll derive it from team_name in drivers
async function getConstructors(year) {
  const drivers = await fetchOpenF1Data("drivers", { year });
  
  // Extract unique constructor (team) data
  const constructorsMap = new Map();
  let constructorId = 1;
  
  drivers.forEach(driver => {
    if (driver.team_name && !constructorsMap.has(driver.team_name)) {
      constructorsMap.set(driver.team_name, {
        constructor_id: constructorId++,
        constructor_name: driver.team_name
      });
    }
  });
  
  return Array.from(constructorsMap.values());
}

// Function to get driver data
async function getDrivers(year) {
  const drivers = await fetchOpenF1Data("drivers", { year });
  
  // Process driver data
  return drivers.map(driver => ({
    driver_id: driver.driver_number,
    driver_name: driver.full_name,
    team_name: driver.team_name,
    country_code: driver.country_code
  }));
}

// Function to get track data
async function getTracks(year) {
  const meetings = await fetchOpenF1Data("meetings", { year });
  
  // Process track data
  let trackId = 1;
  return meetings.map(meeting => ({
    track_id: trackId++,
    track_name: meeting.circuit_short_name,
    circuit_key: meeting.circuit_key,
    country_name: meeting.country_name,
    location: meeting.location
  }));
}

// Function to get qualifying positions for a specific meeting
async function getQualifyingPositions(meetingKey) {
  // Find qualifying session key
  const sessions = await fetchOpenF1Data("sessions", { meeting_key: meetingKey });
  const qualifyingSession = sessions.find(s => s.session_name.includes("Qualifying"));
  
  if (!qualifyingSession) return [];
  
  // Get final qualifying positions
  const positions = await fetchOpenF1Data("position", { 
    session_key: qualifyingSession.session_key 
  });
  
  // Group by driver and get their final position
  const driverPositions = new Map();
  
  positions.forEach(pos => {
    driverPositions.set(pos.driver_number, pos.position);
  });
  
  // Convert to array of {constructor_id, driver_id} objects
  // We'll need to lookup constructor_id based on driver's team_name
  const result = [];
  
  for (let [driverNumber, position] of driverPositions.entries()) {
    // We'll populate the constructor_id later when combining data
    result[position-1] = { driver_id: driverNumber };
  }
  
  return result;
}

// Function to get race positions for a specific meeting
async function getRacePositions(meetingKey) {
  // Find race session key
  const sessions = await fetchOpenF1Data("sessions", { meeting_key: meetingKey });
  const raceSession = sessions.find(s => s.session_name.includes("Race"));
  
  if (!raceSession) return [];
  
  // Get final race positions
  const positions = await fetchOpenF1Data("position", { 
    session_key: raceSession.session_key 
  });
  
  // Group by driver and get their final position
  const driverPositions = new Map();
  
  positions.forEach(pos => {
    driverPositions.set(pos.driver_number, pos.position);
  });
  
  // Convert to array of {constructor_id, driver_id} objects
  const result = [];
  
  for (let [driverNumber, position] of driverPositions.entries()) {
    // We'll populate the constructor_id later when combining data
    result[position-1] = { driver_id: driverNumber };
  }
  
  return result;
}

// Main function to build complete database
async function buildF1Database(year) {
  // Get base data
  const constructors = await getConstructors(year);
  const drivers = await getDrivers(year);
  const tracks = await getTracks(year);
  
  // Build constructor lookup map
  const constructorLookup = new Map();
  constructors.forEach(c => {
    constructorLookup.set(c.constructor_name, c.constructor_id);
  });
  
  // Build driver team lookup map
  const driverTeamLookup = new Map();
  drivers.forEach(d => {
    driverTeamLookup.set(d.driver_id, d.team_name);
  });
  
  // Get all meetings for the year
  const meetings = await fetchOpenF1Data("meetings", { year });
  
  // Process each meeting to get race data
  const raceResults = [];
  let raceId = 1;
  
  for (const meeting of meetings) {
    // Get qualifying and race positions
    const qualiPositions = await getQualifyingPositions(meeting.meeting_key);
    const racePositions = await getRacePositions(meeting.meeting_key);
    
    // Add constructor_id to each position
    qualiPositions.forEach(pos => {
      const teamName = driverTeamLookup.get(pos.driver_id);
      pos.constructor_id = constructorLookup.get(teamName);
    });
    
    racePositions.forEach(pos => {
      const teamName = driverTeamLookup.get(pos.driver_id);
      pos.constructor_id = constructorLookup.get(teamName);
    });
    
    // Add to race results
    raceResults.push({
      race_id: raceId++,
      meeting_key: meeting.meeting_key,
      circuit_key: meeting.circuit_key,
      date: meeting.date_start,
      year: meeting.year,
      quali_positions: JSON.stringify(qualiPositions),
      race_positions: JSON.stringify(racePositions)
    });
  }
  
  // Return complete database structure
  return {
    constructors,
    drivers,
    tracks,
    race_results: raceResults
  };
}

// Example usage:
// buildF1Database(2023).then(db => {
//   console.log("Constructors:", db.constructors);
//   console.log("Drivers:", db.drivers);
//   console.log("Tracks:", db.tracks);
//   console.log("Race Results:", db.race_results);
// });

// SQL Creation statements for database tables
function generateSQLCreateStatements() {
  let sql = '';
  
  // Constructors table
  sql += `CREATE TABLE constructors (
  constructor_id INTEGER PRIMARY KEY,
  constructor_name TEXT NOT NULL
);\n\n`;

  // Drivers table
  sql += `CREATE TABLE drivers (
  driver_id INTEGER PRIMARY KEY,
  driver_name TEXT NOT NULL,
  team_name TEXT,
  country_code TEXT
);\n\n`;

  // Tracks table
  sql += `CREATE TABLE tracks (
  track_id INTEGER PRIMARY KEY,
  track_name TEXT NOT NULL,
  circuit_key TEXT NOT NULL,
  country_name TEXT,
  location TEXT
);\n\n`;

  // Race results table
  sql += `CREATE TABLE race_results (
  race_id INTEGER PRIMARY KEY,
  meeting_key TEXT NOT NULL,
  circuit_key TEXT NOT NULL,
  date TEXT NOT NULL,
  year INTEGER NOT NULL,
  quali_positions TEXT NOT NULL,
  race_positions TEXT NOT NULL
);\n\n`;

  return sql;
}

console.log(generateSQLCreateStatements());
