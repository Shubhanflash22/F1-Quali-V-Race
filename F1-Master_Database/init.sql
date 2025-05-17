-- Create tables for F1 database

-- Constructors table
CREATE TABLE IF NOT EXISTS constructors (
    constructor_id INTEGER PRIMARY KEY,
    constructor_name TEXT NOT NULL
);

-- Drivers table
CREATE TABLE IF NOT EXISTS drivers (
    driver_id INTEGER PRIMARY KEY,
    driver_name TEXT NOT NULL
);

-- Tracks table
CREATE TABLE IF NOT EXISTS tracks (
    track_id INTEGER PRIMARY KEY,
    track_name TEXT NOT NULL
);

-- Quali results table
CREATE TABLE IF NOT EXISTS qualifying_results (
      id SERIAL PRIMARY KEY,
      year INTEGER,
      track_code TEXT,
      driver_code TEXT,
      constructor_code TEXT,
      position INTEGER
);

-- Race Results Table
CREATE TABLE IF NOT EXISTS race_results (
      id SERIAL PRIMARY KEY,
      year INTEGER,
      track_code TEXT,  
      driver_code TEXT,
      constructor_code TEXT,
      position INTEGER,
      starting_grid INTEGER
)