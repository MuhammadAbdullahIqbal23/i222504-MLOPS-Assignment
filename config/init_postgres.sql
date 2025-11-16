-- Create database for APOD data if it doesn't exist
-- Note: This will be run manually or via initialization script

-- Create table for NASA APOD data
CREATE TABLE IF NOT EXISTS nasa_apod_data (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    title VARCHAR(500) NOT NULL,
    explanation TEXT,
    url VARCHAR(1000),
    hdurl VARCHAR(1000),
    media_type VARCHAR(50),
    service_version VARCHAR(50),
    copyright VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on date for faster queries
CREATE INDEX IF NOT EXISTS idx_nasa_apod_date ON nasa_apod_data(date);

-- Create index on title for search
CREATE INDEX IF NOT EXISTS idx_nasa_apod_title ON nasa_apod_data(title);

