-- SQLite schema for the OpenAQ data mart

PRAGMA foreign_keys = ON;

CREATE TABLE dim_location (
    location_id INTEGER PRIMARY KEY,
    location TEXT NOT NULL,
    city TEXT NOT NULL,
    country TEXT NOT NULL,
    latitude REAL,
    longitude REAL,
    attribution TEXT
);

CREATE UNIQUE INDEX idx_location_key ON dim_location(location, city, country);

CREATE TABLE dim_time (
    time_id INTEGER PRIMARY KEY,
    utc TEXT NOT NULL UNIQUE,
    local TEXT,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    day_of_week INTEGER,
    month_name TEXT
);

CREATE TABLE dim_parameter (
    parameter_id INTEGER PRIMARY KEY,
    parameter TEXT NOT NULL UNIQUE,
    name TEXT,
    unit TEXT
);

CREATE TABLE fact_measurement (
    measurement_id INTEGER PRIMARY KEY,
    location_id INTEGER NOT NULL,
    time_id INTEGER NOT NULL,
    parameter_id INTEGER NOT NULL,
    value REAL NOT NULL,
    is_interpolated INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY(location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY(time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY(parameter_id) REFERENCES dim_parameter(parameter_id)
);

CREATE INDEX idx_fact_location ON fact_measurement(location_id);
CREATE INDEX idx_fact_time ON fact_measurement(time_id);
CREATE INDEX idx_fact_parameter ON fact_measurement(parameter_id);

CREATE TABLE bad_data (
    bad_id INTEGER PRIMARY KEY,
    location TEXT,
    city TEXT,
    country TEXT,
    utc TEXT,
    local TEXT,
    parameter TEXT,
    value TEXT,
    unit TEXT,
    latitude TEXT,
    longitude TEXT,
    attribution TEXT,
    bad_reason TEXT
);
