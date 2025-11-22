CREATE DATABASE IF NOT EXISTS datadatabase;

USE datadatabase;

CREATE TABLE IF NOT EXISTS aeroporti (
  icao CHAR(4) PRIMARY KEY NOT NULL,
  iata CHAR(3),                  
  name VARCHAR(255),
  city VARCHAR(255),
  country VARCHAR(255)
  --aggiungere poi la mail: salvare qui utenti interessati per quell'aeroporto
);

CREATE TABLE IF NOT EXISTS voli (
  icao24 VARCHAR(12) PRIMARY KEY,
  callsign VARCHAR(20),
  estDep CHAR(4),
  estArr CHAR(4),
  firstSeen INT,
  lastSeen INT,
  departureDate DATE,
  arrivalDate DATE
);
