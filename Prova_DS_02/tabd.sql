CREATE DATABASE IF NOT EXISTS datadatabase;

USE datadatabase;

CREATE TABLE IF NOT EXISTS aeroporti (
  icao CHAR(4) PRIMARY KEY NOT NULL,                 
  name VARCHAR(255),
  city VARCHAR(255),
  country VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS voli (
  icao24 VARCHAR(12) PRIMARY KEY,
  estDep CHAR(4),
  estArr CHAR(4),
  departureDate DATE,
  arrivalDate DATE
);

CREATE TABLE IF NOT EXISTS pref (
  id INT AUTO_INCREMENT PRIMARY KEY,
  email VARCHAR(255),
  icao CHAR(4),
  FOREIGN KEY (icao) REFERENCES aeroporti(icao),
  UNIQUE (email, icao)
);