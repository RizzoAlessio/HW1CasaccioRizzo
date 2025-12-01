CREATE DATABASE IF NOT EXISTS userdatabase;

USE userdatabase;

CREATE TABLE IF NOT EXISTS utenti (
    email VARCHAR(255) PRIMARY KEY,
    nome VARCHAR(255),
    cognome VARCHAR(255),
    cf VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS richiest (
    request_key VARCHAR(255) PRIMARY KEY,
    _status INT NOT NULL,  
    _timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
