CREATE DATABASE IF NOT EXISTS userdatabase;

USE userdatabase;

CREATE TABLE IF NOT EXISTS utenti (
    email VARCHAR(255) PRIMARY KEY,
    nome VARCHAR(255),
    cognome VARCHAR(255),
    cf VARCHAR(255)
);
