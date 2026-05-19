// db.js - Base de données SQLite3 pour le Microservice Chambres
const Database = require('better-sqlite3');
const path = require('path');

const DB_PATH = path.join(__dirname, 'chambres.db');

function initDB() {
  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');

  db.exec(`
    CREATE TABLE IF NOT EXISTS chambres (
      id TEXT PRIMARY KEY,
      hotel_id TEXT NOT NULL,
      numero TEXT NOT NULL,
      type TEXT NOT NULL,
      prix REAL NOT NULL DEFAULT 0,
      disponible INTEGER NOT NULL DEFAULT 1
    )
  `);

  const count = db.prepare('SELECT COUNT(*) as cnt FROM chambres').get();
  if (count.cnt === 0) {
    const insert = db.prepare(
      'INSERT INTO chambres (id, hotel_id, numero, type, prix, disponible) VALUES (?, ?, ?, ?, ?, ?)'
    );
    // Hôtel h1
    insert.run('c1', 'h1', '101', 'Simple', 150.0, 1);
    insert.run('c2', 'h1', '102', 'Double', 250.0, 1);
    insert.run('c3', 'h1', '201', 'Suite', 500.0, 1);
    // Hôtel h2
    insert.run('c4', 'h2', '101', 'Simple', 80.0, 1);
    insert.run('c5', 'h2', '102', 'Double', 130.0, 0);
    // Hôtel h3
    insert.run('c6', 'h3', '201', 'Suite', 350.0, 1);
    insert.run('c7', 'h3', '301', 'Double', 200.0, 1);
    // Hôtel h4
    insert.run('c8', 'h4', '101', 'Simple', 60.0, 1);
    console.log('🌱 Données initiales Chambres insérées');
  }

  return db;
}

module.exports = initDB;
