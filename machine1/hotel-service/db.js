// db.js - Base de données SQLite3 pour le Microservice Hôtels
const Database = require('better-sqlite3');
const path = require('path');

const DB_PATH = path.join(__dirname, 'hotels.db');

function initDB() {
  const db = new Database(DB_PATH);

  // Activer WAL pour de meilleures performances
  db.pragma('journal_mode = WAL');

  // Créer la table hotels
  db.exec(`
    CREATE TABLE IF NOT EXISTS hotels (
      id TEXT PRIMARY KEY,
      nom TEXT NOT NULL,
      ville TEXT NOT NULL,
      adresse TEXT NOT NULL,
      etoiles INTEGER NOT NULL DEFAULT 3,
      description TEXT
    )
  `);

  // Insérer des données initiales si la table est vide
  const count = db.prepare('SELECT COUNT(*) as cnt FROM hotels').get();
  if (count.cnt === 0) {
    const insert = db.prepare(
      'INSERT INTO hotels (id, nom, ville, adresse, etoiles, description) VALUES (?, ?, ?, ?, ?, ?)'
    );
    insert.run('h1', 'Hotel Carthage Palace', 'Tunis', 'Avenue Habib Bourguiba', 5, 'Hôtel de luxe au cœur de Tunis');
    insert.run('h2', 'Hotel Medina', 'Tunis', 'Rue de la Kasbah', 4, 'Hôtel traditionnel en pleine médina');
    insert.run('h3', 'Hotel Djerba Resort', 'Djerba', 'Zone Touristique', 4, 'Resort en bord de mer');
    insert.run('h4', 'Hotel Sousse Marina', 'Sousse', 'Port El Kantaoui', 3, 'Hôtel avec vue sur le port');
    console.log('🌱 Données initiales Hotels insérées');
  }

  return db;
}

module.exports = initDB;
