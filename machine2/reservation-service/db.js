// db.js - Base de données RxDB (NoSQL) pour le Microservice Réservations
const { createRxDatabase, addRxPlugin } = require('rxdb');
const { getRxStorageMemory } = require('rxdb/plugins/storage-memory');
const { RxDBQueryBuilderPlugin } = require('rxdb/plugins/query-builder');
const { RxDBMigrationPlugin } = require('rxdb/plugins/migration-schema');

addRxPlugin(RxDBQueryBuilderPlugin);
addRxPlugin(RxDBMigrationPlugin);

// ─── Schéma de la collection Réservations ────────────────────────
const reservationSchema = {
  version: 0,
  primaryKey: 'id',
  type: 'object',
  properties: {
    id: { type: 'string', maxLength: 100 },
    chambre_id: { type: 'string' },
    hotel_id: { type: 'string' },
    client_nom: { type: 'string' },
    client_email: { type: 'string' },
    date_arrivee: { type: 'string' },
    date_depart: { type: 'string' },
    statut: { type: 'string', default: 'confirmee' },
    prix_total: { type: 'number' },
  },
  required: ['id', 'chambre_id', 'hotel_id', 'client_nom', 'client_email', 'date_arrivee', 'date_depart'],
};

let dbInstance = null;

async function connectDB() {
  if (dbInstance) return dbInstance;

  const db = await createRxDatabase({
    name: 'reservationsdb',
    storage: getRxStorageMemory(),
    ignoreDuplicate: true,
  });

  await db.addCollections({
    reservations: { schema: reservationSchema },
  });

  // Données initiales
  const existing = await db.reservations.find().exec();
  if (existing.length === 0) {
    await db.reservations.bulkInsert([
      {
        id: 'r1',
        chambre_id: 'c1',
        hotel_id: 'h1',
        client_nom: 'Ahmed Ben Ali',
        client_email: 'ahmed@example.com',
        date_arrivee: '2025-07-01',
        date_depart: '2025-07-05',
        statut: 'confirmee',
        prix_total: 600.0,
      },
      {
        id: 'r2',
        chambre_id: 'c4',
        hotel_id: 'h2',
        client_nom: 'Fatma Mansouri',
        client_email: 'fatma@example.com',
        date_arrivee: '2025-08-10',
        date_depart: '2025-08-15',
        statut: 'confirmee',
        prix_total: 400.0,
      },
    ]);
    console.log('🌱 Données initiales Réservations insérées');
  }

  dbInstance = db;
  return db;
}

module.exports = connectDB;
