// hotelMicroservice.js - Microservice Hôtels
// Port gRPC : 50051
// Base de données : SQLite3

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const { Kafka } = require('kafkajs');
const initDB = require('./db');

// ─── Chargement du proto ─────────────────────────────────────────
const hotelProtoDefinition = protoLoader.loadSync(
  path.join(__dirname, '../protos/hotel.proto'),
  {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  }
);
const hotelProto = grpc.loadPackageDefinition(hotelProtoDefinition).hotel;

// ─── Initialisation de la base de données ────────────────────────
const db = initDB();

// ─── Kafka Producer ───────────────────────────────────────────────
const kafka = new Kafka({
  clientId: 'hotel-service',
  brokers: ['localhost:9092'],
  retry: { retries: 3 },
});
const producer = kafka.producer();

async function connectKafka() {
  try {
    await producer.connect();
    console.log('✅ Kafka producer connecté (Hotel Service)');
  } catch (err) {
    console.warn('⚠️  Kafka non disponible (mode dégradé):', err.message);
  }
}

async function publishEvent(topic, message) {
  try {
    await producer.send({
      topic,
      messages: [{ key: message.id || 'event', value: JSON.stringify(message) }],
    });
  } catch (err) {
    console.warn('⚠️  Impossible de publier sur Kafka:', err.message);
  }
}

// ─── Implémentation des services gRPC ────────────────────────────
const hotelServiceImpl = {
  // GET /hotel/:id
  getHotel: (call, callback) => {
    try {
      const hotel = db.prepare('SELECT * FROM hotels WHERE id = ?').get(call.request.hotel_id);
      if (!hotel) {
        return callback({ code: grpc.status.NOT_FOUND, message: 'Hôtel non trouvé' });
      }
      callback(null, { hotel });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  // GET /hotels (avec filtre ville optionnel)
  searchHotels: (call, callback) => {
    try {
      let hotels;
      const { ville } = call.request;
      if (ville && ville.trim() !== '') {
        hotels = db.prepare('SELECT * FROM hotels WHERE ville LIKE ?').all(`%${ville}%`);
      } else {
        hotels = db.prepare('SELECT * FROM hotels').all();
      }
      callback(null, { hotels });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  // POST /hotels
  createHotel: async (call, callback) => {
    try {
      const { id, nom, ville, adresse, etoiles, description } = call.request;
      const exists = db.prepare('SELECT id FROM hotels WHERE id = ?').get(id);
      if (exists) {
        return callback({ code: grpc.status.ALREADY_EXISTS, message: 'Hôtel déjà existant' });
      }
      db.prepare(
        'INSERT INTO hotels (id, nom, ville, adresse, etoiles, description) VALUES (?, ?, ?, ?, ?, ?)'
      ).run(id, nom, ville, adresse, etoiles || 3, description || '');
      const hotel = db.prepare('SELECT * FROM hotels WHERE id = ?').get(id);

      // Publier événement Kafka
      await publishEvent('hotel-created', { id, nom, ville, adresse, etoiles, description });
      console.log(`📢 Événement Kafka publié : hotel-created [${id}]`);

      callback(null, { hotel });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  // PUT /hotels/:id
  updateHotel: async (call, callback) => {
    try {
      const { id, nom, ville, adresse, etoiles, description } = call.request;
      const exists = db.prepare('SELECT id FROM hotels WHERE id = ?').get(id);
      if (!exists) {
        return callback({ code: grpc.status.NOT_FOUND, message: 'Hôtel non trouvé' });
      }
      db.prepare(
        'UPDATE hotels SET nom = ?, ville = ?, adresse = ?, etoiles = ?, description = ? WHERE id = ?'
      ).run(nom, ville, adresse, etoiles, description, id);
      const hotel = db.prepare('SELECT * FROM hotels WHERE id = ?').get(id);

      // Publier événement Kafka
      await publishEvent('hotel-updated', { id, nom, ville });
      console.log(`📢 Événement Kafka publié : hotel-updated [${id}]`);

      callback(null, { hotel });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  // DELETE /hotels/:id
  deleteHotel: async (call, callback) => {
    try {
      const { hotel_id } = call.request;
      const exists = db.prepare('SELECT id FROM hotels WHERE id = ?').get(hotel_id);
      if (!exists) {
        return callback({ code: grpc.status.NOT_FOUND, message: 'Hôtel non trouvé' });
      }
      db.prepare('DELETE FROM hotels WHERE id = ?').run(hotel_id);

      // Publier événement Kafka
      await publishEvent('hotel-deleted', { id: hotel_id });
      console.log(`📢 Événement Kafka publié : hotel-deleted [${hotel_id}]`);

      callback(null, { message: `Hôtel ${hotel_id} supprimé avec succès` });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },
};

// ─── Démarrage du serveur gRPC ────────────────────────────────────
async function startServer() {
  await connectKafka();

  const server = new grpc.Server();
  server.addService(hotelProto.HotelService.service, hotelServiceImpl);

  server.bindAsync(
    '0.0.0.0:50051',
    grpc.ServerCredentials.createInsecure(),
    (err, port) => {
      if (err) {
        console.error('❌ Erreur démarrage Hotel microservice:', err);
        return;
      }
      console.log(`🏨 Hotel microservice démarré sur le port ${port}`);
    }
  );
}

startServer();
