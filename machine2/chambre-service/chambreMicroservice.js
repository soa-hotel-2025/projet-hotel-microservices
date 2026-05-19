// chambreMicroservice.js - Microservice Chambres
// Port gRPC : 50052
// Base de données : SQLite3

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const { Kafka } = require('kafkajs');
const initDB = require('./db');

// ─── Chargement du proto ─────────────────────────────────────────
const chambreProtoDefinition = protoLoader.loadSync(
  path.join(__dirname, '../protos/chambre.proto'),
  {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  }
);
const chambreProto = grpc.loadPackageDefinition(chambreProtoDefinition).chambre;

// ─── Base de données ──────────────────────────────────────────────
const db = initDB();

// ─── Kafka Consumer (écoute les réservations) ────────────────────
const kafka = new Kafka({
  clientId: 'chambre-service',
  brokers: ['192.168.1.25:9092'],
  retry: { retries: 3 },
});
const consumer = kafka.consumer({ groupId: 'chambre-group' });

async function connectKafkaConsumer() {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: 'reservation-created', fromBeginning: false });
    await consumer.subscribe({ topic: 'reservation-annulee', fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        const data = JSON.parse(message.value.toString());
        console.log(`📥 Événement Kafka reçu : ${topic}`, data);

        if (topic === 'reservation-created') {
          // Marquer la chambre comme non disponible
          db.prepare('UPDATE chambres SET disponible = 0 WHERE id = ?').run(data.chambre_id);
          console.log(`🔒 Chambre ${data.chambre_id} marquée INDISPONIBLE`);
        } else if (topic === 'reservation-annulee') {
          // Remettre la chambre disponible
          db.prepare('UPDATE chambres SET disponible = 1 WHERE id = ?').run(data.chambre_id);
          console.log(`🔓 Chambre ${data.chambre_id} remise DISPONIBLE`);
        }
      },
    });
    console.log('✅ Kafka consumer connecté (Chambre Service)');
  } catch (err) {
    console.warn('⚠️  Kafka non disponible (mode dégradé):', err.message);
  }
}

// ─── Implémentation des services gRPC ────────────────────────────
const chambreServiceImpl = {
  getChambre: (call, callback) => {
    try {
      const chambre = db.prepare('SELECT * FROM chambres WHERE id = ?').get(call.request.chambre_id);
      if (!chambre) {
        return callback({ code: grpc.status.NOT_FOUND, message: 'Chambre non trouvée' });
      }
      callback(null, { chambre: { ...chambre, disponible: chambre.disponible === 1 } });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  getChambresByHotel: (call, callback) => {
    try {
      const chambres = db
        .prepare('SELECT * FROM chambres WHERE hotel_id = ?')
        .all(call.request.hotel_id)
        .map(c => ({ ...c, disponible: c.disponible === 1 }));
      callback(null, { chambres });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  createChambre: (call, callback) => {
    try {
      const { id, hotel_id, numero, type, prix, disponible } = call.request;
      const exists = db.prepare('SELECT id FROM chambres WHERE id = ?').get(id);
      if (exists) {
        return callback({ code: grpc.status.ALREADY_EXISTS, message: 'Chambre déjà existante' });
      }
      db.prepare(
        'INSERT INTO chambres (id, hotel_id, numero, type, prix, disponible) VALUES (?, ?, ?, ?, ?, ?)'
      ).run(id, hotel_id, numero, type, prix || 0, disponible !== false ? 1 : 0);
      const chambre = db.prepare('SELECT * FROM chambres WHERE id = ?').get(id);
      callback(null, { chambre: { ...chambre, disponible: chambre.disponible === 1 } });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  updateChambre: (call, callback) => {
    try {
      const { id, hotel_id, numero, type, prix, disponible } = call.request;
      const exists = db.prepare('SELECT id FROM chambres WHERE id = ?').get(id);
      if (!exists) {
        return callback({ code: grpc.status.NOT_FOUND, message: 'Chambre non trouvée' });
      }
      db.prepare(
        'UPDATE chambres SET hotel_id = ?, numero = ?, type = ?, prix = ?, disponible = ? WHERE id = ?'
      ).run(hotel_id, numero, type, prix, disponible !== false ? 1 : 0, id);
      const chambre = db.prepare('SELECT * FROM chambres WHERE id = ?').get(id);
      callback(null, { chambre: { ...chambre, disponible: chambre.disponible === 1 } });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  deleteChambre: (call, callback) => {
    try {
      const { chambre_id } = call.request;
      const exists = db.prepare('SELECT id FROM chambres WHERE id = ?').get(chambre_id);
      if (!exists) {
        return callback({ code: grpc.status.NOT_FOUND, message: 'Chambre non trouvée' });
      }
      db.prepare('DELETE FROM chambres WHERE id = ?').run(chambre_id);
      callback(null, { message: `Chambre ${chambre_id} supprimée avec succès` });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  updateDisponibilite: (call, callback) => {
    try {
      const { chambre_id, disponible } = call.request;
      const exists = db.prepare('SELECT id FROM chambres WHERE id = ?').get(chambre_id);
      if (!exists) {
        return callback({ code: grpc.status.NOT_FOUND, message: 'Chambre non trouvée' });
      }
      db.prepare('UPDATE chambres SET disponible = ? WHERE id = ?').run(disponible ? 1 : 0, chambre_id);
      const chambre = db.prepare('SELECT * FROM chambres WHERE id = ?').get(chambre_id);
      callback(null, { chambre: { ...chambre, disponible: chambre.disponible === 1 } });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },
};

// ─── Démarrage du serveur gRPC ────────────────────────────────────
async function startServer() {
  await connectKafkaConsumer();

  const server = new grpc.Server();
  server.addService(chambreProto.ChambreService.service, chambreServiceImpl);

  server.bindAsync(
    '0.0.0.0:50052',
    grpc.ServerCredentials.createInsecure(),
    (err, port) => {
      if (err) {
        console.error('❌ Erreur démarrage Chambre microservice:', err);
        return;
      }
      console.log(`🛏️  Chambre microservice démarré sur le port ${port}`);
    }
  );
}

startServer();
