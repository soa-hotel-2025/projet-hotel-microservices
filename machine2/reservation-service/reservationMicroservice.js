// reservationMicroservice.js - Microservice Réservations
// Port gRPC : 50053
// Base de données : RxDB (NoSQL)
// Kafka : Producteur (reservation-created, reservation-annulee)
//         Consommateur (hotel-deleted)

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const { Kafka } = require('kafkajs');
const connectDB = require('./db');

// ─── Chargement du proto ─────────────────────────────────────────
const reservationProtoDefinition = protoLoader.loadSync(
  path.join(__dirname, '../protos/reservation.proto'),
  {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  }
);
const reservationProto = grpc.loadPackageDefinition(reservationProtoDefinition).reservation;

// ─── Kafka ────────────────────────────────────────────────────────
const kafka = new Kafka({
  clientId: 'reservation-service',
  brokers: ['192.168.1.25:9092'],
  retry: { retries: 3 },
});
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'reservation-group' });

async function connectKafka(db) {
  try {
    // Producteur
    await producer.connect();
    console.log('✅ Kafka producer connecté (Reservation Service)');

    // Consommateur : écouter hotel-deleted pour annuler les réservations liées
    await consumer.connect();
    await consumer.subscribe({ topic: 'hotel-deleted', fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        const data = JSON.parse(message.value.toString());
        console.log(`📥 Événement Kafka reçu : ${topic}`, data);

        if (topic === 'hotel-deleted') {
          // Annuler toutes les réservations de cet hôtel
          const reservations = await db.reservations
            .find({ selector: { hotel_id: data.id, statut: 'confirmee' } })
            .exec();
          for (const r of reservations) {
            await r.patch({ statut: 'annulee' });
            console.log(`🚫 Réservation ${r.id} annulée suite à suppression hôtel`);
          }
        }
      },
    });
    console.log('✅ Kafka consumer connecté (Reservation Service)');
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

// ─── Démarrage du serveur gRPC ────────────────────────────────────
async function startServer() {
  const db = await connectDB();
  await connectKafka(db);

  const reservationsCollection = db.reservations;

  // ─── Implémentation des services gRPC ──────────────────────────
  const reservationServiceImpl = {
    getReservation: async (call, callback) => {
      try {
        const reservation = await reservationsCollection
          .findOne({ selector: { id: call.request.reservation_id } })
          .exec();
        if (!reservation) {
          return callback({ code: grpc.status.NOT_FOUND, message: 'Réservation non trouvée' });
        }
        callback(null, {
          reservation: {
            id: reservation.id,
            chambre_id: reservation.chambre_id,
            hotel_id: reservation.hotel_id,
            client_nom: reservation.client_nom,
            client_email: reservation.client_email,
            date_arrivee: reservation.date_arrivee,
            date_depart: reservation.date_depart,
            statut: reservation.statut,
            prix_total: reservation.prix_total,
          },
        });
      } catch (err) {
        callback({ code: grpc.status.INTERNAL, message: err.message });
      }
    },

    getReservationsByClient: async (call, callback) => {
      try {
        const reservations = await reservationsCollection
          .find({ selector: { client_email: call.request.client_email } })
          .exec();
        callback(null, {
          reservations: reservations.map(r => ({
            id: r.id,
            chambre_id: r.chambre_id,
            hotel_id: r.hotel_id,
            client_nom: r.client_nom,
            client_email: r.client_email,
            date_arrivee: r.date_arrivee,
            date_depart: r.date_depart,
            statut: r.statut,
            prix_total: r.prix_total,
          })),
        });
      } catch (err) {
        callback({ code: grpc.status.INTERNAL, message: err.message });
      }
    },

    searchReservations: async (call, callback) => {
      try {
        const reservations = await reservationsCollection.find().exec();
        callback(null, {
          reservations: reservations.map(r => ({
            id: r.id,
            chambre_id: r.chambre_id,
            hotel_id: r.hotel_id,
            client_nom: r.client_nom,
            client_email: r.client_email,
            date_arrivee: r.date_arrivee,
            date_depart: r.date_depart,
            statut: r.statut,
            prix_total: r.prix_total,
          })),
        });
      } catch (err) {
        callback({ code: grpc.status.INTERNAL, message: err.message });
      }
    },

    createReservation: async (call, callback) => {
      try {
        const {
          id, chambre_id, hotel_id, client_nom, client_email,
          date_arrivee, date_depart, prix_total,
        } = call.request;

        const exists = await reservationsCollection.findOne({ selector: { id } }).exec();
        if (exists) {
          return callback({ code: grpc.status.ALREADY_EXISTS, message: 'Réservation déjà existante' });
        }

        const reservation = await reservationsCollection.insert({
          id, chambre_id, hotel_id, client_nom, client_email,
          date_arrivee, date_depart, statut: 'confirmee', prix_total,
        });

        // Publier événement Kafka → Chambre Service met à jour la disponibilité
        await publishEvent('reservation-created', {
          id, chambre_id, hotel_id, client_nom, client_email,
          date_arrivee, date_depart,
        });
        console.log(`📢 Événement Kafka publié : reservation-created [${id}] - Chambre: ${chambre_id}`);

        callback(null, {
          reservation: {
            id: reservation.id,
            chambre_id: reservation.chambre_id,
            hotel_id: reservation.hotel_id,
            client_nom: reservation.client_nom,
            client_email: reservation.client_email,
            date_arrivee: reservation.date_arrivee,
            date_depart: reservation.date_depart,
            statut: reservation.statut,
            prix_total: reservation.prix_total,
          },
        });
      } catch (err) {
        callback({ code: grpc.status.INTERNAL, message: err.message });
      }
    },

    annulerReservation: async (call, callback) => {
      try {
        const { reservation_id } = call.request;
        const reservation = await reservationsCollection
          .findOne({ selector: { id: reservation_id } })
          .exec();
        if (!reservation) {
          return callback({ code: grpc.status.NOT_FOUND, message: 'Réservation non trouvée' });
        }
        if (reservation.statut === 'annulee') {
          return callback({ code: grpc.status.FAILED_PRECONDITION, message: 'Réservation déjà annulée' });
        }

        const chambre_id = reservation.chambre_id;
        await reservation.patch({ statut: 'annulee' });

        // Publier événement Kafka → Chambre Service remet la chambre disponible
        await publishEvent('reservation-annulee', {
          id: reservation_id,
          chambre_id,
        });
        console.log(`📢 Événement Kafka publié : reservation-annulee [${reservation_id}] - Chambre: ${chambre_id}`);

        callback(null, { message: `Réservation ${reservation_id} annulée avec succès` });
      } catch (err) {
        callback({ code: grpc.status.INTERNAL, message: err.message });
      }
    },
  };

  const server = new grpc.Server();
  server.addService(reservationProto.ReservationService.service, reservationServiceImpl);

  server.bindAsync(
    '0.0.0.0:50053',
    grpc.ServerCredentials.createInsecure(),
    (err, port) => {
      if (err) {
        console.error('❌ Erreur démarrage Reservation microservice:', err);
        return;
      }
      console.log(`📋 Reservation microservice démarré sur le port ${port}`);
    }
  );
}

startServer();
