// resolvers.js - Résolveurs GraphQL pour l'API Gateway
// Communique avec les microservices via gRPC

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

// ─── Chargement des protos ────────────────────────────────────────
const hotelProtoDefinition = protoLoader.loadSync(
  path.join(__dirname, '../protos/hotel.proto'),
  { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true }
);
const chambreProtoDefinition = protoLoader.loadSync(
  path.join(__dirname, '../protos/chambre.proto'),
  { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true }
);
const reservationProtoDefinition = protoLoader.loadSync(
  path.join(__dirname, '../protos/reservation.proto'),
  { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true }
);

const hotelProto = grpc.loadPackageDefinition(hotelProtoDefinition).hotel;
const chambreProto = grpc.loadPackageDefinition(chambreProtoDefinition).chambre;
const reservationProto = grpc.loadPackageDefinition(reservationProtoDefinition).reservation;

// ─── Clients gRPC ─────────────────────────────────────────────────
const hotelClient = new hotelProto.HotelService(
  'localhost:50051',
  grpc.credentials.createInsecure()
);
const chambreClient = new chambreProto.ChambreService(
  'localhost:50052',
  grpc.credentials.createInsecure()
);
const reservationClient = new reservationProto.ReservationService(
  'localhost:50053',  // Machine 2
  grpc.credentials.createInsecure()
);

// ─── Helper : promisify gRPC call ────────────────────────────────
function grpcCall(client, method, request) {
  return new Promise((resolve, reject) => {
    client[method](request, (err, response) => {
      if (err) reject(err);
      else resolve(response);
    });
  });
}

// ─── Résolveurs ───────────────────────────────────────────────────
module.exports = {
  Query: {
    // 🏨 Hôtels
    hotel: (_, { id }) =>
      grpcCall(hotelClient, 'getHotel', { hotel_id: id }).then(r => r.hotel),

    hotels: (_, { ville }) =>
      grpcCall(hotelClient, 'searchHotels', { ville: ville || '' }).then(r => r.hotels),

    // 🛏️ Chambres
    chambre: (_, { id }) =>
      grpcCall(chambreClient, 'getChambre', { chambre_id: id }).then(r => r.chambre),

    chambresByHotel: (_, { hotel_id }) =>
      grpcCall(chambreClient, 'getChambresByHotel', { hotel_id }).then(r => r.chambres),

    // 📋 Réservations
    reservation: (_, { id }) =>
      grpcCall(reservationClient, 'getReservation', { reservation_id: id }).then(r => r.reservation),

    reservationsByClient: (_, { client_email }) =>
      grpcCall(reservationClient, 'getReservationsByClient', { client_email }).then(r => r.reservations),

    reservations: () =>
      grpcCall(reservationClient, 'searchReservations', {}).then(r => r.reservations),
  },

  Mutation: {
    // 🏨 Hôtels
    createHotel: (_, args) =>
      grpcCall(hotelClient, 'createHotel', args).then(r => r.hotel),

    updateHotel: (_, args) =>
      grpcCall(hotelClient, 'updateHotel', args).then(r => r.hotel),

    deleteHotel: (_, { id }) =>
      grpcCall(hotelClient, 'deleteHotel', { hotel_id: id }).then(r => r.message),

    // 🛏️ Chambres
    createChambre: (_, args) =>
      grpcCall(chambreClient, 'createChambre', args).then(r => r.chambre),

    updateChambre: (_, args) =>
      grpcCall(chambreClient, 'updateChambre', args).then(r => r.chambre),

    deleteChambre: (_, { id }) =>
      grpcCall(chambreClient, 'deleteChambre', { chambre_id: id }).then(r => r.message),

    // 📋 Réservations
    createReservation: (_, args) =>
      grpcCall(reservationClient, 'createReservation', args).then(r => r.reservation),

    annulerReservation: (_, { id }) =>
      grpcCall(reservationClient, 'annulerReservation', { reservation_id: id }).then(r => r.message),
  },
};
