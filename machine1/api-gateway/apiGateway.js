// apiGateway.js - API Gateway principale
// Port : 3000
// Expose : REST + GraphQL
// Communique avec : Hotel (50051), Chambre (50052), Reservation (50053) via gRPC

const express = require('express');
const { ApolloServer } = require('@apollo/server');
const { expressMiddleware } = require('@as-integrations/express4');
const cors = require('cors');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const fs = require('fs');

// ─── Chargement des protos ────────────────────────────────────────
const protoOptions = {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
};

const hotelProtoDefinition = protoLoader.loadSync(
  path.join(__dirname, '../protos/hotel.proto'), protoOptions
);
const chambreProtoDefinition = protoLoader.loadSync(
  path.join(__dirname, '../protos/chambre.proto'), protoOptions
);
const reservationProtoDefinition = protoLoader.loadSync(
  path.join(__dirname, '../protos/reservation.proto'), protoOptions
);

const hotelProto = grpc.loadPackageDefinition(hotelProtoDefinition).hotel;
const chambreProto = grpc.loadPackageDefinition(chambreProtoDefinition).chambre;
const reservationProto = grpc.loadPackageDefinition(reservationProtoDefinition).reservation;

// ─── Clients gRPC ─────────────────────────────────────────────────
function createHotelClient() {
  return new hotelProto.HotelService('localhost:50051', grpc.credentials.createInsecure());
}
function createChambreClient() {
  return new chambreProto.ChambreService('192.168.1.15:50052', grpc.credentials.createInsecure());
}
function createReservationClient() {
  return new reservationProto.ReservationService('192.168.1.15:50053', grpc.credentials.createInsecure());
}

// ─── GraphQL ──────────────────────────────────────────────────────
const resolvers = require('./resolvers');
const typeDefs = fs.readFileSync(path.join(__dirname, 'schema.gql'), 'utf8');

// ─── Express App ──────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());

// ─── Apollo / GraphQL ─────────────────────────────────────────────
const apolloServer = new ApolloServer({ typeDefs, resolvers });
apolloServer.start().then(() => {
  app.use('/graphql', cors(), express.json(), expressMiddleware(apolloServer));
  console.log('🚀 GraphQL disponible sur http://localhost:3000/graphql');
});

// ════════════════════════════════════════════════════════════════════
// ─── REST HOTELS ──────────────────────────────────────────────────
// ════════════════════════════════════════════════════════════════════

// GET /hotels — liste tous les hôtels (filtre optionnel ?ville=...)
app.get('/hotels', (req, res) => {
  const client = createHotelClient();
  const ville = req.query.ville || '';
  client.searchHotels({ ville }, (err, response) => {
    if (err) return res.status(500).json({ erreur: err.message });
    res.json(response.hotels);
  });
});

// GET /hotels/:id — détail d'un hôtel
app.get('/hotels/:id', (req, res) => {
  const client = createHotelClient();
  client.getHotel({ hotel_id: req.params.id }, (err, response) => {
    if (err) return res.status(err.code === 5 ? 404 : 500).json({ erreur: err.message });
    res.json(response.hotel);
  });
});

// POST /hotels — créer un hôtel
app.post('/hotels', (req, res) => {
  const client = createHotelClient();
  const { id, nom, ville, adresse, etoiles, description } = req.body;
  client.createHotel({ id, nom, ville, adresse, etoiles, description }, (err, response) => {
    if (err) return res.status(500).json({ erreur: err.message });
    res.status(201).json(response.hotel);
  });
});

// PUT /hotels/:id — modifier un hôtel
app.put('/hotels/:id', (req, res) => {
  const client = createHotelClient();
  const { nom, ville, adresse, etoiles, description } = req.body;
  client.updateHotel({ id: req.params.id, nom, ville, adresse, etoiles, description }, (err, response) => {
    if (err) return res.status(err.code === 5 ? 404 : 500).json({ erreur: err.message });
    res.json(response.hotel);
  });
});

// DELETE /hotels/:id — supprimer un hôtel
app.delete('/hotels/:id', (req, res) => {
  const client = createHotelClient();
  client.deleteHotel({ hotel_id: req.params.id }, (err, response) => {
    if (err) return res.status(err.code === 5 ? 404 : 500).json({ erreur: err.message });
    res.json({ message: response.message });
  });
});

// ════════════════════════════════════════════════════════════════════
// ─── REST CHAMBRES ────────────────────────────────────────────────
// ════════════════════════════════════════════════════════════════════

// GET /chambres/:id — détail d'une chambre
app.get('/chambres/:id', (req, res) => {
  const client = createChambreClient();
  client.getChambre({ chambre_id: req.params.id }, (err, response) => {
    if (err) return res.status(err.code === 5 ? 404 : 500).json({ erreur: err.message });
    res.json(response.chambre);
  });
});

// GET /hotels/:hotel_id/chambres — chambres d'un hôtel
app.get('/hotels/:hotel_id/chambres', (req, res) => {
  const client = createChambreClient();
  client.getChambresByHotel({ hotel_id: req.params.hotel_id }, (err, response) => {
    if (err) return res.status(500).json({ erreur: err.message });
    res.json(response.chambres);
  });
});

// POST /chambres — créer une chambre
app.post('/chambres', (req, res) => {
  const client = createChambreClient();
  const { id, hotel_id, numero, type, prix, disponible } = req.body;
  client.createChambre({ id, hotel_id, numero, type, prix, disponible: disponible !== false }, (err, response) => {
    if (err) return res.status(500).json({ erreur: err.message });
    res.status(201).json(response.chambre);
  });
});

// PUT /chambres/:id — modifier une chambre
app.put('/chambres/:id', (req, res) => {
  const client = createChambreClient();
  const { hotel_id, numero, type, prix, disponible } = req.body;
  client.updateChambre({ id: req.params.id, hotel_id, numero, type, prix, disponible }, (err, response) => {
    if (err) return res.status(err.code === 5 ? 404 : 500).json({ erreur: err.message });
    res.json(response.chambre);
  });
});

// DELETE /chambres/:id — supprimer une chambre
app.delete('/chambres/:id', (req, res) => {
  const client = createChambreClient();
  client.deleteChambre({ chambre_id: req.params.id }, (err, response) => {
    if (err) return res.status(err.code === 5 ? 404 : 500).json({ erreur: err.message });
    res.json({ message: response.message });
  });
});

// ════════════════════════════════════════════════════════════════════
// ─── REST RESERVATIONS (proxy vers Machine 2) ─────────────────────
// ════════════════════════════════════════════════════════════════════

// GET /reservations — toutes les réservations
app.get('/reservations', (req, res) => {
  const client = createReservationClient();
  client.searchReservations({}, (err, response) => {
    if (err) return res.status(500).json({ erreur: err.message });
    res.json(response.reservations);
  });
});

// GET /reservations/:id — détail d'une réservation
app.get('/reservations/:id', (req, res) => {
  const client = createReservationClient();
  client.getReservation({ reservation_id: req.params.id }, (err, response) => {
    if (err) return res.status(err.code === 5 ? 404 : 500).json({ erreur: err.message });
    res.json(response.reservation);
  });
});

// GET /clients/:email/reservations — réservations d'un client
app.get('/clients/:email/reservations', (req, res) => {
  const client = createReservationClient();
  client.getReservationsByClient({ client_email: req.params.email }, (err, response) => {
    if (err) return res.status(500).json({ erreur: err.message });
    res.json(response.reservations);
  });
});

// POST /reservations — créer une réservation
app.post('/reservations', (req, res) => {
  const client = createReservationClient();
  const { id, chambre_id, hotel_id, client_nom, client_email, date_arrivee, date_depart, prix_total } = req.body;
  client.createReservation(
    { id, chambre_id, hotel_id, client_nom, client_email, date_arrivee, date_depart, prix_total },
    (err, response) => {
      if (err) return res.status(500).json({ erreur: err.message });
      res.status(201).json(response.reservation);
    }
  );
});

// DELETE /reservations/:id — annuler une réservation
app.delete('/reservations/:id', (req, res) => {
  const client = createReservationClient();
  client.annulerReservation({ reservation_id: req.params.id }, (err, response) => {
    if (err) return res.status(err.code === 5 ? 404 : 500).json({ erreur: err.message });
    res.json({ message: response.message });
  });
});

// ─── Démarrage ───────────────────────────────────────────────────
const PORT = 3000;
app.listen(PORT, () => {
  console.log(`\n🌐 API Gateway démarrée sur http://localhost:${PORT}`);
  console.log(`📡 REST  : http://localhost:${PORT}/hotels`);
  console.log(`📡 REST  : http://localhost:${PORT}/chambres`);
  console.log(`📡 REST  : http://localhost:${PORT}/reservations`);
  console.log(`🚀 GraphQL : http://localhost:${PORT}/graphql\n`);
});
