# 🏨 Plateforme de Réservation d'Hôtels — Machine 1

## 👨‍💻 Responsabilités (Binôme 1)

Cette machine contient :
- **API Gateway** (port 3000) — REST + GraphQL + routage gRPC vers tous les microservices
- **Hotel Microservice** (port gRPC 50051) — CRUD Hôtels, SQLite3, Kafka producer

---

## 🏗️ Architecture

```
Client (REST/GraphQL)
       │
       ▼
  API Gateway :3000
  ├── REST  → gRPC → Hotel Service :50051 → SQLite3 (hotels.db)
  ├── REST  → gRPC → Chambre Service :50052 (Machine 2)
  ├── REST  → gRPC → Reservation Service :50053 (Machine 2)
  └── GraphQL (Apollo)
       ├── Hotels (local)
       └── Chambres & Réservations (Machine 2)

Kafka Broker :9092 (partagé entre les deux machines)
  ├── hotel-created     → produit par Hotel Service
  ├── hotel-updated     → produit par Hotel Service
  └── hotel-deleted     → produit par Hotel Service (consommé par Reservation Service M2)
```

---

## 📁 Structure

```
machine1/
├── protos/
│   ├── hotel.proto
│   ├── chambre.proto
│   └── reservation.proto
├── api-gateway/
│   ├── apiGateway.js        ← Express + Apollo + gRPC clients
│   ├── resolvers.js         ← Résolveurs GraphQL
│   ├── schema.gql           ← Schéma GraphQL
│   └── package.json
├── hotel-service/
│   ├── hotelMicroservice.js ← gRPC server + Kafka producer
│   ├── db.js                ← SQLite3
│   └── package.json
└── README.md
```

---

## ⚙️ Prérequis

- Node.js v18+
- Apache Kafka (avec Zookeeper) sur `localhost:9092`
- Machine 2 accessible sur le réseau (pour les services Chambre et Réservation)

### Démarrer Kafka
```bash
# Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties &

# Kafka
bin/kafka-server-start.sh config/server.properties &

# Créer les topics
bin/kafka-topics.sh --create --topic hotel-created --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic hotel-updated --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic hotel-deleted --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic reservation-created --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic reservation-annulee --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

---

## 🚀 Installation & Démarrage

### 1. Hotel Microservice
```bash
cd hotel-service
npm install
node hotelMicroservice.js
# → 🏨 Hotel microservice démarré sur le port 50051
```

### 2. API Gateway
```bash
cd api-gateway
npm install
node apiGateway.js
# → 🌐 API Gateway démarrée sur http://localhost:3000
```

> ⚠️ **Configuration réseau :** Dans `apiGateway.js`, s'assurer que les adresses des services de Machine 2 pointent vers l'IP correcte :
> ```js
> const chambreClient = new ChambreService('IP_MACHINE_2:50052', ...);
> const reservationClient = new ReservationService('IP_MACHINE_2:50053', ...);
> ```

---

## 📡 Endpoints REST

### Hôtels
| Méthode | URL | Description |
|---------|-----|-------------|
| GET | `/hotels` | Lister tous les hôtels |
| GET | `/hotels?ville=Tunis` | Filtrer par ville |
| GET | `/hotels/:id` | Détail d'un hôtel |
| POST | `/hotels` | Créer un hôtel |
| PUT | `/hotels/:id` | Modifier un hôtel |
| DELETE | `/hotels/:id` | Supprimer un hôtel |

### Chambres (proxy Machine 2)
| Méthode | URL | Description |
|---------|-----|-------------|
| GET | `/chambres/:id` | Détail d'une chambre |
| GET | `/hotels/:hotel_id/chambres` | Chambres d'un hôtel |
| POST | `/chambres` | Créer une chambre |
| PUT | `/chambres/:id` | Modifier une chambre |
| DELETE | `/chambres/:id` | Supprimer une chambre |

### Réservations (proxy Machine 2)
| Méthode | URL | Description |
|---------|-----|-------------|
| GET | `/reservations` | Toutes les réservations |
| GET | `/reservations/:id` | Détail d'une réservation |
| GET | `/clients/:email/reservations` | Réservations d'un client |
| POST | `/reservations` | Créer une réservation |
| DELETE | `/reservations/:id` | Annuler une réservation |

---

## 🚀 Exemples REST (curl)

```bash
# Lister les hôtels
curl http://localhost:3000/hotels

# Créer un hôtel
curl -X POST http://localhost:3000/hotels \
  -H "Content-Type: application/json" \
  -d '{"id":"h5","nom":"Hotel Test","ville":"Sfax","adresse":"Rue Test","etoiles":3,"description":"Test"}'

# Modifier un hôtel
curl -X PUT http://localhost:3000/hotels/h5 \
  -H "Content-Type: application/json" \
  -d '{"nom":"Hotel Test Updated","etoiles":4}'

# Supprimer un hôtel (→ déclenche hotel-deleted sur Kafka)
curl -X DELETE http://localhost:3000/hotels/h5

# Chambres d'un hôtel
curl http://localhost:3000/hotels/h1/chambres

# Créer une réservation
curl -X POST http://localhost:3000/reservations \
  -H "Content-Type: application/json" \
  -d '{"id":"r10","chambre_id":"c2","hotel_id":"h1","client_nom":"Sami Kaabi","client_email":"sami@test.com","date_arrivee":"2025-09-01","date_depart":"2025-09-05","prix_total":1000}'
```

---

## 🚀 Exemples GraphQL

Accès : `http://localhost:3000/graphql`

```graphql
# Lister tous les hôtels
query {
  hotels {
    id nom ville etoiles
  }
}

# Hôtels d'une ville
query {
  hotels(ville: "Tunis") {
    id nom adresse etoiles description
  }
}

# Créer un hôtel
mutation {
  createHotel(id: "h99", nom: "Grand Hotel", ville: "Tunis", adresse: "Av. test", etoiles: 5, description: "Luxe") {
    id nom ville
  }
}

# Créer une réservation (routée vers Machine 2)
mutation {
  createReservation(
    id: "r99", chambre_id: "c3", hotel_id: "h1",
    client_nom: "Leila Ben", client_email: "leila@test.com",
    date_arrivee: "2025-10-01", date_depart: "2025-10-07",
    prix_total: 3500
  ) {
    id statut prix_total
  }
}
```

---

## 📊 Topics Kafka (Machine 1)

| Topic | Rôle | Direction | Déclencheur |
|-------|------|-----------|-------------|
| `hotel-created` | Producteur | → Kafka | Création d'un hôtel |
| `hotel-updated` | Producteur | → Kafka | Modification d'un hôtel |
| `hotel-deleted` | Producteur | → Machine 2 | Suppression → annule réservations liées |

---

## 🗄️ Base de données

| Microservice | Type | Fichier | Tables |
|---|---|---|---|
| Hotel Service | SQLite3 (SQL) | `hotels.db` | `hotels` |
