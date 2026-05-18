# 🏨 Plateforme de Réservation d'Hôtels — Machine 2

## 👨‍💻 Responsabilités (Binôme 2)

Cette machine contient :
- **Chambre Microservice** (port gRPC 50052) — CRUD Chambres, SQLite3, Kafka consumer
- **Reservation Microservice** (port gRPC 50053) — CRUD Réservations, RxDB (NoSQL), Kafka producer/consumer

---

## 🏗️ Architecture

```
API Gateway :3000 (Machine 1)
       │ gRPC
       ├──────────────────────────────────┐
       ▼                                  ▼
Chambre Service :50052             Reservation Service :50053
  → SQLite3 (chambres.db)           → RxDB (NoSQL, en mémoire)
       │                                  │
       └──────────────┬───────────────────┘
                      ▼
            Kafka Broker :9092
  ├── CONSOMME ← reservation-created  (Chambre Service → chambre indisponible)
  ├── CONSOMME ← reservation-annulee  (Chambre Service → chambre disponible)
  ├── PRODUIT  → reservation-created  (Reservation Service)
  ├── PRODUIT  → reservation-annulee  (Reservation Service)
  └── CONSOMME ← hotel-deleted        (Reservation Service → annule réservations liées)
```

---

## 📁 Structure

```
machine2/
├── protos/
│   ├── hotel.proto
│   ├── chambre.proto
│   └── reservation.proto
├── chambre-service/
│   ├── chambreMicroservice.js  ← gRPC server + Kafka consumer
│   ├── db.js                   ← SQLite3
│   └── package.json
├── reservation-service/
│   ├── reservationMicroservice.js ← gRPC server + Kafka producer/consumer
│   ├── db.js                      ← RxDB (NoSQL)
│   └── package.json
└── README.md
```

---

## ⚙️ Prérequis

- Node.js v18+
- Apache Kafka sur `localhost:9092` (lancé sur Machine 1 ou partagé)
- Machine 1 accessible sur le réseau local (pour l'IP du broker Kafka)

> ⚠️ **Si Kafka tourne sur Machine 1**, modifier dans `chambreMicroservice.js` et `reservationMicroservice.js` :
> ```js
> brokers: ['IP_MACHINE_1:9092'],
> ```

---

## 🚀 Installation & Démarrage

### 1. Chambre Microservice
```bash
cd chambre-service
npm install
node chambreMicroservice.js
# → 🛏️  Chambre microservice démarré sur le port 50052
```

### 2. Reservation Microservice
```bash
cd reservation-service
npm install
node reservationMicroservice.js
# → 📋 Reservation microservice démarré sur le port 50053
```

> Les deux services doivent être démarrés avant que l'API Gateway (Machine 1) puisse les appeler.

---

## 📋 Interface gRPC exposée — Chambre Service

Définie dans `protos/chambre.proto` :

| RPC | Description |
|-----|-------------|
| `GetChambre` | Récupérer une chambre par ID |
| `GetChambresByHotel` | Chambres d'un hôtel |
| `CreateChambre` | Créer une chambre |
| `UpdateChambre` | Modifier une chambre |
| `DeleteChambre` | Supprimer une chambre |

## 📋 Interface gRPC exposée — Reservation Service

Définie dans `protos/reservation.proto` :

| RPC | Description |
|-----|-------------|
| `GetReservation` | Récupérer une réservation par ID |
| `GetReservationsByClient` | Réservations d'un client (email) |
| `SearchReservations` | Toutes les réservations |
| `CreateReservation` | Créer une réservation |
| `AnnulerReservation` | Annuler une réservation |

---

## 📊 Topics Kafka (Machine 2)

| Topic | Service | Rôle | Direction | Déclencheur |
|-------|---------|------|-----------|-------------|
| `reservation-created` | Reservation Service | Producteur | → Kafka | Nouvelle réservation confirmée |
| `reservation-annulee` | Reservation Service | Producteur | → Kafka | Annulation d'une réservation |
| `hotel-deleted` | Reservation Service | Consommateur | ← Machine 1 | Hôtel supprimé → annule réservations liées |
| `reservation-created` | Chambre Service | Consommateur | ← Kafka | Réservation créée → chambre indisponible |
| `reservation-annulee` | Chambre Service | Consommateur | ← Kafka | Annulation → chambre remise disponible |

---

## 🗄️ Bases de données

| Microservice | Type | Technologie | Détail |
|---|---|---|---|
| Chambre Service | SQL | SQLite3 | Fichier `chambres.db`, table `chambres` |
| Reservation Service | NoSQL | RxDB (en mémoire) | Collection `reservations` |

**Schéma d'une réservation :**
```json
{
  "id": "r1",
  "chambre_id": "c1",
  "hotel_id": "h1",
  "client_nom": "Ahmed Ben Ali",
  "client_email": "ahmed@example.com",
  "date_arrivee": "2025-07-01",
  "date_depart": "2025-07-05",
  "statut": "confirmee",
  "prix_total": 600.0
}
```

**Statuts possibles :** `confirmee` | `annulee`

---

## 🔗 Liaison avec Machine 1

| Port | Service | Sens |
|------|---------|------|
| `:50052` | Chambre Service (gRPC) | Machine 1 → Machine 2 |
| `:50053` | Reservation Service (gRPC) | Machine 1 → Machine 2 |
| `:9092` | Kafka Broker | Bidirectionnel |
