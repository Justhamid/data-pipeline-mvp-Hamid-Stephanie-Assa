Data Pipeline MVP — Deezer → Kafka → PostgreSQL (Docker)

Un mini-pipeline de données prêt à l’emploi pour collecter des métriques Deezer via API, les diffuser dans Kafka, et les stocker dans PostgreSQL pour analyse (DBeaver, SQL, etc.).
Le tout est dockerisé, avec un producer (ingestion API → Kafka) et un consumer (Kafka → Postgres).

⚙️ Stack

Docker & Docker Compose

Kafka (Confluent image en mode KRaft, sans ZooKeeper)

PostgreSQL 15

Python 3.11 (producer + consumer)

DBeaver (facultatif, pour visualiser la BDD)

🧱 Architecture
        +--------------------+
        |    Deezer API      |
        +---------+----------+
                  |
                  | (HTTP)
                  v
        +---------+----------+
        |     Producer       |  Python
        |  (requests → KFK)  |
        +---------+----------+
                  |
                  | (Kafka topic: deezer_tracks, etc.)
                  v
        +---------+----------+         +----------------------+
        |       Kafka        |         |     PostgreSQL       |
        | (broker/controller)|  --->   |   schema init.sql    |
        +---------+----------+         +----------+-----------+
                  ^                                 ^
                  |                                 |
                  | (KafkaConsumer)                 | (psycopg2)
                  +-------------+-------------------+
                                |
                                v
                         +------+------+
                         |   Consumer  |
                         | (KFK → PG)  |
                         +-------------+

📁 Structure du repo (résumé)
.
├─ docker/
│  ├─ docker-compose.yml
│  └─ ... (volumes: kafka-data/, pgdata/)
├─ db/
│  └─ init.sql               # crée les tables, indexes, vues matérialisées (si activées)
├─ producer/
│  └─ producer.py            # collecte Deezer, envoie sur Kafka (JSON)
├─ consumer/
│  └─ consumer.py            # consomme Kafka, upsert dans Postgres
├─ requirements.txt          # deps Python (producer + consumer)
├─ Dockerfile                # image Python commune
├─ .env                      # variables (tu le crées chez toi)
└─ README.md                 # ce fichier

🧩 Modèle de données (tables principales)

artists(artist_id PK, name, link, nb_fan, nb_album, ... )

albums(album_id PK, title, upc, release_date, artist_id FK, ...)

tracks(track_id PK, title, duration, rank, explicit_lyrics, album_id FK, artist_id FK, ...)

genres(genre_id PK, name, ... )

chart_snapshots(snapshot_time, source, genre_norm, artist_norm, track_id, position, ... )

Contrainte d’unicité pour éviter les doublons exacts par (temps, source, genre/artiste normalisés, track_id)

Des vues matérialisées (ex. mv_latest_chart, mv_daily_rank) peuvent être prévues dans init.sql (désactivables).

🔐 Variables d’environnement (.env)

Crée un fichier .env à la racine (jamais commité) :

# Postgres
POSTGRES_USER=Projet_MVP2025
POSTGRES_PASSWORD=Ynov2025

# Producer / Consumer
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
TOPIC=deezer_tracks

# (optionnel) Tweaks producteur
BATCH_PAGE_LIMIT=100       # Nb max d’items/page à collecter
SLEEP_BETWEEN_CALLS=10     # secondes entre cycles

# (optionnel) Tweaks consumer
MAX_WORKERS=4              # parallélisation inserts (si implémenté)

🚀 Lancement rapide

Docker up (depuis docker/)

docker compose up -d --build


Vérifier les logs

docker compose logs -f kafka
docker compose logs -f postgres
docker compose logs -f producer
docker compose logs -f consumer


Se connecter avec DBeaver

Host: localhost

Port: 5432

Database: datapipeline

User: Projet_MVP2025

Password: Ynov2025

Tester quelques requêtes

-- Combien de lignes par table ?
SELECT 'artists' AS t, COUNT(*) FROM artists
UNION ALL SELECT 'albums', COUNT(*) FROM albums
UNION ALL SELECT 'tracks', COUNT(*) FROM tracks
UNION ALL SELECT 'genres', COUNT(*) FROM genres
UNION ALL SELECT 'chart_snapshots', COUNT(*) FROM chart_snapshots;

-- Derniers snapshots
SELECT snapshot_time, source, track_id, position
FROM chart_snapshots
ORDER BY snapshot_time DESC
LIMIT 20;

🛠️ Commandes utiles
# reconstruire et relancer
docker compose up -d --build

# suivre les logs en live
docker compose logs -f producer
docker compose logs -f consumer
docker compose logs -f kafka
docker compose logs -f postgres

# psql dans le conteneur
docker exec -it postgres psql -U $POSTGRES_USER -d datapipeline

# arrêter
docker compose down

# tout réinitialiser (⚠️ supprime les données Kafka & PG)
docker compose down -v
rm -rf docker/kafka-data
# (et le volume nommé pgdata est supprimé par -v)

🔧 Paramétrage & perfs

Limites API Deezer : on collecte par pages (jusqu’à BATCH_PAGE_LIMIT), et on boucle par endpoints riches (charts, top artist, genres) pour maximiser la variété.

Déduplication : côté consumer, on fait des INSERT ... ON CONFLICT DO UPDATE/DO NOTHING selon la table.

Historique : chart_snapshots enregistre le temps (snapshot_time), la source (global/genre/artist_top), la position et le track_id.

Offsets Kafka : auto_offset_reset="earliest" garantit qu’au premier lancement on consomme l’historique du topic.