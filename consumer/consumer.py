import json
import psycopg2
from kafka import KafkaConsumer

# ---------------------------
# CONFIG
# ---------------------------
KAFKA_SERVER = "kafka:9092"
TOPICS = ["artists", "albums", "tracks"]

POSTGRES_HOST = "postgres"
POSTGRES_DB = "datapipeline"
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "admin"

# Timeout pour arrêter le consumer si pas de nouveau message
CONSUMER_TIMEOUT_MS = 20000  # 20 secondes sans message

# ---------------------------
# CONNECTION POSTGRES
# ---------------------------
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)
cur = conn.cursor()

# Créer les tables si elles n'existent pas
cur.execute("""
CREATE TABLE IF NOT EXISTS artists (
    id BIGINT PRIMARY KEY,
    name TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS albums (
    album_id BIGINT PRIMARY KEY,
    artist_id BIGINT,
    artist_name TEXT,
    album_title TEXT,
    release_date DATE,
    nb_tracks INT,
    cover TEXT,
    link TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS tracks (
    track_id BIGINT PRIMARY KEY,
    album_id BIGINT,
    track_title TEXT,
    duration_sec INT,
    rank INT,
    preview_url TEXT
)
""")
conn.commit()

# ---------------------------
# KAFKA CONSUMER
# ---------------------------
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='earliest',
    group_id='postgres_consumer',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=CONSUMER_TIMEOUT_MS
)

print(f"En attente des messages sur {TOPICS}…")

count = 0
for message in consumer:
    topic = message.topic
    data = message.value
    count += 1

    # Debug print
    if count % 50 == 0:
        print(f"{count} messages lus… (dernier topic: {topic})")

    # Insertion en base selon le topic
    if topic == "artists":
        cur.execute("""
            INSERT INTO artists (id, name)
            VALUES (%s, %s)
            ON CONFLICT (id) DO NOTHING
        """, (data["id"], data["name"]))
    elif topic == "albums":
        cur.execute("""
            INSERT INTO albums (album_id, artist_id, artist_name, album_title, release_date, nb_tracks, cover, link)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (album_id) DO NOTHING
        """, (
            data["album_id"],
            data["artist_id"],
            data["artist_name"],
            data["album_title"],
            data.get("release_date") or None,
            data.get("nb_tracks", 0),
            data.get("cover", ""),
            data.get("link", "")
        ))
    elif topic == "tracks":
        cur.execute("""
            INSERT INTO tracks (track_id, album_id, track_title, duration_sec, rank, preview_url)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (track_id) DO NOTHING
        """, (
            data["track_id"],
            data["album_id"],
            data["track_title"],
            data.get("duration_sec", 0),
            data.get("rank", 0),
            data.get("preview_url", "")
        ))

    conn.commit()

print(f"✅ Tous les messages lus et insérés en base ({count} messages)")

cur.close()
conn.close()
consumer.close()
