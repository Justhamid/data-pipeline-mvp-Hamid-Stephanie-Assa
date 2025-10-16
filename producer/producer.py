import requests
from kafka import KafkaProducer
import json
import time
from tqdm import tqdm

# ---------------------------
# CONFIG KAFKA
# ---------------------------
KAFKA_SERVER = "kafka:9092"  # car le script tourne dans le container
TOPIC_ARTISTS = "artists"
TOPIC_ALBUMS = "albums"
TOPIC_TRACKS = "tracks"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=3
)

# ---------------------------
# 1️⃣ Récupérer les artistes d'un genre
# ---------------------------
genre_id = 165  # Afrobeat
artists_list = []

url = f"https://api.deezer.com/genre/{genre_id}/artists"
while url:
    data = requests.get(url).json()
    for a in data.get("data", []):
        r = requests.get(f"https://api.deezer.com/artist/{a['id']}").json()
        artist_data = {
            "id": r["id"],
            "name": r["name"]
        }
        artists_list.append(artist_data)
        producer.send(TOPIC_ARTISTS, value=artist_data)
        time.sleep(0.1)
    url = data.get("next")
print(f"✅ {len(artists_list)} artistes envoyés dans Kafka")

# ---------------------------
# 2️⃣ Récupérer les albums de ces artistes
# ---------------------------
albums_list = []

for artist in tqdm(artists_list, desc="Albums"):
    url = f"https://api.deezer.com/artist/{artist['id']}/albums"
    while url:
        data = requests.get(url).json()
        for alb in data.get("data", []):
            album_data = {
                "artist_id": artist["id"],
                "artist_name": artist["name"],
                "album_id": alb["id"],
                "album_title": alb["title"],
                "release_date": alb.get("release_date", ""),
                "nb_tracks": alb.get("nb_tracks", 0),
                "cover": alb.get("cover_medium", ""),
                "link": alb.get("link", "")
            }
            albums_list.append(album_data)
            producer.send(TOPIC_ALBUMS, value=album_data)
        url = data.get("next")
        time.sleep(0.1)
print(f"✅ {len(albums_list)} albums envoyés dans Kafka")

# ---------------------------
# 3️⃣ Récupérer les tracks de ces albums
# ---------------------------
tracks_list = []

for album in tqdm(albums_list, desc="Tracks"):
    url = f"https://api.deezer.com/album/{album['album_id']}/tracks"
    while url:
        data = requests.get(url).json()
        for t in data.get("data", []):
            track_data = {
                "album_id": album["album_id"],
                "track_id": t["id"],
                "track_title": t["title"],
                "duration_sec": t["duration"],
                "rank": t["rank"],
                "preview_url": t.get("preview", "")
            }
            tracks_list.append(track_data)
            producer.send(TOPIC_TRACKS, value=track_data)
        url = data.get("next")
        time.sleep(0.1)
print(f"✅ {len(tracks_list)} tracks envoyés dans Kafka")

# ---------------------------
# Fin
# ---------------------------
producer.flush()
print("Toutes les données ont été envoyées dans Kafka !")
