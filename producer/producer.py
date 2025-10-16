import requests
from kafka import KafkaProducer
import json
import time

# --- CONFIG --- #
GENRE_ID = 165  # exemple : Rap Français (change si tu veux)
KAFKA_TOPIC = "artists"
KAFKA_SERVER = "kafka:9092"

# --- INIT KAFKA --- #
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=3
)

# --- FONCTION POUR SCRAPER LES ARTISTES D'UN GENRE --- #
def get_artists_by_genre(genre_id, limit=30):
    """Récupère jusqu’à 'limit' artistes d’un genre Deezer."""
    url = f"https://api.deezer.com/genre/{genre_id}/artists"
    artists = []
    while url and len(artists) < limit:
        res = requests.get(url)
        data = res.json()
        if "data" not in data:
            print("Erreur API :", data)
            break
        artists.extend(data["data"])
        url = data.get("next")  # pagination Deezer
        if url:
            print("→ page suivante…")
        time.sleep(0.2)  # éviter rate limit
    return artists[:limit]


# --- MAIN --- #
if __name__ == "__main__":
    print(f"Récupération des artistes du genre {GENRE_ID}…")
    artists = get_artists_by_genre(GENRE_ID, limit=50)

    print(f"✅ {len(artists)} artistes trouvés. Envoi vers Kafka…")

    for a in artists:
        artist_data = {
            "id": a["id"],
            "name": a["name"]
        }
        producer.send(KAFKA_TOPIC, value=artist_data)
        print(f"→ envoyé : {artist_data['name']}")
        time.sleep(0.1)  # éviter de saturer Kafka

    producer.flush()
    print("Fin d’envoi !")
