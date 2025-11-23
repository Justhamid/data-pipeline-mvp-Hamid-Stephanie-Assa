# producer/producer.py
# ======================================================================
# Producer Deezer enrichi
# - Récupère des données de plusieurs endpoints Deezer (pas de clé API)
# - Uniformise les messages et envoie à Kafka
# - Ajoute "kind" pour typer et "source" pour tracer l'origine
# - Envoie aussi des "chart_entry" (snapshot classement) avec position
# - Variables d'env pour limiter la charge et contrôler le cycle
# ======================================================================

import os
import time
import json
from typing import Any, Dict, Iterable, List

import requests
from kafka import KafkaProducer

# ----------------------------------------------------------------------
# Paramètres env
# ----------------------------------------------------------------------
TOPIC = os.getenv("TOPIC", "deezer_tracks")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "10"))
DEEZER_BASE = "https://api.deezer.com"

# Pagination Deezer : limit <= 100 (max connu)
MAX_LIMIT = int(os.getenv("BATCH_PAGE_LIMIT", "100"))

# Limite d’artistes du top global à explorer
TOP_ARTISTS_TO_EXPLORE = int(os.getenv("TOP_ARTISTS_TO_EXPLORE", "50"))

# Pause entre deux snapshots (secondes)
SLEEP_BETWEEN_CALLS = int(os.getenv("SLEEP_BETWEEN_CALLS", "60"))

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
)


# ----------------------------------------------------------------------
# Utils HTTP
# ----------------------------------------------------------------------
def get_json(url: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """GET JSON avec gestion d’erreurs et timeout."""
    try:
        r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json() or {}
    except Exception as e:
        print(f"[Producer] Erreur HTTP: {url} -> {e}")
        return {}


def paged(url: str, base_params: Dict[str, Any] | None = None, limit: int = MAX_LIMIT) -> Iterable[Dict[str, Any]]:
    """
    Itère sur toutes les pages Deezer qui exposent 'data' et 'next'.
    Renvoie des éléments individuels de data.
    """
    params = dict(base_params or {})
    params["limit"] = min(limit, 100)
    index = 0

    while True:
        params["index"] = index
        payload = get_json(url, params=params)
        data = payload.get("data", []) or []
        if not data:
            break

        for item in data:
            yield item

        # S'il n'y a pas de 'next' ou moins que 'limit' reçus, on s'arrête
        if not payload.get("next") or len(data) < params["limit"]:
            break
        index += params["limit"]


# ----------------------------------------------------------------------
# Normalisation messages
# ----------------------------------------------------------------------
def msg_artist(a: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "kind": "artist",
        "artist_id": a.get("id"),
        "name": a.get("name"),
        "link": a.get("link"),
        "picture": a.get("picture_big") or a.get("picture"),
        "nb_fan": a.get("nb_fan"),
        "nb_album": a.get("nb_album"),
        "source": "artist_object",
    }


def msg_album(alb: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "kind": "album",
        "album_id": alb.get("id"),
        "title": alb.get("title"),
        "link": alb.get("link"),
        "cover": alb.get("cover_big") or alb.get("cover"),
        "release_date": alb.get("release_date"),
        "record_type": alb.get("record_type"),
        "source": "album_object",
    }


def msg_track(t: Dict[str, Any]) -> Dict[str, Any]:
    art = t.get("artist") or {}
    alb = t.get("album") or {}
    return {
        "kind": "track",
        "track_id": t.get("id"),
        "title": t.get("title"),
        "duration": t.get("duration"),
        "rank": t.get("rank"),
        "explicit_lyrics": t.get("explicit_lyrics"),
        "link": t.get("link"),
        "preview": t.get("preview"),
        "artist": {
            "artist_id": art.get("id"),
            "name": art.get("name"),
            "link": art.get("link"),
            "picture": art.get("picture_big") or art.get("picture"),
        },
        "album": {
            "album_id": alb.get("id"),
            "title": alb.get("title"),
            "link": alb.get("link"),
            "cover": alb.get("cover_big") or alb.get("cover"),
            "release_date": alb.get("release_date"),
            "record_type": alb.get("record_type"),
        },
        "source": "track_object",
    }


def msg_genre(g: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "kind": "genre",
        "genre_id": g.get("id"),
        "name": g.get("name"),
        "picture": g.get("picture_big") or g.get("picture"),
        "source": "genre_object",
    }


def msg_chart_entry(track_obj: Dict[str, Any], position: int, source: str,
                    genre_id: int | None = None, artist_id: int | None = None) -> Dict[str, Any]:
    """
    Un "snapshot" de classement : on embarque le track (avec artist/album) pour
    que le consumer puisse upserter les entités avant d’insérer la ligne snapshot.
    """
    base = msg_track(track_obj)
    return {
        "kind": "chart_entry",
        "position": position,
        "source": source,
        "ref_genre_id": genre_id,
        "ref_artist_id": artist_id,
        "track": base,
    }


# ----------------------------------------------------------------------
# Collecteurs par endpoint
# ----------------------------------------------------------------------
def collect_global_charts() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    # Tracks (classement global)
    url_tracks = f"{DEEZER_BASE}/chart/0/tracks"
    tracks: List[Dict[str, Any]] = []
    for pos, t in enumerate(paged(url_tracks), start=1):
        tracks.append(t)
        out.append(msg_chart_entry(t, position=pos, source="chart_global_tracks"))

    # Entités rencontrées via tracks
    for t in tracks:
        if t.get("artist"):
            out.append(msg_artist(t["artist"]))
        if t.get("album"):
            out.append(msg_album(t["album"]))
        out.append(msg_track(t))

    # Artists (classement global d’artistes)
    url_artists = f"{DEEZER_BASE}/chart/0/artists"
    for a in paged(url_artists):
        out.append(msg_artist(a))

    # Albums (classement global d’albums)
    url_albums = f"{DEEZER_BASE}/chart/0/albums"
    for alb in paged(url_albums):
        out.append(msg_album(alb))

    print(f"[Producer] collect_global_charts -> {len(out)} messages")
    return out


def collect_genres_and_charts() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    url_genres = f"{DEEZER_BASE}/genre"

    genres = get_json(url_genres).get("data", []) or []
    for g in genres:
        if not g or g.get("id") in (0,):  # 0 = "All" -> skip
            continue
        out.append(msg_genre(g))

        # Chart par genre
        url_gchart = f"{DEEZER_BASE}/genre/{g['id']}/chart"
        payload = get_json(url_gchart)
        tracks = (payload.get("tracks") or {}).get("data", []) or []
        for pos, t in enumerate(tracks, start=1):
            out.append(msg_chart_entry(
                t, position=pos, source=f"genre_{g['id']}_tracks", genre_id=g["id"]
            ))
            if t.get("artist"):
                out.append(msg_artist(t["artist"]))
            if t.get("album"):
                out.append(msg_album(t["album"]))
            out.append(msg_track(t))

    print(f"[Producer] collect_genres_and_charts -> {len(out)} messages")
    return out


def collect_artist_tops_from_global(n_artists: int = TOP_ARTISTS_TO_EXPLORE) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    # 1) Top artistes globaux
    url_artists = f"{DEEZER_BASE}/chart/0/artists"
    top_artists: List[Dict[str, Any]] = []
    for a in paged(url_artists):
        top_artists.append(a)
        if len(top_artists) >= n_artists:
            break

    for a in top_artists:
        artist_id = a.get("id")
        if not artist_id:
            continue

        out.append(msg_artist(a))  # garder trace de l’artiste

        # 2) Top tracks par artiste
        url_top = f"{DEEZER_BASE}/artist/{artist_id}/top"
        payload = get_json(url_top, params={"limit": MAX_LIMIT})
        tracks = payload.get("data", []) or []
        for pos, t in enumerate(tracks, start=1):
            out.append(msg_chart_entry(
                t, position=pos, source=f"artist_top_tracks:{artist_id}", artist_id=artist_id
            ))
            if t.get("artist"):
                out.append(msg_artist(t["artist"]))
            if t.get("album"):
                out.append(msg_album(t["album"]))
            out.append(msg_track(t))

    print(f"[Producer] collect_artist_tops_from_global({n_artists}) -> {len(out)} messages")
    return out


# ----------------------------------------------------------------------
# Envoi Kafka
# ----------------------------------------------------------------------
def send_batch(msgs: List[Dict[str, Any]]):
    """Envoie un batch de messages sur Kafka (un par send)."""
    sent = 0
    for m in msgs:
        # Filtrer les messages incomplets (ex: track sans id)
        k = m.get("kind")
        if k == "artist" and not m.get("artist_id"):
            continue
        if k == "album" and not m.get("album_id"):
            continue
        if k == "track" and not m.get("track_id"):
            continue
        if k == "genre" and not m.get("genre_id"):
            continue
        if k == "chart_entry" and not ((m.get("track") or {}).get("track_id")):
            continue

        producer.send(TOPIC, m)
        sent += 1
        # Petite respiration pour ne pas flooder
        if sent % 500 == 0:
            producer.flush()

    producer.flush()
    print(f"[Producer] Batch envoyé: {sent} messages.")


# ----------------------------------------------------------------------
# Boucle principale : collecte + envoi Kafka
# ----------------------------------------------------------------------
if __name__ == "__main__":
    while True:
        batch: List[Dict[str, Any]] = []

        part1 = collect_global_charts()
        batch += part1

        part2 = collect_genres_and_charts()
        batch += part2

        part3 = collect_artist_tops_from_global(TOP_ARTISTS_TO_EXPLORE)
        batch += part3

        print(f"[Producer] Total batch = {len(batch)} messages")
        send_batch(batch)

        print(f"[Producer] Pause {SLEEP_BETWEEN_CALLS}s avant prochain snapshot…")
        time.sleep(SLEEP_BETWEEN_CALLS)
