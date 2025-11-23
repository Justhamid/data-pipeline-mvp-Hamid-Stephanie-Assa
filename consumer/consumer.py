# consumer/consumer.py
# ======================================================================
# Consumer Deezer enrichi + insertion brute Phase 1
# - Gère plusieurs "kinds" de messages : artist, album, track, genre, chart_entry
# - Upserts idempotents (ON CONFLICT DO UPDATE/NOTHING)
# - Insère des "chart_snapshots" avec colonnes normalisées pour unicité
# - INSERT du JSON BRUT dans raw_events (exigence Phase 1)
# ======================================================================

import os
import json
import time
import psycopg2
from typing import Any, Dict
from kafka import KafkaConsumer

TOPIC = os.getenv("TOPIC", "deezer_tracks")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

PGHOST = os.getenv("PGHOST", "postgres")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "postgres")  # simplifié
PGDATABASE = os.getenv("PGDATABASE", "datapipeline")
PGPORT = int(os.getenv("PGPORT", "5432"))

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    enable_auto_commit=True,
    auto_offset_reset="earliest",
)

print("[Consumer] En attente de messages Kafka sur 'deezer_tracks'...")

# Connexion PostgreSQL robuste (retry)
conn = None
while True:
    try:
        conn = psycopg2.connect(
            host=PGHOST,
            dbname=PGDATABASE,
            user=PGUSER,
            password=PGPASSWORD,
            port=PGPORT,
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] Connecté à PostgreSQL")
        break
    except Exception as e:
        print("[Consumer] PostgreSQL pas encore prêt, tentative dans 3s...", str(e))
        time.sleep(3)

# ---------------------------------------------------------
# Helpers INSERT / UPSERT
# ---------------------------------------------------------

def insert_raw_event(cur, data: Dict[str, Any]) -> None:
    """Insère le payload brut dans raw_events (Phase 1)."""
    try:
        cur.execute(
            "INSERT INTO raw_events (source, payload) VALUES (%s, %s::jsonb);",
            (data.get("source") or data.get("kind"), json.dumps(data)),
        )
    except Exception as e:
        print("[Consumer] Erreur insert raw_events:", e)


def upsert_artist(cur, a: Dict[str, Any]) -> None:
    if not a or not a.get("artist_id"):
        return
    cur.execute(
        """
        INSERT INTO artists (artist_id, name, link, picture, nb_fan, nb_album)
        VALUES (%(artist_id)s, %(name)s, %(link)s, %(picture)s, %(nb_fan)s, %(nb_album)s)
        ON CONFLICT (artist_id) DO UPDATE SET
          name=EXCLUDED.name,
          link=EXCLUDED.link,
          picture=EXCLUDED.picture,
          nb_fan=COALESCE(EXCLUDED.nb_fan, artists.nb_fan),
          nb_album=COALESCE(EXCLUDED.nb_album, artists.nb_album);
        """,
        a,
    )


def upsert_album(cur, alb: Dict[str, Any]) -> None:
    if not alb or not alb.get("album_id"):
        return
    cur.execute(
        """
        INSERT INTO albums (album_id, title, link, cover, release_date, record_type)
        VALUES (%(album_id)s, %(title)s, %(link)s, %(cover)s, %(release_date)s, %(record_type)s)
        ON CONFLICT (album_id) DO UPDATE SET
          title=EXCLUDED.title,
          link=EXCLUDED.link,
          cover=EXCLUDED.cover,
          release_date=COALESCE(EXCLUDED.release_date, albums.release_date),
          record_type=COALESCE(EXCLUDED.record_type, albums.record_type);
        """,
        alb,
    )


def upsert_track(cur, t: Dict[str, Any]) -> None:
    if not t or not t.get("track_id"):
        return

    # upsert lié : artist & album si présents
    art = t.get("artist") or {}
    if art and art.get("artist_id"):
        upsert_artist(
            cur,
            {
                "artist_id": art.get("artist_id"),
                "name": art.get("name"),
                "link": art.get("link"),
                "picture": art.get("picture"),
                "nb_fan": None,
                "nb_album": None,
            },
        )

    alb = t.get("album") or {}
    if alb and alb.get("album_id"):
        upsert_album(
            cur,
            {
                "album_id": alb.get("album_id"),
                "title": alb.get("title"),
                "link": alb.get("link"),
                "cover": alb.get("cover"),
                "release_date": alb.get("release_date"),
                "record_type": alb.get("record_type"),
            },
        )

    cur.execute(
        """
        INSERT INTO tracks
          (track_id, title, duration, rank, explicit_lyrics, link, preview, artist_id, album_id)
        VALUES
          ( %(track_id)s, %(title)s, %(duration)s, %(rank)s, %(explicit_lyrics)s,
            %(link)s, %(preview)s, %(artist_id)s, %(album_id)s )
        ON CONFLICT (track_id) DO UPDATE SET
          title=EXCLUDED.title,
          duration=COALESCE(EXCLUDED.duration, tracks.duration),
          rank=COALESCE(EXCLUDED.rank, tracks.rank),
          explicit_lyrics=COALESCE(EXCLUDED.explicit_lyrics, tracks.explicit_lyrics),
          link=COALESCE(EXCLUDED.link, tracks.link),
          preview=COALESCE(EXCLUDED.preview, tracks.preview),
          artist_id=COALESCE(EXCLUDED.artist_id, tracks.artist_id),
          album_id=COALESCE(EXCLUDED.album_id, tracks.album_id);
        """,
        {
            "track_id": t.get("track_id"),
            "title": t.get("title"),
            "duration": t.get("duration"),
            "rank": t.get("rank"),
            "explicit_lyrics": t.get("explicit_lyrics"),
            "link": t.get("link"),
            "preview": t.get("preview"),
            "artist_id": (t.get("artist") or {}).get("artist_id"),
            "album_id": (t.get("album") or {}).get("album_id"),
        },
    )


def upsert_genre(cur, g: Dict[str, Any]) -> None:
    if not g or not g.get("genre_id"):
        return
    cur.execute(
        """
        INSERT INTO genres (genre_id, name, picture)
        VALUES (%(genre_id)s, %(name)s, %(picture)s)
        ON CONFLICT (genre_id) DO UPDATE SET
          name=EXCLUDED.name,
          picture=COALESCE(EXCLUDED.picture, genres.picture);
        """,
        g,
    )


def insert_chart_snapshot(cur, entry: Dict[str, Any]) -> None:
    """
    Insère un snapshot de classement.
    - upsert d’abord le track (et son artist/album)
    - genre_norm/artist_norm : colonnes normalisées (-1 si None) pour l’unicité
    """
    track_msg = entry.get("track") or {}
    upsert_track(cur, track_msg)

    genre_id = entry.get("ref_genre_id")
    artist_id = entry.get("ref_artist_id")
    genre_norm = genre_id if genre_id is not None else -1
    artist_norm = artist_id if artist_id is not None else -1

    cur.execute(
        """
        INSERT INTO chart_snapshots
          (snapshot_time, source, ref_genre_id, ref_artist_id, track_id, position, genre_norm, artist_norm)
        VALUES
          (NOW(), %(source)s, %(ref_genre_id)s, %(ref_artist_id)s, %(track_id)s, %(position)s, %(genre_norm)s, %(artist_norm)s)
        ON CONFLICT ON CONSTRAINT uq_chart_snapshot DO NOTHING;
        """,
        {
            "source": entry.get("source"),
            "ref_genre_id": genre_id,
            "ref_artist_id": artist_id,
            "track_id": (track_msg or {}).get("track_id"),
            "position": entry.get("position") or 0,
            "genre_norm": genre_norm,
            "artist_norm": artist_norm,
        },
    )


# ---------------------------------------------------------
# Boucle de consommation
# ---------------------------------------------------------
for msg in consumer:
    data = msg.value
    try:
        # 1) Phase 1 : insérer le JSON BRUT
        insert_raw_event(cur, data)

        # 2) Modèle enrichi (bonus Phase 1+)
        kind = data.get("kind")
        if kind == "artist":
            upsert_artist(cur, data)
        elif kind == "album":
            upsert_album(cur, data)
        elif kind == "track":
            upsert_track(cur, data)
        elif kind == "genre":
            upsert_genre(cur, data)
        elif kind == "chart_entry":
            insert_chart_snapshot(cur, data)
        else:
            # Messages inconnus (ignorer proprement)
            pass
    except Exception as e:
        # Log minimal ; si besoin, ajoute un fichier de dead-letter etc.
        print("[Consumer] Erreur d’upsert:", e)
