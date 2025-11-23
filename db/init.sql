-- =====================================================================
-- Schéma Deezer enrichi + table brute Phase 1 (raw_events)
-- Tables : raw_events, artists, albums, tracks, genres, chart_snapshots
-- =====================================================================

-- Idempotence : on drop dans un ordre sûr si tu relances manuellement
DROP TABLE IF EXISTS chart_snapshots;
DROP TABLE IF EXISTS tracks;
DROP TABLE IF EXISTS albums;
DROP TABLE IF EXISTS artists;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS raw_events;

-- -------------------------
-- RAW_EVENTS (Phase 1 - brut)
-- -------------------------
CREATE TABLE raw_events (
  id BIGSERIAL PRIMARY KEY,
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source TEXT,
  payload JSONB NOT NULL
);
CREATE INDEX idx_raw_events_received_at ON raw_events (received_at DESC);

-- -------------------------
-- ARTISTS
-- -------------------------
CREATE TABLE artists (
  artist_id BIGINT PRIMARY KEY,
  name TEXT NOT NULL,
  link TEXT,
  picture TEXT,
  nb_fan INTEGER,
  nb_album INTEGER
);
CREATE INDEX idx_artists_name ON artists (name);

-- -------------------------
-- ALBUMS
-- -------------------------
CREATE TABLE albums (
  album_id BIGINT PRIMARY KEY,
  title TEXT NOT NULL,
  link TEXT,
  cover TEXT,
  release_date DATE,
  record_type TEXT
);
CREATE INDEX idx_albums_title ON albums (title);

-- -------------------------
-- TRACKS
-- -------------------------
CREATE TABLE tracks (
  track_id BIGINT PRIMARY KEY,
  title TEXT NOT NULL,
  duration INTEGER,
  rank INTEGER,
  explicit_lyrics BOOLEAN,
  link TEXT,
  preview TEXT,
  artist_id BIGINT REFERENCES artists(artist_id) ON DELETE CASCADE,
  album_id BIGINT REFERENCES albums(album_id) ON DELETE CASCADE
);
CREATE INDEX idx_tracks_title ON tracks (title);
CREATE INDEX idx_tracks_artist ON tracks (artist_id);
CREATE INDEX idx_tracks_album ON tracks (album_id);

-- -------------------------
-- GENRES
-- -------------------------
CREATE TABLE genres (
  genre_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  picture TEXT
);
CREATE INDEX idx_genres_name ON genres (name);

-- -------------------------
-- CHART_SNAPSHOTS
-- - photo d’un classement à un instant t
-- - On stocke une "source" (ex: 'chart_global_tracks', 'genre_rap_tracks',
--   'chart_global_artists', 'artist_top_tracks:27', etc.)
-- - position : rang dans la page (1..N)
-- - Pour garantir l’unicité même si genre_id/artist_id sont NULL, on
--   utilise des colonnes *normalisées* (avec défaut -1).
-- -------------------------
CREATE TABLE chart_snapshots (
  id BIGSERIAL PRIMARY KEY,
  snapshot_time TIMESTAMP NOT NULL DEFAULT NOW(),
  source TEXT NOT NULL,
  -- Références informationnelles (NULL autorisé)
  ref_genre_id INTEGER,
  ref_artist_id BIGINT,
  track_id BIGINT NOT NULL REFERENCES tracks(track_id) ON DELETE CASCADE,
  position INTEGER NOT NULL,
  -- Colonnes normalisées pour contrainte d’unicité
  genre_norm INTEGER NOT NULL DEFAULT -1,
  artist_norm BIGINT NOT NULL DEFAULT -1
);

-- Contrainte d’unicité (remplace l’index unique de la version précédente)
ALTER TABLE chart_snapshots
  ADD CONSTRAINT uq_chart_snapshot
  UNIQUE (snapshot_time, source, genre_norm, artist_norm, track_id);

-- Aides à la requête
CREATE INDEX idx_chart_src_time ON chart_snapshots (source, snapshot_time DESC);
CREATE INDEX idx_chart_track ON chart_snapshots (track_id);
