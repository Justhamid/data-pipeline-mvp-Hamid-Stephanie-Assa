CREATE TABLE IF NOT EXISTS raw_events (
    id SERIAL PRIMARY KEY,
    event_type TEXT,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    payload JSONB
);
