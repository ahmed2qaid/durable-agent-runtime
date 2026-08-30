CREATE TABLE IF NOT EXISTS durable_events (
    run_id TEXT NOT NULL REFERENCES durable_runs(id) ON DELETE CASCADE,
    sequence BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    step_key TEXT,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, sequence)
);

CREATE INDEX IF NOT EXISTS durable_events_type_idx
    ON durable_events(event_type, created_at);

CREATE TABLE IF NOT EXISTS durable_dead_letters (
    run_id TEXT PRIMARY KEY REFERENCES durable_runs(id) ON DELETE CASCADE,
    reason TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS durable_dead_letters_created_idx
    ON durable_dead_letters(created_at DESC);
