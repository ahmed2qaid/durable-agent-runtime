CREATE TABLE IF NOT EXISTS durable_runs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'running',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS durable_steps (
    run_id TEXT NOT NULL REFERENCES durable_runs(id) ON DELETE CASCADE,
    step_key TEXT NOT NULL,
    status TEXT NOT NULL,
    output JSONB,
    error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, step_key)
);

CREATE INDEX IF NOT EXISTS durable_steps_status_idx
    ON durable_steps(status, updated_at);
