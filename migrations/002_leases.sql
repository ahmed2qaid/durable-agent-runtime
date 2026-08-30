ALTER TABLE durable_runs
    ALTER COLUMN status SET DEFAULT 'pending';

ALTER TABLE durable_runs
    ADD COLUMN IF NOT EXISTS lease_owner TEXT,
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS cancel_requested BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS last_error TEXT,
    ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS durable_runs_lease_idx
    ON durable_runs(status, lease_expires_at)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS durable_runs_cancel_idx
    ON durable_runs(cancel_requested, updated_at)
    WHERE cancel_requested = true;
