CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    saga_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);


