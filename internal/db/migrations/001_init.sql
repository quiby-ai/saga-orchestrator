CREATE TABLE IF NOT EXISTS saga_instances (
    saga_id UUID PRIMARY KEY,
    status TEXT NOT NULL,
    current_step TEXT NOT NULL,
    trace_id UUID NOT NULL,
    input JSONB DEFAULT '{}'::jsonb NOT NULL,
    output JSONB DEFAULT '{}'::jsonb NOT NULL,
    error JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS saga_events (
    id BIGSERIAL PRIMARY KEY,
    saga_id UUID NOT NULL,
    message_id UUID NOT NULL,
    type TEXT NOT NULL,
    payload JSONB DEFAULT '{}'::jsonb NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(message_id)
);
CREATE INDEX IF NOT EXISTS idx_saga_events_saga ON saga_events(saga_id);

CREATE TABLE IF NOT EXISTS processed_messages (
    message_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- trigger to update updated_at
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_saga_instances_updated ON saga_instances;
CREATE TRIGGER trg_saga_instances_updated
BEFORE UPDATE ON saga_instances
FOR EACH ROW EXECUTE FUNCTION set_updated_at();


