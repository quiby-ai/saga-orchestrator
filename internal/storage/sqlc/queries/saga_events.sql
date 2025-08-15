-- name: InsertSagaEvent :exec
INSERT INTO saga_events (saga_id, message_id, type, payload, occurred_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (message_id) DO NOTHING;

-- name: GetSagaEvents :many
SELECT id, saga_id, message_id, type, payload, occurred_at, created_at
FROM saga_events 
WHERE saga_id = $1 
ORDER BY occurred_at ASC;

-- name: GetSagaEventByMessageID :one
SELECT id, saga_id, message_id, type, payload, occurred_at, created_at
FROM saga_events 
WHERE message_id = $1;
