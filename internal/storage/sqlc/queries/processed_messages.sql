-- name: InsertProcessedMessage :exec
INSERT INTO processed_messages (message_id) VALUES ($1);

-- name: GetProcessedMessage :one
SELECT message_id, processed_at
FROM processed_messages 
WHERE message_id = $1;
