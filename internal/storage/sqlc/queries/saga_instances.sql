-- name: UpdateSagaInstanceStep :exec
UPDATE saga_instances 
SET current_step = $2, status = $3, output = output || $4::jsonb 
WHERE saga_id = $1;

-- name: UpdateSagaInstanceStatus :exec
UPDATE saga_instances 
SET status = $2, error = $3 
WHERE saga_id = $1;

-- name: GetSagaInstance :one
SELECT saga_id, status, current_step, trace_id, input, output, error, created_at, updated_at
FROM saga_instances 
WHERE saga_id = $1;
