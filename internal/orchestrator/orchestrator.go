package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/quiby-ai/saga-orchestrator/internal/config"
	"github.com/quiby-ai/saga-orchestrator/internal/db"
	"github.com/quiby-ai/saga-orchestrator/internal/types"
)

type Orchestrator struct {
	cfg config.Config
	db  *sql.DB
	p   producer
}

type producer interface {
	Publish(ctx context.Context, topic string, key []byte, value []byte, headers []kafka.Header) error
}

func NewOrchestrator(cfg config.Config, db *sql.DB, p producer) *Orchestrator {
	return &Orchestrator{cfg: cfg, db: db, p: p}
}

func (o *Orchestrator) HandleMessage(ctx context.Context, msg kafka.Message) error {
	var env types.Envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		return err
	}

	var alreadyProcessed bool
	messageID := getHeader(msg, "message_id")
	if messageID != "" {
		if err := db.WithTx(ctx, o.db, func(tx *sql.Tx) error {
			if err := o.ensureProcessedNotExists(ctx, tx, messageID); err != nil {
				if errors.Is(err, db.ErrDuplicate) {
					alreadyProcessed = true
					return nil
				}
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if alreadyProcessed {
		return nil
	}

	// Handle only completed/failed events
	switch env.Type {
	case o.cfg.TopicExtractCompleted:
		if err := o.onExtractCompleted(ctx, env); err != nil {
			return err
		}
	case o.cfg.TopicPrepareCompleted:
		if err := o.onPrepareCompleted(ctx, env); err != nil {
			return err
		}
	case o.cfg.TopicPipelineFailed:
		if err := o.onPipelineFailed(ctx, env); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown message type: %s", env.Type)
	}
	return nil
}

func (o *Orchestrator) onExtractCompleted(ctx context.Context, env types.Envelope) error {
	return db.WithTx(ctx, o.db, func(tx *sql.Tx) error {
		if err := o.appendEvent(ctx, tx, env, env.Payload); err != nil {
			return err
		}
		// move to prepare
		const q = `UPDATE saga_instances SET current_step='prepare', status='running', output = output || $2::jsonb WHERE saga_id=$1`
		if _, err := tx.ExecContext(ctx, q, env.SagaID, asJSON(env.Payload)); err != nil {
			return err
		}
		// publish prepare.request and state.changed
		if err := o.publishNext(ctx, env, o.cfg.TopicPrepareRequest, env.Payload); err != nil {
			return err
		}
		return o.publishStateChanged(ctx, env.SagaID, "running", "prepare", env.Payload)
	})
}

func (o *Orchestrator) onPrepareCompleted(ctx context.Context, env types.Envelope) error {
	return db.WithTx(ctx, o.db, func(tx *sql.Tx) error {
		if err := o.appendEvent(ctx, tx, env, env.Payload); err != nil {
			return err
		}
		// complete saga
		const q = `UPDATE saga_instances SET current_step='completed', status='completed', output = output || $2::jsonb WHERE saga_id=$1`
		if _, err := tx.ExecContext(ctx, q, env.SagaID, asJSON(env.Payload)); err != nil {
			return err
		}
		return o.publishStateChanged(ctx, env.SagaID, "completed", "prepare", env.Payload)
	})
}

func (o *Orchestrator) onPipelineFailed(ctx context.Context, env types.Envelope) error {
	// mark failed immediately, no retries/DLQ per spec stage
	return db.WithTx(ctx, o.db, func(tx *sql.Tx) error {
		if err := o.appendEvent(ctx, tx, env, env.Payload); err != nil {
			return err
		}
		const q = `UPDATE saga_instances SET status='failed', error=$2 WHERE saga_id=$1`
		if _, err := tx.ExecContext(ctx, q, env.SagaID, asJSON(env.Payload)); err != nil {
			return err
		}
		return o.publishStateChanged(ctx, env.SagaID, "failed", "", env.Payload)
	})
}

func (o *Orchestrator) ensureProcessedNotExists(ctx context.Context, tx *sql.Tx, messageID string) error {
	const ins = `INSERT INTO processed_messages(message_id) VALUES ($1)`
	if _, err := tx.ExecContext(ctx, ins, messageID); err != nil {
		if isPgUniqueViolation(err) {
			return db.ErrDuplicate
		}
		return err
	}
	return nil
}

func (o *Orchestrator) appendEvent(ctx context.Context, tx *sql.Tx, env types.Envelope, payload json.RawMessage) error {
	const ins = `INSERT INTO saga_events(saga_id, message_id, type, payload, occurred_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT(message_id) DO NOTHING`
	_, err := tx.ExecContext(ctx, ins, env.SagaID, env.MessageID, env.Type, asJSON(payload), env.OccurredAt)
	return err
}

func (o *Orchestrator) publishNext(ctx context.Context, env types.Envelope, topic string, payload json.RawMessage) error {
	out := types.Envelope{
		MessageID:  uuid.NewString(),
		TraceID:    env.TraceID,
		SagaID:     env.SagaID,
		Type:       topic,
		OccurredAt: time.Now().UTC(),
		Payload:    payload,
		Meta:       types.Meta{AppID: o.cfg.AppID, TenantID: env.Meta.TenantID, Initiator: env.Meta.Initiator, SchemaVersion: "v1"},
	}
	return o.publish(ctx, topic, out)
}

func (o *Orchestrator) publishStateChanged(ctx context.Context, envSagaID string, status string, step string, contextPayload json.RawMessage) error {
	// shape per spec: payload: { status, step, context: { message, count }, error }
	payload := map[string]any{"status": status}
	if step != "" {
		payload["step"] = step
	}
	// Try to extract count if present
	var ctxMap map[string]any
	_ = json.Unmarshal(contextPayload, &ctxMap)
	if ctxMap != nil {
		contextObj := map[string]any{}
		if v, ok := ctxMap["count"]; ok {
			contextObj["count"] = v
		}
		if len(contextObj) > 0 {
			payload["context"] = contextObj
		}
	}
	b, _ := json.Marshal(map[string]any{"payload": payload})
	out := types.Envelope{
		MessageID:  uuid.NewString(),
		SagaID:     envSagaID,
		Type:       o.cfg.TopicStateChanged,
		OccurredAt: time.Now().UTC(),
		Payload:    b,
		Meta:       types.Meta{AppID: o.cfg.AppID, SchemaVersion: "v1"},
	}
	return o.publish(ctx, o.cfg.TopicStateChanged, out)
}

func (o *Orchestrator) publish(ctx context.Context, topic string, env types.Envelope) error {
	value, _ := json.Marshal(env)
	key := []byte(env.SagaID)
	headers := []kafka.Header{
		{Key: "message_id", Value: []byte(env.MessageID)},
		{Key: "trace_id", Value: []byte(env.TraceID)},
		{Key: "saga_id", Value: []byte(env.SagaID)},
		{Key: "type", Value: []byte(env.Type)},
		{Key: "schema_version", Value: []byte(env.Meta.SchemaVersion)},
	}
	return o.p.Publish(ctx, topic, key, value, headers)
}

func getHeader(m kafka.Message, key string) string {
	for _, h := range m.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func asJSON(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return json.RawMessage(`{}`)
	}
	return raw
}

func isPgUniqueViolation(err error) bool {
	return err != nil && (contains(err.Error(), "duplicate key") || contains(err.Error(), "unique constraint"))
}

func contains(s, sub string) bool { return len(s) >= len(sub) && (stringIndex(s, sub) >= 0) }

func stringIndex(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
