package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/quiby-ai/common/pkg/events"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/quiby-ai/saga-orchestrator/internal/config"
	"github.com/quiby-ai/saga-orchestrator/internal/db"
	"github.com/quiby-ai/saga-orchestrator/internal/utils"
)

func NewOrchestrator(cfg config.Config, db *sql.DB, p producer) *Orchestrator {
	return &Orchestrator{cfg: cfg, db: db, p: p}
}

func (o *Orchestrator) HandleMessage(ctx context.Context, msg kafka.Message) error {
	// First unmarshal to get the basic envelope structure
	var baseEnv struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(msg.Value, &baseEnv); err != nil {
		return err
	}

	var alreadyProcessed bool
	messageID := utils.GetHeader(msg, "message_id")
	if messageID != "" {
		if err := utils.WithTx(ctx, o.db, func(tx *sql.Tx) error {
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

	// Handle only completed/failed events with proper type handling
	switch baseEnv.Type {
	case o.cfg.TopicExtractCompleted:
		var env events.Envelope[events.ExtractCompleted]
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			return err
		}
		if err := o.onExtractCompleted(ctx, env); err != nil {
			return err
		}
	case o.cfg.TopicPrepareCompleted:
		var env events.Envelope[events.PrepareCompleted]
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			return err
		}
		if err := o.onPrepareCompleted(ctx, env); err != nil {
			return err
		}
	case o.cfg.TopicPipelineFailed:
		var env events.Envelope[events.Failed]
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			return err
		}
		if err := o.onPipelineFailed(ctx, env); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown message type: %s", baseEnv.Type)
	}
	return nil
}

func (o *Orchestrator) onExtractCompleted(ctx context.Context, env events.Envelope[events.ExtractCompleted]) error {
	return utils.WithTx(ctx, o.db, func(tx *sql.Tx) error {
		genericEnv := utils.ToGenericEnvelope(env)

		if err := o.appendEvent(ctx, tx, genericEnv, env.Payload); err != nil {
			return err
		}

		const q = `UPDATE saga_instances SET current_step='prepare', status='running', output = output || $2::jsonb WHERE saga_id=$1`
		if _, err := tx.ExecContext(ctx, q, env.SagaID, utils.AsJSON(env.Payload)); err != nil {
			return err
		}

		payloadJSON, _ := json.Marshal(env.Payload)
		if err := o.publishNext(ctx, genericEnv, o.cfg.TopicPrepareRequest, payloadJSON); err != nil {
			return err
		}
		return o.publishStateChanged(ctx, env.SagaID, "running", "prepare", payloadJSON)
	})
}

func (o *Orchestrator) onPrepareCompleted(ctx context.Context, env events.Envelope[events.PrepareCompleted]) error {
	return utils.WithTx(ctx, o.db, func(tx *sql.Tx) error {
		genericEnv := utils.ToGenericEnvelope(env)

		if err := o.appendEvent(ctx, tx, genericEnv, env.Payload); err != nil {
			return err
		}

		const q = `UPDATE saga_instances SET current_step='completed', status='completed', output = output || $2::jsonb WHERE saga_id=$1`
		if _, err := tx.ExecContext(ctx, q, env.SagaID, utils.AsJSON(env.Payload)); err != nil {
			return err
		}
		payloadJSON, _ := json.Marshal(env.Payload)
		return o.publishStateChanged(ctx, env.SagaID, "completed", "prepare", payloadJSON)
	})
}

func (o *Orchestrator) onPipelineFailed(ctx context.Context, env events.Envelope[events.Failed]) error {
	return utils.WithTx(ctx, o.db, func(tx *sql.Tx) error {
		genericEnv := utils.ToGenericEnvelope(env)

		if err := o.appendEvent(ctx, tx, genericEnv, env.Payload); err != nil {
			return err
		}
		const q = `UPDATE saga_instances SET status='failed', error=$2 WHERE saga_id=$1`
		if _, err := tx.ExecContext(ctx, q, env.SagaID, utils.AsJSON(env.Payload)); err != nil {
			return err
		}
		payloadJSON, _ := json.Marshal(env.Payload)
		return o.publishStateChanged(ctx, env.SagaID, "failed", "", payloadJSON)
	})
}

func (o *Orchestrator) ensureProcessedNotExists(ctx context.Context, tx *sql.Tx, messageID string) error {
	const ins = `INSERT INTO processed_messages(message_id) VALUES ($1)`
	if _, err := tx.ExecContext(ctx, ins, messageID); err != nil {
		if utils.IsUniqueViolation(err) {
			return db.ErrDuplicate
		}
		return err
	}
	return nil
}

func (o *Orchestrator) appendEvent(ctx context.Context, tx *sql.Tx, env events.Envelope[any], payload any) error {
	const ins = `INSERT INTO saga_events(saga_id, message_id, type, payload, occurred_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT(message_id) DO NOTHING`
	_, err := tx.ExecContext(ctx, ins, env.SagaID, env.MessageID, env.Type, utils.AsJSON(payload), env.OccurredAt)
	return err
}

func (o *Orchestrator) publishNext(ctx context.Context, env events.Envelope[any], topic string, payload json.RawMessage) error {
	// TODO: update any to exact type
	out := events.Envelope[any]{
		MessageID:  uuid.NewString(),
		TraceID:    env.TraceID,
		SagaID:     env.SagaID,
		Type:       topic,
		OccurredAt: time.Now().UTC(),
		Payload:    payload,
		Meta: events.Meta{
			AppID:         o.cfg.AppID,
			TenantID:      env.Meta.TenantID,
			Initiator:     env.Meta.Initiator,
			SchemaVersion: events.SchemaVersionV1,
		},
	}
	return o.publish(ctx, topic, out)
}

func (o *Orchestrator) publishStateChanged(ctx context.Context, envSagaID string, status string, step string, contextPayload json.RawMessage) error {
	payload := map[string]any{"status": status}
	if step != "" {
		payload["step"] = step
	}

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
	// TODO: handle any to exact type
	b, _ := json.Marshal(map[string]any{"payload": payload})
	out := events.Envelope[any]{
		MessageID:  uuid.NewString(),
		SagaID:     envSagaID,
		Type:       o.cfg.TopicStateChanged,
		OccurredAt: time.Now().UTC(),
		Payload:    b,
		Meta: events.Meta{
			AppID:         o.cfg.AppID,
			SchemaVersion: events.SchemaVersionV1,
		},
	}
	return o.publish(ctx, o.cfg.TopicStateChanged, out)
}

func (o *Orchestrator) publish(ctx context.Context, topic string, env events.Envelope[any]) error {
	key := []byte(env.SagaID)
	return o.p.PublishEvent(ctx, topic, key, env)
}
