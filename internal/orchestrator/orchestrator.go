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
	"github.com/quiby-ai/saga-orchestrator/internal/storage"
	"github.com/quiby-ai/saga-orchestrator/internal/utils"
)

type Orchestrator struct {
	cfg config.Config
	db  *sql.DB
	p   producer
}

type producer interface {
	PublishEvent(ctx context.Context, topic string, key []byte, envelope events.Envelope[any]) error
}

func NewOrchestrator(cfg config.Config, db *sql.DB, p producer) *Orchestrator {
	return &Orchestrator{cfg: cfg, db: db, p: p}
}

func (o *Orchestrator) HandleMessage(ctx context.Context, msg kafka.Message) error {
	// First unmarshal to get the basic envelope structure
	var baseEnvelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(msg.Value, &baseEnvelope); err != nil {
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
	switch baseEnvelope.Type {
	case events.PipelineExtractCompleted:
		envelope, err := events.UnmarshalEnvelope[events.ExtractCompleted](msg.Value)
		if err != nil {
			return err
		}
		if err = o.onExtractCompleted(ctx, envelope); err != nil {
			return err
		}
	case events.PipelinePrepareCompleted:
		envelope, err := events.UnmarshalEnvelope[events.PrepareCompleted](msg.Value)
		if err != nil {
			return err
		}
		if err = o.onPrepareCompleted(ctx, envelope); err != nil {
			return err
		}
	case events.PipelineFailed:
		envelope, err := events.UnmarshalEnvelope[events.Failed](msg.Value)
		if err != nil {
			return err
		}
		if err = o.onPipelineFailed(ctx, envelope); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown message type: %s", baseEnvelope.Type)
	}
	return nil
}

type IncomingEventType interface {
	events.ExtractCompleted | events.PrepareCompleted | events.Failed
}

func (o *Orchestrator) onExtractCompleted(ctx context.Context, env events.Envelope[events.ExtractCompleted]) error {
	return utils.WithTx(ctx, o.db, func(tx *sql.Tx) error {
		genericEnv := utils.ToGenericEnvelope(env)

		if err := appendEvent(ctx, tx, env, env.Payload); err != nil {
			return err
		}

		// TODO: move it to constructor (if needed)
		repo := storage.NewSagaInstancesRepo(tx)
		sagaUUID, err := uuid.Parse(env.SagaID)
		if err != nil {
			return fmt.Errorf("invalid saga ID format: %w", err)
		}

		payloadJSON := utils.AsJSON(env.Payload)
		var rawPayload json.RawMessage
		if payloadJSON != nil {
			rawPayload = json.RawMessage(*payloadJSON)
		}

		if err := repo.UpdateSagaInstanceStep(ctx, sagaUUID, "prepare", "running", rawPayload); err != nil {
			return err
		}

		payloadBytes, _ := json.Marshal(env.Payload)
		if err := o.publishNext(ctx, genericEnv, events.PipelinePrepareRequest, payloadBytes); err != nil {
			return err
		}
		return o.publishStateChanged(ctx, env.SagaID, events.SagaStatusRunning, events.SagaStepPrepare, payloadBytes)
	})
}

func (o *Orchestrator) onPrepareCompleted(ctx context.Context, env events.Envelope[events.PrepareCompleted]) error {
	return utils.WithTx(ctx, o.db, func(tx *sql.Tx) error {
		if err := appendEvent(ctx, tx, env, env.Payload); err != nil {
			return err
		}

		repo := storage.NewSagaInstancesRepo(tx)
		sagaUUID, err := uuid.Parse(env.SagaID)
		if err != nil {
			return fmt.Errorf("invalid saga ID format: %w", err)
		}

		payloadJSON := utils.AsJSON(env.Payload)
		var rawPayload json.RawMessage
		if payloadJSON != nil {
			rawPayload = json.RawMessage(*payloadJSON)
		}

		if err := repo.UpdateSagaInstanceStep(ctx, sagaUUID, events.SagaStepPrepare, events.SagaStatusCompleted, rawPayload); err != nil {
			return err
		}
		payloadBytes, _ := json.Marshal(env.Payload)
		return o.publishStateChanged(ctx, env.SagaID, events.SagaStatusCompleted, events.SagaStepPrepare, payloadBytes)
	})
}

func (o *Orchestrator) onPipelineFailed(ctx context.Context, env events.Envelope[events.Failed]) error {
	return utils.WithTx(ctx, o.db, func(tx *sql.Tx) error {
		if err := appendEvent(ctx, tx, env, env.Payload); err != nil {
			return err
		}
		repo := storage.NewSagaInstancesRepo(tx)
		sagaUUID, err := uuid.Parse(env.SagaID)
		if err != nil {
			return fmt.Errorf("invalid saga ID format: %w", err)
		}

		payloadJSON := utils.AsJSON(env.Payload)
		var rawPayload json.RawMessage
		if payloadJSON != nil {
			rawPayload = json.RawMessage(*payloadJSON)
		}

		if err := repo.UpdateSagaInstanceStatus(ctx, sagaUUID, events.SagaStatusFailed, rawPayload); err != nil {
			return err
		}
		payloadBytes, _ := json.Marshal(env.Payload)
		// TODO: keep step empty or filled
		return o.publishStateChanged(ctx, env.SagaID, events.SagaStatusFailed, "", payloadBytes)
	})
}

func (o *Orchestrator) ensureProcessedNotExists(ctx context.Context, tx *sql.Tx, messageID string) error {
	repo := storage.NewProcessedMessagesRepo(tx)
	messageUUID, err := uuid.Parse(messageID)
	if err != nil {
		return fmt.Errorf("invalid message ID format: %w", err)
	}

	if err := repo.InsertProcessedMessage(ctx, messageUUID); err != nil {
		if utils.IsUniqueViolation(err) {
			return db.ErrDuplicate
		}
		return err
	}
	return nil
}

func appendEvent[T IncomingEventType](ctx context.Context, tx *sql.Tx, env events.Envelope[T], payload T) error {
	repo := storage.NewSagaRepo(tx)

	sagaUUID, err := uuid.Parse(env.SagaID)
	if err != nil {
		return fmt.Errorf("invalid saga ID format: %w", err)
	}

	messageUUID, err := uuid.Parse(env.MessageID)
	if err != nil {
		return fmt.Errorf("invalid message ID format: %w", err)
	}

	payloadJSON := utils.AsJSON(payload)
	var rawPayload json.RawMessage
	if payloadJSON != nil {
		rawPayload = json.RawMessage(*payloadJSON)
	}

	return repo.InsertEvent(ctx, storage.SagaEventRow{
		SagaID:     sagaUUID,
		MessageID:  messageUUID,
		Type:       string(env.Type),
		Payload:    rawPayload,
		OccurredAt: env.OccurredAt,
	})
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

func (o *Orchestrator) publishStateChanged(ctx context.Context, envSagaID string, status events.SagaStatus, step events.SagaStep, contextPayload json.RawMessage) error {
	payload := map[string]any{
		"status": status,
		"step":   step,
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
