package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/quiby-ai/common/pkg/events"
	"github.com/segmentio/kafka-go"

	"github.com/quiby-ai/saga-orchestrator/config"
	"github.com/quiby-ai/saga-orchestrator/internal/db"
	"github.com/quiby-ai/saga-orchestrator/internal/storage"
	"github.com/quiby-ai/saga-orchestrator/internal/utils"
)

// Orchestrator handles saga orchestration logic
type Orchestrator struct {
	cfg config.Config
	db  *sql.DB
	p   producer
}

// producer interface for publishing events
type producer interface {
	PublishEvent(ctx context.Context, topic string, key []byte, envelope events.Envelope[any]) error
}

// NewOrchestrator creates a new orchestrator instance
func NewOrchestrator(cfg config.Config, db *sql.DB, p producer) *Orchestrator {
	return &Orchestrator{
		cfg: cfg,
		db:  db,
		p:   p,
	}
}

// HandleMessage processes incoming Kafka messages
func (o *Orchestrator) HandleMessage(ctx context.Context, msg kafka.Message) error {
	// Check if message was already processed
	if err := o.checkMessageProcessed(ctx, msg); err != nil {
		return err
	}

	// Parse message type and route to appropriate handler
	return o.routeMessage(ctx, msg)
}

// checkMessageProcessed ensures message hasn't been processed before
func (o *Orchestrator) checkMessageProcessed(ctx context.Context, msg kafka.Message) error {
	messageID := utils.GetHeader(msg, "message_id")
	if messageID == "" {
		return nil // No message ID, proceed normally
	}

	return utils.WithTx(ctx, o.db, func(tx *sql.Tx) error {
		if err := o.ensureProcessedNotExists(ctx, tx, messageID); err != nil {
			if errors.Is(err, db.ErrDuplicate) {
				return db.ErrDuplicate // Signal that message was already processed
			}
			return err
		}
		return nil
	})
}

// routeMessage routes the message to the appropriate handler based on type
func (o *Orchestrator) routeMessage(ctx context.Context, msg kafka.Message) error {
	var baseEnvelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(msg.Value, &baseEnvelope); err != nil {
		return fmt.Errorf("failed to unmarshal base envelope: %w", err)
	}

	handlers := map[string]func(context.Context, []byte) error{
		events.PipelineExtractCompleted: o.handleExtractCompleted,
		events.PipelinePrepareCompleted: o.handlePrepareCompleted,
		events.PipelineFailed:           o.handlePipelineFailed,
	}

	handler, exists := handlers[baseEnvelope.Type]
	if !exists {
		return fmt.Errorf("unknown message type: %s", baseEnvelope.Type)
	}

	return handler(ctx, msg.Value)
}

// handleExtractCompleted processes extract completed events
func (o *Orchestrator) handleExtractCompleted(ctx context.Context, msgValue []byte) error {
	envelope, err := events.UnmarshalEnvelope[events.ExtractCompleted](msgValue)
	if err != nil {
		return fmt.Errorf("failed to unmarshal extract completed envelope: %w", err)
	}

	fmt.Printf("got envelope: %v\n", envelope)

	return o.processSagaEvent(ctx, convertToAnyEnvelope(envelope), func(ctx context.Context, tx *sql.Tx, sagaUUID uuid.UUID, payload json.RawMessage) error {
		repo := storage.NewSagaInstancesRepo(tx)
		return repo.UpdateSagaInstanceStep(ctx, sagaUUID, events.SagaStepPrepare, events.SagaStatusRunning, payload)
	}, events.SagaStatusRunning, events.SagaStepPrepare)
}

// handlePrepareCompleted processes prepare completed events
func (o *Orchestrator) handlePrepareCompleted(ctx context.Context, msgValue []byte) error {
	envelope, err := events.UnmarshalEnvelope[events.PrepareCompleted](msgValue)
	if err != nil {
		return fmt.Errorf("failed to unmarshal prepare completed envelope: %w", err)
	}

	return o.processSagaEvent(ctx, convertToAnyEnvelope(envelope), func(ctx context.Context, tx *sql.Tx, sagaUUID uuid.UUID, payload json.RawMessage) error {
		repo := storage.NewSagaInstancesRepo(tx)
		return repo.UpdateSagaInstanceStep(ctx, sagaUUID, events.SagaStepPrepare, events.SagaStatusCompleted, payload)
	}, events.SagaStatusCompleted, events.SagaStepPrepare)
}

// handlePipelineFailed processes pipeline failed events
func (o *Orchestrator) handlePipelineFailed(ctx context.Context, msgValue []byte) error {
	envelope, err := events.UnmarshalEnvelope[events.Failed](msgValue)
	if err != nil {
		return fmt.Errorf("failed to unmarshal pipeline failed envelope: %w", err)
	}

	return o.processSagaEvent(ctx, convertToAnyEnvelope(envelope), func(ctx context.Context, tx *sql.Tx, sagaUUID uuid.UUID, payload json.RawMessage) error {
		repo := storage.NewSagaInstancesRepo(tx)
		return repo.UpdateSagaInstanceStatus(ctx, sagaUUID, events.SagaStatusFailed, payload)
	}, events.SagaStatusFailed, "")
}

// convertToAnyEnvelope converts a typed envelope to an any envelope
func convertToAnyEnvelope[T any](env events.Envelope[T]) events.Envelope[any] {
	return events.Envelope[any]{
		MessageID:  env.MessageID,
		TraceID:    env.TraceID,
		SagaID:     env.SagaID,
		Type:       env.Type,
		OccurredAt: env.OccurredAt,
		Payload:    env.Payload,
		Meta:       env.Meta,
	}
}

// processSagaEvent is a common helper for processing saga events
func (o *Orchestrator) processSagaEvent(
	ctx context.Context,
	env events.Envelope[any],
	updateFn func(context.Context, *sql.Tx, uuid.UUID, json.RawMessage) error,
	status events.SagaStatus,
	step events.SagaStep,
) error {
	return utils.WithTx(ctx, o.db, func(tx *sql.Tx) error {
		// Store the event
		if err := o.appendEvent(ctx, tx, env); err != nil {
			return fmt.Errorf("failed to append event: %w", err)
		}

		// Update saga instance
		sagaUUID, err := uuid.Parse(env.SagaID)
		if err != nil {
			return fmt.Errorf("invalid saga ID format: %w", err)
		}

		payloadJSON := utils.AsJSON(env.Payload)
		var rawPayload json.RawMessage
		if payloadJSON != nil {
			rawPayload = json.RawMessage(*payloadJSON)
		}

		if err = updateFn(ctx, tx, sagaUUID, rawPayload); err != nil {
			return fmt.Errorf("failed to update saga instance: %w", err)
		}

		// Publish state change
		return o.publishStateChanged(ctx, env.SagaID, status, step)
	})
}

// appendEvent stores a saga event in the database
func (o *Orchestrator) appendEvent(ctx context.Context, tx *sql.Tx, env events.Envelope[any]) error {
	repo := storage.NewSagaRepo(tx)

	sagaUUID, err := uuid.Parse(env.SagaID)
	if err != nil {
		return fmt.Errorf("failed to append event: %w", err)
	}

	messageUUID, err := uuid.Parse(env.MessageID)
	if err != nil {
		return fmt.Errorf("failed to append event: %w", err)
	}

	payloadJSON := utils.AsJSON(env.Payload)
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

// ensureProcessedNotExists ensures a message hasn't been processed before
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
		return fmt.Errorf("failed to insert processed message: %w", err)
	}
	return nil
}

// publishStateChanged publishes a state change event
func (o *Orchestrator) publishStateChanged(ctx context.Context, sagaID string, status events.SagaStatus, step events.SagaStep) error {
	envelope := events.Envelope[events.StateChanged]{
		MessageID:  uuid.NewString(),
		SagaID:     sagaID,
		Type:       events.SagaStateChanged,
		OccurredAt: time.Now().UTC(),
		Payload: events.StateChanged{
			Status:  status,
			Step:    step,
			Context: events.StateChangedContext{Message: "Quiby is doing something..."},
		},
		Meta: events.Meta{
			AppID:         o.cfg.App.ID,
			SchemaVersion: events.SchemaVersionV1,
		},
	}

	return o.publish(ctx, events.SagaStateChanged, convertToAnyEnvelope(envelope))
}

// publish publishes an event to Kafka
func (o *Orchestrator) publish(ctx context.Context, topic string, env events.Envelope[any]) error {
	key := []byte(env.SagaID)
	return o.p.PublishEvent(ctx, topic, key, env)
}
