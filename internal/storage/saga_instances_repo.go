package storage

import (
	"context"
	"encoding/json"
	"github.com/quiby-ai/common/pkg/events"

	"github.com/google/uuid"
	sqlcgen "github.com/quiby-ai/saga-orchestrator/internal/storage/sqlc"
	"github.com/sqlc-dev/pqtype"
)

type SagaInstancesRepo struct {
	q *sqlcgen.Queries
}

func NewSagaInstancesRepo(db TxOrDB) *SagaInstancesRepo {
	return &SagaInstancesRepo{q: sqlcgen.New(db)}
}

// UpdateSagaInstanceStep updates a saga instance's step and status
func (r *SagaInstancesRepo) UpdateSagaInstanceStep(ctx context.Context, sagaID uuid.UUID, step events.SagaStep, status events.SagaStatus, output json.RawMessage) error {
	return r.q.UpdateSagaInstanceStep(ctx, sqlcgen.UpdateSagaInstanceStepParams{
		SagaID:      sagaID,
		CurrentStep: string(step),
		Status:      string(status),
		Column4:     output,
	})
}

// UpdateSagaInstanceStatus updates a saga instance's status and error
func (r *SagaInstancesRepo) UpdateSagaInstanceStatus(ctx context.Context, sagaID uuid.UUID, status events.SagaStatus, errorData json.RawMessage) error {
	var nullError pqtype.NullRawMessage
	if errorData != nil {
		nullError.Valid = true
		nullError.RawMessage = errorData
	}

	return r.q.UpdateSagaInstanceStatus(ctx, sqlcgen.UpdateSagaInstanceStatusParams{
		SagaID: sagaID,
		Status: string(status),
		Error:  nullError,
	})
}

// GetSagaInstance retrieves a saga instance by ID
func (r *SagaInstancesRepo) GetSagaInstance(ctx context.Context, sagaID uuid.UUID) (sqlcgen.SagaInstance, error) {
	return r.q.GetSagaInstance(ctx, sagaID)
}
