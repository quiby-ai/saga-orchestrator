package storage

import (
	"context"

	"github.com/google/uuid"
	sqlcgen "github.com/quiby-ai/saga-orchestrator/internal/storage/sqlc"
)

type ProcessedMessagesRepo struct {
	q *sqlcgen.Queries
}

func NewProcessedMessagesRepo(db TxOrDB) *ProcessedMessagesRepo {
	return &ProcessedMessagesRepo{q: sqlcgen.New(db)}
}

// InsertProcessedMessage inserts a new processed message
func (r *ProcessedMessagesRepo) InsertProcessedMessage(ctx context.Context, messageID uuid.UUID) error {
	return r.q.InsertProcessedMessage(ctx, messageID)
}

// GetProcessedMessage retrieves a processed message by ID
func (r *ProcessedMessagesRepo) GetProcessedMessage(ctx context.Context, messageID uuid.UUID) (sqlcgen.ProcessedMessage, error) {
	return r.q.GetProcessedMessage(ctx, messageID)
}
