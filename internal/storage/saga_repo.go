package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	sqlcgen "github.com/quiby-ai/saga-orchestrator/internal/storage/sqlc"
)

// TxOrDB interface allows the repository to work with both *sql.DB and *sql.Tx
type TxOrDB interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type SagaRepo struct {
	q *sqlcgen.Queries
}

func NewSagaRepo(db TxOrDB) *SagaRepo {
	return &SagaRepo{q: sqlcgen.New(db)}
}

// SagaEventRow represents the data needed to insert a saga event
type SagaEventRow struct {
	SagaID     uuid.UUID
	MessageID  uuid.UUID
	Type       string
	Payload    json.RawMessage
	OccurredAt time.Time
}

// InsertEvent inserts a new saga event
func (r *SagaRepo) InsertEvent(ctx context.Context, e SagaEventRow) error {
	return r.q.InsertSagaEvent(ctx, sqlcgen.InsertSagaEventParams{
		SagaID:     e.SagaID,
		MessageID:  e.MessageID,
		Type:       e.Type,
		Payload:    e.Payload,
		OccurredAt: e.OccurredAt,
	})
}

// GetEvents retrieves all events for a saga
func (r *SagaRepo) GetEvents(ctx context.Context, sagaID uuid.UUID) ([]sqlcgen.SagaEvent, error) {
	return r.q.GetSagaEvents(ctx, sagaID)
}

// GetEventByMessageID retrieves a specific event by message ID
func (r *SagaRepo) GetEventByMessageID(ctx context.Context, messageID uuid.UUID) (sqlcgen.SagaEvent, error) {
	return r.q.GetSagaEventByMessageID(ctx, messageID)
}
