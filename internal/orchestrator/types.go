package orchestrator

import (
	"context"
	"database/sql"

	"github.com/quiby-ai/common/pkg/events"
	"github.com/quiby-ai/saga-orchestrator/internal/config"
)

type Orchestrator struct {
	cfg config.Config
	db  *sql.DB
	p   producer
}

type producer interface {
	PublishEvent(ctx context.Context, topic string, key []byte, envelope events.Envelope[any]) error
}
