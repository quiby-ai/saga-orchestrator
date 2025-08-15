package httpserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/quiby-ai/common/pkg/events"
	"github.com/quiby-ai/saga-orchestrator/internal/config"
	"github.com/quiby-ai/saga-orchestrator/internal/utils"
)

type Server struct {
	cfg  config.Config
	db   *sql.DB
	http *http.Server
	prod orkPublisher
}

type orkPublisher interface {
	PublishEvent(ctx context.Context, topic string, key []byte, envelope events.Envelope[any]) error
}

func NewServer(cfg config.Config, db *sql.DB) *Server {
	mux := http.NewServeMux()
	s := &Server{
		cfg: cfg,
		db:  db,
		http: &http.Server{
			Addr:    cfg.HTTPAddr,
			Handler: mux,
		},
	}

	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/v1/sagas/reviews/start", s.handleStartSaga)

	return s
}

func (s *Server) Start() error {
	return s.http.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// InjectProducer allows wiring after creation
func (s *Server) InjectProducer(p orkPublisher) { s.prod = p }

type startResponse struct {
	SagaID string `json:"saga_id"`
	Status string `json:"status"`
}

func (s *Server) handleStartSaga(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	tenantID := r.Header.Get("X-Tenant-ID")
	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey == "" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("missing Idempotency-Key"))
		return
	}

	var req events.ExtractRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("invalid json"))
		return
	}

	sagaID := uuid.NewString()
	traceID := ""
	now := time.Now().UTC()

	var existingSaga string
	_ = s.db.QueryRowContext(r.Context(), `SELECT saga_id FROM idempotency_keys WHERE key=$1`, idemKey).Scan(&existingSaga)
	if existingSaga != "" {
		utils.WriteJSON(w, http.StatusOK, startResponse{SagaID: existingSaga, Status: "running"})
		return
	}

	err := utils.WithTx(r.Context(), s.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(r.Context(),
			`INSERT INTO saga_instances(saga_id, status, current_step, trace_id, input) VALUES ($1,'running','extract',$2,$3)`,
			sagaID, traceID, utils.MustMarshal(req)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(r.Context(),
			`INSERT INTO idempotency_keys(key, saga_id) VALUES ($1,$2)`, idemKey, sagaID); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		// if duplicate idempotency key, return 200 with some generic response
		if utils.IsUniqueViolation(err) {
			utils.WriteJSON(w, http.StatusOK, startResponse{SagaID: sagaID, Status: "running"})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("failed to start saga"))
		return
	}

	stateEnv := events.Envelope[events.StateChanged]{
		MessageID:  uuid.NewString(),
		SagaID:     sagaID,
		Type:       events.SagaStateChanged,
		OccurredAt: now,
		Payload: events.StateChanged{
			Status: events.SagaStatusRunning,
			Step:   events.SagaStepExtract,
			// TODO: move to texts dictionary
			Context: events.StateChangedContext{Message: "Quiby is extracting reviews..."},
		},
		Meta: events.Meta{
			AppID:         s.cfg.AppID,
			TenantID:      tenantID,
			Initiator:     events.InitiatorUser,
			SchemaVersion: events.SchemaVersionV1,
		},
	}

	if s.prod != nil {
		genericEnv := utils.ToGenericEnvelope(stateEnv)
		_ = s.prod.PublishEvent(r.Context(), s.cfg.TopicStateChanged, []byte(sagaID), genericEnv)
	}

	env := events.Envelope[events.ExtractRequest]{
		MessageID:  uuid.NewString(),
		SagaID:     sagaID,
		Type:       events.PipelineExtractRequest,
		OccurredAt: now,
		Payload:    req,
		Meta: events.Meta{
			AppID:         s.cfg.AppID,
			TenantID:      tenantID,
			Initiator:     events.InitiatorUser,
			SchemaVersion: events.SchemaVersionV1,
		},
	}

	if s.prod != nil {
		genericEnv := utils.ToGenericEnvelope(env)
		_ = s.prod.PublishEvent(r.Context(), s.cfg.TopicExtractRequest, []byte(env.SagaID), genericEnv)
	}

	utils.WriteJSON(w, http.StatusOK, startResponse{SagaID: sagaID, Status: "running"})
}
