package httpserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/quiby-ai/saga-orchestrator/internal/config"
	"github.com/quiby-ai/saga-orchestrator/internal/types"
)

type Server struct {
	cfg  config.Config
	db   *sql.DB
	http *http.Server
	prod orkPublisher
}

type orkPublisher interface {
	Publish(ctx context.Context, topic string, key []byte, value []byte, headers []kafka.Header) error
}

func NewServer(cfg config.Config, db *sql.DB) *Server { // producer will be injected later via setter
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

type startRequest struct {
	AppID     string   `json:"app_id"`
	AppName   string   `json:"app_name"`
	Countries []string `json:"countries"`
	DateFrom  string   `json:"date_from"`
	DateTo    string   `json:"date_to"`
}

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

	var req startRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("invalid json"))
		return
	}

	sagaID := uuid.NewString()
	traceID := ""
	now := time.Now().UTC()

	// idempotency: return existing saga if key exists
	var existingSaga string
	_ = s.db.QueryRowContext(r.Context(), `SELECT saga_id FROM idempotency_keys WHERE key=$1`, idemKey).Scan(&existingSaga)
	if existingSaga != "" {
		writeJSON(w, http.StatusOK, startResponse{SagaID: existingSaga, Status: "running"})
		return
	}

	err := s.withTx(r.Context(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(r.Context(),
			`INSERT INTO saga_instances(saga_id, status, current_step, trace_id, input) VALUES ($1,'running','extract',$2,$3)`,
			sagaID, traceID, jsonMustMarshal(req)); err != nil {
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
		if isUniqueViolation(err) {
			writeJSON(w, http.StatusOK, startResponse{SagaID: sagaID, Status: "running"})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("failed to start saga"))
		return
	}

	// Publish state.changed running and extract.request event
	stateEnv := types.Envelope{MessageID: uuid.NewString(), SagaID: sagaID, Type: s.cfg.TopicStateChanged, OccurredAt: now,
		Payload: jsonMustMarshal(map[string]any{"payload": map[string]any{"status": "running", "step": "extract"}}),
		Meta:    types.Meta{AppID: s.cfg.AppID, TenantID: tenantID, Initiator: "user", SchemaVersion: "v1"}}
	stateValue, _ := json.Marshal(stateEnv)
	if s.prod != nil {
		_ = s.prod.Publish(r.Context(), s.cfg.TopicStateChanged, []byte(sagaID), stateValue, []kafka.Header{{Key: "type", Value: []byte(stateEnv.Type)}, {Key: "saga_id", Value: []byte(sagaID)}})
	}

	// Publish extract.request event
	env := types.Envelope{
		MessageID:  uuid.NewString(),
		SagaID:     sagaID,
		Type:       s.cfg.TopicExtractRequest,
		OccurredAt: now,
		Payload:    jsonMustMarshal(req),
		Meta:       types.Meta{AppID: s.cfg.AppID, TenantID: tenantID, Initiator: "user", SchemaVersion: "v1"},
	}
	value, _ := json.Marshal(env)
	headers := []kafka.Header{
		{Key: "message_id", Value: []byte(env.MessageID)},
		{Key: "saga_id", Value: []byte(env.SagaID)},
		{Key: "type", Value: []byte(env.Type)},
		{Key: "schema_version", Value: []byte(env.Meta.SchemaVersion)},
	}
	if s.prod != nil {
		_ = s.prod.Publish(r.Context(), s.cfg.TopicExtractRequest, []byte(env.SagaID), value, headers)
	}

	writeJSON(w, http.StatusOK, startResponse{SagaID: sagaID, Status: "running"})
}

func (s *Server) withTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func jsonMustMarshal(v any) []byte { b, _ := json.Marshal(v); return b }

func isUniqueViolation(err error) bool {
	return err != nil && (contains(err.Error(), "duplicate key") || contains(err.Error(), "unique"))
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
