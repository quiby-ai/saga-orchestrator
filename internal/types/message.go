package types

import (
	"encoding/json"
	"time"
)

type Meta struct {
	AppID         string `json:"app_id"`
	TenantID      string `json:"tenant_id"`
	Initiator     string `json:"initiator"`
	Retries       int    `json:"retries"`
	SchemaVersion string `json:"schema_version"`
}

type Envelope struct {
	MessageID  string          `json:"message_id"`
	TraceID    string          `json:"trace_id"`
	SagaID     string          `json:"saga_id"`
	Type       string          `json:"type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload"`
	Meta       Meta            `json:"meta"`
}

func (e Envelope) Marshal() []byte {
	b, _ := json.Marshal(e)
	return b
}
