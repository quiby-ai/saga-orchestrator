package utils

import (
	"github.com/quiby-ai/common/pkg/events"
)

// ToGenericEnvelope converts a specific event envelope to a generic one for storage and publishing
func ToGenericEnvelope[T any](env events.Envelope[T]) events.Envelope[any] {
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
