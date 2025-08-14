package utils

import "github.com/segmentio/kafka-go"

// GetHeader extracts a header value from a Kafka message
func GetHeader(m kafka.Message, key string) string {
	for _, h := range m.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
