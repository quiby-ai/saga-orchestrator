package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/quiby-ai/saga-orchestrator/internal/config"
)

type Producer struct {
	w *kafka.Writer
}

func NewProducer(cfg config.Config) *Producer {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      cfg.KafkaBrokers,
		Balancer:     &kafka.Hash{},
		RequiredAcks: int(kafka.RequireAll),
		Async:        false,
	})
	return &Producer{w: w}
}

func (p *Producer) Close() error { return p.w.Close() }

func (p *Producer) Publish(ctx context.Context, topic string, key []byte, value []byte, headers []kafka.Header) error {
	msg := kafka.Message{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: headers,
		Time:    time.Now(),
	}
	return p.w.WriteMessages(ctx, msg)
}

type Consumer struct {
	r *kafka.Reader
}

func NewConsumer(cfg config.Config, groupID string, topics []string) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:               cfg.KafkaBrokers,
		GroupID:               groupID,
		GroupTopics:           topics,
		MinBytes:              1,
		MaxBytes:              10e6,
		MaxWait:               500 * time.Millisecond,
		CommitInterval:        0, // manual commit
		ReadLagInterval:       -1,
		WatchPartitionChanges: true,
	})
	return &Consumer{r: r}
}

func (c *Consumer) Close() error { return c.r.Close() }

func RunConsumerLoop(ctx context.Context, c *Consumer, handler func(context.Context, kafka.Message) error) error {
	for {
		m, err := c.r.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		hErr := handler(ctx, m)
		if hErr != nil {
			// Handler is responsible for publishing retry/DLQ and deciding whether to commit.
			// We still commit to avoid partition stall per spec.
			_ = c.r.CommitMessages(ctx, m)
			continue
		}
		if err := c.r.CommitMessages(ctx, m); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
	}
}
