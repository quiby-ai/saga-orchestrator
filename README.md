# Saga Orchestrator

The Saga Orchestrator manages the lifecycle of review processing pipelines using the Saga pattern.

## Features

- **Saga Management**: Orchestrates multi-step review processing workflows
- **Event-Driven**: Uses Kafka events for communication between services
- **Idempotency**: Ensures message processing is idempotent
- **State Tracking**: Maintains saga state in PostgreSQL
- **HTTP API**: Provides REST endpoints for saga management

## Architecture

The orchestrator listens to pipeline completion events and orchestrates the next steps:

1. **Extract Completed** → Triggers Prepare Request
2. **Prepare Completed** → Marks Saga as Complete
3. **Pipeline Failed** → Marks Saga as Failed

## Kafka Integration

The orchestrator leverages the `github.com/quiby-ai/common/pkg/events` package for:

- **Standardized Event Envelopes**: Consistent event structure across services
- **Automatic Headers**: Kafka headers are automatically generated from event metadata
- **Topic Constants**: Predefined topic names for all pipeline events
- **Event Validation**: Built-in validation for event payloads
- **Simplified Publishing**: Single `PublishEvent` method handles all event publishing

### Event Flow

```
User Request → HTTP API → Extract Request → Extract Service
                                    ↓
                            Extract Completed → Prepare Request → Prepare Service
                                    ↓
                            Prepare Completed → Saga Complete
                                    ↓
                            Pipeline Failed → Saga Failed
```

## Configuration

The service automatically uses topic names from the common events package:

- `pipeline.extract_reviews.request`
- `pipeline.extract_reviews.completed`
- `pipeline.prepare_reviews.request`
- `pipeline.prepare_reviews.completed`
- `pipeline.failed`
- `saga.orchestrator.state.changed`

You can override these in `config.toml` if needed.

## Development

### Prerequisites

- Go 1.24+
- PostgreSQL
- Kafka

### Running

```bash
# Set up config
cp config.example.toml config.toml
# Edit config.toml with your settings

# Run
go run cmd/main.go
```

### Testing

```bash
go test ./...
```

## Dependencies

- **Common Events Package**: `github.com/quiby-ai/common/pkg/events`
- **Kafka**: `github.com/segmentio/kafka-go`
- **Database**: `database/sql` with PostgreSQL driver
- **HTTP**: Standard library `net/http`
