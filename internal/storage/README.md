# Storage Layer

This directory contains the storage layer for the saga orchestrator, implementing a clean separation between business logic and data access.

## Structure

```
internal/storage/
├── sqlc/                    # Generated Go code from SQL queries
│   ├── queries/            # SQL query files
│   ├── models.go           # Generated data models
│   ├── querier.go          # Generated query interface
│   └── db.go               # Generated database utilities
├── saga_repo.go            # Repository for saga events
├── saga_instances_repo.go  # Repository for saga instances
├── processed_messages_repo.go # Repository for processed messages
└── README.md               # This file
```

## Key Benefits

1. **Separation of Concerns**: SQL queries are separated from business logic
2. **Type Safety**: sqlc generates compile-time type-safe Go code
3. **Transaction Support**: All repositories work with both `*sql.DB` and `*sql.Tx`
4. **Maintainability**: SQL is centralized and version-controlled
5. **Performance**: No ORM overhead, pure `database/sql` under the hood

## Usage

### Basic Repository Usage

```go
// With a database connection
db, _ := sql.Open("postgres", connStr)
repo := storage.NewSagaRepo(db)

// With a transaction
tx, _ := db.BeginTx(ctx, nil)
repo := storage.NewSagaRepo(tx)
defer tx.Rollback()
```

### Transaction Boundary Pattern

```go
func (o *Orchestrator) handleEvent(ctx context.Context, env events.Envelope[T]) error {
    return utils.WithTx(ctx, o.db, func(tx *sql.Tx) error {
        // Create repositories with transaction
        sagaRepo := storage.NewSagaRepo(tx)
        instancesRepo := storage.NewSagaInstancesRepo(tx)
        
        // Use repositories within transaction
        if err := sagaRepo.InsertEvent(ctx, eventData); err != nil {
            return err
        }
        
        if err := instancesRepo.UpdateSagaInstanceStep(ctx, sagaID, "prepare", "running", payload); err != nil {
            return err
        }
        
        return nil
    })
}
```

## Adding New Queries

1. Add SQL to the appropriate `.sql` file in `sqlc/queries/`
2. Run `sqlc generate` to regenerate Go code
3. Add methods to the appropriate repository
4. Update the orchestrator to use the new repository methods

## SQL Query Files

- `saga_events.sql` - Saga event operations
- `saga_instances.sql` - Saga instance operations  
- `processed_messages.sql` - Message deduplication

## Generated Code

The `sqlc/` directory contains auto-generated Go code. **Never edit these files manually**. To regenerate:

```bash
sqlc generate
```

## Dependencies

- `github.com/sqlc-dev/sqlc` - SQL code generator
- `github.com/sqlc-dev/pqtype` - PostgreSQL-specific types
- `github.com/google/uuid` - UUID handling
