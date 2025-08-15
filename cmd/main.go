package main

import (
	"context"
	"github.com/quiby-ai/common/pkg/events"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/quiby-ai/saga-orchestrator/internal/config"
	"github.com/quiby-ai/saga-orchestrator/internal/db"
	"github.com/quiby-ai/saga-orchestrator/internal/httpserver"
	kafkax "github.com/quiby-ai/saga-orchestrator/internal/kafka"
	"github.com/quiby-ai/saga-orchestrator/internal/orchestrator"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Load()

	pg, err := db.Connect(ctx, cfg)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pg.Close()

	if err := db.RunMigrations(ctx, pg); err != nil {
		log.Fatalf("db migrate: %v", err)
	}

	producer := kafkax.NewProducer(cfg)
	defer func() {
		_ = producer.Close()
	}()

	orcReader := kafkax.NewConsumer(cfg, cfg.OrchestratorGroupID, []string{
		events.PipelineExtractCompleted,
		events.PipelinePrepareCompleted,
		events.PipelineFailed,
	})
	defer func() {
		_ = orcReader.Close()
	}()

	orc := orchestrator.NewOrchestrator(cfg, pg, producer)

	server := httpserver.NewServer(cfg, pg)
	server.InjectProducer(producer)
	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	go func() {
		if err := kafkax.RunConsumerLoop(ctx, orcReader, orc.HandleMessage); err != nil {
			log.Printf("orchestrator consumer stopped: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
}
