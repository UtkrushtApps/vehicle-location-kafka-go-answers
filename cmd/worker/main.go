package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"vehicle-location-kafka-go/internal/config"
	"vehicle-location-kafka-go/internal/logging"
	"vehicle-location-kafka-go/internal/producer"
	"vehicle-location-kafka-go/internal/service"
)

func main() {
	cfg := config.Local()
	logger := logging.New()
	metrics := producer.NewMetrics()
	publisher := producer.NewKafkaPublisher(cfg.KafkaBrokers, cfg.LocationTopic, logger, metrics)
	api := service.NewAPI(publisher, metrics, logger)

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("vehicle location service started", slog.String("addr", cfg.HTTPAddr), slog.String("topic", cfg.LocationTopic))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", slog.String("error", err.Error()))
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown failed", slog.String("error", err.Error()))
	}
	if err := publisher.Close(); err != nil {
		logger.Error("publisher close failed", slog.String("error", err.Error()))
	}
}
