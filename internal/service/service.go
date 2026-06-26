package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"vehicle-location-kafka-go/internal/events"
	"vehicle-location-kafka-go/internal/producer"
)

type LocationPublisher interface {
	PublishLocation(ctx context.Context, evt events.LocationEvent) error
}

type API struct {
	publisher LocationPublisher
	metrics   *producer.Metrics
	logger    *slog.Logger
}

func NewAPI(publisher LocationPublisher, metrics *producer.Metrics, logger *slog.Logger) *API {
	if metrics == nil {
		metrics = producer.NewMetrics()
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &API{publisher: publisher, metrics: metrics, logger: logger}
}

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.health)
	mux.HandleFunc("/metrics", a.metricsHandler)
	mux.HandleFunc("/api/v1/locations", a.createLocation)
	return mux
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (a *API) metricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(a.metrics.Snapshot())
}

func (a *API) createLocation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var evt events.LocationEvent
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&evt); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if err := evt.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := a.publisher.PublishLocation(r.Context(), evt); err != nil {
		a.logger.Error(
			"location request publish failed",
			slog.String("vehicle_id", evt.VehicleID),
			slog.String("region", evt.Region),
			slog.Int64("sequence", evt.Sequence),
			slog.String("correlation_id", evt.CorrelationID),
			slog.String("error", err.Error()),
		)
		http.Error(w, "publish failed", http.StatusBadGateway)
		return
	}
	a.logger.Info(
		"location request accepted",
		slog.String("vehicle_id", evt.VehicleID),
		slog.String("region", evt.Region),
		slog.Int64("sequence", evt.Sequence),
		slog.String("correlation_id", evt.CorrelationID),
	)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"accepted"}`))
}
