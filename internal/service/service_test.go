package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"vehicle-location-kafka-go/internal/events"
	"vehicle-location-kafka-go/internal/producer"
)

type stubPublisher struct {
	err    error
	events []events.LocationEvent
}

func (s *stubPublisher) PublishLocation(ctx context.Context, evt events.LocationEvent) error {
	s.events = append(s.events, evt)
	return s.err
}

func TestCreateLocationAcceptsValidRequest(t *testing.T) {
	pub := &stubPublisher{}
	api := NewAPI(pub, producer.NewMetrics(), slog.Default())
	req := httptest.NewRequest(http.MethodPost, "/api/v1/locations", strings.NewReader(`{"vehicle_id":"veh-1","region":"north","latitude":12.9,"longitude":77.5,"recorded_at":"2026-01-02T03:04:05Z","sequence":1}`))
	rec := httptest.NewRecorder()
	api.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		body, _ := io.ReadAll(rec.Body)
		t.Fatalf("status = %d body=%s", rec.Code, string(body))
	}
	if len(pub.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(pub.events))
	}
}

func TestCreateLocationReportsPublishFailure(t *testing.T) {
	pub := &stubPublisher{err: errors.New("write failed")}
	api := NewAPI(pub, producer.NewMetrics(), slog.Default())
	req := httptest.NewRequest(http.MethodPost, "/api/v1/locations", strings.NewReader(`{"vehicle_id":"veh-1","region":"north","latitude":12.9,"longitude":77.5,"recorded_at":"2026-01-02T03:04:05Z","sequence":1}`))
	rec := httptest.NewRecorder()
	api.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadGateway)
	}
}

func TestMetricsEndpointReturnsSnapshot(t *testing.T) {
	metrics := producer.NewMetrics()
	metrics.Record(true, 25, 3)
	api := NewAPI(&stubPublisher{}, metrics, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	api.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), `"published":1`) || !strings.Contains(rec.Body.String(), `"last_latency_ms":25`) {
		t.Fatalf("metrics response missing expected fields: %s", rec.Body.String())
	}
}
