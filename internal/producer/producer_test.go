package producer

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"vehicle-location-kafka-go/internal/events"
)

type captureWriter struct {
	messages []kafka.Message
	err      error
}

func (w *captureWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	w.messages = append(w.messages, msgs...)
	return w.err
}

func (w *captureWriter) Close() error { return nil }

func sampleLocation(vehicleID, region string, seq int64) events.LocationEvent {
	return events.LocationEvent{
		VehicleID:     vehicleID,
		Region:        region,
		Latitude:      12.9716,
		Longitude:     77.5946,
		RecordedAt:    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Sequence:      seq,
		CorrelationID: "corr-test",
	}
}

func TestLocationMessageKeyFollowsVehicleIdentity(t *testing.T) {
	evt := sampleLocation("veh-42", "south", 7)
	msg, err := BuildLocationMessage("vehicle-location.v1", evt)
	if err != nil {
		t.Fatalf("build message: %v", err)
	}
	if got, want := string(msg.Key), evt.VehicleID; got != want {
		t.Fatalf("message key = %q, want %q", got, want)
	}
}

func TestLocationMessageKeyIsStableAcrossRegions(t *testing.T) {
	north, err := BuildLocationMessage("vehicle-location.v1", sampleLocation("veh-42", "north", 1))
	if err != nil {
		t.Fatalf("build north message: %v", err)
	}
	south, err := BuildLocationMessage("vehicle-location.v1", sampleLocation("veh-42", "south", 2))
	if err != nil {
		t.Fatalf("build south message: %v", err)
	}
	if string(north.Key) != string(south.Key) {
		t.Fatalf("same vehicle produced different kafka keys: %q vs %q", string(north.Key), string(south.Key))
	}
}

func TestPublishLocationWritesKafkaMessage(t *testing.T) {
	writer := &captureWriter{}
	metrics := NewMetrics()
	publisher := NewKafkaPublisherWithWriter("vehicle-location.v1", writer, slog.Default(), metrics)
	if err := publisher.PublishLocation(context.Background(), sampleLocation("veh-1", "north", 1)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(writer.messages) != 1 {
		t.Fatalf("messages written = %d, want 1", len(writer.messages))
	}
	if got, want := writer.messages[0].Topic, "vehicle-location.v1"; got != want {
		t.Fatalf("message topic = %q, want %q", got, want)
	}
	if metrics.Snapshot().Published != 1 {
		t.Fatalf("published metric not recorded")
	}
}

func TestPublishLocationWrapsWriterErrors(t *testing.T) {
	writerErr := errors.New("broker unavailable")
	writer := &captureWriter{err: writerErr}
	metrics := NewMetrics()
	publisher := NewKafkaPublisherWithWriter("vehicle-location.v1", writer, slog.Default(), metrics)
	err := publisher.PublishLocation(context.Background(), sampleLocation("veh-1", "north", 1))
	if !errors.Is(err, writerErr) {
		t.Fatalf("expected wrapped writer error, got %v", err)
	}
	if metrics.Snapshot().Failed != 1 {
		t.Fatalf("failed metric not recorded")
	}
}

func TestVehicleHashBalancerKeepsVehicleOnSamePartitionAndRecordsSpread(t *testing.T) {
	metrics := NewMetrics()
	balancer := newVehicleHashBalancer(metrics)
	partitions := []int{0, 1, 2, 3, 4, 5}

	first := balancer.Balance(kafka.Message{Key: []byte("veh-42")}, partitions...)
	for i := 0; i < 10; i++ {
		if got := balancer.Balance(kafka.Message{Key: []byte("veh-42")}, partitions...); got != first {
			t.Fatalf("same vehicle moved partitions: got %d want %d", got, first)
		}
	}

	seen := map[int]bool{}
	for i := 0; i < 100; i++ {
		partition := balancer.Balance(kafka.Message{Key: []byte("veh-spread-" + string(rune(i)))}, partitions...)
		seen[partition] = true
	}
	if len(seen) < 3 {
		t.Fatalf("hash balancer did not spread keys enough, saw partitions: %v", seen)
	}
	if len(metrics.Snapshot().Partitions) == 0 {
		t.Fatalf("partition metrics not recorded")
	}
}

func BenchmarkBuildLocationMessage(b *testing.B) {
	evt := sampleLocation("veh-bench", "west", 10)
	for i := 0; i < b.N; i++ {
		_, err := BuildLocationMessage("vehicle-location.v1", evt)
		if err != nil {
			b.Fatal(err)
		}
	}
}
