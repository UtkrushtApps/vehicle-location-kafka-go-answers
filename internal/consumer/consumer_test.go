package consumer

import (
	"testing"
	"time"
)

func TestLagSnapshotDoesNotReturnNegativeLag(t *testing.T) {
	s := LagSnapshot{Topic: "vehicle-location.v1", Partition: 2, HighWater: 10, Current: 15, ObservedAt: time.Now()}
	if got := s.Lag(); got != 0 {
		t.Fatalf("lag = %d, want 0", got)
	}
}
