package events

import (
	"os"
	"strings"
	"testing"
)

func TestDecodeLocationEventFixtures(t *testing.T) {
	data, err := os.ReadFile("../../testdata/sample-events.jsonl")
	if err != nil {
		t.Fatalf("read fixtures: %v", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		evt, err := DecodeLocationEvent([]byte(line))
		if err != nil {
			t.Fatalf("decode fixture %q: %v", line, err)
		}
		if evt.VehicleID == "" || evt.Region == "" {
			t.Fatalf("decoded event missing identity fields: %+v", evt)
		}
	}
}

func TestRejectsInvalidCoordinates(t *testing.T) {
	_, err := DecodeLocationEvent([]byte(`{"vehicle_id":"veh-1","region":"north","latitude":95,"longitude":77.1,"recorded_at":"2026-01-02T03:04:05Z","sequence":1}`))
	if err == nil {
		t.Fatal("expected invalid latitude to be rejected")
	}
}
