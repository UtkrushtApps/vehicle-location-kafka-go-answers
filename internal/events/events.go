package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
)

const LocationEventVersion = "1"

type LocationEvent struct {
	VehicleID     string    `json:"vehicle_id"`
	Region        string    `json:"region"`
	Latitude      float64   `json:"latitude"`
	Longitude     float64   `json:"longitude"`
	RecordedAt    time.Time `json:"recorded_at"`
	Sequence      int64     `json:"sequence"`
	AccuracyM     *float64  `json:"accuracy_m,omitempty"`
	CorrelationID string    `json:"correlation_id,omitempty"`
}

func DecodeLocationEvent(data []byte) (LocationEvent, error) {
	var evt LocationEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return LocationEvent{}, fmt.Errorf("decode location event: %w", err)
	}
	if err := evt.Validate(); err != nil {
		return LocationEvent{}, err
	}
	return evt, nil
}

func (e LocationEvent) Validate() error {
	if e.VehicleID == "" {
		return errors.New("vehicle_id is required")
	}
	if e.Region == "" {
		return errors.New("region is required")
	}
	if e.RecordedAt.IsZero() {
		return errors.New("recorded_at is required")
	}
	if math.IsNaN(e.Latitude) || e.Latitude < -90 || e.Latitude > 90 {
		return errors.New("latitude is out of range")
	}
	if math.IsNaN(e.Longitude) || e.Longitude < -180 || e.Longitude > 180 {
		return errors.New("longitude is out of range")
	}
	if e.Sequence < 0 {
		return errors.New("sequence must be non-negative")
	}
	return nil
}

func (e LocationEvent) Marshal() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("marshal location event: %w", err)
	}
	return b, nil
}
