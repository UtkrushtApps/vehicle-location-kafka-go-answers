package consumer

import "time"

type LagSnapshot struct {
	Topic      string
	Partition  int
	HighWater  int64
	Current    int64
	ObservedAt time.Time
}

func (s LagSnapshot) Lag() int64 {
	if s.HighWater <= s.Current {
		return 0
	}
	return s.HighWater - s.Current
}
