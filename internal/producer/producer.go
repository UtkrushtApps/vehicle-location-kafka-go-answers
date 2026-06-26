package producer

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"vehicle-location-kafka-go/internal/events"
)

const defaultPublishTimeout = 10 * time.Second

type MessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type KafkaPublisher struct {
	topic   string
	writer  MessageWriter
	logger  *slog.Logger
	metrics *Metrics

	locksMu sync.Mutex
	locks   map[string]*vehicleLock
}

type vehicleLock struct {
	mu   sync.Mutex
	refs int
}

type Metrics struct {
	mu            sync.RWMutex
	Published     int64         `json:"published"`
	Failed        int64         `json:"failed"`
	LastLatencyMS int64         `json:"last_latency_ms"`
	Partitions    map[int]int64 `json:"partitions"`
}

func NewMetrics() *Metrics {
	return &Metrics{Partitions: map[int]int64{}}
}

func (m *Metrics) Record(success bool, latency time.Duration, partition int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.LastLatencyMS = latency.Milliseconds()
	if success {
		m.Published++
		if partition >= 0 {
			m.Partitions[partition]++
		}
		return
	}
	m.Failed++
}

func (m *Metrics) RecordPartition(partition int) {
	if m == nil || partition < 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Partitions[partition]++
}

func (m *Metrics) Snapshot() Metrics {
	if m == nil {
		return Metrics{Partitions: map[int]int64{}}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	copyMap := make(map[int]int64, len(m.Partitions))
	for k, v := range m.Partitions {
		copyMap[k] = v
	}
	return Metrics{
		Published:     m.Published,
		Failed:        m.Failed,
		LastLatencyMS: m.LastLatencyMS,
		Partitions:    copyMap,
	}
}

func NewKafkaPublisher(brokers []string, topic string, logger *slog.Logger, metrics *Metrics) *KafkaPublisher {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchSize:    1,
		BatchTimeout: 1 * time.Millisecond,
		MaxAttempts:  5,
		Balancer:     newVehicleHashBalancer(metrics),
		WriteTimeout: defaultPublishTimeout,
		ReadTimeout:  defaultPublishTimeout,
	}
	return NewKafkaPublisherWithWriter(topic, writer, logger, metrics)
}

func NewKafkaPublisherWithWriter(topic string, writer MessageWriter, logger *slog.Logger, metrics *Metrics) *KafkaPublisher {
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		metrics = NewMetrics()
	}
	return &KafkaPublisher{
		topic:   topic,
		writer:  writer,
		logger:  logger,
		metrics: metrics,
		locks:   make(map[string]*vehicleLock),
	}
}

func (p *KafkaPublisher) PublishLocation(ctx context.Context, evt events.LocationEvent) error {
	msg, err := BuildLocationMessage(p.topic, evt)
	if err != nil {
		return err
	}

	unlock := p.lockVehicle(evt.VehicleID)
	defer unlock()

	writeCtx := ctx
	if _, hasDeadline := writeCtx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(ctx, defaultPublishTimeout)
		defer cancel()
	}

	start := time.Now()
	err = p.writer.WriteMessages(writeCtx, msg)
	latency := time.Since(start)
	latencyMS := latency.Milliseconds()
	if err != nil {
		p.metrics.Record(false, latency, -1)
		p.logger.Error(
			"location publish failed",
			slog.String("topic", p.topic),
			slog.String("vehicle_id", evt.VehicleID),
			slog.String("region", evt.Region),
			slog.Int64("sequence", evt.Sequence),
			slog.String("correlation_id", evt.CorrelationID),
			slog.Int64("latency_ms", latencyMS),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("write location event: %w", err)
	}

	p.metrics.Record(true, latency, -1)
	p.logger.Info(
		"location published",
		slog.String("topic", p.topic),
		slog.String("vehicle_id", evt.VehicleID),
		slog.String("region", evt.Region),
		slog.Int64("sequence", evt.Sequence),
		slog.String("correlation_id", evt.CorrelationID),
		slog.Int64("latency_ms", latencyMS),
	)
	return nil
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}

func (p *KafkaPublisher) lockVehicle(vehicleID string) func() {
	p.locksMu.Lock()
	lock := p.locks[vehicleID]
	if lock == nil {
		lock = &vehicleLock{}
		p.locks[vehicleID] = lock
	}
	lock.refs++
	p.locksMu.Unlock()

	lock.mu.Lock()

	return func() {
		lock.mu.Unlock()

		p.locksMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(p.locks, vehicleID)
		}
		p.locksMu.Unlock()
	}
}

func BuildLocationMessage(topic string, evt events.LocationEvent) (kafka.Message, error) {
	payload, err := evt.Marshal()
	if err != nil {
		return kafka.Message{}, fmt.Errorf("build location message: %w", err)
	}
	return kafka.Message{
		Topic: topic,
		Key:   []byte(evt.VehicleID),
		Value: payload,
		Time:  time.Now().UTC(),
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte("vehicle_location")},
			{Key: "event_version", Value: []byte(events.LocationEventVersion)},
			{Key: "vehicle_id", Value: []byte(evt.VehicleID)},
			{Key: "region", Value: []byte(evt.Region)},
			{Key: "sequence", Value: []byte(fmt.Sprintf("%d", evt.Sequence))},
			{Key: "correlation_id", Value: []byte(evt.CorrelationID)},
		},
	}, nil
}

type vehicleHashBalancer struct {
	next    atomic.Uint64
	metrics *Metrics
}

func newVehicleHashBalancer(metrics *Metrics) *vehicleHashBalancer {
	return &vehicleHashBalancer{metrics: metrics}
}

func (b *vehicleHashBalancer) Balance(msg kafka.Message, partitions ...int) int {
	if len(partitions) == 0 {
		return 0
	}

	var partition int
	if len(msg.Key) == 0 {
		n := b.next.Add(1) - 1
		partition = partitions[int(n%uint64(len(partitions)))]
	} else {
		h := fnv.New32a()
		_, _ = h.Write(msg.Key)
		partition = partitions[int(h.Sum32()%uint32(len(partitions)))]
	}

	b.metrics.RecordPartition(partition)
	return partition
}
