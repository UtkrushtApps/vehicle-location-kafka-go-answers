# Solution Steps

1. Change Kafka message construction so the message key is the immutable vehicle identity (`vehicle_id`), not the mutable region. This keeps all events for the same vehicle on the same Kafka partition even when the vehicle crosses regional boundaries.

2. Set the message topic explicitly and add useful Kafka headers such as event type/version, vehicle ID, region, sequence, and correlation ID so individual records are easier to diagnose downstream.

3. Configure the kafka-go writer for synchronous, safely acknowledged publishing: `RequiredAcks: kafka.RequireAll`, `Async: false`, bounded retry attempts, short batch settings for request/response behavior, and read/write timeouts.

4. Replace the writer's default balancing behavior with a deterministic key-aware hash balancer. Hash non-empty message keys across the broker-provided partition list, and round-robin only for messages without keys. Record each selected partition in metrics to expose partition-spread symptoms.

5. Add a small per-vehicle in-process lock around `WriteMessages`. This serializes concurrent publishes for the same vehicle within this service instance while still allowing different vehicles to publish concurrently.

6. Keep delivery failure handling synchronous and explicit: wrap writer errors, increment failure metrics, include latency, vehicle ID, sequence, region, correlation ID, topic, and error in structured logs, and return an HTTP 502 from the API when publishing fails.

7. Make metrics race-safe with a mutex-protected `Metrics` type, support snapshots for JSON output, track published count, failed count, last publish latency, and partition counts.

8. Expose the metrics snapshot through `/metrics` and keep `/healthz` and `/api/v1/locations` handlers simple. Validate JSON input, enforce request method checks, and log accepted or failed publish requests with structured fields.

9. Update tests to assert that Kafka keys follow vehicle identity, remain stable across region changes, publishing records success/failure metrics, the hash balancer keeps one vehicle on one partition while spreading many vehicles, and the metrics endpoint returns expected JSON.

10. Run `go test ./...` locally, then use the provided `run.sh` to build the containers, start Kafka, create the topic, wait for service health, and perform the smoke publish.

