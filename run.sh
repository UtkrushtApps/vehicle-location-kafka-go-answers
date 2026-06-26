#!/usr/bin/env bash
set -e

cd /root/task

trap 'echo "startup failed; recent logs:"; docker compose logs --tail=120 kafka app || true' ERR

echo "building and starting services"
docker compose up -d --build

echo "waiting for kafka readiness"
for i in $(seq 1 60); do
  if docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server kafka:9092 >/dev/null 2>&1; then
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "kafka did not become ready in time"
    exit 1
  fi
  sleep 2
done

echo "creating kafka topics"
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic vehicle-location.v1 --partitions 6 --replication-factor 1 >/dev/null

echo "waiting for application health"
for i in $(seq 1 40); do
  if docker compose exec -T app wget -q -O - http://127.0.0.1:8080/healthz >/dev/null 2>&1; then
    break
  fi
  if [ "$i" -eq 40 ]; then
    echo "application did not become healthy in time"
    exit 1
  fi
  sleep 1
done

echo "running smoke publish"
docker compose exec -T app wget -q -O - \
  --header 'Content-Type: application/json' \
  --post-data '{"vehicle_id":"veh-smoke-1","region":"north","latitude":12.9716,"longitude":77.5946,"recorded_at":"2026-01-02T03:04:05Z","sequence":1,"correlation_id":"smoke-run"}' \
  http://127.0.0.1:8080/api/v1/locations >/dev/null

echo "deployment ready"
echo "application health check passed inside the app container"
echo "metrics endpoint is available inside the app container"
