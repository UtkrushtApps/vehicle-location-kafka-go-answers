FROM golang:1.22-alpine AS build

WORKDIR /root/task
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/vehicle-location-worker ./cmd/worker

FROM alpine:3.20
WORKDIR /root/task
COPY --from=build /out/vehicle-location-worker /usr/local/bin/vehicle-location-worker
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/vehicle-location-worker"]
