# syntax=docker/dockerfile:1
FROM golang:1.27.4-bookworm AS builder
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -o /out/taskflow-api ./cmd/api && \
    CGO_ENABLED=0 go build -trimpath -o /out/taskflow-worker ./cmd/worker && \
    CGO_ENABLED=0 go build -trimpath -o /out/taskflow-reconcile ./cmd/reconcile

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends poppler-utils ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -g 10001 taskflow && useradd -u 10001 -g taskflow taskflow && \
    install -d -o taskflow -g taskflow /app/data/uploads /app/data/results
WORKDIR /app
COPY --from=builder /out/ /usr/local/bin/
USER taskflow
EXPOSE 8080
CMD ["taskflow-api"]
