FROM docker.io/library/golang:1.18.2 AS builder
COPY go.* /app/
WORKDIR /app/
RUN go mod download
COPY . /app/
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" ./cmd/pubsub-broker

FROM gcr.io/distroless/static:nonroot AS final
COPY --from=builder /app/pubsub-broker /app/pubsub-broker
ENTRYPOINT [ "/app/pubsub-broker" ]
