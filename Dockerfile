FROM docker.io/library/golang:1.16.4 AS builder
COPY go.* /app/
WORKDIR /app/
RUN go mod download
COPY . /app/
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" ./cmd/pubsub

FROM gcr.io/distroless/static:nonroot AS final
COPY --from=builder /app/pubsub /app/pubsub
ENTRYPOINT [ "/app/pubsub" ]
