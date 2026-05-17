# Build stage
FROM golang:alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o server .

# Run stage
FROM alpine:latest
# ffmpeg is required at runtime — it is spawned per live stream by the Go server.
# ca-certificates: needed for outbound TLS. tzdata: correct timestamps in logs.
RUN apk --no-cache add ca-certificates tzdata ffmpeg
WORKDIR /app
COPY --from=builder /app/server .
EXPOSE 8080
CMD ["./server"]
