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
RUN apk --no-cache add ca-certificates tzdata ffmpeg curl tar
WORKDIR /app
COPY --from=builder /app/server .
# Download GeoLite2-Country database at build time.
# Set MAXMIND_LICENSE_KEY as a Railway build argument / secret.
ARG MAXMIND_LICENSE_KEY
RUN if [ -n "$MAXMIND_LICENSE_KEY" ]; then \
      curl -fSL "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-Country&license_key=${MAXMIND_LICENSE_KEY}&suffix=tar.gz" \
        -o /tmp/geo.tar.gz && \
      tar -xz -C /tmp -f /tmp/geo.tar.gz && \
      mv /tmp/GeoLite2-Country_*/GeoLite2-Country.mmdb /app/GeoLite2-Country.mmdb && \
      rm -rf /tmp/geo.tar.gz /tmp/GeoLite2-Country_*; \
    fi
EXPOSE 8080
CMD ["./server"]
