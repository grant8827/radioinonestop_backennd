FROM golang:alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/encoder-worker ./cmd/encoder-worker
RUN CGO_ENABLED=0 GOOS=linux go build -o /encoder-worker ./cmd/encoder-worker

FROM alpine:latest
RUN apk --no-cache add ca-certificates ffmpeg tzdata
COPY --from=builder /encoder-worker /usr/local/bin/encoder-worker
EXPOSE 8080
CMD ["encoder-worker"]
