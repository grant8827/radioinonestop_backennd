#!/bin/sh
# Kill anything on the backend ports, then start the server.
lsof -ti tcp:8080 | xargs kill -9 2>/dev/null
lsof -ti tcp:1935 | xargs kill -9 2>/dev/null
cd "$(dirname "$0")"
go run main.go
