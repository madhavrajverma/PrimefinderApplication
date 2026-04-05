# primefinder/Dockerfile
# Build context must be finalproject/ root
# docker build -f primefinder/Dockerfile .

FROM golang:1.24-alpine AS builder

WORKDIR /build

# copy afsfs first (local dependency)
COPY afsfs/ ./afsfs/

# copy primefinder
COPY primefinder/ ./primefinder/

# build from primefinder directory
WORKDIR /build/primefinder
RUN go mod download
RUN go build -o bin/coordinator ./cmd/coordinator
RUN go build -o bin/worker      ./cmd/worker

# Stage 2: final small image
FROM alpine:latest

WORKDIR /app

COPY --from=builder /build/primefinder/bin/coordinator ./bin/coordinator
COPY --from=builder /build/primefinder/bin/worker      ./bin/worker

RUN mkdir -p /tmp/cache

CMD ["./bin/coordinator"]