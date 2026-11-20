FROM golang:1.27.4 AS builder_src

COPY jemalloc-install.sh .
RUN apt-get update -y && apt-get install -y --no-install-recommends bzip2 && rm -rf /var/lib/apt/lists/*
RUN bash jemalloc-install.sh

FROM builder_src AS builder

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

RUN go install github.com/go-delve/delve/cmd/dlv@latest
# Copy the source from the current directory to the Working Directory inside the container
COPY cmd ./cmd
COPY internal ./internal
COPY app.go .env-test ./

# Build the Go app with jemalloc
RUN CGO_ENABLED=1 GOOS=linux go build -pgo=cmd/datahub/default-jemalloc.pprof -ldflags="-extldflags=-static" -tags jemalloc,allocator -o server cmd/datahub/main.go

# Build the Go app with gogc
RUN CGO_ENABLED=0 GOOS=linux go build -pgo=cmd/datahub/default-gogc.pprof -o server-gogc cmd/datahub/main.go

# Run unit tests
RUN go test ./... -v

FROM alpine:latest
RUN apk --no-cache upgrade && apk --no-cache add ca-certificates rsync

WORKDIR /root/

COPY --from=builder /app/server .
COPY --from=builder /app/server-gogc .
COPY --from=builder /go/bin/dlv .

# Expose port 8080 to the outside world, also 40000 for delve
EXPOSE 8080 40000

ENV GOMAXPROCS=128

CMD ["./server"]
