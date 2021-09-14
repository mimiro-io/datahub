FROM golang:1.17.1 as builder

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Build the Go app
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o server cmd/datahub/main.go

# Run unit tests
RUN go test ./... -v

FROM alpine:latest

RUN apk --no-cache add ca-certificates rsync

WORKDIR /root/

COPY --from=builder /app/server .

# Expose port 8080 to the outside world
EXPOSE 8080

ENV GOMAXPROCS=128

CMD ["./server"]
