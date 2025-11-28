# Build stage
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY *.go ./

# Build the application for target platform
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-s -w -X main.Version=${VERSION} -X main.Commit=${COMMIT}" \
    -a -installsuffix cgo -o rs-http-facade .

# Runtime stage
FROM alpine:latest

# Add labels
LABEL org.opencontainers.image.title="rs-http-facade"
LABEL org.opencontainers.image.description="HTTP facade for Redis Streams"
LABEL org.opencontainers.image.source="https://github.com/fred01/rs-http-facade"
LABEL org.opencontainers.image.licenses="MIT"

RUN apk --no-cache add ca-certificates wget

# Create a non-root user and group for security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /home/appuser

# Copy the binary from builder
COPY --from=builder /app/rs-http-facade .

# Set ownership to non-root user
RUN chown appuser:appgroup rs-http-facade

# Switch to the non-root user
USER appuser

# Expose HTTP port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget -q --spider http://localhost:8080/admin/ping || exit 1

# Run the application
ENTRYPOINT ["./rs-http-facade"]
CMD ["-redis-address", "redis:6379", "-http-address", ":8080"]
