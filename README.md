# rs-http-facade

A simple HTTP REST facade for Redis Streams written in Go. This service provides HTTP endpoints to interact with Redis Streams, including publishing messages, consuming messages via Server-Sent Events (SSE), and controlling message lifecycle.

## Features

- **REST API** for Redis Streams operations
- **Producer endpoints** (XADD) - publish single or multiple messages
- **Per-client consumers** - each HTTP client gets its own Redis consumer for native load balancing
- **Consumer SSE endpoint** - consume messages in real-time via Server-Sent Events
- **SSE keepalive** - configurable keepalive comments to maintain long-lived connections
- **Consumer control** - limit-based flow control for consumers
- **Message lifecycle management** - finish messages with automatic expiry
- **Automatic message recovery** - expired messages are automatically claimed by other consumers using `XAUTOCLAIM`
- **Admin endpoints** - ping, info, stream listing, and statistics
- **Secure authentication** - constant-time bearer token validation to prevent timing attacks

## Installation

### From Source

```bash
go get github.com/fred01/rs-http-facade
go build -o rs-http-facade
```

### Using Docker

Build the Docker image:

```bash
docker build -t rs-http-facade .
```

Run with Docker:

```bash
docker run -p 8080:8080 rs-http-facade \
  -bearer-token=your-secret-token \
  -redis-address=redis:6379
```

### Using Docker Compose

The easiest way to get started is using Docker Compose, which sets up Redis and the HTTP facade:

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Edit `.env` and set a strong bearer token:
```bash
BEARER_TOKEN=your-strong-secret-token-here
```

3. Start the services:
```bash
docker-compose up
```

This will start:
- Redis (port 6379)
- Redis Streams HTTP Facade (port 8080)

### Production Deployment

For production deployments, use the `docker-compose.prod.yml` file which includes:
- Redis configured with AOF+RDB persistence for maximum reliability
- Health checks for all services
- Resource limits (CPU/memory)
- Automatic restart policies
- Security hardening (no-new-privileges)
- Structured logging with rotation

1. Review and customize `redis.conf` for your needs:
```bash
# Set a password (uncomment and modify in redis.conf)
# requirepass your-strong-redis-password

# Adjust memory limits
# maxmemory 4gb
```

2. Create external storage directory (optional):
```bash
mkdir -p /mnt/redis-data
```

3. Start production services:
```bash
docker-compose -f docker-compose.prod.yml up -d
```

### Pre-built Docker Images

Pre-built multi-architecture Docker images (amd64, arm64) are available from GitHub Container Registry:

```bash
docker pull ghcr.io/fred01/rs-http-facade:latest

# Or specific version
docker pull ghcr.io/fred01/rs-http-facade:v1.0.0
```

### Binary Releases

Pre-built binaries for multiple platforms are available on the [Releases page](https://github.com/fred01/rs-http-facade/releases):

- Linux: amd64, arm64, armv7
- macOS: amd64, arm64 (Apple Silicon)
- Windows: amd64, arm64

Download and run:
```bash
# Linux/macOS
wget https://github.com/fred01/rs-http-facade/releases/latest/download/rs-http-facade-linux-amd64
chmod +x rs-http-facade-linux-amd64
./rs-http-facade-linux-amd64 -bearer-token=your-token -redis-address=localhost:6379

# Verify checksum
sha256sum -c checksums.txt
```

## Usage

Start the HTTP facade:

```bash
./rs-http-facade -bearer-token=your-secret-token -redis-address=localhost:6379 -http-address=:8080
```

### Configuration

All runtime parameters are required (except redis_password); the process exits if any are missing. Configuration is resolved in the following order (later entries override earlier ones):

1. TOML config file (`/etc/rs-http-facade/config.toml` by default, override with `-config` or `RS_HTTP_FACADE_CONFIG`).
2. Environment variables:
   - `RS_HTTP_FACADE_REDIS_ADDRESS`
   - `RS_HTTP_FACADE_REDIS_PASSWORD`
   - `RS_HTTP_FACADE_REDIS_DB`
   - `RS_HTTP_FACADE_HTTP_ADDRESS`
   - `RS_HTTP_FACADE_BEARER_TOKEN`
   - `RS_HTTP_FACADE_SSE_KEEPALIVE_INTERVAL_SEC`
3. Command-line flags.

Copy `config.toml.example` to your preferred path and fill in all values to bootstrap configuration quickly.

#### Example TOML Configuration
```toml
redis_address = "localhost:6379"
redis_password = ""
redis_db = 0
http_address = ":8080"
bearer_token = "your-secret-token"
# sse_keepalive_interval_sec = 60  # SSE keepalive interval in seconds (default: 60, negative to disable)
```

### Command-line Flags

- `-config` - Path to a TOML configuration file (default: `/etc/rs-http-facade/config.toml`)
- `-bearer-token` - Bearer token for authentication (required)
- `-redis-address` - Redis server address (required)
- `-redis-password` - Redis password (optional)
- `-redis-db` - Redis database number (default: 0)
- `-http-address` - HTTP server listen address (required)
- `-sse-keepalive-interval-sec` - SSE keepalive interval in seconds for consumers (default: 60, negative to disable)

## API Documentation

Complete API documentation is available in the [OpenAPI specification](openapi.yaml).

You can view the API documentation using any OpenAPI viewer such as:
- [Swagger Editor](https://editor.swagger.io/) (paste the contents of `openapi.yaml`)
- [Swagger UI](https://petstore.swagger.io/) (File → Import File → select `openapi.yaml`)
- [Redoc](https://redocly.github.io/redoc/) for a different viewing experience

## API Endpoints

All endpoints require Bearer token authentication via the `Authorization` header:

```
Authorization: Bearer your-secret-token
```

### Producer Endpoints

#### Publish Single Message (XADD)

```http
POST /api/streams/{stream}/messages
Content-Type: application/json
Authorization: Bearer your-secret-token

{
  "data": "your message content as JSON"
}
```

Response:
```json
{
  "status": "ok",
  "stream": "your-stream",
  "id": "1234567890123-0"
}
```

#### Publish Multiple Messages (XADD Pipeline)

```http
POST /api/streams/{stream}/messages/batch
Content-Type: application/json
Authorization: Bearer your-secret-token

{
  "messages": [
    "message 1",
    "message 2",
    "message 3"
  ]
}
```

Response:
```json
{
  "status": "ok",
  "stream": "your-stream",
  "count": 3
}
```

### Consumer Endpoints

#### Consume Messages via SSE

```http
GET /api/events?stream={stream}&group={group}
Authorization: Bearer your-secret-token
```

This endpoint returns a stream of Server-Sent Events. Each event contains:

```json
{
  "id": "1234567890123-0",
  "body": "message content"
}
```

**Important Notes**:
- Messages received via SSE require explicit acknowledgement. You must explicitly finish each message using the message lifecycle endpoint.
- **Native Redis Streams load balancing**: Each HTTP client creates its own consumer within the consumer group. When multiple clients connect to the same stream/group, Redis distributes messages across them, just like native Redis Streams clients.
- This enables horizontal scaling: add more HTTP clients to process messages in parallel.
- **Keepalive**: The server sends SSE comment lines (`: keepalive`) at a configurable interval (default: 60 seconds) to keep the connection alive.

#### Set Consumer Limit

```http
POST /api/consumers/{stream}/{group}/limit
Content-Type: application/json
Authorization: Bearer your-secret-token

{
  "count": 5
}
```

Response:
```json
{
  "status": "ok",
  "consumers": 2
}
```

#### Get Consumer Status

```http
GET /api/consumers/{stream}/{group}
Authorization: Bearer your-secret-token
```

Response:
```json
{
  "stream": "your-stream",
  "group": "your-group",
  "consumers": 3,
  "messages": 100,
  "finished": 95
}
```

### Message Lifecycle Endpoints

#### Finish Message (XACK - Mark as Successfully Processed)

```http
POST /api/messages/{message-id}/finish
Authorization: Bearer your-secret-token
```

Response:
```json
{
  "status": "ok",
  "action": "finished"
}
```

### Admin Endpoints

#### Ping Redis

```http
GET /admin/ping
Authorization: Bearer your-secret-token
```

Response:
```json
{
  "status": "PONG"
}
```

#### Get Redis Info

```http
GET /admin/info
Authorization: Bearer your-secret-token
```

Returns Redis server information as plain text.

#### List All Streams

```http
GET /admin/streams
Authorization: Bearer your-secret-token
```

Response:
```json
{
  "streams": ["stream1", "stream2"]
}
```

#### Get Stream Statistics

```http
GET /admin/stats
Authorization: Bearer your-secret-token
```

Returns detailed statistics for all streams and their consumer groups. Useful for monitoring and displaying in web interfaces.

Response:
```json
{
  "streams": [
    {
      "name": "orders-stream",
      "length": 5000,
      "groups": [
        {
          "name": "processor-group",
          "pending": 25,
          "lag": 100,
          "consumers": 3
        }
      ]
    }
  ]
}
```

Fields:
- `name` - Stream name
- `length` - Total number of messages in the stream
- `groups[].name` - Consumer group name
- `groups[].pending` - Number of pending (unacknowledged) messages
- `groups[].lag` - Number of messages not yet delivered to the group
- `groups[].consumers` - Number of active consumers in the group

#### Flush All Data

```http
POST /admin/flush
Authorization: Bearer your-secret-token
```

Flushes all data from the Redis database. **Use with caution** - this permanently deletes all streams and messages.

Response:
```json
{
  "status": "ok",
  "action": "flushed"
}
```

#### Get Stream Info

```http
GET /admin/{stream-name}
Authorization: Bearer your-secret-token
```

Returns detailed information about a specific stream.

## Example Usage

See the `examples.sh` script for comprehensive examples of all API endpoints.

### Publishing a Message

```bash
curl -X POST http://localhost:8080/api/streams/test-stream/messages \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-secret-token" \
  -d '{"data": {"hello": "world"}}'
```

### Consuming Messages via SSE

```bash
curl -N http://localhost:8080/api/events?stream=test-stream&group=test-group \
  -H "Authorization: Bearer your-secret-token"
```

### Finishing a Message

```bash
curl -X POST http://localhost:8080/api/messages/1234567890123-0/finish \
  -H "Authorization: Bearer your-secret-token"
```

## Architecture

The facade maintains:
- A single Redis client for all operations
- **Per-client consumers**: Each HTTP client connection creates its own consumer within the consumer group
- **Native Redis Streams load balancing**: Messages are distributed by Redis across all consumers in a group (just like native Redis Streams)
- **Active message registry**: Tracks in-flight messages with automatic expiry (5 minutes default)
- **Automatic message redelivery**: Expired messages remain in the pending list and are automatically claimed by other consumers using `XAUTOCLAIM`

### Load Balancing Example

When you have 3 messages in a stream and 3 HTTP clients connected to the same consumer group:
1. Client 1 connects → creates consumer #1 in group
2. Client 2 connects → creates consumer #2 in group  
3. Client 3 connects → creates consumer #3 in group
4. Redis distributes the 3 messages: one to each consumer (client)
5. Each client processes its message independently and calls finish

This mirrors native Redis Streams behavior where each consumer in a group gets a share of the messages.

### Automatic Message Recovery

If a consumer dies or disconnects without acknowledging messages:
1. Messages remain in the pending entries list (PEL) with idle time
2. After 5 minutes, the message expires from our internal tracking
3. Other active consumers automatically claim idle messages using `XAUTOCLAIM`
4. The message is redelivered to the new consumer

This ensures no messages are lost when consumers fail.

## Security

All endpoints require authentication via Bearer token. Set a strong token using the `-bearer-token` flag when starting the service.

**Security Features**:
- **Constant-time token comparison**: Prevents timing attacks on the bearer token
- **Automatic message recovery**: Messages not processed within 5 minutes are automatically claimed by other consumers
- **No default token in Docker**: The Dockerfile requires explicit token configuration

## Testing

### Unit Tests

Run the unit tests:
```bash
go test -v -short
```

Or using Make:
```bash
make test
```

### Integration Tests

The integration tests verify the load balancing behavior using real Redis containers. These tests require Docker to be running.

Run integration tests:
```bash
go test -v -tags=integration -timeout=120s
```

Or using Make:
```bash
make integration-test
```

The integration tests verify:

**Load Balancing (TestLoadBalancingBehavior)**:
- Messages are distributed across multiple HTTP consumers (load balancing)
- Each consumer receives at least one message
- Total messages received equals total messages published
- Proper message lifecycle management (finish)

**Message Finish (TestMessageFinish)**:
- Messages can be acknowledged using XACK
- Finished messages are removed from the pending entries list

**Limit Flow Control (TestLimitControl)**:
- Limit can be set via API
- Verifies messages are delivered according to limit (e.g., limit=5 delivers ~5 messages)
- After finishing messages, more messages are delivered up to limit
- Consumer status endpoint returns correct information
- Flow control applies to all consumers for a stream/group

**SSE Connection Close (TestSSEConnectionClose)**:
- SSE connections can be gracefully closed
- Unconsumed messages remain in the pending list
- Reconnection works correctly after disconnect

**Admin Endpoints (TestAdminEndpoints)**:
- Ping endpoint returns PONG
- Info endpoint returns Redis server information
- Streams endpoint lists all streams

## License

MIT