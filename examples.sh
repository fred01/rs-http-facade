#!/bin/bash

# Example usage script for rs-http-facade
# This script demonstrates how to interact with the HTTP facade for Redis Streams

# Configuration
API_URL="http://localhost:8080"
BEARER_TOKEN="your-secret-token"
STREAM="test-stream"
GROUP="test-group"

echo "=== Redis Streams HTTP Facade Examples ==="
echo ""

# Function to make authenticated requests
auth_curl() {
    curl -H "Authorization: Bearer ${BEARER_TOKEN}" "$@"
}

# Example 1: Publish a single message
echo "1. Publishing a single message to stream '${STREAM}'..."
auth_curl -X POST "${API_URL}/api/streams/${STREAM}/messages" \
  -H "Content-Type: application/json" \
  -d '{"data": {"message": "Hello, Redis Streams!", "timestamp": 1234567890}}'
echo -e "\n"

# Example 2: Publish multiple messages
echo "2. Publishing multiple messages to stream '${STREAM}'..."
auth_curl -X POST "${API_URL}/api/streams/${STREAM}/messages/batch" \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {"id": 1, "text": "First message"},
      {"id": 2, "text": "Second message"},
      {"id": 3, "text": "Third message"}
    ]
  }'
echo -e "\n"

# Example 3: Set consumer RDY count
echo "3. Setting RDY count to 5 for stream '${STREAM}' and group '${GROUP}'..."
auth_curl -X POST "${API_URL}/api/consumers/${STREAM}/${GROUP}/rdy" \
  -H "Content-Type: application/json" \
  -d '{"count": 5}'
echo -e "\n"

# Example 4: Get consumer status
echo "4. Getting consumer status for stream '${STREAM}' and group '${GROUP}'..."
auth_curl -X GET "${API_URL}/api/consumers/${STREAM}/${GROUP}"
echo -e "\n"

# Example 5: Message lifecycle - finish a message
echo "5. Finishing a message (replace MESSAGE_ID with actual ID from SSE stream)..."
MESSAGE_ID="1234567890-0"
auth_curl -X POST "${API_URL}/api/messages/${MESSAGE_ID}/finish"
echo -e "\n"

# Example 6: Message lifecycle - touch a message
echo "6. Touching a message to extend timeout..."
auth_curl -X POST "${API_URL}/api/messages/${MESSAGE_ID}/touch"
echo -e "\n"

# Example 7: Access admin API - ping
echo "7. Checking Redis connection via admin endpoint..."
auth_curl -X GET "${API_URL}/admin/ping"
echo -e "\n"

# Example 8: Access admin API - list streams
echo "8. Listing all streams..."
auth_curl -X GET "${API_URL}/admin/streams"
echo -e "\n"

# Example 9: Access admin API - get statistics
echo "9. Getting statistics for all streams and consumer groups..."
auth_curl -X GET "${API_URL}/admin/stats"
echo -e "\n"

# Example 10: Consume messages via SSE (in background for 10 seconds)
echo "10. Consuming messages via SSE for 10 seconds..."
echo "   (Press Ctrl+C to stop earlier)"
timeout 10s auth_curl -N "${API_URL}/api/events?stream=${STREAM}&group=${GROUP}" || true
echo -e "\n"

echo "=== Examples Complete ==="
echo ""
echo "To consume messages continuously, run:"
echo "  curl -N -H \"Authorization: Bearer ${BEARER_TOKEN}\" \"${API_URL}/api/events?stream=${STREAM}&group=${GROUP}\""
