package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

const testToken = "test-bearer-token"

// createTestServer creates a Server instance for testing with miniredis
func createTestServer() *Server {
	// Start miniredis for testing
	mr, _ := miniredis.Run()

	config := AppConfig{
		RedisAddress:            mr.Addr(),
		HTTPAddress:             ":8080",
		BearerToken:             testToken,
		SSEKeepaliveIntervalSec: 60,
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	return &Server{
		config:          config,
		redisClient:     redisClient,
		consumers:       make(map[string]*consumerState),
		bearerTokenHash: sha256.Sum256([]byte(testToken)),
	}
}

func TestAuthMiddleware(t *testing.T) {
	server := createTestServer()

	handler := server.authMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("authenticated"))
	})

	tests := []struct {
		name           string
		authHeader     string
		expectedStatus int
	}{
		{
			name:           "Valid token",
			authHeader:     "Bearer " + testToken,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Missing token",
			authHeader:     "",
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "Invalid token",
			authHeader:     "Bearer wrong-token",
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "Invalid format",
			authHeader:     "Basic " + testToken,
			expectedStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}

			rr := httptest.NewRecorder()
			handler(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rr.Code)
			}
		})
	}
}

func TestSplitPath(t *testing.T) {
	tests := []struct {
		path     string
		expected []string
	}{
		{
			path:     "stream/group",
			expected: []string{"stream", "group"},
		},
		{
			path:     "stream/group/limit",
			expected: []string{"stream", "group", "limit"},
		},
		{
			path:     "messages/12345/finish",
			expected: []string{"messages", "12345", "finish"},
		},
		{
			path:     "",
			expected: []string{},
		},
		{
			path:     "single",
			expected: []string{"single"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := splitPath(tt.path)
			if len(result) != len(tt.expected) {
				t.Errorf("expected length %d, got %d", len(tt.expected), len(result))
				return
			}
			for i, v := range result {
				if v != tt.expected[i] {
					t.Errorf("at index %d: expected %s, got %s", i, tt.expected[i], v)
				}
			}
		})
	}
}

func TestMessageStructures(t *testing.T) {
	t.Run("Message JSON", func(t *testing.T) {
		jsonData := `{"data": {"key": "value"}}`
		var msg Message
		err := json.Unmarshal([]byte(jsonData), &msg)
		if err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if string(msg.Data) != `{"key": "value"}` {
			t.Errorf("expected data to be preserved, got: %s", msg.Data)
		}
	})

	t.Run("MultiMessage JSON", func(t *testing.T) {
		jsonData := `{"messages": ["msg1", "msg2", "msg3"]}`
		var msg MultiMessage
		err := json.Unmarshal([]byte(jsonData), &msg)
		if err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if len(msg.Messages) != 3 {
			t.Errorf("expected 3 messages, got %d", len(msg.Messages))
		}
	})

	t.Run("LimitRequest JSON", func(t *testing.T) {
		jsonData := `{"count": 10}`
		var req LimitRequest
		err := json.Unmarshal([]byte(jsonData), &req)
		if err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if req.Count != 10 {
			t.Errorf("expected count 10, got %d", req.Count)
		}
	})
}

func TestHandleAddValidation(t *testing.T) {
	server := createTestServer()

	tests := []struct {
		name           string
		method         string
		stream         string
		body           string
		expectedStatus int
	}{
		{
			name:           "Invalid method",
			method:         "GET",
			stream:         "test",
			body:           `{"data": "test"}`,
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "Invalid JSON",
			method:         "POST",
			stream:         "test",
			body:           `invalid json`,
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/api/streams/"+tt.stream+"/messages", bytes.NewBufferString(tt.body))
			req.Header.Set("Authorization", "Bearer "+testToken)
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			server.handleAdd(rr, req, tt.stream)

			if rr.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rr.Code)
			}
		})
	}
}

func TestHandleBatchAddValidation(t *testing.T) {
	server := createTestServer()

	tests := []struct {
		name           string
		method         string
		stream         string
		body           string
		expectedStatus int
	}{
		{
			name:           "Invalid method",
			method:         "GET",
			stream:         "test",
			body:           `{"messages": ["test"]}`,
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "Invalid JSON",
			method:         "POST",
			stream:         "test",
			body:           `invalid json`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Empty messages",
			method:         "POST",
			stream:         "test",
			body:           `{"messages": []}`,
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/api/streams/"+tt.stream+"/messages/batch", bytes.NewBufferString(tt.body))
			req.Header.Set("Authorization", "Bearer "+testToken)
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			server.handleBatchAdd(rr, req, tt.stream)

			if rr.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rr.Code)
			}
		})
	}
}

func TestHandleConsumerLimitValidation(t *testing.T) {
	server := createTestServer()

	tests := []struct {
		name           string
		method         string
		body           string
		expectedStatus int
	}{
		{
			name:           "Invalid method",
			method:         "GET",
			body:           `{"count": 5}`,
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "Invalid JSON",
			method:         "POST",
			body:           `invalid json`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Consumer not found",
			method:         "POST",
			body:           `{"count": 5}`,
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/api/consumers/test/group/limit", bytes.NewBufferString(tt.body))
			req.Header.Set("Authorization", "Bearer "+testToken)
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			server.handleConsumerLimit(rr, req, "test", "group")

			if rr.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rr.Code)
			}
		})
	}
}

func TestHandleFinishNewRouteValidation(t *testing.T) {
	server := createTestServer()

	tests := []struct {
		name           string
		stream         string
		group          string
		consumer       string
		messageId      string
		expectedStatus int
	}{
		{
			name:           "Message not found in pending list",
			stream:         "test-stream",
			group:          "test-group",
			consumer:       "consumer-1",
			messageId:      "nonexistent-0",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "Missing stream param",
			stream:         "",
			group:          "test-group",
			consumer:       "consumer-1",
			messageId:      "nonexistent-0",
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/api/streams/"+tt.stream+"/groups/"+tt.group+"/consumers/"+tt.consumer+"/messages/"+tt.messageId+"/finish", nil)
			req.Header.Set("Authorization", "Bearer "+testToken)
			req.SetPathValue("stream", tt.stream)
			req.SetPathValue("group", tt.group)
			req.SetPathValue("consumer", tt.consumer)
			req.SetPathValue("messageId", tt.messageId)

			rr := httptest.NewRecorder()
			server.handleFinishNewRoute(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rr.Code)
			}
		})
	}
}

func TestHandleConsumerEventsValidation(t *testing.T) {
	server := createTestServer()

	tests := []struct {
		name           string
		query          string
		expectedStatus int
	}{
		{
			name:           "Missing stream",
			query:          "group=group",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Missing group",
			query:          "stream=test",
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/api/events?"+tt.query, nil)
			req.Header.Set("Authorization", "Bearer "+testToken)

			rr := httptest.NewRecorder()
			server.handleConsumerEvents(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rr.Code)
			}
		})
	}
}
