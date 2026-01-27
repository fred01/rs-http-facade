package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/redis/go-redis/v9"
)

const defaultConfigFile = "/etc/rs-http-facade/config.toml"
const defaultSSEKeepaliveIntervalSec = 60 // seconds

type AppConfig struct {
	RedisAddress            string `toml:"redis_address"`
	RedisPassword           string `toml:"redis_password"`
	RedisDB                 int    `toml:"redis_db"`
	HTTPAddress             string `toml:"http_address"`
	BearerToken             string `toml:"bearer_token"`
	SSEKeepaliveIntervalSec int    `toml:"sse_keepalive_interval_sec"` // in seconds, negative to disable
}

var (
	configPath              = flag.String("config", envOrDefault("RS_HTTP_FACADE_CONFIG", defaultConfigFile), "Path to TOML config file")
	redisAddress            = flag.String("redis-address", "", "Redis server address (required)")
	redisPassword           = flag.String("redis-password", "", "Redis password (optional)")
	redisDB                 = flag.Int("redis-db", 0, "Redis database number (default: 0)")
	httpAddress             = flag.String("http-address", "", "HTTP server address (required)")
	bearerToken             = flag.String("bearer-token", "", "Bearer token for authentication (required)")
	sseKeepaliveIntervalSec = flag.Int("sse-keepalive-interval-sec", 0, "SSE keepalive interval in seconds for consumers (default: 60, negative to disable)")

	messageExpiryDuration = 5 * time.Minute
)

// Server encapsulates all application state and dependencies
type Server struct {
	config            AppConfig
	redisClient       *redis.Client
	consumers         map[string]*consumerState
	consumersMutex    sync.RWMutex
	bearerTokenHash   [32]byte
	consumerIDCounter uint64
}

// NewServer creates a new Server instance with the given configuration
func NewServer(config AppConfig) (*Server, error) {
	// Initialize Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr:     config.RedisAddress,
		Password: config.RedisPassword,
		DB:       config.RedisDB,
	})

	// Test Redis connection
	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Pre-calculate bearer token hash for constant-time comparison
	bearerTokenHash := sha256.Sum256([]byte(config.BearerToken))

	// Clear plaintext bearer token after hashing
	config.BearerToken = ""

	server := &Server{
		config:          config,
		redisClient:     redisClient,
		consumers:       make(map[string]*consumerState),
		bearerTokenHash: bearerTokenHash,
	}

	return server, nil
}

// Start begins the background tasks and HTTP server
func (s *Server) Start() error {
	// Setup HTTP routes using Go 1.22+ method-based routing patterns
	mux := http.NewServeMux()

	// Producer endpoints
	mux.HandleFunc("POST /api/streams/{stream}/messages/batch", s.authMiddleware(s.handleBatchAddRoute))
	mux.HandleFunc("POST /api/streams/{stream}/messages", s.authMiddleware(s.handleAddRoute))

	// Message lifecycle endpoints
	mux.HandleFunc("POST /api/streams/{stream}/groups/{group}/consumers/{consumer}/messages/{messageId}/finish", s.authMiddleware(s.handleFinishNewRoute))
	mux.HandleFunc("POST /api/streams/{stream}/groups/{group}/consumers/{consumer}/finish", s.authMiddleware(s.handleBatchFinishRoute))

	// Consumer endpoints
	mux.HandleFunc("GET /api/events", s.authMiddleware(s.handleConsumerEvents))
	mux.HandleFunc("GET /api/consumers/{stream}/{group}", s.authMiddleware(s.handleConsumerStatusRoute))

	// Admin endpoints
	mux.HandleFunc("GET /admin/ping", s.authMiddleware(s.handleAdminPing))
	mux.HandleFunc("GET /admin/info", s.authMiddleware(s.handleAdminInfo))
	mux.HandleFunc("GET /admin/streams", s.authMiddleware(s.handleAdminStreams))
	mux.HandleFunc("GET /admin/stats", s.authMiddleware(s.handleAdminStats))
	mux.HandleFunc("POST /admin/flush", s.authMiddleware(s.handleAdminFlush))
	mux.HandleFunc("GET /admin/{streamName}", s.authMiddleware(s.handleAdminStreamInfo))

	log.Printf("Starting HTTP server on %s", s.config.HTTPAddress)
	log.Printf("Connected to Redis at %s", s.config.RedisAddress)
	return http.ListenAndServe(s.config.HTTPAddress, mux)
}

// Stop gracefully stops the server
func (s *Server) Stop() {
	if s.redisClient != nil {
		s.redisClient.Close()
	}
}

func envOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}

func mergeConfig(base *AppConfig, override AppConfig) {
	if override.RedisAddress != "" {
		base.RedisAddress = override.RedisAddress
	}

	if override.RedisPassword != "" {
		base.RedisPassword = override.RedisPassword
	}

	// Note: RedisDB=0 is valid but we use it as "not set" since it's the default.
	// Use environment variables or CLI flags to explicitly set database 0.
	if override.RedisDB != 0 {
		base.RedisDB = override.RedisDB
	}

	if override.HTTPAddress != "" {
		base.HTTPAddress = override.HTTPAddress
	}

	if override.BearerToken != "" {
		base.BearerToken = override.BearerToken
	}

	// Note: SSEKeepaliveIntervalSec=0 means "use default" (set in main),
	// negative values disable keepalive
	if override.SSEKeepaliveIntervalSec != 0 {
		base.SSEKeepaliveIntervalSec = override.SSEKeepaliveIntervalSec
	}
}

func loadConfigFile(path string) (AppConfig, bool, error) {
	var cfg AppConfig

	if path == "" {
		return cfg, false, nil
	}

	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		if os.IsNotExist(err) {
			return cfg, false, nil
		}

		return cfg, false, err
	}

	return cfg, true, nil
}

func applyEnvOverrides(cfg *AppConfig) {
	if value := os.Getenv("RS_HTTP_FACADE_REDIS_ADDRESS"); value != "" {
		cfg.RedisAddress = value
	}

	if value := os.Getenv("RS_HTTP_FACADE_REDIS_PASSWORD"); value != "" {
		cfg.RedisPassword = value
	}

	if value := os.Getenv("RS_HTTP_FACADE_REDIS_DB"); value != "" {
		if db, err := strconv.Atoi(value); err == nil {
			cfg.RedisDB = db
		}
	}

	if value := os.Getenv("RS_HTTP_FACADE_HTTP_ADDRESS"); value != "" {
		cfg.HTTPAddress = value
	}

	if value := os.Getenv("RS_HTTP_FACADE_BEARER_TOKEN"); value != "" {
		cfg.BearerToken = value
	}

	if value := os.Getenv("RS_HTTP_FACADE_SSE_KEEPALIVE_INTERVAL_SEC"); value != "" {
		if interval, err := strconv.Atoi(value); err == nil {
			cfg.SSEKeepaliveIntervalSec = interval
		}
	}
}

func applyCLIOverrides(cfg *AppConfig, visited map[string]bool) {
	if visited["redis-address"] {
		cfg.RedisAddress = *redisAddress
	}

	if visited["redis-password"] {
		cfg.RedisPassword = *redisPassword
	}

	if visited["redis-db"] {
		cfg.RedisDB = *redisDB
	}

	if visited["http-address"] {
		cfg.HTTPAddress = *httpAddress
	}

	if visited["bearer-token"] {
		cfg.BearerToken = *bearerToken
	}

	if visited["sse-keepalive-interval-sec"] {
		cfg.SSEKeepaliveIntervalSec = *sseKeepaliveIntervalSec
	}
}

func validateConfig(cfg AppConfig) error {
	var missing []string

	if cfg.RedisAddress == "" {
		missing = append(missing, "redis_address")
	}

	if cfg.HTTPAddress == "" {
		missing = append(missing, "http_address")
	}

	if cfg.BearerToken == "" {
		missing = append(missing, "bearer_token")
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing required configuration values: %s", strings.Join(missing, ", "))
	}

	return nil
}

// consumerState tracks the state of an SSE consumer
type consumerState struct {
	stream       string
	group        string
	consumerName string
	stopChan     chan struct{}
	messageChan  chan redis.XMessage
	limit        int32 // maximum messages in-flight for this consumer
	inFlight     int32 // current messages in-flight
	received     int64
	finished     int64
}

func main() {
	flag.Parse()

	visitedFlags := map[string]bool{}
	flag.CommandLine.Visit(func(f *flag.Flag) {
		visitedFlags[f.Name] = true
	})

	var config AppConfig
	fileConfig, loaded, err := loadConfigFile(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config file %s: %v", *configPath, err)
	}

	if loaded {
		log.Printf("Loaded configuration from %s", *configPath)
	}

	mergeConfig(&config, fileConfig)
	applyEnvOverrides(&config)
	applyCLIOverrides(&config, visitedFlags)

	if err := validateConfig(config); err != nil {
		log.Fatalf("%v", err)
	}

	// Set default SSE keepalive interval if not configured
	if config.SSEKeepaliveIntervalSec == 0 {
		config.SSEKeepaliveIntervalSec = defaultSSEKeepaliveIntervalSec
	}

	// Clear plaintext bearer token flag after reading
	tokenForServer := config.BearerToken
	*bearerToken = ""

	// Create and start server
	config.BearerToken = tokenForServer
	server, err := NewServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}

// authMiddleware validates bearer token using constant-time comparison
func (s *Server) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Authorization header required", http.StatusUnauthorized)
			return
		}

		const bearerPrefix = "Bearer "
		if !strings.HasPrefix(authHeader, bearerPrefix) {
			http.Error(w, "Invalid bearer token format", http.StatusUnauthorized)
			return
		}

		sentToken := authHeader[len(bearerPrefix):]
		sentTokenHash := sha256.Sum256([]byte(sentToken))

		if subtle.ConstantTimeCompare(sentTokenHash[:], s.bearerTokenHash[:]) != 1 {
			http.Error(w, "Invalid bearer token", http.StatusUnauthorized)
			return
		}

		next(w, r)
	}
}

// handleAddRoute is the Go 1.22+ route handler for single message publishing
func (s *Server) handleAddRoute(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	if stream == "" {
		http.Error(w, "Stream name required", http.StatusBadRequest)
		return
	}

	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	id, err := s.redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"data": string(msg.Data)},
	}).Result()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to publish: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "stream": stream, "id": id})
}

// handleBatchAddRoute is the Go 1.22+ route handler for batch message publishing
func (s *Server) handleBatchAddRoute(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	if stream == "" {
		http.Error(w, "Stream name required", http.StatusBadRequest)
		return
	}

	var msg MultiMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	if len(msg.Messages) == 0 {
		http.Error(w, "At least one message is required", http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	pipe := s.redisClient.Pipeline()

	for _, m := range msg.Messages {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"data": string(m)},
		})
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to publish: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"stream": stream,
		"count":  len(msg.Messages),
	})
}

// handleFinishNewRoute is the new route handler for finishing messages with explicit params in URL
// POST /api/streams/{stream}/groups/{group}/consumers/{consumer}/messages/{messageId}/finish
func (s *Server) handleFinishNewRoute(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	group := r.PathValue("group")
	consumer := r.PathValue("consumer")
	messageID := r.PathValue("messageId")

	if stream == "" || group == "" || consumer == "" || messageID == "" {
		http.Error(w, "Stream, group, consumer, and message ID required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Acknowledge the message in Redis Stream
	acked, err := s.redisClient.XAck(ctx, stream, group, messageID).Result()
	if err != nil {
		log.Printf("Failed to acknowledge message %s: %v", messageID, err)
		http.Error(w, "Failed to acknowledge message", http.StatusInternalServerError)
		return
	}

	// Delete the message from the stream to prevent queue overflow (SQS-like behavior)
	// This happens after XACK so the message is properly acknowledged first
	if acked > 0 {
		deleted, err := s.redisClient.XDel(ctx, stream, messageID).Result()
		if err != nil {
			log.Printf("Failed to delete message %s from stream: %v", messageID, err)
			// Don't fail the request - message was already acknowledged
		} else if deleted > 0 {
			log.Printf("Deleted message %s from stream %s", messageID, stream)
		}
	}

	// Update consumer stats (if consumer is still connected)
	// IMPORTANT: Always decrement inFlight, even if XAck returns 0 (message not found).
	// Otherwise the consumer will be stuck forever waiting for a slot that will never free up.
	// This can happen if another consumer claimed the message via XAutoClaim while this
	// consumer was processing it (e.g., processing took longer than messageExpiryDuration).
	consumerKey := fmt.Sprintf("%s:%s:%s", stream, group, consumer)
	s.consumersMutex.RLock()
	if consumerState, ok := s.consumers[consumerKey]; ok {
		atomic.AddInt64(&consumerState.finished, 1)
		// Decrement inFlight and ensure it doesn't go below 0
		if newVal := atomic.AddInt32(&consumerState.inFlight, -1); newVal < 0 {
			// This case should ideally not happen with correct accounting,
			// but as a safeguard, log it and reset to 0.
			log.Printf("Warning: inFlight for consumer %s went negative (%d). Resetting to 0.", consumerKey, newVal)
			atomic.StoreInt32(&consumerState.inFlight, 0)
		}
	}
	s.consumersMutex.RUnlock()

	if acked == 0 {
		log.Printf("Message %s not found in pending list for stream=%s group=%s (already claimed or acknowledged)", messageID, stream, group)
		http.Error(w, "Message not found in pending list or already acknowledged", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "action": "finished"})
}

// handleBatchFinishRoute handles batch message acknowledgment
// POST /api/streams/{stream}/groups/{group}/consumers/{consumer}/finish
func (s *Server) handleBatchFinishRoute(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	group := r.PathValue("group")
	consumer := r.PathValue("consumer")

	if stream == "" || group == "" || consumer == "" {
		http.Error(w, "Stream, group, and consumer required", http.StatusBadRequest)
		return
	}

	var req BatchFinishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	if len(req.MessageIDs) == 0 {
		http.Error(w, "At least one message_id is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Use pipeline for efficient batch operations
	pipe := s.redisClient.Pipeline()
	ackCmd := pipe.XAck(ctx, stream, group, req.MessageIDs...)
	delCmd := pipe.XDel(ctx, stream, req.MessageIDs...)
	_, err := pipe.Exec(ctx)

	if err != nil {
		log.Printf("Failed to batch finish messages: %v", err)
		http.Error(w, "Failed to finish messages", http.StatusInternalServerError)
		return
	}

	acked, _ := ackCmd.Result()
	deleted, _ := delCmd.Result()

	if acked > 0 {
		log.Printf("Batch finished %d messages from stream %s (deleted %d)", acked, stream, deleted)
	}

	// Update consumer stats (if consumer is still connected)
	// Decrement inFlight by the number of messages we tried to finish
	// (same logic as single finish - always decrement to prevent deadlock)
	consumerKey := fmt.Sprintf("%s:%s:%s", stream, group, consumer)
	s.consumersMutex.RLock()
	if consumerState, ok := s.consumers[consumerKey]; ok {
		atomic.AddInt64(&consumerState.finished, int64(len(req.MessageIDs)))
		newVal := atomic.AddInt32(&consumerState.inFlight, -int32(len(req.MessageIDs)))
		if newVal < 0 {
			log.Printf("Warning: inFlight for consumer %s went negative (%d). Resetting to 0.", consumerKey, newVal)
			atomic.StoreInt32(&consumerState.inFlight, 0)
		}
	}
	s.consumersMutex.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "ok",
		"action":   "batch_finished",
		"acked":    acked,
		"deleted":  deleted,
		"received": len(req.MessageIDs),
	})
}

// handleConsumerStatusRoute is the Go 1.22+ route handler for consumer status
func (s *Server) handleConsumerStatusRoute(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	group := r.PathValue("group")

	if stream == "" || group == "" {
		http.Error(w, "Stream and group required", http.StatusBadRequest)
		return
	}

	prefix := fmt.Sprintf("%s:%s:", stream, group)

	s.consumersMutex.RLock()
	var matchingConsumers []*consumerState
	for key, consumer := range s.consumers {
		if strings.HasPrefix(key, prefix) {
			matchingConsumers = append(matchingConsumers, consumer)
		}
	}
	s.consumersMutex.RUnlock()

	if len(matchingConsumers) == 0 {
		http.Error(w, "No consumers found for this stream/group", http.StatusNotFound)
		return
	}

	var totalMessages, totalFinished int64
	for _, consumer := range matchingConsumers {
		totalMessages += atomic.LoadInt64(&consumer.received)
		totalFinished += atomic.LoadInt64(&consumer.finished)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"stream":    stream,
		"group":     group,
		"consumers": len(matchingConsumers),
		"messages":  totalMessages,
		"finished":  totalFinished,
	})
}

// Admin route handlers

// handleAdminPing handles GET /admin/ping
func (s *Server) handleAdminPing(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	result, err := s.redisClient.Ping(ctx).Result()
	if err != nil {
		http.Error(w, fmt.Sprintf("Redis ping failed: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": result})
}

// handleAdminInfo handles GET /admin/info
func (s *Server) handleAdminInfo(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	result, err := s.redisClient.Info(ctx).Result()
	if err != nil {
		http.Error(w, fmt.Sprintf("Redis info failed: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(result))
}

// handleAdminStreams handles GET /admin/streams
func (s *Server) handleAdminStreams(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	var streams []string
	var cursor uint64
	for {
		var keys []string
		var err error
		keys, cursor, err = s.redisClient.Scan(ctx, cursor, "*", 100).Result()
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to scan keys: %v", err), http.StatusInternalServerError)
			return
		}

		for _, key := range keys {
			keyType, err := s.redisClient.Type(ctx, key).Result()
			if err == nil && keyType == "stream" {
				streams = append(streams, key)
			}
		}

		if cursor == 0 {
			break
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"streams": streams})
}

// handleAdminFlush handles POST /admin/flush
func (s *Server) handleAdminFlush(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	err := s.redisClient.FlushDB(ctx).Err()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to flush database: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "action": "flushed"})
}

// handleAdminStats handles GET /admin/stats
func (s *Server) handleAdminStats(w http.ResponseWriter, r *http.Request) {
	type GroupStats struct {
		Name      string `json:"name"`
		Pending   int64  `json:"pending"`
		Lag       int64  `json:"lag"`
		Consumers int64  `json:"consumers"`
	}
	type StreamStats struct {
		Name   string       `json:"name"`
		Length int64        `json:"length"`
		Groups []GroupStats `json:"groups"`
	}

	ctx := context.Background()
	var streamNames []string
	var cursor uint64
	for {
		var keys []string
		var err error
		keys, cursor, err = s.redisClient.Scan(ctx, cursor, "*", 100).Result()
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to scan keys: %v", err), http.StatusInternalServerError)
			return
		}

		for _, key := range keys {
			keyType, err := s.redisClient.Type(ctx, key).Result()
			if err == nil && keyType == "stream" {
				streamNames = append(streamNames, key)
			}
		}

		if cursor == 0 {
			break
		}
	}

	var stats []StreamStats
	for _, streamName := range streamNames {
		streamStat := StreamStats{
			Name:   streamName,
			Groups: []GroupStats{},
		}

		length, err := s.redisClient.XLen(ctx, streamName).Result()
		if err == nil {
			streamStat.Length = length
		}

		groups, err := s.redisClient.XInfoGroups(ctx, streamName).Result()
		if err == nil {
			for _, group := range groups {
				groupStat := GroupStats{
					Name:      group.Name,
					Pending:   group.Pending,
					Lag:       group.Lag,
					Consumers: group.Consumers,
				}
				streamStat.Groups = append(streamStat.Groups, groupStat)
			}
		}

		stats = append(stats, streamStat)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"streams": stats})
}

// handleAdminStreamInfo handles GET /admin/{streamName}
func (s *Server) handleAdminStreamInfo(w http.ResponseWriter, r *http.Request) {
	streamName := r.PathValue("streamName")
	if streamName == "" {
		http.Error(w, "Stream name required", http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	info, err := s.redisClient.XInfoStream(ctx, streamName).Result()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get stream info: %v", err), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

// Message structure for JSON input
type Message struct {
	Data json.RawMessage `json:"data"`
}

// MultiMessage structure for batch publishing
type MultiMessage struct {
	Messages []json.RawMessage `json:"messages"`
}

// BatchFinishRequest structure for batch message acknowledgment
type BatchFinishRequest struct {
	MessageIDs []string `json:"message_ids"`
}

// handleAdd handles single message publishing (XADD) - kept for tests
func (s *Server) handleAdd(w http.ResponseWriter, r *http.Request, stream string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	id, err := s.redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"data": string(msg.Data)},
	}).Result()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to publish: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "stream": stream, "id": id})
}

// handleBatchAdd handles multiple message publishing (XADD pipeline) - kept for tests
func (s *Server) handleBatchAdd(w http.ResponseWriter, r *http.Request, stream string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var msg MultiMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	if len(msg.Messages) == 0 {
		http.Error(w, "At least one message is required", http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	pipe := s.redisClient.Pipeline()

	for _, m := range msg.Messages {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"data": string(m)},
		})
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to publish: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"stream": stream,
		"count":  len(msg.Messages),
	})
}

// handleConsumerEvents handles SSE endpoint for consuming messages
// GET /api/events?stream=<stream>&group=<group>&consumer=<name>&limit=<N>
// Each HTTP client gets its own Redis consumer for load balancing
func (s *Server) handleConsumerEvents(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters for stream and group
	stream := r.URL.Query().Get("stream")
	group := r.URL.Query().Get("group")

	if stream == "" || group == "" {
		http.Error(w, "Stream and group query parameters are required", http.StatusBadRequest)
		return
	}

	// Parse optional consumer name parameter
	consumerNameParam := r.URL.Query().Get("consumer")

	// Parse limit parameter (default to 1 for backward compatibility)
	// A limit of 0 is valid and pauses message delivery (limit is set at connection time only)
	limitStr := r.URL.Query().Get("limit")
	limit := int32(1) // Default limit
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err != nil || parsedLimit < 0 {
			http.Error(w, "Invalid limit parameter: must be a non-negative integer", http.StatusBadRequest)
			return
		}
		limit = int32(parsedLimit)
	}

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Create consumer name - use provided name or generate a unique one
	var consumerName string
	if consumerNameParam != "" {
		consumerName = consumerNameParam
	} else {
		consumerID := atomic.AddUint64(&s.consumerIDCounter, 1)
		consumerName = fmt.Sprintf("consumer-%d", consumerID)
	}
	consumerKey := fmt.Sprintf("%s:%s:%s", stream, group, consumerName)

	ctx := context.Background()

	// Create consumer group if it doesn't exist
	err := s.redisClient.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		http.Error(w, fmt.Sprintf("Failed to create consumer group: %v", err), http.StatusInternalServerError)
		return
	}

	// Create consumer state
	state := &consumerState{
		stream:       stream,
		group:        group,
		consumerName: consumerName,
		stopChan:     make(chan struct{}),
		messageChan:  make(chan redis.XMessage, 100),
		limit:        limit,
		inFlight:     0,
	}

	// Store consumer state (check for duplicate consumer)
	s.consumersMutex.Lock()
	if _, exists := s.consumers[consumerKey]; exists {
		s.consumersMutex.Unlock()
		log.Printf("Consumer %s is already connected", consumerKey)
		http.Error(w, fmt.Sprintf("Consumer %s is already connected", consumerName), http.StatusConflict)
		return
	}
	s.consumers[consumerKey] = state
	s.consumersMutex.Unlock()

	// Cleanup on disconnect
	defer func() {
		// Signal goroutine to stop first
		select {
		case <-state.stopChan:
		// Already closed
		default:
			close(state.stopChan)
		}
		// Wait a bit for goroutine to finish
		time.Sleep(100 * time.Millisecond)

		s.consumersMutex.Lock()
		delete(s.consumers, consumerKey)
		s.consumersMutex.Unlock()
		log.Printf("Consumer %s stopped and cleaned up", consumerKey)
	}()

	// Start background goroutine to read messages from Redis Stream
	go func() {
		readCtx := context.Background()
		for {
			select {
			case <-state.stopChan:
				return
			default:
				// Gate: Check if limit allows reading
				// Note: We use polling with sleep instead of channel-based notification
				// for simplicity. The 100ms sleep is a trade-off between responsiveness
				// and CPU usage. For high-throughput scenarios, consider implementing
				// a notification channel from the finish handler.
				currentLimit := atomic.LoadInt32(&state.limit)
				if currentLimit <= 0 {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// Gate: Check if inFlight count allows reading
				currentInFlight := atomic.LoadInt32(&state.inFlight)
				if currentInFlight >= currentLimit {
					// Consumer has maximum messages in-flight, don't read more from Redis
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// First, try to claim idle messages from other consumers
				// that have been pending for more than 5 minutes (messageExpiryDuration)
				claimedMsgs, _, err := s.redisClient.XAutoClaim(readCtx, &redis.XAutoClaimArgs{
					Stream:   stream,
					Group:    group,
					Consumer: consumerName,
					MinIdle:  messageExpiryDuration,
					Start:    "0-0",
					Count:    1,
				}).Result()

				// Check for NOGROUP error on XAutoClaim
				if err != nil {
					errMsg := err.Error()
					if strings.Contains(errMsg, "NOGROUP") {
						log.Printf("Fatal error in XAutoClaim (stream/group deleted): %v - closing connection", err)
						close(state.messageChan)
						return
					}
					// For other errors, just skip claiming and continue to XReadGroup
				}

				if err == nil && len(claimedMsgs) > 0 {
					for _, msg := range claimedMsgs {
						log.Printf("Claimed idle message %s from pending list", msg.ID)
						select {
						case state.messageChan <- msg:
							atomic.AddInt64(&state.received, 1)
							atomic.AddInt32(&state.inFlight, 1)
						case <-state.stopChan:
							return
						}
					}
					continue // Process claimed messages before reading new ones
				}

				// Read new messages from stream
				streams, err := s.redisClient.XReadGroup(readCtx, &redis.XReadGroupArgs{
					Group:    group,
					Consumer: consumerName,
					Streams:  []string{stream, ">"},
					Count:    1,
					Block:    time.Second,
				}).Result()

				if err != nil {
					if err == redis.Nil {
						continue
					}
					// Check if stopped
					select {
					case <-state.stopChan:
						return
					default:
					}

					// Check if error is NOGROUP (stream or consumer group was deleted)
					errMsg := err.Error()
					if strings.Contains(errMsg, "NOGROUP") {
						log.Printf("Fatal error (stream/group deleted): %v - closing connection", err)
						// Close messageChan to signal SSE handler to terminate connection
						close(state.messageChan)
						return
					}

					log.Printf("Error reading from stream: %v", err)
					time.Sleep(time.Second)
					continue
				}

				for _, st := range streams {
					for _, msg := range st.Messages {
						select {
						case state.messageChan <- msg:
							atomic.AddInt64(&state.received, 1)
							atomic.AddInt32(&state.inFlight, 1)
						case <-state.stopChan:
							return
						}
					}
				}
			}
		}
	}()

	log.Printf("Consumer %s connected for stream=%s group=%s limit=%d", consumerKey, stream, group, limit)

	// Setup keepalive ticker if enabled (interval > 0)
	// Note: When keepalive is disabled, keepaliveChan remains nil.
	// In Go, nil channels in select statements are never selected,
	// so the keepalive case is effectively disabled.
	var keepaliveTicker *time.Ticker
	var keepaliveChan <-chan time.Time
	if s.config.SSEKeepaliveIntervalSec > 0 {
		keepaliveTicker = time.NewTicker(time.Duration(s.config.SSEKeepaliveIntervalSec) * time.Second)
		keepaliveChan = keepaliveTicker.C
		defer keepaliveTicker.Stop()
	}

	// Stream messages as SSE
	requestCtx := r.Context()
	for {
		select {
		case <-requestCtx.Done():
			// Client disconnected
			return
		case <-keepaliveChan:
			// Send SSE comment to keep connection alive
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case msg, ok := <-state.messageChan:
			if !ok {
				return
			}

			// Get the data field from the message
			var body interface{}
			if data, ok := msg.Values["data"]; ok {
				// Try to parse as JSON
				var jsonData interface{}
				if err := json.Unmarshal([]byte(data.(string)), &jsonData); err == nil {
					body = jsonData
				} else {
					body = data
				}
			} else {
				body = msg.Values
			}

			// Convert message to JSON
			data := map[string]interface{}{
				"id":   msg.ID,
				"body": body,
			}

			jsonData, err := json.Marshal(data)
			if err != nil {
				log.Printf("Failed to marshal message: %v", err)
				continue
			}

			// Send as SSE event
			fmt.Fprintf(w, "data: %s\n\n", jsonData)
			flusher.Flush()
		}
	}
}

// splitPath splits URL path by slashes
func splitPath(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return []string{}
	}
	return strings.Split(path, "/")
}
