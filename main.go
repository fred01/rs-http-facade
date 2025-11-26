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

	finalConfig         AppConfig
	redisClient         *redis.Client
	consumers           = make(map[string]*consumerState)
	consumersMutex      sync.RWMutex
	activeMessages      = make(map[string]*messageWithExpiry)
	activeMessagesMutex sync.RWMutex
	bearerTokenHash     [32]byte

	messageCleanupTicker  *time.Ticker
	messageExpiryDuration = 5 * time.Minute
	consumerIDCounter     uint64
)

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

	if override.RedisDB != 0 {
		base.RedisDB = override.RedisDB
	}

	if override.HTTPAddress != "" {
		base.HTTPAddress = override.HTTPAddress
	}

	if override.BearerToken != "" {
		base.BearerToken = override.BearerToken
	}

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

// messageWithExpiry wraps a Redis Stream message with an expiry time
type messageWithExpiry struct {
	stream      string
	group       string
	messageID   string
	consumerKey string
	expiry      time.Time
}

// consumerState tracks the state of an SSE consumer
type consumerState struct {
	stream       string
	group        string
	consumerName string
	stopChan     chan struct{}
	messageChan  chan redis.XMessage
	ready        int32 // RDY count
	received     int64
	finished     int64
	requeued     int64
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

	finalConfig = config
	// Pre-calculate bearer token hash for constant-time comparison
	bearerTokenHash = sha256.Sum256([]byte(finalConfig.BearerToken))

	// Clear plaintext bearer token copies after hashing to limit exposure
	config.BearerToken = ""
	finalConfig.BearerToken = ""
	*bearerToken = ""

	// Initialize Redis client
	redisClient = redis.NewClient(&redis.Options{
		Addr:     finalConfig.RedisAddress,
		Password: finalConfig.RedisPassword,
		DB:       finalConfig.RedisDB,
	})

	// Test Redis connection
	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redisClient.Close()

	// Start background cleanup for expired messages
	messageCleanupTicker = time.NewTicker(30 * time.Second)
	defer messageCleanupTicker.Stop()
	go cleanupExpiredMessages()

	// Setup HTTP routes with authentication middleware
	http.HandleFunc("/api/streams/", authMiddleware(handleStreams))
	http.HandleFunc("/api/messages/", authMiddleware(handleMessages))
	http.HandleFunc("/api/consumers/", authMiddleware(handleConsumers))
	http.HandleFunc("/api/events", authMiddleware(handleConsumerEvents))
	http.HandleFunc("/admin/", authMiddleware(handleAdmin))

	log.Printf("Starting HTTP server on %s", finalConfig.HTTPAddress)
	log.Printf("Connected to Redis at %s", finalConfig.RedisAddress)
	if err := http.ListenAndServe(finalConfig.HTTPAddress, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}

// cleanupExpiredMessages removes expired messages from the activeMessages map
func cleanupExpiredMessages() {
	ctx := context.Background()
	for range messageCleanupTicker.C {
		now := time.Now()
		activeMessagesMutex.Lock()
		for id, msgWithExpiry := range activeMessages {
			if now.After(msgWithExpiry.expiry) {
				// Message expired, claim it back to the pending list
				// This makes it available for redelivery to any consumer
				log.Printf("Cleaned up expired message: %s", id)
				delete(activeMessages, id)
				// XCLAIM the message back to make it available
				_, err := redisClient.XClaim(ctx, &redis.XClaimArgs{
					Stream:   msgWithExpiry.stream,
					Group:    msgWithExpiry.group,
					Consumer: msgWithExpiry.consumerKey,
					MinIdle:  0,
					Messages: []string{msgWithExpiry.messageID},
				}).Result()
				if err != nil {
					log.Printf("Failed to reclaim expired message %s: %v", id, err)
				}
			}
		}
		activeMessagesMutex.Unlock()
	}
}

// authMiddleware validates bearer token using constant-time comparison
func authMiddleware(next http.HandlerFunc) http.HandlerFunc {
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

		if subtle.ConstantTimeCompare(sentTokenHash[:], bearerTokenHash[:]) != 1 {
			http.Error(w, "Invalid bearer token", http.StatusUnauthorized)
			return
		}

		next(w, r)
	}
}

// handleStreams handles REST operations for streams (POST for publishing)
// POST /api/streams/:stream/messages - publish single message (XADD)
// POST /api/streams/:stream/messages/batch - publish multiple messages (XADD pipeline)
func handleStreams(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/api/streams/"):]

	if path == "" {
		http.Error(w, "Stream name required", http.StatusBadRequest)
		return
	}

	// Check if batch endpoint
	if len(path) > len("/messages/batch") && path[len(path)-len("/messages/batch"):] == "/messages/batch" {
		stream := path[:len(path)-len("/messages/batch")]
		handleBatchAdd(w, r, stream)
		return
	}

	// Check if single message endpoint
	if len(path) > len("/messages") && path[len(path)-len("/messages"):] == "/messages" {
		stream := path[:len(path)-len("/messages")]
		handleAdd(w, r, stream)
		return
	}

	http.Error(w, "Invalid endpoint", http.StatusNotFound)
}

// Message structure for JSON input
type Message struct {
	Data json.RawMessage `json:"data"`
}

// MultiMessage structure for batch publishing
type MultiMessage struct {
	Messages []json.RawMessage `json:"messages"`
}

// handleAdd handles single message publishing (XADD)
func handleAdd(w http.ResponseWriter, r *http.Request, stream string) {
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
	// Store the message body as a single "data" field
	id, err := redisClient.XAdd(ctx, &redis.XAddArgs{
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

// handleBatchAdd handles multiple message publishing (XADD pipeline)
func handleBatchAdd(w http.ResponseWriter, r *http.Request, stream string) {
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
	pipe := redisClient.Pipeline()

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

// handleConsumers handles REST operations for consumers
// POST /api/consumers/:stream/:group/rdy - set RDY count
// GET /api/consumers/:stream/:group - get consumer status
func handleConsumers(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/api/consumers/"):]
	parts := splitPath(path)

	if len(parts) < 2 {
		http.Error(w, "Stream and group required", http.StatusBadRequest)
		return
	}

	stream := parts[0]
	group := parts[1]

	if len(parts) == 3 && parts[2] == "rdy" {
		handleConsumerRdy(w, r, stream, group)
		return
	}

	if len(parts) == 2 {
		if r.Method == http.MethodGet {
			handleConsumerStatus(w, r, stream, group)
			return
		}
	}

	http.Error(w, "Invalid endpoint", http.StatusNotFound)
}

// handleConsumerStatus returns aggregated status of all consumers for a stream/group
func handleConsumerStatus(w http.ResponseWriter, r *http.Request, stream, group string) {
	prefix := fmt.Sprintf("%s:%s:", stream, group)

	consumersMutex.RLock()
	var matchingConsumers []*consumerState
	for key, consumer := range consumers {
		if strings.HasPrefix(key, prefix) {
			matchingConsumers = append(matchingConsumers, consumer)
		}
	}
	consumersMutex.RUnlock()

	if len(matchingConsumers) == 0 {
		http.Error(w, "No consumers found for this stream/group", http.StatusNotFound)
		return
	}

	// Aggregate stats from all consumers
	var totalMessages, totalFinished, totalRequeued int64
	for _, consumer := range matchingConsumers {
		totalMessages += atomic.LoadInt64(&consumer.received)
		totalFinished += atomic.LoadInt64(&consumer.finished)
		totalRequeued += atomic.LoadInt64(&consumer.requeued)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"stream":    stream,
		"group":     group,
		"consumers": len(matchingConsumers),
		"messages":  totalMessages,
		"finished":  totalFinished,
		"requeued":  totalRequeued,
	})
}

// RdyRequest structure for controlling consumer RDY state
type RdyRequest struct {
	Count int `json:"count"`
}

// handleConsumerRdy handles RDY control for all consumers of a stream/group
func handleConsumerRdy(w http.ResponseWriter, r *http.Request, stream, group string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req RdyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	prefix := fmt.Sprintf("%s:%s:", stream, group)

	consumersMutex.RLock()
	var matchingConsumers []*consumerState
	for key, consumer := range consumers {
		if strings.HasPrefix(key, prefix) {
			matchingConsumers = append(matchingConsumers, consumer)
		}
	}
	consumersMutex.RUnlock()

	if len(matchingConsumers) == 0 {
		http.Error(w, "No consumers found for this stream/group", http.StatusNotFound)
		return
	}

	// Apply RDY count to all consumers for this stream/group
	for _, consumer := range matchingConsumers {
		atomic.StoreInt32(&consumer.ready, int32(req.Count))
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "ok",
		"consumers": len(matchingConsumers),
	})
}

// handleMessages handles message lifecycle operations
// POST /api/messages/:messageId/touch - extend message timeout
// POST /api/messages/:messageId/finish - mark message as successfully processed (XACK)
// POST /api/messages/:messageId/requeue - requeue message (XCLAIM back)
func handleMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := r.URL.Path[len("/api/messages/"):]
	parts := splitPath(path)

	if len(parts) < 2 {
		http.Error(w, "Message ID and action required", http.StatusBadRequest)
		return
	}

	messageID := parts[0]
	action := parts[1]

	activeMessagesMutex.RLock()
	msgWithExpiry, exists := activeMessages[messageID]
	activeMessagesMutex.RUnlock()

	if !exists {
		http.Error(w, "Message not found or already processed", http.StatusNotFound)
		return
	}

	ctx := context.Background()

	switch action {
	case "touch":
		// Extend expiry time when touched
		activeMessagesMutex.Lock()
		if mwe, ok := activeMessages[messageID]; ok {
			mwe.expiry = time.Now().Add(messageExpiryDuration)
		}
		activeMessagesMutex.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "action": "touched"})
	case "finish":
		// Acknowledge the message
		_, err := redisClient.XAck(ctx, msgWithExpiry.stream, msgWithExpiry.group, msgWithExpiry.messageID).Result()
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to acknowledge message: %v", err), http.StatusInternalServerError)
			return
		}
		// Update consumer stats
		consumersMutex.RLock()
		if consumer, ok := consumers[msgWithExpiry.consumerKey]; ok {
			atomic.AddInt64(&consumer.finished, 1)
		}
		consumersMutex.RUnlock()
		activeMessagesMutex.Lock()
		delete(activeMessages, messageID)
		activeMessagesMutex.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "action": "finished"})
	case "requeue":
		// Parse delay from query parameter (optional, in seconds)
		delayStr := r.URL.Query().Get("delay")
		var delay time.Duration = 0
		if delayStr != "" {
			if d, err := strconv.Atoi(delayStr); err == nil {
				delay = time.Duration(d) * time.Second
			}
		}

		// For requeue with delay, we store the delay time in a separate key
		// and handle it during the next read cycle
		if delay > 0 {
			// Set a delayed requeue using a separate sorted set
			delayedKey := fmt.Sprintf("rs:delayed:%s:%s", msgWithExpiry.stream, msgWithExpiry.group)
			score := float64(time.Now().Add(delay).Unix())
			_, err := redisClient.ZAdd(ctx, delayedKey, redis.Z{
				Score:  score,
				Member: msgWithExpiry.messageID,
			}).Result()
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to schedule delayed requeue: %v", err), http.StatusInternalServerError)
				return
			}
		}

		// Acknowledge the original message first to remove from PEL
		_, err := redisClient.XAck(ctx, msgWithExpiry.stream, msgWithExpiry.group, msgWithExpiry.messageID).Result()
		if err != nil {
			log.Printf("Warning: Failed to acknowledge message for requeue: %v", err)
		}

		// Update consumer stats
		consumersMutex.RLock()
		if consumer, ok := consumers[msgWithExpiry.consumerKey]; ok {
			atomic.AddInt64(&consumer.requeued, 1)
		}
		consumersMutex.RUnlock()
		activeMessagesMutex.Lock()
		delete(activeMessages, messageID)
		activeMessagesMutex.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "action": "requeued"})
	default:
		http.Error(w, "Invalid action. Use: touch, finish, or requeue", http.StatusBadRequest)
	}
}

// handleConsumerEvents handles SSE endpoint for consuming messages
// GET /api/events?stream=<stream>&group=<group>
// Each HTTP client gets its own Redis consumer for load balancing
func handleConsumerEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse query parameters for stream and group
	stream := r.URL.Query().Get("stream")
	group := r.URL.Query().Get("group")

	if stream == "" || group == "" {
		http.Error(w, "Stream and group query parameters are required", http.StatusBadRequest)
		return
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

	// Create a unique consumer ID for this HTTP client
	consumerID := atomic.AddUint64(&consumerIDCounter, 1)
	consumerName := fmt.Sprintf("consumer-%d", consumerID)
	consumerKey := fmt.Sprintf("%s:%s:%d", stream, group, consumerID)

	ctx := context.Background()

	// Create consumer group if it doesn't exist
	err := redisClient.XGroupCreateMkStream(ctx, stream, group, "0").Err()
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
		ready:        1, // Default RDY=1
	}

	// Store consumer for RDY control
	consumersMutex.Lock()
	consumers[consumerKey] = state
	consumersMutex.Unlock()

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

		consumersMutex.Lock()
		delete(consumers, consumerKey)
		consumersMutex.Unlock()
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
				// Check if RDY count allows reading
				if atomic.LoadInt32(&state.ready) <= 0 {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// Read messages from stream
				streams, err := redisClient.XReadGroup(readCtx, &redis.XReadGroupArgs{
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
					log.Printf("Error reading from stream: %v", err)
					time.Sleep(time.Second)
					continue
				}

				for _, s := range streams {
					for _, msg := range s.Messages {
						select {
						case state.messageChan <- msg:
							atomic.AddInt64(&state.received, 1)
						case <-state.stopChan:
							return
						}
					}
				}
			}
		}
	}()

	log.Printf("Consumer %s connected for stream=%s group=%s", consumerKey, stream, group)

	// Setup keepalive ticker if enabled (interval > 0)
	var keepaliveTicker *time.Ticker
	var keepaliveChan <-chan time.Time
	if finalConfig.SSEKeepaliveIntervalSec > 0 {
		keepaliveTicker = time.NewTicker(time.Duration(finalConfig.SSEKeepaliveIntervalSec) * time.Second)
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

			// Store message for lifecycle management
			activeMessagesMutex.Lock()
			activeMessages[msg.ID] = &messageWithExpiry{
				stream:      stream,
				group:       group,
				messageID:   msg.ID,
				consumerKey: consumerKey,
				expiry:      time.Now().Add(messageExpiryDuration),
			}
			activeMessagesMutex.Unlock()

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
				activeMessagesMutex.Lock()
				delete(activeMessages, msg.ID)
				activeMessagesMutex.Unlock()
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

// handleAdmin handles Redis admin operations
func handleAdmin(w http.ResponseWriter, r *http.Request) {
	targetPath := strings.TrimPrefix(r.URL.Path, "/admin/")

	ctx := context.Background()

	switch targetPath {
	case "ping":
		result, err := redisClient.Ping(ctx).Result()
		if err != nil {
			http.Error(w, fmt.Sprintf("Redis ping failed: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": result})

	case "info":
		result, err := redisClient.Info(ctx).Result()
		if err != nil {
			http.Error(w, fmt.Sprintf("Redis info failed: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte(result))

	case "streams":
		// List all streams
		keys, err := redisClient.Keys(ctx, "*").Result()
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to list keys: %v", err), http.StatusInternalServerError)
			return
		}

		var streams []string
		for _, key := range keys {
			keyType, err := redisClient.Type(ctx, key).Result()
			if err == nil && keyType == "stream" {
				streams = append(streams, key)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"streams": streams})

	default:
		// Try to get stream info if path looks like a stream name
		if targetPath != "" {
			info, err := redisClient.XInfoStream(ctx, targetPath).Result()
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to get stream info: %v", err), http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(info)
			return
		}
		http.Error(w, "Unknown admin endpoint", http.StatusNotFound)
	}
}
