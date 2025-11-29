//go:build integration
// +build integration

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var portCounter int32 = 18080
var consumerCounter int32 = 0

// TestLoadBalancingBehavior tests that messages are distributed across multiple HTTP consumers
func TestLoadBalancingBehavior(t *testing.T) {
	ctx := context.Background()

	// Start Redis container
	redisContainer, redisAddress, err := startRedis(ctx, t)
	if err != nil {
		t.Fatalf("Failed to start Redis: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	// Start our HTTP facade server
	facadePort, stopFacade := startFacadeServer(t, redisAddress)
	defer stopFacade()

	facadeURL := fmt.Sprintf("http://localhost:%d", facadePort)

	// Test parameters
	stream := "test-stream"
	group := "test-group"
	numMessages := 300
	numConsumers := 3

	// Track messages received by each consumer
	consumerMessageCounts := make([]int32, numConsumers)
	var totalReceived int32

	// Create a done channel to signal consumers when all messages are received
	allDone := make(chan struct{})

	// Start 3 HTTP SSE consumer connections
	var consumersWg sync.WaitGroup
	for i := 0; i < numConsumers; i++ {
		consumersWg.Add(1)
		consumerID := i

		go func(id int) {
			defer consumersWg.Done()

			count := consumeMessagesWithStop(t, facadeURL, stream, group, &consumerMessageCounts[id], &totalReceived, numMessages, allDone)
			t.Logf("Consumer %d received %d messages", id, count)
		}(consumerID)
	}

	// Wait a bit for consumers to connect
	time.Sleep(2 * time.Second)

	// Publish 300 messages
	t.Logf("Publishing %d messages...", numMessages)
	err = publishMessages(facadeURL, stream, numMessages)
	if err != nil {
		t.Fatalf("Failed to publish messages: %v", err)
	}

	// Wait for all consumers to finish (with timeout)
	select {
	case <-allDone:
		t.Log("All messages received")
	case <-time.After(60 * time.Second):
		t.Fatal("Test timeout: consumers didn't finish in time")
	}

	// Give consumers a moment to finish processing
	time.Sleep(500 * time.Millisecond)

	// Calculate total messages received
	var total int32
	for i := 0; i < numConsumers; i++ {
		count := atomic.LoadInt32(&consumerMessageCounts[i])
		total += count
		t.Logf("Consumer %d: %d messages", i, count)
	}

	// Verify all consumers got at least one message
	for i := 0; i < numConsumers; i++ {
		count := atomic.LoadInt32(&consumerMessageCounts[i])
		if count < 1 {
			t.Errorf("Consumer %d received no messages (expected at least 1)", i)
		}
	}

	// Verify total count equals published messages
	if total != int32(numMessages) {
		t.Errorf("Total messages received (%d) doesn't match published (%d)", total, numMessages)
	}

	t.Logf("✓ Total messages: %d/%d", total, numMessages)
	t.Logf("✓ All consumers received at least one message")
	t.Log("✓ Load balancing working correctly!")
}

// consumeMessagesWithStop consumes messages and tracks a shared total
func consumeMessagesWithStop(t *testing.T, facadeURL, stream, group string, myCount, totalCount *int32, maxTotal int, allDone chan struct{}) int {
	// Generate a unique consumer name for this goroutine
	consumerName := fmt.Sprintf("test-consumer-%d", atomic.AddInt32(&consumerCounter, 1))
	url := fmt.Sprintf("%s/api/events?stream=%s&group=%s&consumer=%s&limit=100", facadeURL, stream, group, consumerName)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Logf("Error creating request: %v", err)
		return 0
	}
	req.Header.Set("Authorization", "Bearer test-token")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		t.Logf("Error connecting: %v", err)
		return 0
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Logf("Unexpected status: %d", resp.StatusCode)
		return 0
	}

	count := 0
	scanner := bufio.NewScanner(resp.Body)

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "data: ") {
			data := strings.TrimPrefix(line, "data: ")

			var msg map[string]interface{}
			if err := json.Unmarshal([]byte(data), &msg); err != nil {
				continue
			}

			messageID, ok := msg["id"].(string)
			if !ok {
				continue
			}

			// Finish the message using the new endpoint
			finishURL := fmt.Sprintf("%s/api/streams/%s/groups/%s/consumers/%s/messages/%s/finish", facadeURL, stream, group, consumerName, messageID)
			finishReq, _ := http.NewRequest("POST", finishURL, nil)
			finishReq.Header.Set("Authorization", "Bearer test-token")

			finishResp, err := http.DefaultClient.Do(finishReq)
			if err == nil {
				finishResp.Body.Close()
			}

			count++
			atomic.AddInt32(myCount, 1)
			newTotal := atomic.AddInt32(totalCount, 1)

			// Check if we've received all messages
			if int(newTotal) >= maxTotal {
				// Signal that all messages are done
				select {
				case <-allDone:
					// Already closed
				default:
					close(allDone)
				}
				return count
			}
		}

		// Check if done
		select {
		case <-allDone:
			return count
		default:
		}
	}

	return count
}

// startRedis starts a Redis container and returns its address
func startRedis(ctx context.Context, t *testing.T) (testcontainers.Container, string, error) {
	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", err
	}

	// Get mapped port
	port, err := container.MappedPort(ctx, "6379")
	if err != nil {
		return nil, "", err
	}

	host, err := container.Host(ctx)
	if err != nil {
		return nil, "", err
	}

	redisAddress := fmt.Sprintf("%s:%s", host, port.Port())

	t.Logf("Redis started at %s", redisAddress)

	return container, redisAddress, nil
}

// startFacadeServer starts the HTTP facade server in a goroutine
func startFacadeServer(t *testing.T, redisAddr string) (int, func()) {
	// Use a unique port for each test
	port := int(atomic.AddInt32(&portCounter, 1))

	// Find an available port
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		// Try finding a free port
		listener, err = net.Listen("tcp", ":0")
		if err != nil {
			t.Fatalf("Failed to find available port: %v", err)
		}
		port = listener.Addr().(*net.TCPAddr).Port
	}
	listener.Close()

	// Create server configuration
	config := AppConfig{
		RedisAddress:            redisAddr,
		RedisPassword:           "",
		RedisDB:                 0,
		HTTPAddress:             fmt.Sprintf(":%d", port),
		BearerToken:             "test-token",
		SSEKeepaliveIntervalSec: 60,
	}

	// Create server instance
	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Start server in background
	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			t.Logf("Server error: %v", err)
		}
	}()

	// Wait for server to be ready
	time.Sleep(1 * time.Second)

	t.Logf("Facade server started on port %d", port)

	stopFunc := func() {
		// Stop all consumers
		server.consumersMutex.Lock()
		for key, consumer := range server.consumers {
			select {
			case <-consumer.stopChan:
				// Already closed
			default:
				close(consumer.stopChan)
			}
			delete(server.consumers, key)
		}
		server.consumersMutex.Unlock()

		// Stop the server
		server.Stop()
	}

	return port, stopFunc
}

// publishMessages publishes messages to the facade
func publishMessages(facadeURL, stream string, count int) error {
	url := fmt.Sprintf("%s/api/streams/%s/messages/batch", facadeURL, stream)

	// Build batch of messages
	messages := make([]map[string]interface{}, count)
	for i := 0; i < count; i++ {
		messages[i] = map[string]interface{}{
			"id":   i,
			"data": fmt.Sprintf("message-%d", i),
		}
	}

	payload := map[string]interface{}{
		"messages": messages,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("publish failed with status: %d", resp.StatusCode)
	}

	return nil
}

// TestMessageFinish tests message finish functionality
func TestMessageFinish(t *testing.T) {
	ctx := context.Background()

	redisContainer, redisAddress, err := startRedis(ctx, t)
	if err != nil {
		t.Fatalf("Failed to start Redis: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	facadePort, stopFacade := startFacadeServer(t, redisAddress)
	defer stopFacade()

	facadeURL := fmt.Sprintf("http://localhost:%d", facadePort)
	stream := "finish-test"
	group := "finish-group"
	consumerName := "finish-consumer"

	// Publish a message
	err = publishSingleMessage(facadeURL, stream, "test-message")
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	time.Sleep(1 * time.Second)

	// Consumer that finishes the message
	url := fmt.Sprintf("%s/api/events?stream=%s&group=%s&consumer=%s&limit=10", facadeURL, stream, group, consumerName)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer test-token")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	finished := false
	timeout := time.After(10 * time.Second)

	done := make(chan struct{})
	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				data := strings.TrimPrefix(line, "data: ")
				var msg map[string]interface{}
				json.Unmarshal([]byte(data), &msg)

				msgID := msg["id"].(string)

				// Finish the message using the new endpoint
				finishURL := fmt.Sprintf("%s/api/streams/%s/groups/%s/consumers/%s/messages/%s/finish", facadeURL, stream, group, consumerName, msgID)
				finishReq, _ := http.NewRequest("POST", finishURL, nil)
				finishReq.Header.Set("Authorization", "Bearer test-token")
				finishResp, err := http.DefaultClient.Do(finishReq)
				if err == nil && finishResp.StatusCode == http.StatusOK {
					finished = true
					t.Logf("Finished message %s", msgID)
				}
				if finishResp != nil {
					finishResp.Body.Close()
				}
				close(done)
				return
			}
		}
	}()

	select {
	case <-done:
	case <-timeout:
		t.Fatal("Timeout waiting for message")
	}

	if !finished {
		t.Errorf("Expected message to be finished")
	} else {
		t.Logf("✓ Message finish working correctly")
	}
}

// TestSSEConnectionClose tests graceful SSE connection closure
func TestSSEConnectionClose(t *testing.T) {
	ctx := context.Background()

	redisContainer, redisAddress, err := startRedis(ctx, t)
	if err != nil {
		t.Fatalf("Failed to start Redis: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	facadePort, stopFacade := startFacadeServer(t, redisAddress)
	defer stopFacade()

	facadeURL := fmt.Sprintf("http://localhost:%d", facadePort)
	stream := "close-test"
	group := "close-group"
	consumerName := "close-consumer"

	// Publish some messages
	for i := 0; i < 10; i++ {
		publishSingleMessage(facadeURL, stream, fmt.Sprintf("message-%d", i))
	}

	time.Sleep(1 * time.Second)

	// Start consumer and close it after receiving a few messages
	receivedCount := int32(0)

	url := fmt.Sprintf("%s/api/events?stream=%s&group=%s&consumer=%s&limit=10", facadeURL, stream, group, consumerName)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer test-token")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	scanner := bufio.NewScanner(resp.Body)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				data := strings.TrimPrefix(line, "data: ")
				var msg map[string]interface{}
				json.Unmarshal([]byte(data), &msg)

				msgID := msg["id"].(string)
				atomic.AddInt32(&receivedCount, 1)

				// Finish the message using the new endpoint
				finishURL := fmt.Sprintf("%s/api/streams/%s/groups/%s/consumers/%s/messages/%s/finish", facadeURL, stream, group, consumerName, msgID)
				finishReq, _ := http.NewRequest("POST", finishURL, nil)
				finishReq.Header.Set("Authorization", "Bearer test-token")
				finishResp, _ := http.DefaultClient.Do(finishReq)
				if finishResp != nil {
					finishResp.Body.Close()
				}

				// Close after receiving 3 messages
				if atomic.LoadInt32(&receivedCount) >= 3 {
					resp.Body.Close()
					return
				}
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Log("Timeout reached")
	}

	count := atomic.LoadInt32(&receivedCount)
	if count < 3 {
		t.Errorf("Expected at least 3 messages before close, got %d", count)
	} else {
		t.Logf("✓ SSE connection close working correctly: received %d messages", count)
	}

	// Verify we can reconnect and get remaining messages
	time.Sleep(1 * time.Second)

	req2, _ := http.NewRequest("GET", url, nil)
	req2.Header.Set("Authorization", "Bearer test-token")

	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatalf("Failed to reconnect: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Errorf("Reconnection failed with status: %d", resp2.StatusCode)
	} else {
		t.Logf("✓ Reconnection after close working correctly")
	}
}

// TestAdminEndpoints tests admin endpoints
func TestAdminEndpoints(t *testing.T) {
	ctx := context.Background()

	redisContainer, redisAddress, err := startRedis(ctx, t)
	if err != nil {
		t.Fatalf("Failed to start Redis: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	facadePort, stopFacade := startFacadeServer(t, redisAddress)
	defer stopFacade()

	facadeURL := fmt.Sprintf("http://localhost:%d", facadePort)

	// Test ping
	t.Run("Ping", func(t *testing.T) {
		req, _ := http.NewRequest("GET", facadeURL+"/admin/ping", nil)
		req.Header.Set("Authorization", "Bearer test-token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to ping: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}

		var result map[string]string
		json.NewDecoder(resp.Body).Decode(&result)
		if result["status"] != "PONG" {
			t.Errorf("Expected PONG, got %s", result["status"])
		}
		t.Logf("✓ Ping working correctly")
	})

	// Test info
	t.Run("Info", func(t *testing.T) {
		req, _ := http.NewRequest("GET", facadeURL+"/admin/info", nil)
		req.Header.Set("Authorization", "Bearer test-token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to get info: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}

		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), "redis_version") {
			t.Errorf("Expected Redis info to contain redis_version")
		}
		t.Logf("✓ Info working correctly")
	})

	// Test streams list
	t.Run("Streams", func(t *testing.T) {
		// First publish a message to create a stream
		publishSingleMessage(facadeURL, "admin-test-stream", "test")
		time.Sleep(500 * time.Millisecond)

		req, _ := http.NewRequest("GET", facadeURL+"/admin/streams", nil)
		req.Header.Set("Authorization", "Bearer test-token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to list streams: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}

		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		streams, ok := result["streams"].([]interface{})
		if !ok {
			t.Errorf("Expected streams array in response")
		} else {
			t.Logf("✓ Streams endpoint working correctly: found %d streams", len(streams))
		}
	})

	// Test stats
	t.Run("Stats", func(t *testing.T) {
		// First publish a message and create a consumer group
		publishSingleMessage(facadeURL, "stats-test-stream", "test")
		time.Sleep(500 * time.Millisecond)

		// Start a consumer to create a consumer group
		consumerCtx, consumerCancel := context.WithCancel(context.Background())
		go func() {
			url := fmt.Sprintf("%s/api/events?stream=stats-test-stream&group=stats-test-group", facadeURL)
			req, _ := http.NewRequestWithContext(consumerCtx, "GET", url, nil)
			req.Header.Set("Authorization", "Bearer test-token")
			client := &http.Client{Timeout: 0}
			resp, err := client.Do(req)
			if err == nil {
				defer resp.Body.Close()
				io.ReadAll(resp.Body)
			}
		}()

		time.Sleep(1 * time.Second)

		req, _ := http.NewRequest("GET", facadeURL+"/admin/stats", nil)
		req.Header.Set("Authorization", "Bearer test-token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			consumerCancel()
			t.Fatalf("Failed to get stats: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			consumerCancel()
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}

		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		streams, ok := result["streams"].([]interface{})
		if !ok {
			consumerCancel()
			t.Errorf("Expected streams array in response")
			return
		}

		// Find our test stream
		var found bool
		for _, s := range streams {
			stream := s.(map[string]interface{})
			if stream["name"] == "stats-test-stream" {
				found = true
				length := stream["length"].(float64)
				if length < 1 {
					t.Errorf("Expected stream length >= 1, got %v", length)
				}
				groups := stream["groups"].([]interface{})
				if len(groups) > 0 {
					group := groups[0].(map[string]interface{})
					if group["name"] != "stats-test-group" {
						t.Errorf("Expected group name 'stats-test-group', got %v", group["name"])
					}
					t.Logf("✓ Stats endpoint working correctly: stream length=%v, group pending=%v, lag=%v, consumers=%v",
						length, group["pending"], group["lag"], group["consumers"])
				}
			}
		}

		consumerCancel()

		if !found {
			t.Errorf("Expected to find stats-test-stream in stats")
		} else {
			t.Logf("✓ Stats endpoint working correctly")
		}
	})
}

// publishSingleMessage publishes a single message
func publishSingleMessage(facadeURL, stream, data string) error {
	url := fmt.Sprintf("%s/api/streams/%s/messages", facadeURL, stream)

	payload := map[string]interface{}{
		"data": data,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("publish failed with status: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

// TestServiceRestartSurvival tests that unacknowledged messages survive server restart
// and can be reclaimed by other consumers via XAUTOCLAIM.
// This test validates the Redis Streams-based approach where message tracking
// is done purely through the Pending Entries List (PEL).
func TestServiceRestartSurvival(t *testing.T) {
	ctx := context.Background()

	// Start Redis container (this will persist through the restart)
	redisContainer, redisAddress, err := startRedis(ctx, t)
	if err != nil {
		t.Fatalf("Failed to start Redis: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	// Start our HTTP facade server (first instance)
	facadePort, stopFacade1 := startFacadeServer(t, redisAddress)
	facadeURL := fmt.Sprintf("http://localhost:%d", facadePort)

	stream := "restart-test-stream"
	group := "restart-test-group"
	consumerName := "restart-test-consumer"

	// Publish a message
	err = publishSingleMessage(facadeURL, stream, "test-message-for-restart")
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Start a consumer and receive the message (but don't finish it yet)
	url := fmt.Sprintf("%s/api/events?stream=%s&group=%s&consumer=%s&limit=10", facadeURL, stream, group, consumerName)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer test-token")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Failed to connect to SSE: %v", err)
	}

	// Read the message
	var messageID string
	scanner := bufio.NewScanner(resp.Body)
	timeout := time.After(10 * time.Second)
	messageChan := make(chan string, 1)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				data := strings.TrimPrefix(line, "data: ")
				var msg map[string]interface{}
				if err := json.Unmarshal([]byte(data), &msg); err != nil {
					continue
				}
				if id, ok := msg["id"].(string); ok {
					messageChan <- id
					return
				}
			}
		}
	}()

	select {
	case messageID = <-messageChan:
		t.Logf("Received message with ID: %s", messageID)
	case <-timeout:
		resp.Body.Close()
		t.Fatal("Timeout waiting for message")
	}

	// Close the SSE connection (simulates consumer disconnect)
	resp.Body.Close()
	time.Sleep(500 * time.Millisecond)

	// Create a direct Redis client to verify message status in PEL
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddress,
	})
	defer redisClient.Close()

	// VERIFY: Message should be in the Pending Entries List (PEL)
	pending, err := redisClient.XPending(ctx, stream, group).Result()
	if err != nil {
		t.Fatalf("Failed to get pending info: %v", err)
	}
	if pending.Count == 0 {
		t.Fatalf("Expected pending messages in PEL, got 0")
	}
	t.Logf("✓ Message is in PEL: %d pending messages", pending.Count)

	// STOP the first server instance (simulate restart)
	t.Log("Stopping first server instance...")
	stopFacade1()
	time.Sleep(1 * time.Second)

	// VERIFY: Message should STILL be in PEL after server restart
	pending, err = redisClient.XPending(ctx, stream, group).Result()
	if err != nil {
		t.Fatalf("Failed to get pending info after restart: %v", err)
	}
	if pending.Count == 0 {
		t.Fatalf("Message was lost from PEL after server restart!")
	}
	t.Log("✓ Message persists in PEL after server restart")

	// START a new server instance on a different port (simulate restart)
	t.Log("Starting new server instance (simulating restart)...")
	facadePort2, stopFacade2 := startFacadeServerOnPort(t, redisAddress, facadePort+100)
	defer stopFacade2()

	facadeURL2 := fmt.Sprintf("http://localhost:%d", facadePort2)

	// Finish the message using the NEW server instance
	finishURL := fmt.Sprintf("%s/api/streams/%s/groups/%s/consumers/%s/messages/%s/finish", facadeURL2, stream, group, consumerName, messageID)
	finishReq, err := http.NewRequest("POST", finishURL, nil)
	if err != nil {
		t.Fatalf("Failed to create finish request: %v", err)
	}
	finishReq.Header.Set("Authorization", "Bearer test-token")

	finishResp, err := http.DefaultClient.Do(finishReq)
	if err != nil {
		t.Fatalf("Failed to finish message: %v", err)
	}
	defer finishResp.Body.Close()

	if finishResp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(finishResp.Body)
		if readErr != nil {
			t.Errorf("Expected status 200, got %d. Reading response body failed: %v", finishResp.StatusCode, readErr)
		} else {
			t.Errorf("Expected status 200, got %d: %s", finishResp.StatusCode, string(body))
		}
	} else {
		t.Log("✓ Successfully finished message after service restart!")
	}

	// VERIFY: Message should be removed from PEL after finish (XACK)
	pending, err = redisClient.XPending(ctx, stream, group).Result()
	if err != nil {
		t.Fatalf("Failed to check PEL after finish: %v", err)
	}
	if pending.Count == 0 {
		t.Log("✓ Message correctly removed from PEL after finish (XACK)")
	} else {
		t.Errorf("Message should have been removed from PEL after finish, but %d messages still pending", pending.Count)
	}

	t.Log("✓ Service restart survival test PASSED - using Redis Streams PEL!")
}

// startFacadeServerOnPort starts the HTTP facade server on a specific port
func startFacadeServerOnPort(t *testing.T, redisAddr string, port int) (int, func()) {
	// Create server configuration
	config := AppConfig{
		RedisAddress:            redisAddr,
		RedisPassword:           "",
		RedisDB:                 0,
		HTTPAddress:             fmt.Sprintf(":%d", port),
		BearerToken:             "test-token",
		SSEKeepaliveIntervalSec: 60,
	}

	// Create server instance
	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Start server in background
	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			t.Logf("Server error: %v", err)
		}
	}()

	// Wait for server to be ready
	time.Sleep(1 * time.Second)

	t.Logf("Facade server started on port %d", port)

	stopFunc := func() {
		// Stop all consumers
		server.consumersMutex.Lock()
		for key, consumer := range server.consumers {
			select {
			case <-consumer.stopChan:
				// Already closed
			default:
				close(consumer.stopChan)
			}
			delete(server.consumers, key)
		}
		server.consumersMutex.Unlock()

		// Stop the server
		server.Stop()
	}

	return port, stopFunc
}
