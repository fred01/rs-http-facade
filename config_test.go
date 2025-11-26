package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigFile(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.toml")
	content := []byte(`
redis_address = "file:6379"
redis_password = "file-password"
redis_db = 1
http_address = ":8081"
bearer_token = "file-token"
`)

	if err := os.WriteFile(configPath, content, 0o600); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, loaded, err := loadConfigFile(configPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !loaded {
		t.Fatalf("expected config to be loaded")
	}

	if cfg.RedisAddress != "file:6379" || cfg.RedisPassword != "file-password" || cfg.RedisDB != 1 || cfg.HTTPAddress != ":8081" || cfg.BearerToken != "file-token" {
		t.Fatalf("unexpected config loaded: %+v", cfg)
	}

	_, loaded, err = loadConfigFile(filepath.Join(tmpDir, "missing.toml"))
	if err != nil {
		t.Fatalf("unexpected error for missing file: %v", err)
	}

	if loaded {
		t.Fatalf("expected missing config file to be skipped")
	}

	malformedPath := filepath.Join(tmpDir, "malformed.toml")
	malformedContent := []byte(`redis_address = "file:6379"
[[`)
	if err := os.WriteFile(malformedPath, malformedContent, 0o600); err != nil {
		t.Fatalf("failed to write malformed config file: %v", err)
	}

	_, loaded, err = loadConfigFile(malformedPath)
	if err == nil {
		t.Fatalf("expected error for malformed config file")
	}

	if loaded {
		t.Fatalf("expected malformed config file to be treated as not loaded")
	}
}

func TestConfigPrecedence(t *testing.T) {
	cfg := AppConfig{}

	mergeConfig(&cfg, AppConfig{
		RedisAddress:  "file:6379",
		RedisPassword: "file-password",
		RedisDB:       1,
		HTTPAddress:   "file:8081",
		BearerToken:   "file-token",
	})

	t.Setenv("RS_HTTP_FACADE_REDIS_ADDRESS", "env:6379")
	t.Setenv("RS_HTTP_FACADE_BEARER_TOKEN", "env-token")
	applyEnvOverrides(&cfg)

	visited := map[string]bool{
		"http-address": true,
		"bearer-token": true,
	}

	originalHTTPAddress := *httpAddress
	originalBearerToken := *bearerToken
	defer func() {
		*httpAddress = originalHTTPAddress
		*bearerToken = originalBearerToken
	}()

	*httpAddress = "cli:8080"
	*bearerToken = "cli-token"

	applyCLIOverrides(&cfg, visited)

	if cfg.RedisAddress != "env:6379" {
		t.Fatalf("expected env override to win for RedisAddress, got %s", cfg.RedisAddress)
	}

	if cfg.HTTPAddress != "cli:8080" {
		t.Fatalf("expected CLI override to win for HTTPAddress, got %s", cfg.HTTPAddress)
	}

	if cfg.BearerToken != "cli-token" {
		t.Fatalf("expected CLI override to win for BearerToken, got %s", cfg.BearerToken)
	}

	if cfg.RedisPassword != "file-password" {
		t.Fatalf("expected RedisPassword to remain configured, got %s", cfg.RedisPassword)
	}
}

func TestValidateConfig(t *testing.T) {
	err := validateConfig(AppConfig{})
	if err == nil || err.Error() != "missing required configuration values: redis_address, http_address, bearer_token" {
		t.Fatalf("expected aggregated missing parameters error, got %v", err)
	}

	cfg := AppConfig{
		RedisAddress: "r:6379",
		HTTPAddress:  "r:8080",
		BearerToken:  "token",
	}

	if err := validateConfig(cfg); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}
}

func TestKeepaliveIntervalConfig(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("Load sse_keepalive_interval_sec from config file", func(t *testing.T) {
		configPath := filepath.Join(tmpDir, "config_keepalive.toml")
		content := []byte(`
redis_address = "file:6379"
http_address = ":8081"
bearer_token = "file-token"
sse_keepalive_interval_sec = 30
`)
		if err := os.WriteFile(configPath, content, 0o600); err != nil {
			t.Fatalf("failed to write config file: %v", err)
		}

		cfg, loaded, err := loadConfigFile(configPath)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !loaded {
			t.Fatalf("expected config to be loaded")
		}

		if cfg.SSEKeepaliveIntervalSec != 30 {
			t.Fatalf("expected sse_keepalive_interval_sec 30, got %d", cfg.SSEKeepaliveIntervalSec)
		}
	})

	t.Run("mergeConfig with sse_keepalive_interval_sec", func(t *testing.T) {
		base := AppConfig{}
		mergeConfig(&base, AppConfig{SSEKeepaliveIntervalSec: 45})

		if base.SSEKeepaliveIntervalSec != 45 {
			t.Fatalf("expected sse_keepalive_interval_sec 45, got %d", base.SSEKeepaliveIntervalSec)
		}

		// mergeConfig should not override with zero value
		mergeConfig(&base, AppConfig{SSEKeepaliveIntervalSec: 0})
		if base.SSEKeepaliveIntervalSec != 45 {
			t.Fatalf("expected sse_keepalive_interval_sec to remain 45, got %d", base.SSEKeepaliveIntervalSec)
		}
	})

	t.Run("applyEnvOverrides with sse_keepalive_interval_sec", func(t *testing.T) {
		cfg := AppConfig{}
		t.Setenv("RS_HTTP_FACADE_SSE_KEEPALIVE_INTERVAL_SEC", "120")
		applyEnvOverrides(&cfg)

		if cfg.SSEKeepaliveIntervalSec != 120 {
			t.Fatalf("expected sse_keepalive_interval_sec 120 from env, got %d", cfg.SSEKeepaliveIntervalSec)
		}
	})

	t.Run("applyCLIOverrides with sse_keepalive_interval_sec", func(t *testing.T) {
		cfg := AppConfig{SSEKeepaliveIntervalSec: 60}

		originalSSEKeepaliveIntervalSec := *sseKeepaliveIntervalSec
		defer func() {
			*sseKeepaliveIntervalSec = originalSSEKeepaliveIntervalSec
		}()

		*sseKeepaliveIntervalSec = 90
		visited := map[string]bool{"sse-keepalive-interval-sec": true}

		applyCLIOverrides(&cfg, visited)

		if cfg.SSEKeepaliveIntervalSec != 90 {
			t.Fatalf("expected sse_keepalive_interval_sec 90 from CLI, got %d", cfg.SSEKeepaliveIntervalSec)
		}
	})
}
