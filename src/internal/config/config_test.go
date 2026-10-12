package config

import "testing"

func baseValidConfig() *Config {
	return &Config{
		Storage: StorageConfig{Addr: "localhost:6379"},
		Auth:    AuthConfig{Enabled: false},
	}
}

func TestValidateRejectsNamespaceWithColon(t *testing.T) {
	cfg := baseValidConfig()
	cfg.Cache.Namespace = "ex1:gradle:ex2"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for namespace containing ':', got nil")
	}
}

func TestValidateAllowsNamespaceWithoutColon(t *testing.T) {
	cfg := baseValidConfig()
	cfg.Cache.Namespace = "ex1"

	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error for valid namespace: %v", err)
	}
}
