package handler

import (
	"github.com/kevingruber/gradle-cache/internal/storage"
	"github.com/rs/zerolog"
)

// BazelHandler handles Bazel HTTP remote cache requests.
// Bazel uses two namespaces: /ac/ (action cache) and /cas/ (content-addressable storage).
type BazelHandler struct {
	acStorage    storage.Storage
	casStorage   storage.Storage
	maxEntrySize int64
	verifyCAS    bool
	logger       zerolog.Logger
	metrics      *BazelMetrics
}

// NewBazelHandler creates a new Bazel cache handler.
// The store must implement NamespacedStorage to isolate AC and CAS keys.
func NewBazelHandler(store storage.NamespacedStorage, maxEntrySize int64, verifyCAS bool, logger zerolog.Logger) (*BazelHandler, error) {
	metrics, err := NewBazelMetrics()
	if err != nil {
		return nil, err
	}

	return &BazelHandler{
		acStorage:    store.WithNamespace("bazel:ac"),
		casStorage:   store.WithNamespace("bazel:cas"),
		maxEntrySize: maxEntrySize,
		verifyCAS:    verifyCAS,
		logger:       logger,
		metrics:      metrics,
	}, nil
}
