package handler

import (
	"io"

	"github.com/kevingruber/gradle-cache/internal/analysis"
	"github.com/kevingruber/gradle-cache/internal/storage"
	"github.com/rs/zerolog"
)

// CacheHandler handles Gradle build cache HTTP requests.
type CacheHandler struct {
	storage      storage.Storage
	maxEntrySize int64
	logger       zerolog.Logger
	metrics      *Metrics
	analyzer     *analysis.Analyzer // nil when static analysis is disabled
}

// NewCacheHandler creates a new cache handler.
// Pass a nil analyzer to disable static analysis.
func NewCacheHandler(store storage.Storage, maxEntrySize int64, logger zerolog.Logger, analyzer *analysis.Analyzer) (*CacheHandler, error) {
	metrics, err := NewMetrics()
	if err != nil {
		return nil, err
	}

	return &CacheHandler{
		storage:      store,
		maxEntrySize: maxEntrySize,
		logger:       logger,
		metrics:      metrics,
		analyzer:     analyzer,
	}, nil
}

// bytesReaderAt implements io.ReaderAt for a byte slice.
type bytesReaderAt struct {
	data []byte
}

func (b *bytesReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(b.data)) {
		return 0, io.EOF
	}
	n = copy(p, b.data[off:])
	if n < len(p) {
		err = io.EOF
	}
	return
}
