package handler

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/kevingruber/gradle-cache/internal/storage"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// GetAC handles GET requests for Bazel action cache entries.
func (h *BazelHandler) GetAC(c *gin.Context) {
	h.get(c, h.acStorage, "ac")
}

// GetCAS handles GET requests for Bazel content-addressable storage entries.
func (h *BazelHandler) GetCAS(c *gin.Context) {
	h.get(c, h.casStorage, "cas")
}

func (h *BazelHandler) get(c *gin.Context, store storage.Storage, cacheType string) {
	hash := c.Param("hash")
	if !isValidSHA256Hex(hash) {
		c.Status(http.StatusBadRequest)
		return
	}

	attrs := metric.WithAttributes(attribute.String("cache_type", cacheType))

	reader, size, err := store.Get(c.Request.Context(), hash)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			h.metrics.CacheMisses.Add(c.Request.Context(), 1, attrs)
			c.Status(http.StatusNotFound)
			return
		}
		h.logger.Error().Err(err).Str("hash", hash).Str("cache_type", cacheType).Msg("failed to get bazel cache entry")
		c.Status(http.StatusInternalServerError)
		return
	}
	defer reader.Close()

	h.metrics.CacheHits.Add(c.Request.Context(), 1, attrs)
	c.DataFromReader(http.StatusOK, size, "application/octet-stream", reader, nil)
}
