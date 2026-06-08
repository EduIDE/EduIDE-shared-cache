package handler

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Put handles PUT requests to store cache entries.
// Gradle expects: 2xx on success, 413 if too large.
// If static analysis is enabled and the artifact is a JAR, forbidden API usage
// causes a 403 and the artifact is not stored.
func (h *CacheHandler) Put(c *gin.Context) {
	key := c.Param("key")
	if key == "" {
		c.Status(http.StatusBadRequest)
		return
	}

	// Reject early if Content-Length already exceeds the limit.
	contentLength := c.Request.ContentLength
	if contentLength > h.maxEntrySize {
		h.logger.Warn().
			Str("key", key).
			Int64("size", contentLength).
			Int64("max_size", h.maxEntrySize).
			Msg("cache entry too large")
		c.Status(http.StatusRequestEntityTooLarge)
		return
	}

	// Always buffer the full body so analysis can inspect it before storage.
	// The +1 lets us detect an over-limit chunked body after reading.
	data, err := io.ReadAll(io.LimitReader(c.Request.Body, h.maxEntrySize+1))
	if err != nil {
		h.logger.Error().Err(err).Str("key", key).Msg("failed to read request body")
		c.Status(http.StatusInternalServerError)
		return
	}

	h.logger.Debug().
		Str("key", key).
		Int("size", len(data)).
		Str("magic", fmt.Sprintf("%x", data[:min(4, len(data))])).
		Msg("artifact received")

	if int64(len(data)) > h.maxEntrySize {
		c.Status(http.StatusRequestEntityTooLarge)
		return
	}

	// Run static analysis when enabled. Analysis errors are non-fatal: a broken
	// parser should not block a legitimate upload, so we log and continue.
	if h.analyzer != nil {
		violations, err := h.analyzer.Check(data)
		if err != nil {
			h.logger.Warn().Err(err).Str("key", key).Msg("static analysis failed, skipping")
		} else if len(violations) > 0 {
			h.logger.Warn().
				Str("key", key).
				Int("violations", len(violations)).
				Msg("rejected artifact: forbidden API usage detected")
			c.JSON(http.StatusForbidden, gin.H{
				"error":      "artifact contains forbidden API usage",
				"violations": violations,
			})
			return
		}
	}

	err = h.storage.Put(c.Request.Context(), key, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		h.logger.Error().Err(err).Str("key", key).Msg("failed to store cache entry")
		c.Status(http.StatusInternalServerError)
		return
	}

	c.Status(http.StatusCreated)
}
