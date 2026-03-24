package handler

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/kevingruber/gradle-cache/internal/storage"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// PutAC handles PUT requests to store Bazel action cache entries.
func (h *BazelHandler) PutAC(c *gin.Context) {
	h.put(c, h.acStorage, "ac", false)
}

// PutCAS handles PUT requests to store Bazel CAS entries.
// If verifyCAS is enabled, the content hash is verified against the URL hash.
func (h *BazelHandler) PutCAS(c *gin.Context) {
	h.put(c, h.casStorage, "cas", h.verifyCAS)
}

func (h *BazelHandler) put(c *gin.Context, store storage.Storage, cacheType string, verifyHash bool) {
	hash := c.Param("hash")
	if !isValidSHA256Hex(hash) {
		c.Status(http.StatusBadRequest)
		return
	}

	attrs := metric.WithAttributes(attribute.String("cache_type", cacheType))

	// Check Content-Length for size validation
	contentLength := c.Request.ContentLength
	if contentLength > h.maxEntrySize {
		h.logger.Warn().
			Str("hash", hash).
			Str("cache_type", cacheType).
			Int64("size", contentLength).
			Int64("max_size", h.maxEntrySize).
			Msg("bazel cache entry too large")
		c.Status(http.StatusRequestEntityTooLarge)
		return
	}

	// Read the body (needed for both hash verification and chunked transfers)
	limitedReader := io.LimitReader(c.Request.Body, h.maxEntrySize+1)
	data, err := io.ReadAll(limitedReader)
	if err != nil {
		h.logger.Error().Err(err).Str("hash", hash).Str("cache_type", cacheType).Msg("failed to read request body")
		c.Status(http.StatusInternalServerError)
		return
	}

	if int64(len(data)) > h.maxEntrySize {
		c.Status(http.StatusRequestEntityTooLarge)
		return
	}

	// Verify content hash for CAS entries
	if verifyHash {
		computed := sha256.Sum256(data)
		computedHex := hex.EncodeToString(computed[:])
		if computedHex != hash {
			h.metrics.HashMismatches.Add(c.Request.Context(), 1, attrs)
			h.logger.Warn().
				Str("expected", hash).
				Str("computed", computedHex).
				Msg("bazel CAS hash mismatch")
			c.Status(http.StatusBadRequest)
			return
		}
	}

	contentLength = int64(len(data))
	h.metrics.EntrySize.Record(c.Request.Context(), float64(contentLength), attrs)

	err = store.Put(c.Request.Context(), hash, bytes.NewReader(data), contentLength)
	if err != nil {
		h.logger.Error().Err(err).Str("hash", hash).Str("cache_type", cacheType).Msg("failed to store bazel cache entry")
		c.Status(http.StatusInternalServerError)
		return
	}

	// Bazel expects 200 OK on successful PUT (not 201 Created like Gradle)
	c.Status(http.StatusOK)
}
