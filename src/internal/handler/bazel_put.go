package handler

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"

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

	// Early rejection if Content-Length is known and too large
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

	if verifyHash {
		h.putWithVerify(c, store, hash, cacheType, attrs)
	} else {
		h.putDirect(c, store, hash, cacheType, contentLength, attrs)
	}
}

// putDirect streams the request body to storage without hash verification.
// If Content-Length is known, streams directly. Otherwise spools to a temp file.
func (h *BazelHandler) putDirect(c *gin.Context, store storage.Storage, hash, cacheType string, contentLength int64, attrs metric.MeasurementOption) {
	if contentLength >= 0 {
		// Content-Length known: stream directly to storage
		limited := io.LimitReader(c.Request.Body, contentLength)
		h.metrics.EntrySize.Record(c.Request.Context(), float64(contentLength), attrs)

		if err := store.Put(c.Request.Context(), hash, limited, contentLength); err != nil {
			h.logger.Error().Err(err).Str("hash", hash).Str("cache_type", cacheType).Msg("failed to store bazel cache entry")
			c.Status(http.StatusInternalServerError)
			return
		}
		c.Status(http.StatusOK)
		return
	}

	// Chunked transfer: spool to temp file to determine size
	size, reader, cleanup, err := h.spoolToTempFile(c.Request.Body)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		h.logger.Error().Err(err).Str("hash", hash).Str("cache_type", cacheType).Msg("failed to read request body")
		c.Status(http.StatusInternalServerError)
		return
	}
	if size > h.maxEntrySize {
		c.Status(http.StatusRequestEntityTooLarge)
		return
	}

	h.metrics.EntrySize.Record(c.Request.Context(), float64(size), attrs)

	if err := store.Put(c.Request.Context(), hash, reader, size); err != nil {
		h.logger.Error().Err(err).Str("hash", hash).Str("cache_type", cacheType).Msg("failed to store bazel cache entry")
		c.Status(http.StatusInternalServerError)
		return
	}
	c.Status(http.StatusOK)
}

// putWithVerify spools the upload to a temp file while computing the SHA-256 hash,
// then verifies the hash before storing.
func (h *BazelHandler) putWithVerify(c *gin.Context, store storage.Storage, hash, cacheType string, attrs metric.MeasurementOption) {
	f, err := os.CreateTemp("", "bazel-cas-*")
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to create temp file for CAS verification")
		c.Status(http.StatusInternalServerError)
		return
	}
	defer os.Remove(f.Name())
	defer f.Close()

	hasher := sha256.New()
	limited := io.LimitReader(c.Request.Body, h.maxEntrySize+1)
	tee := io.TeeReader(limited, hasher)

	written, err := io.Copy(f, tee)
	if err != nil {
		h.logger.Error().Err(err).Str("hash", hash).Str("cache_type", cacheType).Msg("failed to read request body")
		c.Status(http.StatusInternalServerError)
		return
	}

	if written > h.maxEntrySize {
		c.Status(http.StatusRequestEntityTooLarge)
		return
	}

	computedHex := hex.EncodeToString(hasher.Sum(nil))
	if computedHex != hash {
		h.metrics.HashMismatches.Add(c.Request.Context(), 1, attrs)
		h.logger.Warn().
			Str("expected", hash).
			Str("computed", computedHex).
			Msg("bazel CAS hash mismatch")
		c.Status(http.StatusBadRequest)
		return
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		h.logger.Error().Err(err).Msg("failed to seek temp file")
		c.Status(http.StatusInternalServerError)
		return
	}

	h.metrics.EntrySize.Record(c.Request.Context(), float64(written), attrs)

	if err := store.Put(c.Request.Context(), hash, f, written); err != nil {
		h.logger.Error().Err(err).Str("hash", hash).Str("cache_type", cacheType).Msg("failed to store bazel cache entry")
		c.Status(http.StatusInternalServerError)
		return
	}
	c.Status(http.StatusOK)
}

// spoolToTempFile copies from r (limited to maxEntrySize+1) into a temp file
// and returns the written size, a reader seeked to start, and a cleanup function.
func (h *BazelHandler) spoolToTempFile(r io.Reader) (int64, io.Reader, func(), error) {
	f, err := os.CreateTemp("", "bazel-spool-*")
	if err != nil {
		return 0, nil, nil, fmt.Errorf("create temp file: %w", err)
	}
	cleanup := func() {
		f.Close()
		os.Remove(f.Name())
	}

	limited := io.LimitReader(r, h.maxEntrySize+1)
	written, err := io.Copy(f, limited)
	if err != nil {
		return 0, nil, cleanup, fmt.Errorf("spool to temp file: %w", err)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, nil, cleanup, fmt.Errorf("seek temp file: %w", err)
	}

	return written, f, cleanup, nil
}
