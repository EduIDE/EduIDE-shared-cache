package handler

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type BazelMetrics struct {
	CacheHits      metric.Int64Counter
	CacheMisses    metric.Int64Counter
	HashMismatches metric.Int64Counter
	EntrySize      metric.Float64Histogram
}

func NewBazelMetrics() (*BazelMetrics, error) {
	meter := otel.Meter("bazel-cache")

	cacheHits, err := meter.Int64Counter(
		"bazel_cache.cache_hits",
		metric.WithDescription("Total number of Bazel cache hits"))
	if err != nil {
		return nil, err
	}

	cacheMisses, err := meter.Int64Counter(
		"bazel_cache.cache_misses",
		metric.WithDescription("Total number of Bazel cache misses"))
	if err != nil {
		return nil, err
	}

	hashMismatches, err := meter.Int64Counter(
		"bazel_cache.hash_mismatches",
		metric.WithDescription("Total number of CAS hash verification failures"))
	if err != nil {
		return nil, err
	}

	entrySize, err := meter.Float64Histogram(
		"bazel_cache.entry_size",
		metric.WithDescription("Size of Bazel cache entries in bytes"))
	if err != nil {
		return nil, err
	}

	return &BazelMetrics{
		CacheHits:      cacheHits,
		CacheMisses:    cacheMisses,
		HashMismatches: hashMismatches,
		EntrySize:      entrySize,
	}, nil
}
