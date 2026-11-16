package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/redis/go-redis/v9"
)

type RedisStorage struct {
	client    *redis.Client
	namespace string
}

type RedisConfig struct {
	Addr     string
	Password string
}

func NewRedisStorage(cfg RedisConfig) (*RedisStorage, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       0,
	})
	storage := &RedisStorage{client: client}

	// Test connection
	if err := storage.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}
	return storage, nil
}

func (s *RedisStorage) redisKey(key string) string {
	if s.namespace == "" {
		return key
	}
	return s.namespace + ":" + key
}

func (s *RedisStorage) Get(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	data, err := s.client.Get(ctx, s.redisKey(key)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, 0, ErrNotFound
		}
		return nil, 0, fmt.Errorf("failed to get key from Redis: %w", err)
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

func (s *RedisStorage) Put(ctx context.Context, key string, reader io.Reader, size int64) error {
	data, err := io.ReadAll(reader)

	if err != nil {
		return fmt.Errorf("failed to read data: %w", err)
	}
	return s.client.Set(ctx, s.redisKey(key), data, 0).Err()
}

func (s *RedisStorage) Exists(ctx context.Context, key string) (bool, error) {
	n, err := s.client.Exists(ctx, s.redisKey(key)).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check key existence in Redis: %w", err)
	}
	return n > 0, nil
}

func (s *RedisStorage) Delete(ctx context.Context, key string) error {
	return s.client.Del(ctx, s.redisKey(key)).Err()
}

func (s *RedisStorage) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

func (s *RedisStorage) WithNamespace(namespace string) Storage {
	// Compose with any existing namespace so callers can nest scopes
	// (e.g. an exercise namespace with a per-cache "gradle"/"bazel:ac" scope).
	ns := namespace
	if s.namespace != "" {
		ns = s.namespace + ":" + namespace
	}
	return &RedisStorage{
		client:    s.client,
		namespace: ns,
	}
}
