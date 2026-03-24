package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server  ServerConfig  `mapstructure:"server"`
	Storage StorageConfig `mapstructure:"storage"`
	Cache   CacheConfig   `mapstructure:"cache"`
	Auth    AuthConfig    `mapstructure:"auth"`
	Metrics MetricsConfig `mapstructure:"metrics"`
	Logging LoggingConfig `mapstructure:"logging"`
	Sentry  SentryConfig  `mapstructure:"sentry"`
}

type ServerConfig struct {
	Port         int           `mapstructure:"port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	TLS          TLSConfig     `mapstructure:"tls"`
}

type TLSConfig struct {
	Enabled  bool   `mapstructure:"enabled"`
	CertFile string `mapstructure:"cert_file"`
	KeyFile  string `mapstructure:"key_file"`
}

type StorageConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

type CacheConfig struct {
	MaxEntrySizeMB int64 `mapstructure:"max_entry_size_mb"`
	VerifyCASHash  bool  `mapstructure:"verify_cas_hash"`
}

type AuthConfig struct {
	Enabled bool     `mapstructure:"enabled"`
	Reader  UserAuth `mapstructure:"reader"`
	Writer  UserAuth `mapstructure:"writer"`
}

type UserAuth struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type MetricsConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

type SentryConfig struct {
	Dsn     string `mapstructure:"dsn"`
	Enabled bool   `mapstructure:"enabled"`
}

func Load(configPath string) (*Config, error) {
	v := viper.New()

	// Set defaults
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout", "30s")
	v.SetDefault("server.write_timeout", "120s")
	v.SetDefault("server.tls.enabled", false)
	v.SetDefault("server.tls.cert_file", "/etc/certs/tls.crt")
	v.SetDefault("server.tls.key_file", "/etc/certs/tls.key")

	v.SetDefault("storage.addr", "localhost:6379")
	v.SetDefault("storage.password", "")
	v.SetDefault("storage.db", 0)

	v.SetDefault("cache.max_entry_size_mb", 100)
	v.SetDefault("cache.verify_cas_hash", true)

	v.SetDefault("auth.enabled", true)

	v.SetDefault("metrics.enabled", true)

	v.SetDefault("sentry.enabled", false)

	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")

	// Read from config file if provided
	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	// Enable environment variable overrides
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Bind specific environment variables
	v.BindEnv("storage.password", "REDIS_PASSWORD")

	v.BindEnv("auth.reader.password", "CACHE_READER_PASSWORD")
	v.BindEnv("auth.writer.password", "CACHE_WRITER_PASSWORD")

	v.BindEnv("sentry.dsn", "SENTRY_DSN")

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}

func (c *Config) Validate() error {
	if c.Storage.Addr == "" {
		return fmt.Errorf("storage.addr is required")
	}
	if c.Auth.Enabled {
		if c.Auth.Reader.Username == "" || c.Auth.Reader.Password == "" {
			return fmt.Errorf("auth.reader.username and auth.reader.password are required when auth is enabled")
		}
		if c.Auth.Writer.Username == "" || c.Auth.Writer.Password == "" {
			return fmt.Errorf("auth.writer.username and auth.writer.password are required when auth is enabled")
		}
	}
	if c.Server.TLS.Enabled {
		if c.Server.TLS.CertFile == "" {
			return fmt.Errorf("server.tls.cert_file is required when TLS is enabled")
		}
		if c.Server.TLS.KeyFile == "" {
			return fmt.Errorf("server.tls.key_file is required when TLS is enabled")
		}
	}
	return nil
}

func (c *Config) MaxEntrySizeBytes() int64 {
	return c.Cache.MaxEntrySizeMB * 1024 * 1024
}

