package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/kevingruber/gradle-cache/internal/analysis"
	"github.com/kevingruber/gradle-cache/internal/config"
	"github.com/kevingruber/gradle-cache/internal/handler"
	"github.com/kevingruber/gradle-cache/internal/middleware"
	"github.com/kevingruber/gradle-cache/internal/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

// Server represents the HTTP server.
type Server struct {
	cfg     *config.Config
	router  *gin.Engine
	storage storage.Storage
	logger  zerolog.Logger
	metrics *middleware.Metrics
}

// New creates a new server instance.
func New(cfg *config.Config, store storage.Storage, logger zerolog.Logger) *Server {
	// Set Gin mode based on log level
	if cfg.Logging.Level == "debug" {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	s := &Server{
		cfg:     cfg,
		router:  gin.New(),
		storage: store,
		logger:  logger,
	}

	// Initialize metrics if enabled
	if cfg.Metrics.Enabled {
		metrics, err := middleware.NewMetrics()
		if err != nil {
			logger.Fatal().Err(err).Msg("Failed to initialize metrics")
		}
		s.metrics = metrics
	}

	s.setupRoutes()
	return s
}

// setupRoutes configures all HTTP routes.
func (s *Server) setupRoutes() {
	// Recovery middleware
	s.router.Use(gin.Recovery())

	// Logging middleware
	s.router.Use(middleware.RequestLogger(s.logger))

	// Metrics middleware
	if s.metrics != nil {
		s.router.Use(s.metrics.Middleware())
	}

	// Health endpoints (no auth required)
	s.router.GET("/ping", s.handlePing)
	s.router.GET("/health", s.handleHealth)

	// Metrics endpoint (no auth required)
	if s.cfg.Metrics.Enabled {
		s.router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	}

	// Build an analyzer when static analysis is enabled; nil disables it.
	var analyzer *analysis.Analyzer
	if s.cfg.StaticAnalysis.Enabled {
		analyzer = analysis.New(s.cfg.StaticAnalysis)
		s.logger.Info().
			Bool("check_network", s.cfg.StaticAnalysis.CheckNetwork).
			Bool("check_exec", s.cfg.StaticAnalysis.CheckExec).
			Bool("check_reflection", s.cfg.StaticAnalysis.CheckReflection).
			Bool("check_filesystem", s.cfg.StaticAnalysis.CheckFilesystem).
			Msg("static analysis enabled")
	}

	// All cache keys are namespaced by a server-trusted prefix so that a
	// client can never address another deployment's keyspace. The optional
	// cache.namespace isolates deployments that share one Redis; the "gradle"
	// and "bazel:*" scopes keep the two cache protocols from colliding.
	nsStorage, ok := s.storage.(storage.NamespacedStorage)
	if !ok {
		s.logger.Fatal().Msg("Storage backend does not support namespaces, required for cache isolation")
	}

	base := nsStorage
	if ns := s.cfg.Cache.Namespace; ns != "" {
		// WithNamespace only promises a Storage. Nested scoping (gradle/bazel on
		// top of the deployment namespace) needs a NamespacedStorage, so verify
		// the backend returns one instead of asserting and panicking.
		nested, ok := base.WithNamespace(ns).(storage.NamespacedStorage)
		if !ok {
			s.logger.Fatal().Msg("Storage backend does not support nested namespaces, required for cache isolation")
		}
		base = nested
	}

	cacheHandler, err := handler.NewCacheHandler(
		base.WithNamespace("gradle"),
		s.cfg.MaxEntrySizeBytes(),
		s.logger,
		analyzer,
	)
	if err != nil {
		s.logger.Fatal().Err(err).Msg("Failed to initialize cache handler")
	}

	// Gradle cache endpoints
	gradleGroup := s.router.Group("/gradle")
	gradleGroup.GET("/:key", s.cacheAuth(false), cacheHandler.Get)
	gradleGroup.HEAD("/:key", s.cacheAuth(false), cacheHandler.Head)
	gradleGroup.PUT("/:key", s.cacheAuth(true), cacheHandler.Put)

	// Bazel HTTP remote cache endpoints
	bazelHandler, err := handler.NewBazelHandler(
		base,
		s.cfg.MaxEntrySizeBytes(),
		s.cfg.Cache.VerifyCASHash,
		s.logger,
	)
	if err != nil {
		s.logger.Fatal().Err(err).Msg("Failed to initialize Bazel cache handler")
	}

	bazelGroup := s.router.Group("/bazel")
	bazelGroup.GET("/ac/:hash", s.cacheAuth(false), bazelHandler.GetAC)
	bazelGroup.PUT("/ac/:hash", s.cacheAuth(true), bazelHandler.PutAC)
	bazelGroup.GET("/cas/:hash", s.cacheAuth(false), bazelHandler.GetCAS)
	bazelGroup.PUT("/cas/:hash", s.cacheAuth(true), bazelHandler.PutCAS)
}

func (s *Server) cacheAuth(requireWriter bool) gin.HandlerFunc {
	// Writes must always be authenticated, even when auth.enabled is false.
	// Disabling auth only opens read-only (GET/HEAD) anonymous access; it never
	// permits anonymous PUTs, which could poison caches that other builds execute.
	if !s.cfg.Auth.Enabled && !requireWriter {
		return func(c *gin.Context) {
			c.Next()
		}
	}
	return middleware.CacheAuth(s.cfg.Auth, requireWriter)
}

// handlePing is a simple health check endpoint.
func (s *Server) handlePing(c *gin.Context) {
	c.String(http.StatusOK, "pong")
}

// handleHealth performs a detailed health check including storage connectivity.
func (s *Server) handleHealth(c *gin.Context) {
	ctx := c.Request.Context()

	if err := s.storage.Ping(ctx); err != nil {
		// Log the detailed error internally, but never leak storage internals
		// (addresses, driver error strings) to unauthenticated callers.
		s.logger.Error().Err(err).Msg("health check failed: storage unreachable")
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "unhealthy",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "healthy",
	})
}

// Run starts the HTTP server.
func (s *Server) Run(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.cfg.Server.Port)

	srv := &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  s.cfg.Server.ReadTimeout,
		WriteTimeout: s.cfg.Server.WriteTimeout,
	}

	// Channel to capture server errors
	errCh := make(chan error, 1)

	go func() {
		if s.cfg.Server.TLS.Enabled {
			s.logger.Info().
				Str("addr", addr).
				Str("mode", "https").
				Msg("starting server with TLS")
			if err := srv.ListenAndServeTLS(s.cfg.Server.TLS.CertFile, s.cfg.Server.TLS.KeyFile); err != nil && err != http.ErrServerClosed {
				errCh <- err
			}
		} else {
			s.logger.Info().
				Str("addr", addr).
				Str("mode", "http").
				Msg("starting server")
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				errCh <- err
			}
		}
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		s.logger.Info().Msg("shutting down server")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("server shutdown failed %w", err)

		}
		return nil
	case err := <-errCh:
		return err
	}
}

// Router returns the Gin router for testing purposes.
func (s *Server) Router() *gin.Engine {
	return s.router
}
