# Gradle Build Cache Server

A high-performance, Kubernetes-native HTTP build cache server for Gradle builds. Built with Go and backed by Redis for fast, in-memory storage.

## Overview

This project provides a lightweight, self-hosted Gradle Build Cache server that can be deployed in Kubernetes clusters to accelerate build times across teams and CI/CD pipelines. It implements the Gradle HTTP Build Cache API and uses Redis as an in-memory storage backend.

### Key Features

- **Gradle HTTP Build Cache API** - Fully compatible with Gradle's remote cache protocol
- **In-Memory Storage** - Uses Redis for fast cache lookups and storage
- **Role-Based Authentication** - HTTP Basic Authentication with separate reader/writer roles
- **Kubernetes-Native** - Designed for containerized deployments with production-ready Helm charts
- **Observability** - Built-in Prometheus metrics, Grafana dashboard, and structured logging
- **Dependency Proxy** - Optional Reposilite integration for caching Maven/Gradle dependencies
- **Lightweight** - Minimal resource footprint (~256Mi RAM, ~100m CPU)
- **Health Checks** - Kubernetes-ready liveness and readiness probes

### Use Cases

- **Team Development** - Share build cache across development teams
- **CI/CD Pipelines** - Accelerate build times in Jenkins, GitLab CI, GitHub Actions, etc.
- **Monorepos** - Reduce build times for large multi-module projects
- **Cost Optimization** - Reduce build server compute costs by avoiding redundant work

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                         │
│                                                              │
│  ┌─────────────────┐       ┌──────────────────────────────┐ │
│  │  Cache Server    │──────▶│  Redis (Deployment)         │ │
│  │  (Deployment)    │       │  Port: 6379                 │ │
│  │  Port: 8080      │       │  + Redis Exporter sidecar   │ │
│  └────────┬─────────┘       └──────────────────────────────┘ │
│           │                                                   │
│  ┌────────▼─────────┐       ┌──────────────────────────────┐ │
│  │    Service        │       │  Reposilite (optional)      │ │
│  │  Port: 8080       │       │  Maven/Gradle dependency    │ │
│  └──────────────────┘       │  proxy with caching          │ │
│           ▲                  └──────────────────────────────┘ │
│           │                                                   │
│   Developer Workstations / CI/CD Pipelines                   │
└──────────────────────────────────────────────────────────────┘
```

### Components

| Component | Description | Technology |
|-----------|-------------|------------|
| **Cache Server** | HTTP API server implementing Gradle Build Cache protocol | Go, Gin Framework |
| **Storage Backend** | In-memory key-value store for cache artifacts | Redis 7 |
| **Redis Exporter** | Sidecar that exposes Redis metrics to Prometheus | oliver006/redis_exporter |
| **Reposilite** | Maven/Gradle dependency proxy and cache (optional) | Reposilite 3.x |
| **Helm Chart** | Declarative deployment configuration | Helm 3 |

## Quick Start

### Prerequisites

- Kubernetes cluster (v1.19+)
- Helm 3.x
- kubectl configured to access your cluster

### Installation

1. Clone the repository:
```bash
git clone https://github.com/eduide/eduide-shared-cache.git
cd eduide-shared-cache
```

2. Deploy using Helm:
```bash
helm install gradle-cache ./chart
```

The Redis password is auto-generated on first install and stored as a Kubernetes Secret. No manual credential configuration is needed for the storage backend.

3. Verify the deployment:
```bash
kubectl get pods
kubectl logs -f deployment/gradle-cache-cache-server
```

### Testing the Cache

Port-forward the service to your local machine:
```bash
kubectl port-forward svc/gradle-cache-cache 8080:8080
```

Test the health endpoint:
```bash
curl http://localhost:8080/ping
# Expected response: pong
```

Test cache operations:
```bash
# Store a cache entry (requires writer role)
curl -X PUT -u writer:changeme-writer \
  -H "Content-Type: application/octet-stream" \
  -d "test data" \
  http://localhost:8080/cache/test-key

# Retrieve the cache entry (reader or writer role)
curl -u reader:changeme-reader http://localhost:8080/cache/test-key

# Check if entry exists
curl -I -u reader:changeme-reader http://localhost:8080/cache/test-key
```

## Configuration

### Gradle Configuration

Configure your Gradle builds to use the remote cache:

**settings.gradle.kts (Kotlin DSL):**
```kotlin
buildCache {
    remote<HttpBuildCache> {
        url = uri("http://<release-name>-cache:8080/cache/")
        credentials {
            username = "writer"
            password = "changeme-writer"
        }
        isPush = true  // set to false for read-only access
    }
}
```

**settings.gradle (Groovy DSL):**
```groovy
buildCache {
    remote(HttpBuildCache) {
        url = 'http://<release-name>-cache:8080/cache/'
        credentials {
            username = 'writer'
            password = 'changeme-writer'
        }
        push = true
    }
}
```

**gradle.properties:**
```properties
org.gradle.caching=true
```

### Helm Chart Configuration

Key configuration options in `chart/values.yaml`:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `enabled` | Enable/disable the entire deployment | `true` |
| `image.repository` | Cache server image | `ghcr.io/eduide/eduide-shared-cache/gradle-cache` |
| `image.tag` | Cache server image tag | `main` |
| `auth.enabled` | Enable authentication | `true` |
| `auth.username` | Cache username | `gradle` |
| `auth.password` | Cache password | `changeme` |
| `resources.cacheServer` | Cache server resource limits | See values.yaml |
| `resources.redis` | Redis resource limits | See values.yaml |
| `tls.enabled` | Enable TLS/HTTPS | `false` |
| `tls.secretName` | TLS certificate secret name | `""` |
| `metrics.serviceMonitor.enabled` | Create ServiceMonitor for Prometheus Operator | `false` |
| `reposilite.enabled` | Deploy Reposilite dependency proxy | `true` |

For a complete list of configuration options, see the [Helm chart documentation](chart/README.md).

### Environment Variables

The cache server can be configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_PASSWORD` | Redis authentication password | Auto-generated |
| `CACHE_READER_USERNAME` | Reader role username | From values.yaml |
| `CACHE_READER_PASSWORD` | Reader role password | From values.yaml |
| `CACHE_WRITER_USERNAME` | Writer role username | From values.yaml |
| `CACHE_WRITER_PASSWORD` | Writer role password | From values.yaml |
| `SENTRY_DSN` | Sentry error tracking DSN | Disabled |

## API Reference

### Endpoints

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/ping` | GET | No | Health check (liveness probe) |
| `/health` | GET | No | Storage connectivity check (readiness probe) |
| `/metrics` | GET | No | Prometheus metrics |
| `/cache/:key` | GET | reader/writer | Retrieve cache entry |
| `/cache/:key` | HEAD | reader/writer | Check if cache entry exists |
| `/cache/:key` | PUT | writer only | Store cache entry |

### HTTP Status Codes

| Code | Description |
|------|-------------|
| `200 OK` | Cache hit (GET), entry exists (HEAD) |
| `201 Created` | Cache entry stored successfully (PUT) |
| `401 Unauthorized` | Authentication failed |
| `403 Forbidden` | Insufficient role (e.g., reader trying to PUT) |
| `404 Not Found` | Cache miss (GET/HEAD) |
| `413 Payload Too Large` | Entry exceeds maximum size (default: 100MB) |
| `500 Internal Server Error` | Server or storage error |

## Development

### Prerequisites
- Go 1.24+
- Docker & Docker Compose

### Local Development with Docker Compose

The fastest way to run the full stack locally:

```bash
cd src/deployments
docker compose up --build
```

This starts all services:

| Service | URL | Description |
|---------|-----|-------------|
| Cache Server | http://localhost:8080 | Gradle build cache API |
| Redis | localhost:6379 | In-memory storage |
| Redis Exporter | http://localhost:9121 | Redis metrics for Prometheus |
| Prometheus | http://localhost:9090 | Metrics collection |
| Grafana | http://localhost:3000 | Dashboards (no login required) |
| Reposilite | http://localhost:8081 | Dependency proxy |

A pre-built Grafana dashboard ("Gradle Build Cache") is automatically provisioned with panels for cache hit rate, request latency, Redis memory usage, and more.

### Building from Source

```bash
cd src
go build -o bin/cache-server ./cmd/server
```

### Running Tests

```bash
cd src
go test ./...
```

### Project Structure

```
.
├── chart/                      # Helm chart for Kubernetes deployment
│   ├── templates/
│   │   ├── _helpers.tpl        # Shared label templates
│   │   ├── deployment.yaml     # Cache server Deployment
│   │   ├── redis-deployment.yaml  # Redis Deployment
│   │   ├── redis-service.yaml  # Redis Service
│   │   ├── service.yaml        # Cache server Service
│   │   ├── configmap.yaml      # Server configuration
│   │   └── secrets.yaml        # Auto-generated Redis password
│   ├── values.yaml             # Default configuration
│   └── Chart.yaml              # Chart metadata
├── src/                        # Go source code
│   ├── cmd/server/             # Application entry point
│   ├── internal/
│   │   ├── config/             # Configuration management
│   │   ├── handler/            # HTTP handlers (GET/PUT/HEAD)
│   │   ├── middleware/         # Auth, logging, metrics middleware
│   │   ├── server/             # HTTP server and routes
│   │   ├── storage/            # Redis storage backend
│   │   └── telemetry/          # OpenTelemetry setup
│   ├── deployments/            # Docker Compose + monitoring config
│   │   ├── docker-compose.yaml
│   │   ├── prometheus.yaml
│   │   └── grafana/            # Grafana provisioning & dashboards
│   ├── configs/config.yaml     # Default app configuration
│   ├── Dockerfile              # Multi-stage Docker build
│   └── go.mod                  # Go module dependencies
└── .github/workflows/          # GitHub Actions CI/CD
```

## Monitoring

### Prometheus Metrics

The cache server exposes metrics at `/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `gradle_cache_requests_total` | Counter | Total requests by method and status |
| `gradle_cache_cache_hits` | Counter | Cache hit count |
| `gradle_cache_cache_misses` | Counter | Cache miss count |
| `gradle_cache_request_duration_seconds` | Histogram | Request latency (p50/p95/p99) |
| `gradle_cache_entry_size` | Histogram | Cache entry sizes |

Redis metrics are exposed via the redis-exporter sidecar:

| Metric | Type | Description |
|--------|------|-------------|
| `redis_memory_used_bytes` | Gauge | Redis memory consumption |
| `redis_db_keys` | Gauge | Number of cached entries |
| `redis_keyspace_hits_total` | Counter | Redis-level cache hits |
| `redis_keyspace_misses_total` | Counter | Redis-level cache misses |
| `redis_commands_processed_total` | Counter | Total Redis operations |

### Grafana Dashboard

A pre-built dashboard is included at `src/deployments/grafana/dashboards/gradle-build-cache.json`. It can be imported into any Grafana instance and includes panels for:

- Cache hit rate (with color-coded thresholds)
- Request rate by HTTP method
- Request latency percentiles (p50, p95, p99)
- Cache hits vs misses over time
- Server error rate (5xx)
- Redis memory usage and trends
- Redis keyspace hit/miss ratio
- Redis operations per second

### ServiceMonitor (Prometheus Operator)

If your cluster uses the Prometheus Operator, enable automatic scrape discovery:

```bash
helm install gradle-cache ./chart --set metrics.serviceMonitor.enabled=true
```

Otherwise, Prometheus pod annotations are included by default for annotation-based discovery.

## Security

### Authentication Model

The cache server uses role-based HTTP Basic Authentication:

| Role | Permissions | Use Case |
|------|-------------|----------|
| **reader** | GET, HEAD | CI/CD pipelines that only consume cache |
| **writer** | GET, HEAD, PUT | Build agents that produce and consume cache |

### Redis Password

The Redis password is auto-generated on first `helm install` and stored in a Kubernetes Secret. Both the cache server and Redis read it from the same Secret. No human ever needs to know this password.

### Recommendations for Production

1. **Enable TLS** - Encrypt traffic between Gradle clients and the cache server
2. **Change default credentials** - Override `auth.password` with strong passwords
3. **Network Policies** - Restrict access to authorized pods only
4. **Set Redis memory limits** - Configure `maxmemory` and `maxmemory-policy allkeys-lru` to handle cache eviction gracefully

## Troubleshooting

### Common Issues

**Pods not starting**
```bash
kubectl describe pod <pod-name>
kubectl logs <pod-name>
```

**Redis connection errors**
- Verify Redis is running: `kubectl get pods -l app.kubernetes.io/component=storage`
- Check Redis logs: `kubectl logs -l app.kubernetes.io/component=storage`
- Verify the Redis Secret exists: `kubectl get secret <release-name>-redis-secret`

**Authentication failures**
- Ensure Gradle credentials match those configured in `values.yaml`
- Check user role — readers cannot PUT, only writers can

**Cache not helping (low hit rate)**
- Verify Gradle has `org.gradle.caching=true` in `gradle.properties`
- Check that `isPush = true` is set for at least one build agent
- Different Gradle versions or JDKs produce different cache keys

## Upgrading

### From 0.2.x to 0.3.0

Version 0.3.0 replaces MinIO with Redis as the storage backend.

Changes:
- Storage backend changed from MinIO (S3-compatible) to Redis (in-memory)
- Redis password is auto-generated (no manual credential configuration)
- MinIO StatefulSet replaced with Redis Deployment (no persistent volume needed)
- Redis exporter sidecar added for Prometheus metrics
- Grafana dashboard included for local development
- Helm labels updated to Kubernetes recommended labels (`app.kubernetes.io/*`)

Migration steps:
1. Cache data cannot be migrated (MinIO objects to Redis keys) — the cache will be cold after upgrade
2. Uninstall the old release: `helm uninstall <release-name>`
3. Install the new version: `helm install <release-name> ./chart`
4. Gradle builds will repopulate the cache automatically on first run
