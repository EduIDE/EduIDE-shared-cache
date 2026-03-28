# eduide-shared-cache Helm Chart

Helm chart to deploy a Gradle Build Cache server with Redis backend for Kubernetes.

## Architecture

This chart deploys the following components:

1. **Cache Server** - A Go-based Gradle build cache server
2. **Redis** - In-memory storage for cache artifacts (with optional redis-exporter sidecar)
3. **Reposilite** (optional) - Maven/Gradle dependency proxy

```
┌──────────────────────────────────────────────────────────┐
│                  Kubernetes Cluster                       │
│                                                          │
│  ┌─────────────────┐       ┌─────────────────────────┐  │
│  │  Cache Server    │──────▶│  Redis (Deployment)    │  │
│  │  (Deployment)    │       │  Port: 6379            │  │
│  │  Port: 8080      │       │  + Exporter :9121      │  │
│  └────────┬─────────┘       └─────────────────────────┘  │
│           │                                              │
│  ┌────────▼─────────┐       ┌─────────────────────────┐  │
│  │    Service        │       │  Reposilite (optional) │  │
│  │  Port: 8080       │       │  Dependency proxy      │  │
│  └──────────────────┘       └─────────────────────────┘  │
│           ▲                                              │
│     Gradle Builds                                        │
└──────────────────────────────────────────────────────────┘
```

## Features

- Go-based cache server with Redis storage backend
- Auto-generated Redis password (stored in Kubernetes Secret)
- Prometheus metrics endpoint (`/metrics`)
- Redis metrics via redis-exporter sidecar
- Optional ServiceMonitor for Prometheus Operator
- Health checks (`/ping`, `/health`)
- Optional Reposilite dependency proxy
- Kubernetes recommended labels (`app.kubernetes.io/*`)

## Quick Start

### Install with default values

```bash
helm install gradle-cache ./chart
```

### Install with custom values

```bash
helm install gradle-cache ./chart \
  --set auth.password=mysecretpassword \
  --set metrics.serviceMonitor.enabled=true
```

## Configuration

### Cache Server

| Parameter | Description | Default |
|-----------|-------------|---------|
| `enabled` | Enable/disable the entire deployment | `true` |
| `image.repository` | Cache server image repository | `ghcr.io/ls1intum/eduide-shared-cache/gradle-cache` |
| `image.tag` | Cache server image tag | `main` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `auth.enabled` | Enable authentication | `true` |
| `auth.username` | Cache username | `gradle` |
| `auth.password` | Cache password | `changeme` |
| `resources.cacheServer.requests.memory` | Memory request | `256Mi` |
| `resources.cacheServer.requests.cpu` | CPU request | `100m` |
| `resources.cacheServer.limits.memory` | Memory limit | `1Gi` |
| `resources.cacheServer.limits.cpu` | CPU limit | `500m` |

### Redis

| Parameter | Description | Default |
|-----------|-------------|---------|
| `resources.redis.requests.memory` | Memory request | `128Mi` |
| `resources.redis.requests.cpu` | CPU request | `100m` |
| `resources.redis.limits.memory` | Memory limit | `2Gi` |
| `resources.redis.limits.cpu` | CPU limit | `1000m` |

The Redis password is auto-generated on first install and persisted across `helm upgrade`. Both Redis and the cache server read it from the same Kubernetes Secret.

### TLS

| Parameter | Description | Default |
|-----------|-------------|---------|
| `tls.enabled` | Enable TLS for the cache server | `false` |
| `tls.secretName` | Kubernetes TLS Secret name | `""` |

### Metrics

| Parameter | Description | Default |
|-----------|-------------|---------|
| `metrics.serviceMonitor.enabled` | Create ServiceMonitor for Prometheus Operator | `false` |
| `metrics.serviceMonitor.interval` | Scrape interval | `15s` |

Prometheus pod annotations (`prometheus.io/scrape`, `prometheus.io/port`) are always included on pod templates for annotation-based discovery.

### Reposilite

| Parameter | Description | Default |
|-----------|-------------|---------|
| `reposilite.enabled` | Deploy Reposilite dependency proxy | `true` |
| `reposilite.persistence.enabled` | Enable persistent storage | `true` |
| `reposilite.persistence.size` | Storage size | `20Gi` |
| `reposilite.persistence.storageClass` | Storage class | `csi-rbd-sc` |

## Gradle Configuration

Configure your Gradle build to use the cache:

```kotlin
// settings.gradle.kts
buildCache {
    remote<HttpBuildCache> {
        url = uri("http://<release-name>-cache:8080/cache/")
        credentials {
            username = "writer"
            password = "changeme-writer"
        }
        isPush = true
    }
}
```

Enable caching in `gradle.properties`:
```properties
org.gradle.caching=true
```

## Port Forwarding (Development)

For local testing, port-forward the cache service:

```bash
kubectl port-forward svc/<release-name>-cache 8080:8080

# Test the cache server
curl http://localhost:8080/ping
curl http://localhost:8080/health
```

## Monitoring

### Prometheus Metrics

Cache server metrics at `/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `gradle_cache_requests_total` | Counter | Total requests by method and status |
| `gradle_cache_cache_hits_total` | Counter | Cache hit count |
| `gradle_cache_cache_misses_total` | Counter | Cache miss count |
| `gradle_cache_request_duration_seconds` | Histogram | Request latency |
| `gradle_cache_entry_size` | Histogram | Cache entry sizes |

Redis metrics via redis-exporter sidecar at `:9121/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `redis_memory_used_bytes` | Gauge | Redis memory consumption |
| `redis_db_keys` | Gauge | Number of cached entries |
| `redis_keyspace_hits_total` | Counter | Redis-level cache hits |
| `redis_keyspace_misses_total` | Counter | Redis-level cache misses |

### Grafana Dashboard

A pre-built dashboard is available at `src/deployments/grafana/dashboards/gradle-build-cache.json`. Import it into your Grafana instance for cache hit rate, latency, Redis memory, and error monitoring.

## Upgrading

### From 0.2.x to 0.3.0

Version 0.3.0 is a breaking change that replaces MinIO with Redis.

Changes:
- Storage backend: MinIO (S3-compatible) replaced with Redis (in-memory)
- Redis password auto-generated (no manual credential config)
- MinIO StatefulSet replaced with Redis Deployment
- Redis exporter sidecar for Prometheus metrics
- Helm labels updated to `app.kubernetes.io/*` standard

Migration steps:
1. Cache data cannot be migrated — the cache will start cold
2. Uninstall the old release: `helm uninstall <release-name>`
3. Install the new version: `helm install <release-name> ./chart`
4. Gradle builds repopulate the cache automatically
