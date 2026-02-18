# Prometheus Integration Guide

**Kuber v2.6.0+**

This guide covers the Prometheus metrics integration for monitoring Kuber cache performance, 
resource usage, and operational health.

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Configuration](#configuration)
4. [Available Metrics](#available-metrics)
5. [Prometheus Server Setup](#prometheus-server-setup)
6. [Grafana Dashboards](#grafana-dashboards)
7. [Alerting](#alerting)
8. [Best Practices](#best-practices)

---

## Overview

Kuber exposes metrics in Prometheus format through Spring Boot Actuator and Micrometer. 
The integration provides:

- **Cache Operations**: GET/SET/DELETE counts, hit rates, evictions
- **Memory Usage**: Heap utilization, value cache statistics
- **Performance Metrics**: Operation latencies, requests per second
- **Per-Region Metrics**: Keys and memory usage per region
- **Server Information**: Version, persistence type, uptime

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Monitoring Stack                            │
│                                                                  │
│  ┌─────────┐     ┌─────────────┐     ┌─────────────────────┐   │
│  │ Grafana │◄────│  Prometheus │◄────│   Kuber Server      │   │
│  │  :3000  │     │    :9090    │     │   :8080/actuator/   │   │
│  └─────────┘     └─────────────┘     │     prometheus      │   │
│       │                │             └─────────────────────┘   │
│       ▼                ▼                                        │
│  Dashboards      Alert Manager                                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### 1. Enable Prometheus in Kuber

Add to `application.properties`:

```properties
# Enable Prometheus endpoint
kuber.prometheus.enabled=true

# Expose actuator endpoints
management.endpoints.web.exposure.include=health,info,metrics,prometheus
management.metrics.tags.application=kuber
```

### 2. Verify Endpoint

```bash
curl http://localhost:8080/actuator/prometheus
```

### 3. Configure Prometheus

Add to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'kuber-cache'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['localhost:8080']
```

---

## Configuration

### application.properties

```properties
# ==============================================================================
# Prometheus Metrics Configuration
# ==============================================================================

# Enable/disable Prometheus metrics endpoint
kuber.prometheus.enabled=true

# Metrics refresh interval in milliseconds
kuber.prometheus.update-interval-ms=5000

# Include JVM metrics (memory, GC, threads)
kuber.prometheus.include-jvm-metrics=true

# Include per-region metrics
kuber.prometheus.include-region-metrics=true

# Custom metric prefix (default: kuber)
kuber.prometheus.metric-prefix=kuber

# Include latency histograms (increases memory usage)
kuber.prometheus.include-latency-histograms=false

# Histogram bucket boundaries in microseconds (if enabled)
# kuber.prometheus.latency-buckets=100,500,1000,5000,10000,50000,100000
```

### Actuator Configuration

```properties
# Expose specific endpoints
management.endpoints.web.exposure.include=health,info,metrics,prometheus

# Add application tag to all metrics
management.metrics.tags.application=kuber

# Health endpoint details
management.endpoint.health.show-details=always
```

---

## Available Metrics

### Cache Operations

| Metric | Type | Description |
|--------|------|-------------|
| `kuber_cache_gets_total` | Gauge | Total GET operations |
| `kuber_cache_sets_total` | Gauge | Total SET operations |
| `kuber_cache_deletes_total` | Gauge | Total DELETE operations |
| `kuber_cache_hits_total` | Gauge | Total cache hits |
| `kuber_cache_misses_total` | Gauge | Total cache misses |
| `kuber_cache_hit_rate` | Gauge | Cache hit rate (0.0 to 1.0) |
| `kuber_cache_evictions_total` | Gauge | Total evictions |
| `kuber_cache_expirations_total` | Gauge | Total TTL expirations |

### Performance

| Metric | Type | Description |
|--------|------|-------------|
| `kuber_cache_get_latency_microseconds` | Gauge | Average GET latency |
| `kuber_cache_set_latency_microseconds` | Gauge | Average SET latency |
| `kuber_cache_requests_per_second` | Gauge | Current RPS |

### Memory

| Metric | Type | Description |
|--------|------|-------------|
| `kuber_value_cache_size` | Gauge | Entries in value cache |
| `kuber_value_cache_limit` | Gauge | Max value cache entries |
| `kuber_heap_used_bytes` | Gauge | JVM heap used |
| `kuber_heap_max_bytes` | Gauge | JVM heap max |
| `kuber_heap_usage_ratio` | Gauge | Heap usage (0.0 to 1.0) |

### Regions

| Metric | Type | Description |
|--------|------|-------------|
| `kuber_total_keys` | Gauge | Total keys all regions |
| `kuber_regions_count` | Gauge | Number of regions |
| `kuber_region_keys{region="..."}` | Gauge | Keys per region |
| `kuber_region_memory_bytes{region="..."}` | Gauge | Memory per region |

### Server Info

| Metric | Type | Labels |
|--------|------|--------|
| `kuber_server_info` | Gauge | version, persistence |

---

## Prometheus Server Setup

### prometheus.yml

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

alerting:
  alertmanagers:
    - static_configs:
        - targets:
          - alertmanager:9093

rule_files:
  - "alerts/*.yml"

scrape_configs:
  # Single Kuber instance
  - job_name: 'kuber-cache'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['kuber-server:8080']
        labels:
          environment: 'production'
          cluster: 'primary'

  # Kuber cluster with service discovery
  - job_name: 'kuber-cluster'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets:
          - 'kuber-node1:8080'
          - 'kuber-node2:8080'
          - 'kuber-node3:8080'
        labels:
          environment: 'production'
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
        regex: '([^:]+).*'
        replacement: '$1'
```

### Docker Compose

```yaml
version: '3.8'

services:
  kuber:
    image: kuber-cache:2.6.0
    ports:
      - "8080:8080"
      - "6380:6380"
    environment:
      - KUBER_PROMETHEUS_ENABLED=true
    networks:
      - monitoring

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - ./alerts:/etc/prometheus/alerts
      - prometheus-data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--storage.tsdb.retention.time=30d'
    networks:
      - monitoring

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana-data:/var/lib/grafana
      - ./grafana/provisioning:/etc/grafana/provisioning
    networks:
      - monitoring

  alertmanager:
    image: prom/alertmanager:latest
    ports:
      - "9093:9093"
    volumes:
      - ./alertmanager.yml:/etc/alertmanager/alertmanager.yml
    networks:
      - monitoring

networks:
  monitoring:
    driver: bridge

volumes:
  prometheus-data:
  grafana-data:
```

---

## Grafana Dashboards

### Example PromQL Queries

#### Cache Performance Panel

```promql
# Hit Rate (percentage)
kuber_cache_hit_rate{application="kuber"} * 100

# Operations per second
rate(kuber_cache_gets_total[5m]) + rate(kuber_cache_sets_total[5m])

# Cache operations breakdown
rate(kuber_cache_gets_total[1m])
rate(kuber_cache_sets_total[1m])
rate(kuber_cache_deletes_total[1m])
```

#### Memory Usage Panel

```promql
# Heap usage percentage
kuber_heap_usage_ratio * 100

# Value cache utilization
(kuber_value_cache_size / kuber_value_cache_limit) * 100

# Memory used in GB
kuber_heap_used_bytes / 1024 / 1024 / 1024
```

#### Per-Region Panel

```promql
# Keys by region (top 10)
topk(10, kuber_region_keys)

# Memory by region
kuber_region_memory_bytes

# Total keys growth rate
rate(kuber_total_keys[5m])
```

#### Latency Panel

```promql
# GET vs SET latency
kuber_cache_get_latency_microseconds
kuber_cache_set_latency_microseconds

# Latency in milliseconds
kuber_cache_get_latency_microseconds / 1000
```

### Dashboard JSON

Import this dashboard into Grafana:

```json
{
  "dashboard": {
    "title": "Kuber Cache Dashboard",
    "panels": [
      {
        "title": "Cache Hit Rate",
        "type": "gauge",
        "targets": [
          {
            "expr": "kuber_cache_hit_rate * 100",
            "legendFormat": "Hit Rate %"
          }
        ]
      },
      {
        "title": "Operations/sec",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(kuber_cache_gets_total[1m])",
            "legendFormat": "GETs"
          },
          {
            "expr": "rate(kuber_cache_sets_total[1m])",
            "legendFormat": "SETs"
          }
        ]
      },
      {
        "title": "Heap Usage",
        "type": "gauge",
        "targets": [
          {
            "expr": "kuber_heap_usage_ratio * 100",
            "legendFormat": "Heap %"
          }
        ]
      },
      {
        "title": "Keys by Region",
        "type": "bargauge",
        "targets": [
          {
            "expr": "kuber_region_keys",
            "legendFormat": "{{region}}"
          }
        ]
      }
    ]
  }
}
```

---

## Alerting

### alerts/kuber-alerts.yml

```yaml
groups:
  - name: kuber-cache-alerts
    rules:
      # Low cache hit rate
      - alert: KuberLowHitRate
        expr: kuber_cache_hit_rate < 0.5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Low cache hit rate on {{ $labels.instance }}"
          description: "Cache hit rate is {{ $value | humanizePercentage }} (below 50%)"

      # Critical heap usage
      - alert: KuberHighHeapUsage
        expr: kuber_heap_usage_ratio > 0.85
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Critical heap usage on {{ $labels.instance }}"
          description: "Heap usage is {{ $value | humanizePercentage }}"

      # Warning heap usage
      - alert: KuberElevatedHeapUsage
        expr: kuber_heap_usage_ratio > 0.70
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Elevated heap usage on {{ $labels.instance }}"
          description: "Heap usage is {{ $value | humanizePercentage }}"

      # High eviction rate
      - alert: KuberHighEvictionRate
        expr: rate(kuber_cache_evictions_total[5m]) > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High eviction rate on {{ $labels.instance }}"
          description: "Evicting {{ $value | humanize }} entries/sec"

      # Instance down
      - alert: KuberInstanceDown
        expr: up{job="kuber-cache"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kuber instance down"
          description: "{{ $labels.instance }} has been down for more than 1 minute"

      # High GET latency
      - alert: KuberHighGetLatency
        expr: kuber_cache_get_latency_microseconds > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High GET latency on {{ $labels.instance }}"
          description: "Average GET latency is {{ $value }}µs (>10ms)"

      # High SET latency
      - alert: KuberHighSetLatency
        expr: kuber_cache_set_latency_microseconds > 50000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High SET latency on {{ $labels.instance }}"
          description: "Average SET latency is {{ $value }}µs (>50ms)"

      # Value cache full
      - alert: KuberValueCacheFull
        expr: (kuber_value_cache_size / kuber_value_cache_limit) > 0.95
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Value cache near capacity on {{ $labels.instance }}"
          description: "Value cache is {{ $value | humanizePercentage }} full"

      # No regions
      - alert: KuberNoRegions
        expr: kuber_regions_count == 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "No cache regions on {{ $labels.instance }}"
          description: "Kuber has no active cache regions"
```

### alertmanager.yml

```yaml
global:
  resolve_timeout: 5m

route:
  group_by: ['alertname', 'severity']
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  receiver: 'default'
  routes:
    - match:
        severity: critical
      receiver: 'critical'
    - match:
        severity: warning
      receiver: 'warning'

receivers:
  - name: 'default'
    email_configs:
      - to: 'ops@example.com'

  - name: 'critical'
    email_configs:
      - to: 'oncall@example.com'
    pagerduty_configs:
      - service_key: 'your-pagerduty-key'

  - name: 'warning'
    slack_configs:
      - api_url: 'https://hooks.slack.com/services/xxx'
        channel: '#alerts'
```

---

## Best Practices

### 1. Scrape Interval

```yaml
# Production: 15-30 seconds
scrape_interval: 15s

# Development: 5-10 seconds for faster feedback
scrape_interval: 5s
```

### 2. Retention

```bash
# Prometheus startup flags
--storage.tsdb.retention.time=30d
--storage.tsdb.retention.size=50GB
```

### 3. Recording Rules

Create pre-computed metrics for dashboards:

```yaml
groups:
  - name: kuber-recording-rules
    rules:
      - record: kuber:hit_rate:5m
        expr: avg_over_time(kuber_cache_hit_rate[5m])
      
      - record: kuber:ops_per_second:1m
        expr: |
          rate(kuber_cache_gets_total[1m]) + 
          rate(kuber_cache_sets_total[1m]) + 
          rate(kuber_cache_deletes_total[1m])
```

### 4. Labels

Use consistent labels across your infrastructure:

```yaml
static_configs:
  - targets: ['kuber1:8080']
    labels:
      environment: 'production'
      datacenter: 'us-east-1'
      service: 'kuber-cache'
      team: 'platform'
```

### 5. High Availability

For critical deployments:

```yaml
# Run multiple Prometheus instances
# Use Thanos or Cortex for long-term storage
# Configure federation for global views
```

---

## Troubleshooting

### Endpoint Not Available

```bash
# Check if actuator is enabled
curl http://localhost:8080/actuator

# Verify prometheus endpoint
curl http://localhost:8080/actuator/prometheus

# Check logs
grep -i prometheus kuber.log
```

### No Metrics

1. Verify `kuber.prometheus.enabled=true`
2. Check `management.endpoints.web.exposure.include` includes `prometheus`
3. Ensure Kuber has processed some requests

### Missing Labels

```properties
# Add custom tags
management.metrics.tags.application=kuber
management.metrics.tags.environment=production
```

---

## Related Documentation

- [Architecture Overview](ARCHITECTURE.md)
- [Configuration Reference](APPLICATION_PROPERTIES.md)
- [System Internals](help/internals)

---

**Version**: 2.6.0  
**Copyright** © 2025-2030, All Rights Reserved  
Ashutosh Sinha | ajsinha@gmail.com
