# Observability Guide

This guide explains how to integrate `tokio-rate-limit` with tracing and metrics for production observability.

## Table of Contents

- [Feature Flags](#feature-flags)
- [Tracing Integration](#tracing-integration)
- [Metrics Integration](#metrics-integration)
- [OpenTelemetry Setup](#opentelemetry-setup)
- [Performance Impact](#performance-impact)
- [Common Patterns](#common-patterns)

## Feature Flags

The library provides two feature flags for observability:

```toml
[dependencies]
tokio-rate-limit = { version = "0.2", features = ["observability", "metrics-support"] }
```

- **`observability`**: Enables tracing support (adds `tracing` dependency)
- **`metrics-support`**: Enables metrics support (adds `metrics` dependency, requires `observability`)

Both features are **zero-overhead when disabled** - no performance impact if you don't need observability.

## Tracing Integration

### Basic Setup

```rust
use tokio_rate_limit::{RateLimiter, RateLimiterConfig};
use tracing_subscriber;

#[tokio::main]
async fn main() {
    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let limiter = RateLimiter::new(RateLimiterConfig {
        requests_per_second: 100,
        burst: 200,
    });

    // Rate limit checks will now emit tracing spans
    limiter.check("client-123").await.unwrap();
}
```

### What Gets Traced

When `observability` is enabled, the following methods emit tracing spans:

- `check(key)` - Basic rate limit check
- `check_with_cost(key, cost)` - Cost-based rate limit check
- `acquire_timeout(key, timeout)` - Blocking acquire with timeout
- `acquire(key)` - Blocking acquire

Each span includes:
- **key**: The rate limit key
- **permitted**: Whether the request was allowed
- **remaining**: Remaining tokens
- **limit**: Configured limit
- **cost**: Token cost (for `check_with_cost`)
- **retry_after**: Time to wait (if denied)

### OpenTelemetry Integration

Export traces to OpenTelemetry-compatible backends:

```rust
use opentelemetry::sdk::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::layer::SubscriberExt;

#[tokio::main]
async fn main() {
    // Setup OpenTelemetry
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint("http://localhost:4317")
        )
        .install_batch(opentelemetry::runtime::Tokio)
        .expect("Failed to install OpenTelemetry tracer");

    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    let subscriber = tracing_subscriber::registry()
        .with(telemetry)
        .with(tracing_subscriber::fmt::layer());

    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set subscriber");

    // Use rate limiter - traces will be exported
    let limiter = RateLimiter::new(RateLimiterConfig {
        requests_per_second: 100,
        burst: 200,
    });

    limiter.check("client-123").await.unwrap();
}
```

### Jaeger Integration

```toml
[dependencies]
opentelemetry-jaeger = "0.20"
tracing-opentelemetry = "0.22"
```

```rust
use opentelemetry::sdk::trace::TracerProvider;

let tracer = opentelemetry_jaeger::new_agent_pipeline()
    .with_service_name("my-service")
    .install_batch(opentelemetry::runtime::Tokio)
    .expect("Failed to install Jaeger tracer");
```

### Honeycomb Integration

```toml
[dependencies]
opentelemetry-otlp = "0.15"
```

```rust
let tracer = opentelemetry_otlp::new_pipeline()
    .tracing()
    .with_exporter(
        opentelemetry_otlp::new_exporter()
            .http()
            .with_endpoint("https://api.honeycomb.io")
            .with_headers(vec![
                ("x-honeycomb-team".to_string(), "YOUR_API_KEY".to_string()),
                ("x-honeycomb-dataset".to_string(), "your-dataset".to_string()),
            ].into_iter().collect())
    )
    .install_batch(opentelemetry::runtime::Tokio)
    .expect("Failed to install Honeycomb tracer");
```

## Metrics Integration

### Basic Setup

```rust
use tokio_rate_limit::middleware::RateLimitLayer;
use metrics_exporter_prometheus::PrometheusBuilder;

#[tokio::main]
async fn main() {
    // Setup Prometheus exporter
    let builder = PrometheusBuilder::new();
    builder
        .install()
        .expect("failed to install Prometheus recorder");

    // Use middleware - metrics will be automatically recorded
    let limiter = Arc::new(RateLimiter::new(RateLimiterConfig {
        requests_per_second: 100,
        burst: 200,
    }));

    let app = Router::new()
        .route("/", get(handler))
        .layer(RateLimitLayer::new(limiter));

    // Metrics are now available at /metrics (default Prometheus endpoint)
}
```

### Available Metrics

When using the Axum middleware with `metrics-support` enabled:

| Metric | Type | Description |
|--------|------|-------------|
| `tokio_rate_limit.requests.allowed` | Counter | Total requests that were permitted |
| `tokio_rate_limit.requests.denied` | Counter | Total requests that were rate limited |
| `tokio_rate_limit.remaining_tokens` | Histogram | Distribution of remaining tokens |

### Prometheus Integration

```toml
[dependencies]
metrics-exporter-prometheus = "0.15"
```

```rust
use axum::{Router, routing::get};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    // Setup Prometheus exporter with custom endpoint
    let (recorder, exporter) = PrometheusBuilder::new()
        .with_http_listener(SocketAddr::from(([0, 0, 0, 0], 9090)))
        .build()
        .expect("failed to build Prometheus exporter");

    metrics::set_global_recorder(recorder)
        .expect("failed to install metrics recorder");

    // Spawn metrics exporter
    tokio::spawn(exporter);

    // Your application...
    let limiter = Arc::new(RateLimiter::new(RateLimiterConfig {
        requests_per_second: 100,
        burst: 200,
    }));

    let app = Router::new()
        .route("/api", get(handler))
        .layer(RateLimitLayer::new(limiter));

    // Metrics available at http://localhost:9090/metrics
    axum::serve(
        tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap(),
        app
    ).await.unwrap();
}
```

### StatsD Integration

```toml
[dependencies]
metrics-exporter-statsd = "0.8"
```

```rust
use metrics_exporter_statsd::StatsdBuilder;

#[tokio::main]
async fn main() {
    // Setup StatsD exporter
    StatsdBuilder::from("127.0.0.1:8125", "myapp")
        .install()
        .expect("failed to install StatsD recorder");

    // Use rate limiter with middleware
    // Metrics will be sent to StatsD
}
```

### CloudWatch Integration

```toml
[dependencies]
metrics-exporter-cloudwatch = "0.1"
```

```rust
use metrics_exporter_cloudwatch::CloudWatchBuilder;

#[tokio::main]
async fn main() {
    let client = aws_sdk_cloudwatch::Client::new(&aws_config::load_from_env().await);

    CloudWatchBuilder::new("myapp", client)
        .install()
        .expect("failed to install CloudWatch recorder");

    // Metrics will be sent to AWS CloudWatch
}
```

## OpenTelemetry Setup

Complete example with both tracing and metrics:

```rust
use opentelemetry::sdk::metrics::SdkMeterProvider;
use opentelemetry::sdk::trace::TracerProvider;
use opentelemetry_otlp::{WithExportConfig, MetricsExporterBuilder, SpanExporterBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup tracing
    let trace_exporter = SpanExporterBuilder::default()
        .with_tonic()
        .with_endpoint("http://localhost:4317")
        .build()?;

    let tracer = TracerProvider::builder()
        .with_batch_exporter(trace_exporter, opentelemetry::runtime::Tokio)
        .build();

    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    // Setup metrics
    let metrics_exporter = MetricsExporterBuilder::default()
        .with_tonic()
        .with_endpoint("http://localhost:4317")
        .build()?;

    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metrics_exporter, opentelemetry::runtime::Tokio)
        .build();

    opentelemetry::global::set_meter_provider(meter_provider);

    // Combine with tracing subscriber
    let subscriber = tracing_subscriber::registry()
        .with(telemetry)
        .with(tracing_subscriber::fmt::layer());

    tracing::subscriber::set_global_default(subscriber)?;

    // Use rate limiter - both traces and metrics will be exported
    let limiter = RateLimiter::new(RateLimiterConfig {
        requests_per_second: 100,
        burst: 200,
    });

    limiter.check("client-123").await?;

    Ok(())
}
```

## Performance Impact

### When Features Are Disabled

**Zero overhead** - No performance impact when `observability` and `metrics-support` features are disabled.

Benchmarks show identical performance to the base implementation:
- Single-threaded: 17.7M ops/sec
- Multi-threaded (4 cores): 13.5M ops/sec

### When Features Are Enabled

Minimal overhead with features enabled:

| Configuration | Throughput | Overhead |
|--------------|------------|----------|
| No observability | 17.7M ops/sec | Baseline |
| With tracing | 17.5M ops/sec | ~1% |
| With tracing + metrics | 17.2M ops/sec | ~3% |

The overhead is primarily from:
1. Span creation and field recording (tracing)
2. Metric recording and aggregation (metrics)
3. Atomic operations for counters

For most applications, this 1-3% overhead is negligible compared to the value of observability.

## Common Patterns

### Pattern 1: Production Observability Stack

```rust
// Full observability with OpenTelemetry, Prometheus, and structured logging
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() {
    // Setup OpenTelemetry for traces
    let tracer = setup_opentelemetry_tracer();

    // Setup Prometheus for metrics
    setup_prometheus_exporter();

    // Combine with structured logging
    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .with(tracing_subscriber::fmt::layer().json())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Rate limiter will now emit both traces and metrics
    let limiter = Arc::new(RateLimiter::new(RateLimiterConfig {
        requests_per_second: 1000,
        burst: 2000,
    }));

    let app = Router::new()
        .route("/api", get(handler))
        .layer(RateLimitLayer::new(limiter));

    axum::serve(listener, app).await.unwrap();
}
```

### Pattern 2: Development with Console Subscriber

```toml
[dependencies]
console-subscriber = "0.2"
```

```rust
#[tokio::main]
async fn main() {
    // Setup console subscriber for tokio-console
    console_subscriber::init();

    // Rate limiter with observability
    let limiter = RateLimiter::new(RateLimiterConfig {
        requests_per_second: 100,
        burst: 200,
    });

    // Monitor with: tokio-console
}
```

### Pattern 3: Custom Metrics

```rust
// Add custom metrics alongside built-in ones
use metrics::{counter, histogram};

async fn handle_request(limiter: &RateLimiter, client: &str) {
    let decision = limiter.check(client).await.unwrap();

    // Custom metric: track by client
    if decision.permitted {
        counter!("my_app.requests.by_client", "client" => client.to_string()).increment(1);
    }

    // Custom metric: track utilization
    if let Some(remaining) = decision.remaining {
        let utilization = 100.0 - (remaining as f64 / decision.limit as f64 * 100.0);
        histogram!("my_app.rate_limit.utilization").record(utilization);
    }
}
```

### Pattern 4: Conditional Observability

```rust
// Enable observability only in production
#[cfg(feature = "observability")]
use tracing::info;

async fn process_request(limiter: &RateLimiter, client: &str) {
    let decision = limiter.check(client).await.unwrap();

    #[cfg(feature = "observability")]
    if !decision.permitted {
        info!(client = %client, "Rate limit exceeded");
    }

    // Process request...
}
```

## Troubleshooting

### High Cardinality Warnings

**Problem**: Metrics with high cardinality (many unique labels) can cause memory issues.

**Solution**: Use fixed labels or aggregate at the application level:

```rust
// Bad: High cardinality
counter!("requests", "client_id" => client_id).increment(1);

// Good: Aggregate by category
let category = categorize_client(client_id);
counter!("requests", "category" => category).increment(1);
```

### Missing Traces

**Problem**: Traces not appearing in backend.

**Checklist**:
1. Verify feature flags are enabled: `observability`
2. Check subscriber is initialized before creating rate limiter
3. Verify exporter endpoint is correct
4. Check network connectivity to backend
5. Ensure sampling is not dropping all traces

### Performance Degradation

**Problem**: Observability causing performance issues.

**Solutions**:
1. Use sampling to reduce trace volume
2. Buffer and batch metric exports
3. Use asynchronous exporters
4. Profile with `tracing-subscriber`'s performance layer
5. Consider disabling in hot paths with `#[cfg]` attributes

## Best Practices

1. **Use structured logging**: Always use structured fields, not string interpolation
2. **Add context**: Include relevant context (tenant ID, request ID) in spans
3. **Monitor metrics**: Set up alerts on rate limit denial rates
4. **Sample traces**: Use sampling (e.g., 1%) in high-traffic scenarios
5. **Aggregate metrics**: Roll up high-cardinality metrics
6. **Test locally**: Use console subscriber or local exporters during development
7. **Document custom metrics**: Keep a metrics catalog for your team

## Example Dashboards

### Grafana Dashboard (Prometheus)

```yaml
# Rate Limit Overview Dashboard
- title: Rate Limit Allowed
  expr: rate(tokio_rate_limit_requests_allowed_total[5m])

- title: Rate Limit Denied
  expr: rate(tokio_rate_limit_requests_denied_total[5m])

- title: Denial Rate
  expr: rate(tokio_rate_limit_requests_denied_total[5m]) / rate(tokio_rate_limit_requests_allowed_total[5m])

- title: Remaining Tokens (P50)
  expr: histogram_quantile(0.50, tokio_rate_limit_remaining_tokens_bucket)
```

### CloudWatch Alarms

```yaml
# High rate limit denial rate
- MetricName: tokio_rate_limit.requests.denied
  ComparisonOperator: GreaterThanThreshold
  Threshold: 100  # requests/minute
  EvaluationPeriods: 2
  Period: 60
```

## Further Reading

- [Tracing Documentation](https://docs.rs/tracing)
- [Metrics Documentation](https://docs.rs/metrics)
- [OpenTelemetry Rust](https://github.com/open-telemetry/opentelemetry-rust)
- [Prometheus Best Practices](https://prometheus.io/docs/practices/naming/)
