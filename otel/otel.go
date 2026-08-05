// Package otel centralizes OpenTelemetry setup shared by all tellmi services.
// It initializes the three signals:
//
//   - traces: exported via OTLP/gRPC to the OpenTelemetry Collector
//   - metrics: exposed on /metrics via a Prometheus exporter, scraped directly
//   - logs: forwarded via the zerolog bridge to OTLP/gRPC
//
// Standard OTLP environment variables (OTEL_SERVICE_NAME,
// OTEL_EXPORTER_OTLP_ENDPOINT, ...) are honored with local dev defaults.
package otel

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

const (
	defaultOTLPEndpoint = "localhost:4317"
	defaultServiceName  = "unknown-service"
)

// Config controls the OpenTelemetry setup. Zero values fall back to standard
// OTEL_* environment variables, then to local dev defaults.
type Config struct {
	// ServiceName identifies this service in traces, metrics and logs.
	// Default: OTEL_SERVICE_NAME, then "unknown-service".
	ServiceName string
	// Endpoint is the OTLP/gRPC endpoint (host:port) of the Collector.
	// Default: OTEL_EXPORTER_OTLP_ENDPOINT, then "localhost:4317".
	Endpoint string
	// Insecure disables TLS for the OTLP/gRPC exporter. Local dev defaults to
	// true unless OTEL_EXPORTER_OTLP_ENDPOINT is set.
	Insecure bool
}

// SDK holds the three providers plus a shutdown that drains and closes them.
type SDK struct {
	TracerProvider *sdktrace.TracerProvider
	MeterProvider  *sdkmetric.MeterProvider
	LoggerProvider *sdklog.LoggerProvider
}

// Shutdown flushes and shuts down all providers, collecting errors.
func (s *SDK) Shutdown(ctx context.Context) error {
	var errs []error
	if s.LoggerProvider != nil {
		if err := s.LoggerProvider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("shutdown logger provider: %w", err))
		}
	}
	if s.MeterProvider != nil {
		if err := s.MeterProvider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("shutdown meter provider: %w", err))
		}
	}
	if s.TracerProvider != nil {
		if err := s.TracerProvider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("shutdown tracer provider: %w", err))
		}
	}
	return errors.Join(errs...)
}

// Init sets up the global TracerProvider, MeterProvider and LoggerProvider and
// installs the W3C tracecontext + baggage propagators. Call once at startup and
// defer sdk.Shutdown(ctx).
func Init(ctx context.Context, cfg Config) (*SDK, error) {
	serviceName := firstNonEmpty(cfg.ServiceName, os.Getenv("OTEL_SERVICE_NAME"), defaultServiceName)
	endpoint := firstNonEmpty(cfg.Endpoint, os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), defaultOTLPEndpoint)
	insecure := cfg.Insecure || os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == ""

	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithProcess(),
		resource.WithHost(),
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("create otel resource: %w", err)
	}

	traceOpts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(endpoint)}
	logOpts := []otlploggrpc.Option{otlploggrpc.WithEndpoint(endpoint)}
	if insecure {
		traceOpts = append(traceOpts, otlptracegrpc.WithInsecure())
		logOpts = append(logOpts, otlploggrpc.WithInsecure())
	}

	traceExporter, err := otlptracegrpc.New(ctx, traceOpts...)
	if err != nil {
		return nil, fmt.Errorf("create otlp trace exporter: %w", err)
	}

	logExporter, err := otlploggrpc.New(ctx, logOpts...)
	if err != nil {
		return nil, fmt.Errorf("create otlp log exporter: %w", err)
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExporter),
	)

	loggerProvider := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
	)

	promExporter, err := otelprom.New()
	if err != nil {
		return nil, fmt.Errorf("create prometheus exporter: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(promExporter),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{
				Name: "db.client.operation.duration",
				Unit: "s",
			},
			sdkmetric.Stream{
				Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
					Boundaries: []float64{
						0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
						0.1, 0.25, 0.5, 1, 2.5, 5, 10,
					},
				},
			},
		)),
	)

	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)
	global.SetLoggerProvider(loggerProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &SDK{
		TracerProvider: tracerProvider,
		MeterProvider:  meterProvider,
		LoggerProvider: loggerProvider,
	}, nil
}

// MetricsHandler returns the /metrics handler for direct Prometheus scraping.
func MetricsHandler() http.Handler {
	return promhttp.Handler()
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// injectTraceContext serializes the current W3C trace context from ctx into a
// compact string so it can be persisted (e.g. in an outbox row) and resumed
// later by a background worker.
// InjectTraceContext serializes the W3C trace context from ctx into a compact
// string so it can be persisted (e.g. in an outbox row) and resumed later by a
// background worker.
func InjectTraceContext(ctx context.Context) string {
	c := &mapCarrier{values: make(map[string]string)}
	otel.GetTextMapPropagator().Inject(ctx, c)
	if v, ok := c.values["traceparent"]; ok {
		return v
	}
	return ""
}

// extractTraceContext returns a context carrying the W3C trace context encoded
// in traceparent, so an asynchronous worker can continue the original trace.
// ExtractTraceContext returns a context carrying the W3C trace context encoded
// in traceparent, so an asynchronous worker can continue the original trace.
func ExtractTraceContext(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	c := &mapCarrier{values: map[string]string{"traceparent": traceparent}}
	return otel.GetTextMapPropagator().Extract(ctx, c)
}

type mapCarrier struct {
	values map[string]string
}

func (c *mapCarrier) Get(key string) string {
	return c.values[key]
}

func (c *mapCarrier) Set(key, value string) {
	c.values[key] = value
}

func (c *mapCarrier) Keys() []string {
	out := make([]string, 0, len(c.values))
	for k := range c.values {
		out = append(out, k)
	}
	return out
}

var _ propagation.TextMapCarrier = (*mapCarrier)(nil)
