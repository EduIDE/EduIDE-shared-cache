package telemetry

import (
	"context"
	"time"

	"github.com/getsentry/sentry-go"
	sentryotel "github.com/getsentry/sentry-go/otel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func SetupTelemetry(sentryEnabled bool, sentryDSN string) (func(), error) {

	// setup prometheus
	exporter, err := prometheus.New()
	if err != nil {
		return nil, err
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	otel.SetMeterProvider(provider)

	// sentry setup

	if !sentryEnabled {
		return nil, nil
	}

	err = sentry.Init(sentry.ClientOptions{
		Dsn:              sentryDSN,
		EnableTracing:    true,
		TracesSampleRate: 1,
	})
	if err != nil {
		return nil, err
	}

	spanExporter := sentryotel.NewSentrySpanProcessor()

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(spanExporter))

	otel.SetTracerProvider(tp)

	cleanup := func() {
		tp.Shutdown(context.Background())
		sentry.Flush(2 * time.Second)
	}
	return cleanup, nil
}
