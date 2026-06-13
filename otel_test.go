package slogseq

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Expectation: An event with exception.message should overwrite the event name and set the level to error.
func Test_LoggingSpanProcessor_OnEnd_WithException_SetsErrorLevel_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithWorkers(1),
		withNoFlush(),
	)
	defer handler.Close()

	processor := &LoggingSpanProcessor{Handler: handler}

	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("test-tracer")

	// Start a span, add an event with exception attributes, then end the span.
	ctx := context.Background()
	_, span := tracer.Start(ctx, "testSpan", trace.WithSpanKind(trace.SpanKindServer))
	span.AddEvent("originalEventName", trace.WithAttributes(
		attribute.String("exception.message", "error occurred"),
		attribute.Int("code", 500),
	))
	span.End()

	var evt CLEFEvent

	select {
	case evt = <-handler.workers[0].eventsCh:
	case <-time.After(1000 * time.Millisecond):
		t.Fatal("timed out waiting for event")
	}

	require.Equal(t, "error occurred", evt.Message)
	require.Equal(t, CLEFLevelError.String(), evt.Level)

	code, ok := evt.Properties["code"]
	require.True(t, ok, "expected property 'code' to be set")
	require.Equal(t, int64(500), code.(int64))
}

// Expectation: Resource attributes should be propagated to all emitted CLEF events.
func Test_LoggingSpanProcessor_OnEnd_PropagatesResourceAttributes_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithWorkers(1),
		withNoFlush(),
	)
	defer handler.Close()

	processor := &LoggingSpanProcessor{Handler: handler}

	res := resource.NewSchemaless(
		attribute.String("service.name", "testsvc"),
		attribute.String("service.version", "1.2.3"),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(processor),
		sdktrace.WithResource(res),
	)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer("test-tracer")
	_, span := tracer.Start(context.Background(), "rootSpan")
	span.AddEvent("anEvent")
	span.End()

	// One event emitted from AddEvent, one from span end.
	var events []CLEFEvent
	for i := range 2 {
		select {
		case e := <-handler.workers[0].eventsCh:
			events = append(events, e)
		case <-time.After(1000 * time.Millisecond):
			t.Fatalf("timed out waiting for event %d", i)
		}
	}

	for _, evt := range events {
		require.Equal(t, "testsvc", evt.ResourceAttributes["service.name"])
		require.Equal(t, "1.2.3", evt.ResourceAttributes["service.version"])
	}
}
