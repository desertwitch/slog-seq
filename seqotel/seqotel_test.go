package seqotel

import (
	"context"
	"log/slog"
	"testing"
	"time"

	slogseq "github.com/desertwitch/slog-seq"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// stubInvalidSpan is a minimal ReadOnlySpan with an invalid SpanContext.
type stubInvalidSpan struct {
	sdktrace.ReadOnlySpan
}

func (s *stubInvalidSpan) SpanContext() trace.SpanContext {
	return trace.SpanContext{} // invalid: zero trace ID and span ID
}

func newTestProcessor(t *testing.T) (*LoggingSpanProcessor, *SeqOTelHandler) {
	t.Helper()

	_, handler := NewLogger("http://fake",
		slogseq.WithWorkers(1),
		slogseq.WithNoFlush(),
	)
	t.Cleanup(func() { _ = handler.Close() })

	return &LoggingSpanProcessor{Handler: handler}, handler
}

func newTestTracerProvider(processor *LoggingSpanProcessor, res *resource.Resource) *sdktrace.TracerProvider {
	opts := []sdktrace.TracerProviderOption{sdktrace.WithSpanProcessor(processor)}
	if res != nil {
		opts = append(opts, sdktrace.WithResource(res))
	}

	return sdktrace.NewTracerProvider(opts...)
}

func drainEvents(t *testing.T, handler *SeqOTelHandler, n int) []slogseq.CLEFEvent {
	t.Helper()

	ch := handler.Events(0)

	events := make([]slogseq.CLEFEvent, 0, n)
	for i := range n {
		select {
		case e := <-ch:
			events = append(events, e)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for event %d", i)
		}
	}

	return events
}

// Expectation: NewLogger should return a non-nil logger and handler.
func Test_NewLogger_ReturnsNonNil_Success(t *testing.T) {
	t.Parallel()

	logger, handler := NewLogger("http://fake", slogseq.WithNoFlush())
	defer handler.Close()

	require.NotNil(t, logger)
	require.NotNil(t, handler)
}

// Expectation: NewSeqOTelHandler should return a non-nil handler.
func Test_NewSeqOTelHandler_ReturnsNonNil_Success(t *testing.T) {
	t.Parallel()

	handler := NewSeqOTelHandler("http://fake", slogseq.WithNoFlush())
	defer handler.Close()

	require.NotNil(t, handler)
}

// Expectation: NewLoggingSpanProcessor should return a processor with the given handler.
func Test_NewLoggingSpanProcessor_ReturnsProcessor_Success(t *testing.T) {
	t.Parallel()

	handler := NewSeqOTelHandler("http://fake", slogseq.WithNoFlush())
	defer handler.Close()

	processor := NewLoggingSpanProcessor(handler)

	require.NotNil(t, processor)
	require.Same(t, handler, processor.Handler)
}

// Expectation: Without a span context, TraceID and SpanID should remain empty.
func Test_SeqOTelHandler_Handle_NoTraceContext_EmptyTraceFields_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		slogseq.WithWorkers(1),
		slogseq.WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger.Info("no trace")

	select {
	case evt := <-handler.Events(0):
		require.Empty(t, evt.TraceID)
		require.Empty(t, evt.SpanID)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

// Expectation: When a valid span context is present, TraceID and SpanID should be populated.
func Test_SeqOTelHandler_Handle_WithTraceContext_SetsTraceAndSpanID_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		slogseq.WithWorkers(1),
		slogseq.WithNoFlush(),
	)
	defer handler.Close()

	traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	spanID, _ := trace.SpanIDFromHex("0102030405060708")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	logger := slog.New(handler)
	logger.InfoContext(ctx, "traced event")

	select {
	case evt := <-handler.Events(0):
		require.Equal(t, "0102030405060708090a0b0c0d0e0f10", evt.TraceID)
		require.Equal(t, "0102030405060708", evt.SpanID)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

// Expectation: OnStart should be a no-op and not panic.
func Test_LoggingSpanProcessor_OnStart_NoPanic_Success(t *testing.T) {
	t.Parallel()

	processor, _ := newTestProcessor(t)

	require.NotPanics(t, func() {
		processor.OnStart(context.Background(), nil)
	})
}

// Expectation: ForceFlush should return nil.
func Test_LoggingSpanProcessor_ForceFlush_ReturnsNil_Success(t *testing.T) {
	t.Parallel()

	processor, _ := newTestProcessor(t)

	err := processor.ForceFlush(context.Background())
	require.NoError(t, err)
}

// Expectation: Shutdown should return nil.
func Test_LoggingSpanProcessor_Shutdown_ReturnsNil_Success(t *testing.T) {
	t.Parallel()

	processor, _ := newTestProcessor(t)

	err := processor.Shutdown(context.Background())
	require.NoError(t, err)
}

// Expectation: OnEnd should emit a CLEF event with the span name, trace ID, and span ID.
func Test_LoggingSpanProcessor_OnEnd_BasicSpan_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "mySpan")
	span.End()

	events := drainEvents(t, handler, 1)

	require.Equal(t, "mySpan", events[0].Message)
	require.NotEmpty(t, events[0].TraceID)
	require.NotEmpty(t, events[0].SpanID)
	require.Equal(t, "mySpan", events[0].Properties["SpanName"])
}

// Expectation: OnEnd should set the span kind on the emitted event.
func Test_LoggingSpanProcessor_OnEnd_SpanKind_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "serverSpan",
		trace.WithSpanKind(trace.SpanKindServer))
	span.End()

	events := drainEvents(t, handler, 1)

	require.Equal(t, "server", events[0].SpanKind)
}

// Expectation: OnEnd should set SpanStart to the span's start time.
func Test_LoggingSpanProcessor_OnEnd_SpanStart_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	before := time.Now()
	_, span := tp.Tracer("test").Start(context.Background(), "timedSpan")
	span.End()

	events := drainEvents(t, handler, 1)

	require.False(t, events[0].SpanStart.IsZero())
	require.True(t, events[0].SpanStart.After(before) || events[0].SpanStart.Equal(before))
}

// Expectation: OnEnd should set the timestamp to the span's end time.
func Test_LoggingSpanProcessor_OnEnd_Timestamp_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "endTimeSpan")
	span.End()

	events := drainEvents(t, handler, 1)

	require.False(t, events[0].Timestamp.IsZero())
	require.True(t, events[0].Timestamp.After(events[0].SpanStart) ||
		events[0].Timestamp.Equal(events[0].SpanStart))
}

// Expectation: OnEnd should set the default level to Information for non-error spans.
func Test_LoggingSpanProcessor_OnEnd_DefaultLevel_Information_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "infoSpan")
	span.End()

	events := drainEvents(t, handler, 1)

	require.Equal(t, slogseq.CLEFLevelInformation.String(), events[0].Level)
}

// Expectation: OnEnd should set ParentSpanID when the span has a parent.
func Test_LoggingSpanProcessor_OnEnd_ParentSpan_SetsParentSpanID_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	ctx, parent := tp.Tracer("test").Start(context.Background(), "parent")
	_, child := tp.Tracer("test").Start(ctx, "child")
	child.End()
	parent.End()

	events := drainEvents(t, handler, 2)

	childEvt := events[0]
	require.Equal(t, "child", childEvt.Message)
	require.NotEmpty(t, childEvt.ParentSpanID)
	require.Equal(t, parent.SpanContext().SpanID().String(), childEvt.ParentSpanID)

	parentEvt := events[1]
	require.Equal(t, "parent", parentEvt.Message)
	require.Empty(t, parentEvt.ParentSpanID)
}

// Expectation: Span attributes should appear as properties on the emitted event.
func Test_LoggingSpanProcessor_OnEnd_SpanAttributes_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "attrSpan")
	span.SetAttributes(
		attribute.String("http.method", "GET"),
		attribute.Int("http.status_code", 200),
	)
	span.End()

	events := drainEvents(t, handler, 1)

	require.Equal(t, "GET", events[0].Properties["http.method"])
	require.Equal(t, int64(200), events[0].Properties["http.status_code"])
}

// Expectation: A span with error status should set the level to Error.
func Test_LoggingSpanProcessor_OnEnd_ErrorStatus_SetsErrorLevel_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "errorSpan")
	span.SetStatus(codes.Error, "something failed")
	span.End()

	events := drainEvents(t, handler, 1)

	require.Equal(t, slogseq.CLEFLevelError.String(), events[0].Level)
	require.Equal(t, "something failed", events[0].Message)
}

// Expectation: A span with error status but no description should keep the span name as message.
func Test_LoggingSpanProcessor_OnEnd_ErrorStatusNoDescription_KeepsSpanName_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "errorNoDesc")
	span.SetStatus(codes.Error, "")
	span.End()

	events := drainEvents(t, handler, 1)

	require.Equal(t, slogseq.CLEFLevelError.String(), events[0].Level)
	require.Equal(t, "errorNoDesc", events[0].Message)
}

// Expectation: A span with OK status should not set the level to Error.
func Test_LoggingSpanProcessor_OnEnd_OKStatus_NoErrorLevel_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "okSpan")
	span.SetStatus(codes.Ok, "all good")
	span.End()

	events := drainEvents(t, handler, 1)

	require.Equal(t, slogseq.CLEFLevelInformation.String(), events[0].Level)
	require.Equal(t, "okSpan", events[0].Message)
}

// Expectation: Span events should be emitted as separate CLEF events before the span itself.
func Test_LoggingSpanProcessor_OnEnd_SpanEvents_EmittedBeforeSpan_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "parentSpan")
	span.AddEvent("event1")
	span.AddEvent("event2")
	span.End()

	events := drainEvents(t, handler, 3)

	require.Equal(t, "event1", events[0].Message)
	require.Equal(t, "event2", events[1].Message)
	require.Equal(t, "parentSpan", events[2].Message)
}

// Expectation: Span event attributes should appear as properties.
func Test_LoggingSpanProcessor_OnEnd_SpanEventAttributes_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "attrEventSpan")
	span.AddEvent("myEvent", trace.WithAttributes(
		attribute.String("detail", "important"),
		attribute.Bool("retry", true),
	))
	span.End()

	events := drainEvents(t, handler, 2)

	eventEvt := events[0]
	require.Equal(t, "myEvent", eventEvt.Message)
	require.Equal(t, "important", eventEvt.Properties["detail"])
	require.Equal(t, true, eventEvt.Properties["retry"])
}

// Expectation: Span events should inherit the span's trace ID, span ID, and span kind.
func Test_LoggingSpanProcessor_OnEnd_SpanEvent_InheritsSpanContext_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "contextSpan",
		trace.WithSpanKind(trace.SpanKindClient))
	span.AddEvent("inheritedEvent")
	span.End()

	events := drainEvents(t, handler, 2)

	eventEvt := events[0]
	spanEvt := events[1]

	require.Equal(t, spanEvt.TraceID, eventEvt.TraceID)
	require.Equal(t, spanEvt.SpanID, eventEvt.SpanID)
	require.Equal(t, spanEvt.SpanKind, eventEvt.SpanKind)
}

// Expectation: Span events should carry the parent span's SpanName in properties.
func Test_LoggingSpanProcessor_OnEnd_SpanEvent_CarriesSpanName_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "namedSpan")
	span.AddEvent("childEvent")
	span.End()

	events := drainEvents(t, handler, 2)

	require.Equal(t, "namedSpan", events[0].Properties["SpanName"])
}

// Expectation: Span events should have a default level of Information.
func Test_LoggingSpanProcessor_OnEnd_SpanEvent_DefaultLevel_Information_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "infoEventSpan")
	span.AddEvent("normalEvent")
	span.End()

	events := drainEvents(t, handler, 2)

	require.Equal(t, slogseq.CLEFLevelInformation.String(), events[0].Level)
}

// Expectation: A span with Unset status (default) should have Information level.
func Test_LoggingSpanProcessor_OnEnd_UnsetStatus_InformationLevel_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "unsetSpan")
	// No SetStatus call - status remains codes.Unset.
	span.End()

	events := drainEvents(t, handler, 1)

	require.Equal(t, slogseq.CLEFLevelInformation.String(), events[0].Level)
	require.Equal(t, "unsetSpan", events[0].Message)
}

// Expectation: EventName should be preserved in properties even when exception.message overrides Message.
func Test_LoggingSpanProcessor_OnEnd_EventName_PreservedOnException_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "span")
	span.AddEvent("database.query", trace.WithAttributes(
		attribute.String("exception.message", "connection refused"),
	))
	span.End()

	events := drainEvents(t, handler, 2)

	eventEvt := events[0]
	require.Equal(t, "connection refused", eventEvt.Message)
	require.Equal(t, "database.query", eventEvt.Properties["EventName"])
}

// Expectation: An event with exception.message should overwrite the event name and set the level to error.
func Test_LoggingSpanProcessor_OnEnd_WithException_SetsErrorLevel_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "testSpan",
		trace.WithSpanKind(trace.SpanKindServer))
	span.AddEvent("originalEventName", trace.WithAttributes(
		attribute.String("exception.message", "error occurred"),
		attribute.Int("code", 500),
	))
	span.End()

	events := drainEvents(t, handler, 2)

	eventEvt := events[0]
	require.Equal(t, "error occurred", eventEvt.Message)
	require.Equal(t, slogseq.CLEFLevelError.String(), eventEvt.Level)
	require.Equal(t, int64(500), eventEvt.Properties["code"])
}

// Expectation: Span events with parent span should inherit the parent span ID.
func Test_LoggingSpanProcessor_OnEnd_SpanEvent_InheritsParentSpanID_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	ctx, parent := tp.Tracer("test").Start(context.Background(), "parent")
	_, child := tp.Tracer("test").Start(ctx, "child")
	child.AddEvent("childEvent")
	child.End()
	parent.End()

	events := drainEvents(t, handler, 3)

	childEventEvt := events[0]
	require.Equal(t, "childEvent", childEventEvt.Message)
	require.Equal(t, parent.SpanContext().SpanID().String(), childEventEvt.ParentSpanID)
}

// Expectation: A span with no events should emit exactly one CLEF event (the span itself).
func Test_LoggingSpanProcessor_OnEnd_NoEvents_EmitsOneEvent_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "lonelySpan")
	span.End()

	events := drainEvents(t, handler, 1)

	require.Equal(t, "lonelySpan", events[0].Message)

	select {
	case <-handler.Events(0):
		t.Fatal("unexpected extra event")
	default:
	}
}

// Expectation: Resource attributes should be propagated to all emitted CLEF events.
func Test_LoggingSpanProcessor_OnEnd_PropagatesResourceAttributes_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	res := resource.NewSchemaless(
		attribute.String("service.name", "testsvc"),
		attribute.String("service.version", "1.2.3"),
	)
	tp := newTestTracerProvider(processor, res)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "rootSpan")
	span.AddEvent("anEvent")
	span.End()

	events := drainEvents(t, handler, 2)

	for _, evt := range events {
		require.Equal(t, "testsvc", evt.ResourceAttributes["service.name"])
		require.Equal(t, "1.2.3", evt.ResourceAttributes["service.version"])
	}
}

// Expectation: When an empty resource is set, ResourceAttributes should be nil.
func Test_LoggingSpanProcessor_OnEnd_EmptyResource_NilResourceAttributes_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, resource.Empty())
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "emptyResSpan")
	span.End()

	events := drainEvents(t, handler, 1)

	require.Nil(t, events[0].ResourceAttributes)
}

// Expectation: Resource attributes of multiple types should be propagated correctly.
func Test_LoggingSpanProcessor_OnEnd_ResourceAttributes_MultipleTypes_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	res := resource.NewSchemaless(
		attribute.String("str", "hello"),
		attribute.Int("num", 42),
		attribute.Bool("flag", true),
		attribute.Float64("ratio", 3.14),
	)
	tp := newTestTracerProvider(processor, res)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "typedResSpan")
	span.End()

	events := drainEvents(t, handler, 1)

	require.Equal(t, "hello", events[0].ResourceAttributes["str"])
	require.Equal(t, int64(42), events[0].ResourceAttributes["num"])
	require.Equal(t, true, events[0].ResourceAttributes["flag"])
	require.InDelta(t, 3.14, events[0].ResourceAttributes["ratio"], 0.001)
}

// Expectation: logOtelSpanAsCLEF should not emit when span context is invalid.
func Test_LoggingSpanProcessor_OnEnd_InvalidSpanContext_NoEmit_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)

	// A stubInvalidSpan returns an invalid SpanContext.
	processor.logOtelSpanAsCLEF(&stubInvalidSpan{})

	select {
	case <-handler.Events(0):
		t.Fatal("should not emit event for invalid span context")
	default:
		// good
	}
}

// Expectation: logOtelEventAsCLEF should not emit when span context is invalid.
func Test_LoggingSpanProcessor_OnEnd_InvalidSpanContext_EventNotEmitted_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)

	processor.logOtelEventAsCLEF(&stubInvalidSpan{}, sdktrace.Event{
		Name: "should-not-appear",
		Time: time.Now(),
	})

	select {
	case <-handler.Events(0):
		t.Fatal("should not emit event for invalid span context")
	default:
		// good
	}
}

// Expectation: An event with exception.stacktrace should populate the Exception field.
func Test_LoggingSpanProcessor_OnEnd_WithStacktrace_SetsException_Success(t *testing.T) {
	t.Parallel()

	processor, handler := newTestProcessor(t)
	tp := newTestTracerProvider(processor, nil)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "errSpan")
	span.AddEvent("exception", trace.WithAttributes(
		attribute.String("exception.message", "null pointer"),
		attribute.String("exception.type", "NullPointerException"),
		attribute.String("exception.stacktrace", "at main.go:42\nat handler.go:15"),
	))
	span.End()

	events := drainEvents(t, handler, 2)

	eventEvt := events[0]
	require.Equal(t, "null pointer", eventEvt.Message)
	require.Equal(t, slogseq.CLEFLevelError.String(), eventEvt.Level)
	require.Equal(t, "at main.go:42\nat handler.go:15", eventEvt.Exception)
	require.Equal(t, "NullPointerException", eventEvt.Properties["exception.type"])
}

// Expectation: ExportSpans with nil input should return nil and not panic.
func Test_LoggingSpanProcessor_ExportSpans_NilInput_Success(t *testing.T) {
	t.Parallel()

	processor, _ := newTestProcessor(t)

	require.NotPanics(t, func() {
		err := processor.ExportSpans(context.Background(), nil)
		require.NoError(t, err)
	})
}

// Expectation: ExportSpans with empty slice should return nil and not panic.
func Test_LoggingSpanProcessor_ExportSpans_EmptySlice_Success(t *testing.T) {
	t.Parallel()

	processor, _ := newTestProcessor(t)

	require.NotPanics(t, func() {
		err := processor.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{})
		require.NoError(t, err)
	})
}

// Expectation: dottedToNested should convert a flat dotted key to nested maps.
func Test_dottedToNested_SingleDottedKey_Success(t *testing.T) {
	t.Parallel()

	input := map[string]any{"a.b.c": "value"}
	result := dottedToNested(input)

	a := result["a"].(map[string]any)
	b := a["b"].(map[string]any)
	require.Equal(t, "value", b["c"])
}

// Expectation: dottedToNested should handle keys with no dots as top-level keys.
func Test_dottedToNested_NoDots_TopLevel_Success(t *testing.T) {
	t.Parallel()

	input := map[string]any{"simple": "value"}
	result := dottedToNested(input)

	require.Equal(t, "value", result["simple"])
}

// Expectation: dottedToNested should merge keys that share a common prefix.
func Test_dottedToNested_SharedPrefix_Merges_Success(t *testing.T) {
	t.Parallel()

	input := map[string]any{
		"service.name":    "myapp",
		"service.version": "1.0",
	}
	result := dottedToNested(input)

	service := result["service"].(map[string]any)
	require.Equal(t, "myapp", service["name"])
	require.Equal(t, "1.0", service["version"])
}

// Expectation: dottedToNested should handle deeply nested dotted keys.
func Test_dottedToNested_DeepNesting_Success(t *testing.T) {
	t.Parallel()

	input := map[string]any{"a.b.c.d.e": "deep"}
	result := dottedToNested(input)

	a := result["a"].(map[string]any)
	b := a["b"].(map[string]any)
	c := b["c"].(map[string]any)
	d := c["d"].(map[string]any)
	require.Equal(t, "deep", d["e"])
}

// Expectation: dottedToNested with empty input should return an empty map.
func Test_dottedToNested_EmptyInput_Success(t *testing.T) {
	t.Parallel()

	result := dottedToNested(map[string]any{})

	require.Empty(t, result)
}

// Expectation: dottedToNested should handle mixed dotted and non-dotted keys.
func Test_dottedToNested_MixedKeys_Success(t *testing.T) {
	t.Parallel()

	input := map[string]any{
		"flat":          "value1",
		"nested.key":    "value2",
		"nested.deep.k": "value3",
	}
	result := dottedToNested(input)

	require.Equal(t, "value1", result["flat"])

	nested := result["nested"].(map[string]any)
	require.Equal(t, "value2", nested["key"])

	deep := nested["deep"].(map[string]any)
	require.Equal(t, "value3", deep["k"])
}

// Expectation: dottedToNested should handle a key that is just a dot.
func Test_dottedToNested_SingleDot_Success(t *testing.T) {
	t.Parallel()

	input := map[string]any{".": "dotval"}
	result := dottedToNested(input)

	empty := result[""].(map[string]any)
	require.Equal(t, "dotval", empty[""])
}

// Expectation: addNested with empty path should be a no-op.
func Test_addNested_EmptyPath_NoOp_Success(t *testing.T) {
	t.Parallel()

	dst := make(map[string]any)
	addNested(dst, []string{}, "value")

	require.Empty(t, dst)
}

// Expectation: addNested with single-element path should set the value directly.
func Test_addNested_SingleElement_Success(t *testing.T) {
	t.Parallel()

	dst := make(map[string]any)
	addNested(dst, []string{"key"}, "value")

	require.Equal(t, "value", dst["key"])
}

// Expectation: addNested with multi-element path should create nested maps.
func Test_addNested_MultiElement_Success(t *testing.T) {
	t.Parallel()

	dst := make(map[string]any)
	addNested(dst, []string{"a", "b", "c"}, "deep")

	a := dst["a"].(map[string]any)
	b := a["b"].(map[string]any)
	require.Equal(t, "deep", b["c"])
}

// Expectation: addNested should merge into existing nested maps.
func Test_addNested_MergesExisting_Success(t *testing.T) {
	t.Parallel()

	dst := make(map[string]any)
	addNested(dst, []string{"a", "b"}, "first")
	addNested(dst, []string{"a", "c"}, "second")

	a := dst["a"].(map[string]any)
	require.Equal(t, "first", a["b"])
	require.Equal(t, "second", a["c"])
}

// Expectation: addNested should overwrite non-map value when path extends through it.
func Test_addNested_OverwritesNonMap_Success(t *testing.T) {
	t.Parallel()

	dst := map[string]any{"a": "scalar"}
	addNested(dst, []string{"a", "b"}, "nested")

	a := dst["a"].(map[string]any)
	require.Equal(t, "nested", a["b"])
}
