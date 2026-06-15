// Package seqotel provides OpenTelemetry integration for slog-seq, including
// automatic trace context enrichment for slog events and a SpanProcessor that
// forwards completed spans to Seq as CLEF events.
//
// Use [NewSeqOTelHandler] to create a handler with trace correlation enabled:
//
//	handler := seqotel.NewSeqOTelHandler("http://seq:5341/ingest/clef",
//		slogseq.WithAPIKey("your-api-key"),
//		slogseq.WithBatchSize(50),
//	)
//	defer handler.Close()
//	slog.SetDefault(slog.New(handler))
//
// Use [NewLoggingSpanProcessor] to forward spans to Seq:
//
//	processor := seqotel.NewLoggingSpanProcessor(handler)
//	tp := trace.NewTracerProvider(trace.WithSpanProcessor(processor))
package seqotel

import (
	"context"
	"fmt"
	"log/slog"

	slogseq "github.com/desertwitch/slog-seq"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace"
	tr "go.opentelemetry.io/otel/trace"
)

var (
	_ trace.SpanProcessor = (*LoggingSpanProcessor)(nil)
	_ trace.SpanExporter  = (*LoggingSpanProcessor)(nil)
)

// NewLogger is a convenience function that creates a [SeqOTelHandler] and wraps
// it in an [slog.Logger]. Refer to [NewSeqOTelHandler] for teardown information.
func NewLogger(seqURL string, opts ...slogseq.SeqOption) (*slog.Logger, *SeqOTelHandler) {
	handler := NewSeqOTelHandler(seqURL, opts...)

	return slog.New(handler), handler
}

// SeqOTelHandler is a [slog.Handler] extending [slogseq.SeqHandler] with
// OpenTelemetry trace context enrichment.
//
// Create one using [NewSeqOTelHandler]. Do not construct directly.
type SeqOTelHandler struct { //nolint:revive
	*slogseq.SeqHandler
}

// NewSeqOTelHandler creates and starts a new [SeqOTelHandler] with OpenTelemetry
// trace context enrichment enabled. Log events are automatically correlated
// with active spans. Derived handlers (via WithAttrs and WithGroup) share the
// same workers and connection. Close must be called on the original handler
// when no longer needed, rendering all (sub-)handlers unusable.
//
// See [slogseq] package documentation for all possible [slogseq.SeqOption].
func NewSeqOTelHandler(seqURL string, opts ...slogseq.SeqOption) *SeqOTelHandler {
	opts = append(opts, slogseq.WithEventEnricher(
		func(ctx context.Context, event *slogseq.CLEFEvent) {
			if spanCtx := tr.SpanContextFromContext(ctx); spanCtx.IsValid() {
				event.TraceID = spanCtx.TraceID().String()
				event.SpanID = spanCtx.SpanID().String()
			}
		}))

	handler := slogseq.NewSeqHandler(seqURL, opts...)

	return &SeqOTelHandler{handler}
}

// LoggingSpanProcessor is an OpenTelemetry [trace.SpanProcessor] and
// [trace.SpanExporter] that converts completed spans and their events into CLEF
// events and dispatches them to Seq via a [SeqOTelHandler].
//
// Span events are emitted before the span itself to preserve chronological
// ordering. The processor does not manage the handler's lifecycle - the caller
// is responsible for closing the handler.
type LoggingSpanProcessor struct {
	Handler *SeqOTelHandler
}

// NewLoggingSpanProcessor creates a [LoggingSpanProcessor] that forwards
// completed spans to Seq via the given [SeqOTelHandler].
func NewLoggingSpanProcessor(handler *SeqOTelHandler) *LoggingSpanProcessor {
	return &LoggingSpanProcessor{Handler: handler}
}

// OnStart is a no-op. This processor only acts on completed spans.
func (p *LoggingSpanProcessor) OnStart(_ context.Context, _ trace.ReadWriteSpan) {}

// OnEnd converts a completed span and its events into CLEF events and
// dispatches them to the handler. Span events are emitted first, followed by
// the span itself.
func (p *LoggingSpanProcessor) OnEnd(s trace.ReadOnlySpan) {
	events := s.Events()

	for _, e := range events {
		p.logOtelEventAsCLEF(s, e)
	}

	p.logOtelSpanAsCLEF(s)
}

// ForceFlush is a no-op. Events are flushed on the configured interval and
// fully drained when the handler is closed.
func (p *LoggingSpanProcessor) ForceFlush(_ context.Context) error {
	return nil
}

// Shutdown is a no-op. The handler's lifecycle is managed by the caller who
// created it, not by the span processor.
func (p *LoggingSpanProcessor) Shutdown(_ context.Context) error {
	return nil
}

// ExportSpans converts a batch of completed spans and their events into CLEF
// events and dispatches them to the handler. This method satisfies the
// [trace.SpanExporter] interface.
func (p *LoggingSpanProcessor) ExportSpans(_ context.Context, spans []trace.ReadOnlySpan) error {
	for _, s := range spans {
		for _, e := range s.Events() {
			p.logOtelEventAsCLEF(s, e)
		}

		p.logOtelSpanAsCLEF(s)
	}

	return nil
}

func (p *LoggingSpanProcessor) logOtelSpanAsCLEF(span trace.ReadOnlySpan) {
	sc := span.SpanContext()
	if !sc.IsValid() {
		return
	}

	spanKind := tr.ValidateSpanKind(span.SpanKind()).String()
	event := &slogseq.CLEFEvent{
		Timestamp:          span.EndTime(),
		Message:            span.Name(),
		TraceID:            sc.TraceID().String(),
		SpanID:             sc.SpanID().String(),
		SpanStart:          span.StartTime(),
		SpanKind:           spanKind,
		ResourceAttributes: resourceAttrs(span),
		Properties:         map[string]any{"SpanName": span.Name()},
	}

	if parent := span.Parent(); parent.IsValid() {
		event.ParentSpanID = parent.SpanID().String()
	}

	// Include span attributes as properties
	for _, attr := range span.Attributes() {
		event.Properties[string(attr.Key)] = attr.Value.AsInterface()
	}

	// Set level based on span status
	status := span.Status()

	event.Level = slogseq.CLEFLevelInformation.String()
	if status.Code == codes.Error {
		event.Level = slogseq.CLEFLevelError.String()
		if status.Description != "" {
			event.Message = status.Description
		}
	}

	p.Handler.HandleCLEFEvent(*event)
}

func (p *LoggingSpanProcessor) logOtelEventAsCLEF(span trace.ReadOnlySpan, e trace.Event) {
	sc := span.SpanContext()
	if !sc.IsValid() {
		return
	}

	spanKind := tr.ValidateSpanKind(span.SpanKind()).String()
	event := &slogseq.CLEFEvent{
		Timestamp:          e.Time,
		Message:            e.Name,
		Level:              slogseq.CLEFLevelInformation.String(),
		TraceID:            sc.TraceID().String(),
		SpanID:             sc.SpanID().String(),
		SpanStart:          span.StartTime(),
		SpanKind:           spanKind,
		ResourceAttributes: resourceAttrs(span),
		Properties:         map[string]any{"SpanName": span.Name(), "EventName": e.Name},
	}

	if parent := span.Parent(); parent.IsValid() {
		event.ParentSpanID = parent.SpanID().String()
	}

	for _, attr := range e.Attributes {
		k := string(attr.Key)
		v := attr.Value.AsInterface()
		event.Properties[k] = v
		if k == "exception.message" {
			event.Level = slogseq.CLEFLevelError.String()
			event.Message = fmt.Sprint(v)
		}
		if k == "exception.stacktrace" {
			event.Exception = fmt.Sprint(v)
		}
	}

	p.Handler.HandleCLEFEvent(*event)
}

// resourceAttrs flattens the span's OTel Resource into a map suitable for
// CLEF's @ra field. Returns nil when the resource is empty so the field is
// omitted from the JSON output.
func resourceAttrs(span trace.ReadOnlySpan) map[string]any {
	res := span.Resource()
	if res == nil || res.Len() == 0 {
		return nil
	}

	attrs := res.Attributes()
	out := make(map[string]any, len(attrs))
	for _, kv := range attrs {
		out[string(kv.Key)] = kv.Value.AsInterface()
	}

	return out
}
