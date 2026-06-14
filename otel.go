package slogseq

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace"
	tr "go.opentelemetry.io/otel/trace"
)

var _ trace.SpanProcessor = (*LoggingSpanProcessor)(nil)
var _ trace.SpanExporter = (*LoggingSpanProcessor)(nil)

type LoggingSpanProcessor struct {
	Handler *SeqHandler
}

func (p *LoggingSpanProcessor) OnStart(_ context.Context, _ trace.ReadWriteSpan) {}

func (p *LoggingSpanProcessor) OnEnd(s trace.ReadOnlySpan) {
	events := s.Events()

	for _, e := range events {
		p.logOtelEventAsCLEF(s, e)
	}

	p.logOtelSpanAsCLEF(s)
}

// ForceFlush is a no-op. Events are flushed on the configured interval
// and fully drained when the handler is closed.
func (p *LoggingSpanProcessor) ForceFlush(_ context.Context) error {
	return nil
}

// Shutdown is a no-op. The handler's lifecycle is managed by the caller
// who created it, not by the span processor.
func (p *LoggingSpanProcessor) Shutdown(_ context.Context) error {
	return nil
}

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
	event := &CLEFEvent{
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

	event.Level = CLEFLevelInformation.String()
	if status.Code == codes.Error {
		event.Level = CLEFLevelError.String()
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
	event := &CLEFEvent{
		Timestamp:          e.Time,
		Message:            e.Name,
		Level:              CLEFLevelInformation.String(),
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

	for _, attr := range e.Attributes {
		k := string(attr.Key)
		v := attr.Value.AsInterface()
		event.Properties[k] = v
		if k == "exception.message" {
			event.Level = CLEFLevelError.String()
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

// dottedToNested converts a flat map with dotted keys ("a.b.c") into a
// nested map structure. Used for ResourceAttributes encoding.
func dottedToNested(props map[string]any) map[string]any {
	out := make(map[string]any, len(props))

	for k, v := range props {
		path := strings.Split(k, ".")
		addNested(out, path, v)
	}

	return out
}

func addNested(dst map[string]any, path []string, val any) {
	if len(path) == 1 {
		dst[path[0]] = val

		return
	}

	head := path[0]
	child, ok := dst[head].(map[string]any)
	if !ok {
		child = make(map[string]any)
		dst[head] = child
	}

	addNested(child, path[1:], val)
}
