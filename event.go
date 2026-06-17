package slogseq

import (
	"encoding/json"
	"maps"
	"sort"
	"strings"
	"time"
)

// CLEF does not enforce a strict set of recognized log levels, but these are
// the commonly used log levels which seem to offer the best Seq UI experience.
const (
	CLEFLevelDebug       string = "Debug"
	CLEFLevelVerbose     string = "Verbose"
	CLEFLevelInformation string = "Information"
	CLEFLevelWarning     string = "Warning"
	CLEFLevelError       string = "Error"
	CLEFLevelFatal       string = "Fatal"
)

var _ json.Marshaler = (*CLEFEvent)(nil)

// CLEFEvent represents a log event in CLEF (Compact Log Event Format), the
// JSON-based format used by Seq, with Seq-specific extensions. See
// https://clef-json.org and https://datalust.co/docs/posting-raw-events.
//
// CLEFEvent implements [json.Marshaler] to produce valid CLEF JSON,
// including escaping user properties that start with @.
type CLEFEvent struct {
	// Timestamp is the time the event occurred.
	Timestamp time.Time // required

	// Message is the log message.
	//
	// For multi-line slog messages, this contains only the first line; the
	// remainder goes into [CLEFEvent.Exception].
	Message string // optional

	// Exception holds supplementary detail such as a stack trace or the
	// continuation of a multi-line [CLEFEvent.Message].
	Exception string // optional

	// Level describes the severity of the event (use CLEFLevel* constants).
	Level string // optional

	// Properties holds structured key-value data attached to the event. These
	// are serialized as top-level CLEF properties. Nesting is represented by
	// sub-maps; a key must not be both a leaf value and a parent of other keys.
	//
	// As opposed to [CLEFEvent.ResourceAttributes], dotted keys ("a.b.c": val)
	// are not converted to nested maps but treated as literal top-level keys.
	Properties map[string]any // optional

	// TraceID is a telemetry trace identifier.
	TraceID string // optional

	// SpanID is a telemetry span identifier.
	SpanID string // optional

	// SpanStart is the start time of the telemetry span.
	SpanStart time.Time // optional

	// SpanKind is the telemetry span kind.
	SpanKind string // optional

	// ResourceAttributes holds telemetry resource attributes. Values can be a
	// flat map with dotted keys ("a.b.c": val) or an already-nested map ("a":
	// {"b": {"c": val}}). Dotted keys, as common with telemetry frameworks, are
	// converted to nested maps internally.
	//
	// A key must not be both a leaf value and a parent of other keys (e.g.
	// "a.b" cannot hold a value if "a.b.c" also exists in dotted form, or "b"
	// cannot be a scalar if it also contains a sub-map in pre-nested form).
	ResourceAttributes map[string]any // optional

	// ParentSpanID is the identifier of the parent telemetry span.
	ParentSpanID string // optional
}

// MarshalJSON produces a valid CLEF JSON object, spreading [CLEFEvent.Properties]
// as top-level keys and escaping any user properties that start with @.
func (e CLEFEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(encodeEvent(e)) //nolint:wrapcheck
}

// encodeEvent converts a [CLEFEvent] into a flat map ready for JSON marshaling.
func encodeEvent(e CLEFEvent) map[string]any {
	topLevel := make(map[string]any, len(e.Properties)+10) //nolint:mnd
	maps.Copy(topLevel, e.Properties)

	// Escape any @ to @@ (according to specification).
	for k, v := range topLevel {
		if strings.HasPrefix(k, "@") && !strings.HasPrefix(k, "@@") {
			topLevel["@"+k] = v
			delete(topLevel, k)
		}
	}

	topLevel["@t"] = e.Timestamp.Format(time.RFC3339Nano)

	if e.Message != "" {
		topLevel["@m"] = e.Message
	}
	if e.Level != "" {
		topLevel["@l"] = e.Level
	}
	if e.Exception != "" {
		topLevel["@x"] = e.Exception
	}
	if !e.SpanStart.IsZero() {
		topLevel["@st"] = e.SpanStart.Format(time.RFC3339Nano)
	}
	if e.TraceID != "" {
		topLevel["@tr"] = e.TraceID
	}
	if e.SpanID != "" {
		topLevel["@sp"] = e.SpanID
	}
	if e.ParentSpanID != "" {
		topLevel["@ps"] = e.ParentSpanID
	}
	if len(e.ResourceAttributes) > 0 {
		topLevel["@ra"] = dottedToNested(e.ResourceAttributes)
	}
	if e.SpanKind != "" {
		topLevel["@sk"] = e.SpanKind
	}

	return topLevel
}

// dottedToNested converts a flat map with dotted keys ("a.b.c") into a
// nested map structure. Used for [CLEFEvent.ResourceAttributes] encoding.
func dottedToNested(props map[string]any) map[string]any {
	out := make(map[string]any, len(props))

	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		path := strings.Split(k, ".")
		addNested(out, path, props[k])
	}

	return out
}

// addNested sets val at the position described by path within dst, creating
// intermediate maps as needed. If a non-map value already occupies a path
// segment, it is replaced with a new map - see [nestInto].
func addNested(dst map[string]any, path []string, val any) {
	if len(path) == 0 {
		return
	}

	dst = nestInto(dst, path[:len(path)-1])
	dst[path[len(path)-1]] = val
}
