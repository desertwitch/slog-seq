package slogseq

import "time"

// CLEF does not enforce strict log levels, but these are the commonly
// used and understood log levels which offer the best Seq UI experience.
const (
	CLEFLevelDebug       string = "Debug"
	CLEFLevelVerbose     string = "Verbose"
	CLEFLevelInformation string = "Information"
	CLEFLevelWarning     string = "Warning"
	CLEFLevelError       string = "Error"
	CLEFLevelFatal       string = "Fatal"
)

// CLEFEvent represents a log event in CLEF (Compact Log Event Format), the
// JSON-based format used by Seq. See https://clef-json.org for the
// specification.
type CLEFEvent struct {
	// Timestamp is the time the event occurred.
	Timestamp time.Time `json:"@t,omitzero"`

	// Message is the log message. For multi-line slog messages, this contains
	// only the first line; the remainder goes into [CLEFEvent.Exception].
	Message string `json:"@m,omitempty"`

	// Exception holds supplementary detail such as a stack trace or the
	// continuation of a multi-line message.
	Exception string `json:"@x,omitempty"`

	// Level describes the severity of the event (see CLEFLevel* constants).
	Level string `json:"@l"`

	// Properties holds structured key-value data attached to the event. These
	// are serialized as top-level CLEF properties, not under a reserved key.
	Properties map[string]any `json:"-"`

	// TraceID is a telemetry trace identifier, if present.
	TraceID string `json:"@tr,omitempty"`

	// SpanID is a telemetry span identifier, if present.
	SpanID string `json:"@sp,omitempty"`

	// SpanStart is the start time of the telemetry span, if present.
	SpanStart time.Time `json:"@st,omitzero"`

	// SpanKind is the telemetry span kind, if present.
	SpanKind string `json:"@sk,omitempty"`

	// ResourceAttributes holds telemetry resource attributes, if present.
	ResourceAttributes map[string]any `json:"@ra,omitempty,omitzero"`

	// ParentSpanID is the identifier of the parent telemetry span, if present.
	ParentSpanID string `json:"@ps,omitempty"`
}
