package slogseq

import "time"

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

	// Level is the severity of the event (e.g. "Information", "Error").
	Level string `json:"@l"`

	// Properties holds structured key-value data attached to the event. These
	// are serialized as top-level CLEF properties, not under a reserved key.
	Properties map[string]any `json:"-"`

	// TraceID is a telemetry trace identifier, if present.
	TraceID string `json:"@tr,omitempty"`

	// SpanID is a telemetry span identifier, if present.
	SpanID string `json:"@sp,omitempty"`

	// SpanStart is the start time of the span, if present.
	SpanStart time.Time `json:"@st,omitzero"`

	// SpanKind is the telemetry span kind ("server", "client"), if present.
	SpanKind string `json:"@sk,omitempty"`

	// ResourceAttributes holds telemetry resource attributes, if present.
	ResourceAttributes map[string]any `json:"@ra,omitempty,omitzero"`

	// ParentSpanID is the span identifier of the parent span, if present.
	ParentSpanID string `json:"@ps,omitempty"`
}

// CLEFLevel represents a CLEF severity level as defined by the Seq server.
type CLEFLevel string

const (
	// CLEFLevelDebug is the lowest standard severity level.
	CLEFLevelDebug CLEFLevel = "Debug"

	// CLEFLevelVerbose is between Debug and Information.
	CLEFLevelVerbose CLEFLevel = "Verbose"

	// CLEFLevelInformation is the default severity level.
	CLEFLevelInformation CLEFLevel = "Information"

	// CLEFLevelWarning indicates a potential problem.
	CLEFLevelWarning CLEFLevel = "Warning"

	// CLEFLevelError indicates a failure.
	CLEFLevelError CLEFLevel = "Error"

	// CLEFLevelFatal indicates an unrecoverable failure.
	CLEFLevelFatal CLEFLevel = "Fatal"
)

// String returns the CLEF level as a string.
func (l CLEFLevel) String() string {
	return string(l)
}
