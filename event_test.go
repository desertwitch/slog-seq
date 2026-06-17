package slogseq

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Expectation: encodeEvent should set all required CLEF keys.
func Test_encodeEvent_RequiredKeys_Success(t *testing.T) {
	t.Parallel()

	now := time.Now()
	e := CLEFEvent{
		Timestamp: now,
		Message:   "hello",
		Level:     "Information",
	}

	m := encodeEvent(e)

	require.Equal(t, now.Format(time.RFC3339Nano), m["@t"])
	require.Equal(t, "hello", m["@m"])
	require.Equal(t, "Information", m["@l"])
}

// Expectation: encodeEvent should include exception when non-empty.
func Test_encodeEvent_WithException_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Error",
		Exception: "stack trace here",
	}

	m := encodeEvent(e)

	require.Equal(t, "stack trace here", m["@x"])
}

// Expectation: encodeEvent should omit exception when empty.
func Test_encodeEvent_EmptyException_Omitted_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
	}

	m := encodeEvent(e)

	_, hasX := m["@x"]
	require.False(t, hasX, "@x should be omitted when exception is empty")
}

// Expectation: encodeEvent should include trace and span IDs when set.
func Test_encodeEvent_TraceAndSpanID_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
		TraceID:   "abc123",
		SpanID:    "def456",
	}

	m := encodeEvent(e)

	require.Equal(t, "abc123", m["@tr"])
	require.Equal(t, "def456", m["@sp"])
}

// Expectation: encodeEvent should omit trace and span IDs when empty.
func Test_encodeEvent_EmptyTraceAndSpanID_Omitted_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
	}

	m := encodeEvent(e)

	_, hasTr := m["@tr"]
	_, hasSp := m["@sp"]
	require.False(t, hasTr)
	require.False(t, hasSp)
}

// Expectation: encodeEvent should include parent span ID when set.
func Test_encodeEvent_ParentSpanID_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp:    time.Now(),
		Message:      "msg",
		Level:        "Information",
		ParentSpanID: "parent123",
	}

	m := encodeEvent(e)

	require.Equal(t, "parent123", m["@ps"])
}

// Expectation: encodeEvent should omit parent span ID when empty.
func Test_encodeEvent_EmptyParentSpanID_Omitted_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
	}

	m := encodeEvent(e)

	_, hasPs := m["@ps"]
	require.False(t, hasPs)
}

// Expectation: encodeEvent should include span start when non-zero.
func Test_encodeEvent_SpanStart_Success(t *testing.T) {
	t.Parallel()

	start := time.Now().Add(-time.Second)
	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
		SpanStart: start,
	}

	m := encodeEvent(e)

	require.Equal(t, start.Format(time.RFC3339Nano), m["@st"])
}

// Expectation: encodeEvent should omit span start when zero.
func Test_encodeEvent_ZeroSpanStart_Omitted_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
	}

	m := encodeEvent(e)

	_, hasSt := m["@st"]
	require.False(t, hasSt)
}

// Expectation: encodeEvent should include span kind when set.
func Test_encodeEvent_SpanKind_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
		SpanKind:  "Server",
	}

	m := encodeEvent(e)

	require.Equal(t, "Server", m["@sk"])
}

// Expectation: encodeEvent should omit span kind when empty.
func Test_encodeEvent_EmptySpanKind_Omitted_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
	}

	m := encodeEvent(e)

	_, hasSk := m["@sk"]
	require.False(t, hasSk)
}

// Expectation: encodeEvent should copy properties into the top-level map.
func Test_encodeEvent_PropertiesCopied_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp:  time.Now(),
		Message:    "msg",
		Level:      "Information",
		Properties: map[string]any{"user": "alice", "count": 42},
	}

	m := encodeEvent(e)

	require.Equal(t, "alice", m["user"])
	require.Equal(t, 42, m["count"])
}

// Expectation: CLEF reserved keys should override user properties with the same name.
func Test_encodeEvent_ReservedKeysOverrideProperties_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp:  time.Now(),
		Message:    "real message",
		Level:      "Information",
		Properties: map[string]any{"@m": "fake message", "@l": "Fake"},
	}

	m := encodeEvent(e)

	require.Equal(t, "real message", m["@m"])
	require.Equal(t, "Information", m["@l"])
}

// Expectation: encodeEvent should include resource attributes when non-empty.
func Test_encodeEvent_ResourceAttributes_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp:          time.Now(),
		Message:            "msg",
		Level:              "Information",
		ResourceAttributes: map[string]any{"service.name": "myapp"},
	}

	m := encodeEvent(e)

	ra, ok := m["@ra"]
	require.True(t, ok, "expected @ra to be set")
	require.NotNil(t, ra)
}

// Expectation: encodeEvent should omit resource attributes when empty.
func Test_encodeEvent_EmptyResourceAttributes_Omitted_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
	}

	m := encodeEvent(e)

	_, hasRa := m["@ra"]
	require.False(t, hasRa)
}

// Expectation: MarshalJSON should produce valid JSON that round-trips back to
// the expected CLEF structure.
func Test_MarshalJSON_RoundTrip_Success(t *testing.T) {
	t.Parallel()

	now := time.Now().Truncate(time.Nanosecond)
	e := CLEFEvent{
		Timestamp: now,
		Message:   "hello",
		Level:     "Information",
		Properties: map[string]any{
			"user":  "alice",
			"count": 42,
		},
	}

	data, err := json.Marshal(e)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(data, &m))

	require.Equal(t, now.Format(time.RFC3339Nano), m["@t"])
	require.Equal(t, "hello", m["@m"])
	require.Equal(t, "Information", m["@l"])
	require.Equal(t, "alice", m["user"])
	require.EqualValues(t, 42, m["count"])
}

// Expectation: encodeEvent should escape user properties starting with @ by
// doubling the prefix to @@.
func Test_encodeEvent_AtSignEscaping_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
		Properties: map[string]any{
			"@custom":  "val1",
			"@another": "val2",
			"normal":   "val3",
		},
	}

	m := encodeEvent(e)

	require.Equal(t, "val1", m["@@custom"], "@ should be escaped to @@")
	require.Equal(t, "val2", m["@@another"], "@ should be escaped to @@")
	require.Equal(t, "val3", m["normal"], "non-@ keys should be unchanged")

	_, hasOriginal1 := m["@custom"]
	_, hasOriginal2 := m["@another"]
	require.False(t, hasOriginal1, "original @custom key should be removed")
	require.False(t, hasOriginal2, "original @another key should be removed")
}

// Expectation: encodeEvent should not double-escape properties already prefixed
// with @@.
func Test_encodeEvent_AlreadyEscaped_NoDoubleEscape_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
		Properties: map[string]any{
			"@@already": "escaped",
		},
	}

	m := encodeEvent(e)

	require.Equal(t, "escaped", m["@@already"], "already-escaped key should stay as @@")
	_, hasTriple := m["@@@already"]
	require.False(t, hasTriple, "should not triple-escape")
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

// Expectation: dottedToNested should produce deterministic output with conflicting keys.
func Test_dottedToNested_ConflictingKeys_Deterministic_Success(t *testing.T) {
	t.Parallel()

	input := map[string]any{
		"a":   "scalar",
		"a.b": "nested",
	}

	// Run multiple times to verify determinism.
	for range 100 {
		result := dottedToNested(input)
		a, ok := result["a"].(map[string]any)
		require.True(t, ok, "a should be a map after conflict resolution")
		require.Equal(t, "nested", a["b"])
	}
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
