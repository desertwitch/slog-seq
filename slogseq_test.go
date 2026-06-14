package slogseq

import (
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Expectation: NewLogger should return a non-nil logger and handler.
func Test_NewLogger_ReturnsNonNil_Success(t *testing.T) {
	t.Parallel()

	logger, handler := NewLogger("http://fake", withNoFlush())
	defer handler.Close()

	require.NotNil(t, logger)
	require.NotNil(t, handler)
}

// Expectation: NewLogger with no options should apply all defaults.
func Test_NewLogger_Defaults_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", withNoFlush())
	defer handler.Close()

	require.Equal(t, "http://fake", handler.seqURL)
	require.Empty(t, handler.apiKey)
	require.Equal(t, defaultBatchSize, handler.batchSize)
	require.Equal(t, defaultFlushInterval, handler.flushInterval)
	require.Equal(t, defaultWorkerCount, handler.workerCount)
	require.True(t, handler.nonBlocking, "default should be non-blocking")
	require.False(t, handler.disableTLSVerify)
	require.Equal(t, slog.SourceKey, handler.sourceKey)
	require.Nil(t, handler.options.Level)
	require.Nil(t, handler.options.ReplaceAttr)
	require.False(t, handler.options.AddSource)
}

// Expectation: WithAPIKey should set the API key on the handler.
func Test_WithAPIKey_SetsAPIKey_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey("my-secret-key"),
		withNoFlush(),
	)
	defer handler.Close()

	require.Equal(t, "my-secret-key", handler.apiKey)
}

// Expectation: WithAPIKey with empty string should set an empty API key.
func Test_WithAPIKey_EmptyString_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		withNoFlush(),
	)
	defer handler.Close()

	require.Empty(t, handler.apiKey)
}

// Expectation: WithBatchSize should set the batch size on the handler.
func Test_WithBatchSize_SetsBatchSize_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithBatchSize(100),
		withNoFlush(),
	)
	defer handler.Close()

	require.Equal(t, 100, handler.batchSize)
}

// Expectation: WithBatchSize of 1 should flush on every event.
func Test_WithBatchSize_One_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithBatchSize(1),
		withNoFlush(),
	)
	defer handler.Close()

	require.Equal(t, 1, handler.batchSize)
}

// Expectation: WithBatchSize with zero should fall back to the default.
func Test_WithBatchSize_Zero_FallsBackToDefault_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithBatchSize(0),
		withNoFlush(),
	)
	defer handler.Close()

	require.Equal(t, defaultBatchSize, handler.batchSize)
}

// Expectation: WithBatchSize with negative value should fall back to the default.
func Test_WithBatchSize_Negative_FallsBackToDefault_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithBatchSize(-10),
		withNoFlush(),
	)
	defer handler.Close()

	require.Equal(t, defaultBatchSize, handler.batchSize)
}

// Expectation: WithFlushInterval should set the flush interval on the handler.
func Test_WithFlushInterval_SetsInterval_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithFlushInterval(10*time.Second),
		withNoFlush(),
	)
	defer handler.Close()

	require.Equal(t, 10*time.Second, handler.flushInterval)
}

// Expectation: WithFlushInterval with zero should fall back to the default.
func Test_WithFlushInterval_Zero_FallsBackToDefault_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithFlushInterval(0),
		withNoFlush(),
	)
	defer handler.Close()

	require.Equal(t, defaultFlushInterval, handler.flushInterval)
}

// Expectation: WithFlushInterval with negative value should fall back to the default.
func Test_WithFlushInterval_Negative_FallsBackToDefault_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithFlushInterval(-5*time.Second),
		withNoFlush(),
	)
	defer handler.Close()

	require.Equal(t, defaultFlushInterval, handler.flushInterval)
}

// Expectation: WithHandlerOptions should set the slog handler options.
func Test_WithHandlerOptions_SetsOptions_Success(t *testing.T) {
	t.Parallel()

	replaceAttr := func(_ []string, a slog.Attr) slog.Attr { return a }

	_, handler := NewLogger("http://fake",
		WithHandlerOptions(&slog.HandlerOptions{
			Level:       slog.LevelWarn,
			AddSource:   true,
			ReplaceAttr: replaceAttr,
		}),
		withNoFlush(),
	)
	defer handler.Close()

	require.Equal(t, slog.LevelWarn, handler.options.Level.Level())
	require.True(t, handler.options.AddSource)
	require.NotNil(t, handler.options.ReplaceAttr)
}

// Expectation: WithHandlerOptions should copy the struct, not hold a reference to the original.
func Test_WithHandlerOptions_CopiesStruct_Success(t *testing.T) {
	t.Parallel()

	opts := &slog.HandlerOptions{Level: slog.LevelWarn}

	_, handler := NewLogger("http://fake",
		WithHandlerOptions(opts),
		withNoFlush(),
	)
	defer handler.Close()

	// Mutating the original should not affect the handler.
	opts.Level = slog.LevelError

	require.Equal(t, slog.LevelWarn, handler.options.Level.Level())
}

// Expectation: WithInsecure should enable TLS skip verify.
func Test_WithInsecure_EnablesSkipVerify_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithInsecure(),
		withNoFlush(),
	)
	defer handler.Close()

	require.True(t, handler.disableTLSVerify)
}

// Expectation: WithHTTPClient should set the HTTP client on the handler.
func Test_WithHTTPClient_SetsClient_Success(t *testing.T) {
	t.Parallel()

	custom := &http.Client{Timeout: 42 * time.Second}

	_, handler := NewLogger("http://fake",
		WithHTTPClient(custom),
		withNoFlush(),
	)
	defer handler.Close()

	require.Same(t, custom, handler.client)
}

// Expectation: WithHTTPClient should prevent start() from creating a default client.
func Test_WithHTTPClient_PreventsDefaultClient_Success(t *testing.T) {
	t.Parallel()

	custom := &http.Client{}

	_, handler := NewLogger("http://fake",
		WithHTTPClient(custom),
		withNoFlush(),
	)
	defer handler.Close()

	// After start(), the client should still be the custom one.
	require.Same(t, custom, handler.client)
}

// Expectation: When WithHTTPClient is not set, start() should create a default client.
func Test_NoHTTPClient_StartCreatesDefault_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		withNoFlush(),
	)
	defer handler.Close()

	require.NotNil(t, handler.client)
}

// Expectation: WithGlobalAttrs should add attributes to every log event.
func Test_WithGlobalAttrs_AddsToHandlerAttrs_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithGlobalAttrs(
			slog.String("env", "production"),
			slog.Int("version", 3),
		),
		WithWorkers(1),
		withNoFlush(),
	)
	defer handler.Close()

	require.Len(t, handler.handlerAttrs, 1)
	require.Len(t, handler.handlerAttrs[0].attrs, 2)
	require.Equal(t, "env", handler.handlerAttrs[0].attrs[0].Key)
	require.Equal(t, "version", handler.handlerAttrs[0].attrs[1].Key)
}

// Expectation: WithGlobalAttrs should attach attrs with no group scope.
func Test_WithGlobalAttrs_NoGroupScope_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithGlobalAttrs(slog.String("k", "v")),
		withNoFlush(),
	)
	defer handler.Close()

	require.Empty(t, handler.handlerAttrs[0].groups, "global attrs should have no group scope")
}

// Expectation: Multiple WithGlobalAttrs calls should accumulate.
func Test_WithGlobalAttrs_Multiple_Accumulates_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithGlobalAttrs(slog.String("a", "1")),
		WithGlobalAttrs(slog.String("b", "2")),
		withNoFlush(),
	)
	defer handler.Close()

	require.Len(t, handler.handlerAttrs, 2)
	require.Equal(t, "a", handler.handlerAttrs[0].attrs[0].Key)
	require.Equal(t, "b", handler.handlerAttrs[1].attrs[0].Key)
}

// Expectation: WithGlobalAttrs should appear in emitted events.
func Test_WithGlobalAttrs_AppearsInEvents_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithGlobalAttrs(slog.String("service", "myapp")),
		WithWorkers(1),
		withNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger.Info("test event")

	select {
	case evt := <-handler.workers[0].eventsCh:
		require.Equal(t, "myapp", evt.Properties["service"])
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

// Expectation: WithGlobalAttrs should resolve LogValuer types eagerly.
func Test_WithGlobalAttrs_ResolvesLogValuer_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithGlobalAttrs(slog.Any("p", payload{ID: 7, Name: "eager"})),
		WithWorkers(1),
		withNoFlush(),
	)
	defer handler.Close()

	// The LogValuer should have been resolved to a GroupValue at option time.
	require.Equal(t, slog.KindGroup, handler.handlerAttrs[0].attrs[0].Value.Kind())
}

// Expectation: WithSourceKey should set the source key on the handler.
func Test_WithSourceKey_SetsKey_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithSourceKey("caller"),
		withNoFlush(),
	)
	defer handler.Close()

	require.Equal(t, "caller", handler.sourceKey)
}

// Expectation: WithWorkers should set the worker count.
func Test_WithWorkers_SetsCount_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(4),
		withNoFlush(),
	)
	defer handler.Close()

	require.Equal(t, 4, handler.workerCount)
	require.Len(t, handler.workers, 4)
}

// Expectation: WithWorkers with zero should fall back to the default worker count.
func Test_WithWorkers_Zero_FallsBackToDefault_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(0),
		withNoFlush(),
	)
	defer handler.Close()

	require.Equal(t, defaultWorkerCount, handler.workerCount)
}

// Expectation: WithWorkers with negative value should fall back to the default worker count.
func Test_WithWorkers_Negative_FallsBackToDefault_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(-5),
		withNoFlush(),
	)
	defer handler.Close()

	require.Equal(t, defaultWorkerCount, handler.workerCount)
}

// Expectation: WithNonBlocking true should set non-blocking mode.
func Test_WithNonBlocking_True_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithNonBlocking(true),
		withNoFlush(),
	)
	defer handler.Close()

	require.True(t, handler.nonBlocking)
}

// Expectation: WithNonBlocking false should set blocking mode.
func Test_WithNonBlocking_False_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithNonBlocking(false),
		withNoFlush(),
	)
	defer handler.Close()

	require.False(t, handler.nonBlocking)
}

// Expectation: WithErrorHandlerFunc should set the error handler function.
func Test_WithErrorHandlerFunc_SetsFunc_Success(t *testing.T) {
	t.Parallel()

	called := false
	fn := func(_ error) { called = true }

	_, handler := NewLogger("http://fake",
		WithErrorHandlerFunc(fn),
		withNoFlush(),
	)
	defer handler.Close()

	require.NotNil(t, handler.errorHandlerFunc)
	handler.errorHandlerFunc(nil)
	require.True(t, called)
}

// Expectation: WithErrorHandlerFunc should prevent start() from setting the default no-op.
func Test_WithErrorHandlerFunc_PreventsDefault_Success(t *testing.T) {
	t.Parallel()

	var captured error
	fn := func(err error) { captured = err }

	_, handler := NewLogger("http://fake",
		WithErrorHandlerFunc(fn),
		WithHTTPClient(GetHTTPClientMock(500, "error", func() {})),
		WithBatchSize(1),
		WithFlushInterval(time.Millisecond),
	)

	logger := slog.New(handler)
	logger.Info("trigger error")

	time.Sleep(50 * time.Millisecond)
	_ = handler.Close()

	require.Error(t, captured, "custom error handler should have been called, not the default")
}

// Expectation: When no error handler is set, start() should assign a default that doesn't panic.
func Test_NoErrorHandlerFunc_DefaultNoPanic_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithHTTPClient(GetHTTPClientMock(500, "error", func() {})),
		WithBatchSize(1),
		WithFlushInterval(time.Millisecond),
	)

	logger := slog.New(handler)
	logger.Info("trigger error")

	time.Sleep(50 * time.Millisecond)

	require.NotPanics(t, func() {
		_ = handler.Close()
	})
}

// Expectation: withNoFlush should set the noFlush flag.
func Test_withNoFlush_SetsFlag_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", withNoFlush())
	defer handler.Close()

	require.True(t, handler.noFlush)
}

// Expectation: The last option should win when the same option is applied multiple times.
func Test_Options_LastOneWins_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey("first"),
		WithAPIKey("second"),
		WithBatchSize(10),
		WithBatchSize(20),
		WithFlushInterval(1*time.Second),
		WithFlushInterval(5*time.Second),
		withNoFlush(),
	)
	defer handler.Close()

	require.Equal(t, "second", handler.apiKey)
	require.Equal(t, 20, handler.batchSize)
	require.Equal(t, 5*time.Second, handler.flushInterval)
}

// Expectation: Options should be applied in order.
func Test_Options_AppliedInOrder_Success(t *testing.T) {
	t.Parallel()

	var order []string

	opt1 := seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		order = append(order, "first")

		return h
	})
	opt2 := seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		order = append(order, "second")

		return h
	})
	opt3 := seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		order = append(order, "third")

		return h
	})

	_, handler := NewLogger("http://fake", opt1, opt2, opt3, withNoFlush())
	defer handler.Close()

	require.Equal(t, []string{"first", "second", "third"}, order)
}
