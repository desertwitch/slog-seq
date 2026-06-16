package slogseq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/slogtest"
	"time"

	"github.com/stretchr/testify/require"
)

// A tiny payload that implements slog.LogValuer.
type payload struct {
	ID   int64
	Name string
}

func (p payload) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int64("id", p.ID),
		slog.String("name", p.Name),
	)
}

type stubError struct{ msg string }

func (e stubError) Error() string { return e.msg }

// stringValuer implements slog.LogValuer returning a string (non-group) value.
type stringValuer string

func (s stringValuer) LogValue() slog.Value {
	return slog.StringValue(string(s))
}

// Expectation: NewLogger should return a non-nil logger and handler.
func Test_NewLogger_ReturnsNonNil_Success(t *testing.T) {
	t.Parallel()

	logger, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	require.NotNil(t, logger)
	require.NotNil(t, handler)
}

// Expectation: NewSeqHandler should return a non-nil handler with all defaults applied.
func Test_NewSeqHandler_Defaults_Success(t *testing.T) {
	t.Parallel()

	handler := NewSeqHandler("http://fake", WithNoFlush())
	defer handler.Close()

	require.NotNil(t, handler)
	require.Equal(t, "http://fake", handler.seqURL)
	require.Empty(t, handler.apiKey)
	require.Equal(t, defaultBatchSize, handler.batchSize)
	require.Equal(t, defaultBufferSize, handler.bufferSize)
	require.Equal(t, defaultRetryBufferSize, handler.retryBufferSize)
	require.Equal(t, defaultFlushInterval, handler.flushInterval)
	require.Equal(t, defaultWorkerCount, handler.workerCount)
	require.False(t, handler.blockingMode, "default should be non-blocking")
	require.False(t, handler.disableTLSVerify)
	require.Equal(t, slog.SourceKey, handler.sourceKey)
	require.Nil(t, handler.options.Level)
	require.Nil(t, handler.options.ReplaceAttr)
	require.False(t, handler.options.AddSource)
}

// Expectation: NewSeqHandler should apply all provided options correctly.
func Test_NewSeqHandler_WithOptions_Success(t *testing.T) {
	t.Parallel()

	handler := NewSeqHandler("http://localhost:5341",
		WithAPIKey("test-key"),
		WithBatchSize(50),
		WithFlushInterval(5*time.Second),
		WithHandlerOptions(&slog.HandlerOptions{Level: slog.LevelWarn}),
	)
	defer handler.Close()

	require.Equal(t, "http://localhost:5341", handler.seqURL)
	require.Equal(t, "test-key", handler.apiKey)
	require.Equal(t, 50, handler.batchSize)
	require.Equal(t, 5*time.Second, handler.flushInterval)
	require.Equal(t, slog.LevelWarn, handler.options.Level.Level())
}

// Expectation: start() should create workers with buffered channels.
func Test_start_CreatesWorkerChannels_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(3),
		WithNoFlush(),
	)
	defer handler.Close()

	require.Len(t, handler.workers, 3)
	for i, w := range handler.workers {
		require.NotNil(t, w.eventsCh, "worker %d should have a channel", i)
		require.Equal(t, handler.bufferSize, cap(w.eventsCh), "worker %d channel capacity", i)
	}
}

// Expectation: The default error handler should not panic when called.
func Test_start_DefaultErrorHandler_NoPanic_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithHTTPClient(GetHTTPClientMock(500, "error", func() {})),
		WithBatchSize(1),
		WithFlushInterval(time.Millisecond),
		// No WithErrorHandler - uses the default no-op.
	)

	logger := slog.New(handler)
	logger.Info("trigger flush")

	// Give the flusher time to attempt sending and hit the error path.
	time.Sleep(50 * time.Millisecond)

	require.NotPanics(t, func() {
		_ = handler.Close()
	})
}

// Expectation: The handler should pass the stdlib slogtest compliance suite.
func Test_SeqHandler_Slogtest_Compliance_Success(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var captured []string

	client := &http.Client{
		Transport: &mockTransport{
			RoundTripFunc: func(req *http.Request) (*http.Response, error) {
				body, _ := io.ReadAll(req.Body)

				mu.Lock()
				for line := range strings.SplitSeq(strings.TrimSpace(string(body)), "\n") {
					if line != "" {
						captured = append(captured, line)
					}
				}
				mu.Unlock()

				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			},
		},
	}

	_, handler := NewLogger("http://fake",
		WithHTTPClient(client),
		WithBatchSize(1),
		WithFlushInterval(time.Millisecond),
	)

	err := slogtest.TestHandler(handler, func() []map[string]any {
		time.Sleep(50 * time.Millisecond)
		_ = handler.Close()

		mu.Lock()
		defer mu.Unlock()

		results := make([]map[string]any, 0, len(captured))
		for _, line := range captured {
			var m map[string]any

			if err := json.Unmarshal([]byte(line), &m); err != nil {
				t.Fatalf("failed to parse CLEF line: %v", err)
			}

			// slogtest expects standard keys
			parsed := make(map[string]any)
			for k, v := range m {
				switch k {
				case "@t":
					// Slog specification expects zero times to be omitted, but
					// CLEF specification requires even zero timestamps be sent.
					// We do this little dance to satisfy the slog test suite...
					if s, ok := v.(string); ok {
						t, err := time.Parse(time.RFC3339Nano, s)
						if err == nil && t.IsZero() {
							break // Omit it for this test.
						}
					}
					parsed["time"] = v
				case "@m":
					parsed["msg"] = v
				case "@l":
					parsed["level"] = v
				default:
					parsed[k] = v
				}
			}

			results = append(results, parsed)
		}

		return results
	})

	require.NoError(t, err)
}

// Expectation: Debug and Info levels should be disabled when minimum level is Warn.
func Test_SeqHandler_Enabled_DebugDisabled_Success(t *testing.T) {
	t.Parallel()

	opts := &slog.HandlerOptions{Level: slog.LevelWarn}
	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithHandlerOptions(opts),
	)
	defer handler.Close()

	require.False(t, handler.Enabled(context.Background(), slog.LevelDebug))
	require.False(t, handler.Enabled(context.Background(), slog.LevelInfo))
	require.True(t, handler.Enabled(context.Background(), slog.LevelWarn))
	require.True(t, handler.Enabled(context.Background(), slog.LevelError))
}

// Expectation: Enabled should return true for all levels when no Level option is set.
func Test_SeqHandler_Enabled_NilLevel_AllEnabled_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithNoFlush(),
	)
	defer handler.Close()

	require.True(t, handler.Enabled(context.Background(), slog.LevelDebug))
	require.True(t, handler.Enabled(context.Background(), slog.LevelInfo))
	require.True(t, handler.Enabled(context.Background(), slog.LevelWarn))
	require.True(t, handler.Enabled(context.Background(), slog.LevelError))
}

// Expectation: Ping should return nil when the Seq server responds with 200 OK.
func Test_SeqHandler_Ping_ReturnsNil_Success(t *testing.T) {
	t.Parallel()

	client := GetHTTPClientMock(200, `{"status":"healthy"}`, func() {})

	handler := NewSeqHandler("http://fake/ingest/clef",
		WithHTTPClient(client),
		WithNoFlush(),
	)
	defer handler.Close()

	err := handler.Ping()
	require.NoError(t, err)
}

// Expectation: Ping should return an error when the Seq server responds with 503.
func Test_SeqHandler_Ping_Returns503_Error(t *testing.T) {
	t.Parallel()

	client := GetHTTPClientMock(503, `{"status":"unavailable"}`, func() {})

	handler := NewSeqHandler("http://fake/ingest/clef",
		WithHTTPClient(client),
		WithNoFlush(),
	)
	defer handler.Close()

	err := handler.Ping()
	require.Error(t, err)
	require.Contains(t, err.Error(), "503")
}

// Expectation: Ping should return an error when the server is unreachable.
func Test_SeqHandler_Ping_Unreachable_Error(t *testing.T) {
	t.Parallel()

	client := &http.Client{
		Transport: &mockTransport{
			RoundTripFunc: func(*http.Request) (*http.Response, error) {
				return nil, errors.New("dial tcp: connect: connection refused")
			},
		},
	}

	handler := NewSeqHandler("http://fake/ingest/clef",
		WithHTTPClient(client),
		WithNoFlush(),
	)
	defer handler.Close()

	err := handler.Ping()
	require.Error(t, err)
	require.Contains(t, err.Error(), "health check")
}

// Expectation: Ping should derive /health from any ingestion path.
func Test_SeqHandler_Ping_DerivesHealthPath_Success(t *testing.T) {
	t.Parallel()

	var capturedPath string
	client := &http.Client{
		Transport: &mockTransport{
			RoundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedPath = req.URL.String()

				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			},
		},
	}

	handler := NewSeqHandler("http://fake/ingest/clef",
		WithHTTPClient(client),
		WithNoFlush(),
	)
	defer handler.Close()

	err := handler.Ping()
	require.NoError(t, err)
	require.Equal(t, "http://fake/health", capturedPath)
}

// Expectation: Ping should send the API key when one is configured.
func Test_SeqHandler_Ping_SendsAPIKey_Success(t *testing.T) {
	t.Parallel()

	var capturedKey string
	client := &http.Client{
		Transport: &mockTransport{
			RoundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedKey = req.Header.Get("X-Seq-ApiKey")

				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			},
		},
	}

	handler := NewSeqHandler("http://fake/ingest/clef",
		WithHTTPClient(client),
		WithAPIKey("my-secret-key"),
		WithNoFlush(),
	)
	defer handler.Close()

	err := handler.Ping()
	require.NoError(t, err)
	require.Equal(t, "my-secret-key", capturedKey)
}

// Expectation: Ping should not send an API key header when none is configured.
func Test_SeqHandler_Ping_NoAPIKey_OmitsHeader_Success(t *testing.T) {
	t.Parallel()

	var hasHeader bool
	client := &http.Client{
		Transport: &mockTransport{
			RoundTripFunc: func(req *http.Request) (*http.Response, error) {
				hasHeader = req.Header.Get("X-Seq-ApiKey") != ""

				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			},
		},
	}

	handler := NewSeqHandler("http://fake/ingest/clef",
		WithHTTPClient(client),
		WithNoFlush(),
	)
	defer handler.Close()

	err := handler.Ping()
	require.NoError(t, err)
	require.False(t, hasHeader)
}

// Expectation: Ping should return an error when the URL is unparseable.
func Test_SeqHandler_Ping_InvalidURL_Error(t *testing.T) {
	t.Parallel()

	handler := NewSeqHandler("://bad-url",
		WithNoFlush(),
	)
	defer handler.Close()

	err := handler.Ping()
	require.Error(t, err)
	require.Contains(t, err.Error(), "seq URL")
}

// Expectation: Ping should strip query parameters from the URL.
func Test_SeqHandler_Ping_StripsQueryParams_Success(t *testing.T) {
	t.Parallel()

	var capturedRawQuery string
	client := &http.Client{
		Transport: &mockTransport{
			RoundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedRawQuery = req.URL.RawQuery

				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			},
		},
	}

	handler := NewSeqHandler("http://fake/ingest/clef?token=abc",
		WithHTTPClient(client),
		WithNoFlush(),
	)
	defer handler.Close()

	err := handler.Ping()
	require.NoError(t, err)
	require.Empty(t, capturedRawQuery)
}

// Expectation: Handle should return nil error on success.
func Test_SeqHandler_Handle_ReturnsNilError_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithNoFlush(),
	)
	defer handler.Close()

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "test", 0)
	err := handler.Handle(context.Background(), r)

	require.NoError(t, err)

	<-handler.Events(0) // drain
}

// Expectation: Handle should send events with the correct message, level, and properties.
func Test_SeqHandler_Handle_CorrectProperties_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithWorkers(1),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)

	logger.Info("Hello, slog-seq!", "user", "alice", "count", 123)

	select {
	case evt := <-handler.Events(0):
		require.Equal(t, "Hello, slog-seq!", evt.Message)
		require.Equal(t, "Information", evt.Level)
		require.Equal(t, "alice", evt.Properties["user"])
		require.Equal(t, int64(123), evt.Properties["count"].(int64))
	case <-time.After(2000 * time.Millisecond):
		t.Fatal("Timed out waiting for log event in eventsCh")
	}
}

// Expectation: WithAttrs should merge attributes into subsequent log events.
func Test_SeqHandler_Handle_WithAttrs_MergesAttributes_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithWorkers(1),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger2 := logger.With("service", "testsvc")

	logger2.Info("WithAttrs test", "version", "1.2.3")

	select {
	case evt := <-handler.Events(0):
		require.Equal(t, "testsvc", evt.Properties["service"])
		require.Equal(t, "1.2.3", evt.Properties["version"])
	case <-time.After(2000 * time.Millisecond):
		t.Fatal("Timed out waiting for WithAttrs event")
	}
}

// Expectation: WithGroup should prefix attribute keys with the group name.
func Test_SeqHandler_Handle_WithGroup_PrefixesKeys_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithWorkers(1),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	grouped := logger.WithGroup("request").With("id", "1234").WithGroup("headers").With("Accept", "application/json")

	grouped.Info("Grouped log")

	select {
	case evt := <-handler.Events(0):
		request := evt.Properties["request"].(map[string]any)
		headers := request["headers"].(map[string]any)
		require.Equal(t, "1234", request["id"])
		require.Equal(t, "application/json", headers["Accept"])
	case <-time.After(2000 * time.Millisecond):
		t.Fatal("Timed out waiting for grouped event")
	}
}

// Expectation: LogValuer types should be resolved before being stored in properties.
func Test_SeqHandler_Handle_LogValuer_Resolved_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger.Info("valuer", "payload", payload{ID: 99, Name: "test"})

	select {
	case evt := <-handler.Events(0):
		p := evt.Properties["payload"].(map[string]any)
		require.Equal(t, int64(99), p["id"])
		require.Equal(t, "test", p["name"])
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

// Expectation: WithGroup as argument and as inline group should produce identical events.
func Test_SeqHandler_Handle_Grouping_ConsistentOutput_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithWorkers(1),
		WithNoFlush(),
	)
	defer handler.Close()

	ctx := context.Background()
	logger := slog.New(handler)
	logger.WithGroup("s").LogAttrs(ctx, slog.LevelInfo, "huba", slog.Int("a", 1), slog.Int("b", 2))
	logger.LogAttrs(ctx, slog.LevelInfo, "huba", slog.Group("s", slog.Int("a", 1), slog.Int("b", 2)))

	event1 := <-handler.Events(0)
	event2 := <-handler.Events(0)

	event1.Timestamp = event2.Timestamp
	require.Equal(t, event1, event2)
}

// Expectation: Named groups should create nested maps, and same-key groups should merge.
func Test_SeqHandler_Handle_NamedGroupMerge_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger.Info("merge test",
		slog.Group("db", slog.String("host", "localhost")),
		slog.Group("db", slog.Int("port", 5432)),
	)

	select {
	case evt := <-handler.Events(0):
		db, ok := evt.Properties["db"].(map[string]any)
		require.True(t, ok, "expected db to be a nested map")
		require.Equal(t, "localhost", db["host"])
		require.Equal(t, int64(5432), db["port"])
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

// Expectation: ReplaceAttr should be able to redact sensitive attribute values.
func Test_SeqHandler_Handle_ReplaceAttr_RedactsPassword_Success(t *testing.T) {
	t.Parallel()

	opts := &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == "password" {
				a.Value = slog.StringValue("*****")
			}

			return a
		},
	}
	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithWorkers(1),
		WithHandlerOptions(opts),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger.Info("Super secret info", "password", "2Fat2Fly")
	logger.WithGroup("secret_info").Info("Wohoo", "password", "secret")

	event1 := <-handler.Events(0)
	event2 := <-handler.Events(0)

	require.Equal(t, "*****", event1.Properties["password"])

	secretInfo := event2.Properties["secret_info"].(map[string]any)
	require.Equal(t, "*****", secretInfo["password"])
}

// Expectation: ReplaceAttr returning an empty key should drop the attribute.
func Test_SeqHandler_Handle_ReplaceAttr_EmptyKey_DropsAttr_Success(t *testing.T) {
	t.Parallel()

	opts := &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == "secret" {
				return slog.Attr{} // empty key = drop
			}

			return a
		},
	}

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithHandlerOptions(opts),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger.Info("filtered", "secret", "hidden", "visible", "shown")

	select {
	case evt := <-handler.Events(0):
		require.Nil(t, evt.Properties["secret"], "dropped attr should not appear")
		require.Equal(t, "shown", evt.Properties["visible"])
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

// Expectation: ReplaceAttr should receive the correct group scope for each attribute.
func Test_SeqHandler_Handle_ReplaceAttrGroupScope_CorrectGroups_Success(t *testing.T) {
	t.Parallel()

	var captured [][]string

	opts := &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			captured = append(captured, append([]string{}, groups...))

			return a
		},
	}

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithHandlerOptions(opts),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger.With("a", 1).WithGroup("g").With("b", 2).Info("test", "c", 3)

	<-handler.Events(0)

	require.GreaterOrEqual(t, len(captured), 3, "expected at least 3 ReplaceAttr calls")
	require.Empty(t, captured[0], "expected no groups for 'a'")
	require.Equal(t, []string{"g"}, captured[1], "expected [g] for 'b'")
	require.Equal(t, []string{"g"}, captured[2], "expected [g] for 'c'")
}

// Expectation: Anonymous groups should inline their attributes into the parent properties.
func Test_SeqHandler_Handle_AnonymousGroup_InlinesAttributes_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)

	logger.Info("anon-group",
		slog.Any("", payload{ID: 42, Name: "keyname"}))

	logger.With("", payload{ID: 42, Name: "keyname"}).
		Info("anon-group")

	evt1 := <-handler.Events(0)
	evt2 := <-handler.Events(0)

	require.Equal(t, int64(42), evt1.Properties["id"])
	require.Equal(t, "keyname", evt1.Properties["name"])

	require.Equal(t, int64(42), evt2.Properties["id"])
	require.Equal(t, "keyname", evt2.Properties["name"])

	evt1.Timestamp = evt2.Timestamp

	require.Equal(t, evt1, evt2)
}

// Expectation: Source information should be added to log events when AddSource is enabled.
func Test_SeqHandler_Handle_AddSource_IncludesSourceInfo_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithSourceKey("gosource"),
		WithHandlerOptions(&slog.HandlerOptions{AddSource: true}),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)

	logger.Info("Hello, slog-seq!", "user", "alice", "count", 123)

	select {
	case evt := <-handler.Events(0):
		require.NotNil(t, evt.Properties["gosource"], "expected gosource to be set")
		source := evt.Properties["gosource"].(*slog.Source)
		require.NotEmpty(t, source.File)
		require.NotZero(t, source.Line)
		require.NotEmpty(t, source.Function)
		require.Contains(t, source.Function, "Test_SeqHandler_Handle_AddSource_IncludesSourceInfo_Success")
	case <-time.After(2000 * time.Millisecond):
		t.Fatal("Timed out waiting for log event in eventsCh")
	}
}

// Expectation: All events should be received across multiple workers.
func Test_SeqHandler_Handle_MultipleWorkers_AllEventsReceived_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(4),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)

	const n = 100
	for i := range n {
		logger.Info("event", "i", i)
	}

	total := 0
	for w := range handler.workers {
	drain:
		for {
			select {
			case <-handler.Events(w):
				total++
			default:
				break drain
			}
		}
	}

	require.Equal(t, n, total)
}

// Expectation: Events should be distributed across all workers, not concentrated on one.
func Test_SeqHandler_Handle_MultipleWorkers_EvenDistribution_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(4),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)

	const n = 1000
	for i := range n {
		logger.Info("event", "i", i)
	}

	counts := make([]int, len(handler.workers))
	for w := range handler.workers {
		for {
			select {
			case <-handler.Events(w):
				counts[w]++
			default:
				goto next
			}
		}
	next:
	}

	for w, c := range counts {
		require.NotZero(t, c, "worker %d received no events", w)
	}
}

// Expectation: Concurrent Handle calls should not lose any events.
func Test_SeqHandler_Handle_ConcurrentCalls_NoLostEvents_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(2),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)

	const goroutines = 10
	const perGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			for i := range perGoroutine {
				logger.Info("concurrent", "goroutine", id, "i", i)
			}
		}(g)
	}

	wg.Wait()

	total := 0
	for w := range handler.workers {
		for {
			select {
			case <-handler.Events(w):
				total++
			default:
				goto next
			}
		}
	next:
	}

	expected := goroutines * perGoroutine
	require.Equal(t, expected, total)
}

// Expectation: Concurrent WithAttrs, WithGroup, and Handle calls should not lose events.
func Test_SeqHandler_Handle_ConcurrentWithAttrsAndHandle_NoLostEvents_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(2),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		for i := range 50 {
			logger.Info("base", "i", i)
		}
	}()

	go func() {
		defer wg.Done()
		l := logger.With("service", "svc")
		for i := range 50 {
			l.Info("with-attrs", "i", i)
		}
	}()

	go func() {
		defer wg.Done()
		l := logger.WithGroup("g").With("k", "v")
		for i := range 50 {
			l.Info("with-group", "i", i)
		}
	}()

	wg.Wait()

	total := 0
	for w := range handler.workers {
		for {
			select {
			case <-handler.Events(w):
				total++
			default:
				goto next
			}
		}
	next:
	}

	require.Equal(t, 150, total)
}

// Expectation: Non-blocking mode should drop events when the channel is full.
func Test_SeqHandler_Handle_NonBlocking_DropsOnFullChannel_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)

	for i := range handler.bufferSize + 500 {
		logger.Info("flood", "i", i)
	}

	count := 0
	for {
		select {
		case <-handler.Events(0):
			count++
		default:
			goto done
		}
	}
done:

	require.LessOrEqual(t, count, handler.bufferSize)
	require.NotZero(t, count, "expected some events to be received")
}

// Expectation: Multi-line messages should split into message (first line) and exception (rest).
func Test_SeqHandler_Handle_MultilineMessage_SplitsException_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger.Info("first line\nsecond line\nthird line")

	select {
	case evt := <-handler.Events(0):
		require.Equal(t, "first line", evt.Message)
		require.Equal(t, "second line\nthird line", evt.Exception)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

// Expectation: A single-line message should produce an empty exception.
func Test_SeqHandler_Handle_SingleLineMessage_EmptyException_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger.Info("no newline here")

	select {
	case evt := <-handler.Events(0):
		require.Equal(t, "no newline here", evt.Message)
		require.Empty(t, evt.Exception)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

// Expectation: A message that is just a newline should produce an empty message and empty exception.
func Test_SeqHandler_Handle_OnlyNewline_EmptyMessageAndException_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger.Info("\n")

	select {
	case evt := <-handler.Events(0):
		require.Empty(t, evt.Message)
		require.Empty(t, evt.Exception)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

// Expectation: Error values in attributes should be converted to their string representation.
func Test_SeqHandler_Handle_ErrorAttr_ConvertedToString_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger.Info("has error", "err", errors.New("something broke"))

	select {
	case evt := <-handler.Events(0):
		require.Equal(t, "something broke", evt.Properties["err"])
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

// Expectation: An error attr inside a group should be converted to its string representation.
func Test_SeqHandler_Handle_ErrorAttrInGroup_ConvertedToString_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger.Info("grouped error",
		slog.Group("details", slog.Any("err", errors.New("nested failure"))))

	select {
	case evt := <-handler.Events(0):
		details := evt.Properties["details"].(map[string]any)
		require.Equal(t, "nested failure", details["err"])
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

// Expectation: Close should complete without error.
func Test_SeqHandler_Close_NoError_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
	)

	err := handler.Close()

	require.NoError(t, err)
}

// Expectation: Calling Close twice should not return an error.
func Test_SeqHandler_Close_DoubleClose_NoError_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
	)

	err := handler.Close()
	require.NoError(t, err, "first Close returned error")

	err = handler.Close()
	require.NoError(t, err, "second Close returned error")
}

// Expectation: Logging after Close should not panic.
func Test_SeqHandler_Close_LogAfterClose_NoPanic_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithNoFlush(),
	)

	_ = handler.Close()

	logger := slog.New(handler)
	require.NotPanics(t, func() {
		logger.Info("after close", "key", "value")
	})
}

// Expectation: Close should unblock senders that are blocked on a full channel.
func Test_SeqHandler_Close_BlockingClose_UnblocksSenders_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithBlocking(),
		WithNoFlush(),
	)

	logger := slog.New(handler)

	for range handler.bufferSize {
		logger.Info("fill")
	}

	done := make(chan struct{})
	go func() {
		logger.Info("blocked")
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)

	_ = handler.Close()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("blocked sender was not unblocked by Close")
	}
}

// Expectation: WithAttrs with empty slice should return the same handler.
func Test_SeqHandler_WithAttrs_EmptySlice_ReturnsSame_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithNoFlush(),
	)
	defer handler.Close()

	h2 := handler.WithAttrs(nil)
	require.Same(t, handler, h2)

	h3 := handler.WithAttrs([]slog.Attr{})
	require.Same(t, handler, h3)
}

// Expectation: Handler attrs from WithAttrs should not leak between derived handlers.
func Test_SeqHandler_WithAttrs_DoesNotLeakToSibling_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	l1 := logger.With("from", "l1")
	l2 := logger.With("from", "l2")

	l1.Info("msg1")
	l2.Info("msg2")

	evt1 := <-handler.Events(0)
	evt2 := <-handler.Events(0)

	require.Equal(t, "l1", evt1.Properties["from"])
	require.Equal(t, "l2", evt2.Properties["from"])
}

// Expectation: WithAttrs should return a new handler, not the same pointer.
func Test_SeqHandler_WithAttrs_ReturnsNewHandler_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	h2 := handler.WithAttrs([]slog.Attr{slog.String("k", "v")}).(*SeqHandler)

	require.NotSame(t, handler, h2)
}

// Expectation: WithAttrs should allocate a new handlerAttrs slice, not share the backing array.
func Test_SeqHandler_WithAttrs_CopiesHandlerAttrs_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	h2 := handler.WithAttrs([]slog.Attr{slog.String("a", "1")}).(*SeqHandler)
	h3 := handler.WithAttrs([]slog.Attr{slog.String("b", "2")}).(*SeqHandler)

	require.NotSame(t, &h2.handlerAttrs[0], &h3.handlerAttrs[0])
	require.Empty(t, handler.handlerAttrs, "parent should be unchanged")
	require.Len(t, h2.handlerAttrs, 1)
	require.Len(t, h3.handlerAttrs, 1)
	require.Equal(t, "a", h2.handlerAttrs[0].attrs[0].Key)
	require.Equal(t, "b", h3.handlerAttrs[0].attrs[0].Key)
}

// Expectation: Chained WithAttrs calls should each produce independent slices.
func Test_SeqHandler_WithAttrs_ChainedCopiesIndependent_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	h2 := handler.WithAttrs([]slog.Attr{slog.String("a", "1")}).(*SeqHandler)
	h3 := h2.WithAttrs([]slog.Attr{slog.String("b", "2")}).(*SeqHandler)

	require.Empty(t, handler.handlerAttrs)
	require.Len(t, h2.handlerAttrs, 1)
	require.Len(t, h3.handlerAttrs, 2)

	// Mutating h3's slice must not affect h2.
	h3.handlerAttrs[0] = attrSet{}
	require.Equal(t, "a", h2.handlerAttrs[0].attrs[0].Key, "h2 must be unaffected by h3 mutation")
}

// Expectation: WithAttrs should snapshot handlerGroups at call time, not share the slice.
func Test_SeqHandler_WithAttrs_SnapshotsGroups_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	g1 := handler.WithGroup("g1").(*SeqHandler)
	a1 := g1.WithAttrs([]slog.Attr{slog.String("k", "v")}).(*SeqHandler)

	// The attrSet should carry a groups snapshot of ["g1"].
	require.Equal(t, []string{"g1"}, a1.handlerAttrs[0].groups)

	// Adding another group to g1 after the fact must not change a1's snapshot.
	g1g2 := g1.WithGroup("g2").(*SeqHandler)
	require.Equal(t, []string{"g1"}, a1.handlerAttrs[0].groups, "snapshot must be independent")
	require.Equal(t, []string{"g1", "g2"}, g1g2.handlerGroups)
}

// Expectation: The shared pointer should be the same across all derived handlers.
func Test_SeqHandler_WithAttrs_SharesSharedPointer_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	h2 := handler.WithAttrs([]slog.Attr{slog.String("k", "v")}).(*SeqHandler)

	require.Same(t, handler.shared, h2.shared)
}

// Expectation: WithGroup with empty name should return the same handler.
func Test_SeqHandler_WithGroup_EmptyName_ReturnsSame_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithNoFlush(),
	)
	defer handler.Close()

	h2 := handler.WithGroup("")
	require.Same(t, handler, h2)
}

// Expectation: Groups from WithGroup should not leak between derived handlers.
func Test_SeqHandler_WithGroup_DoesNotLeakToSibling_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	l1 := logger.WithGroup("g1").With("k", "v1")
	l2 := logger.WithGroup("g2").With("k", "v2")

	l1.Info("msg1")
	l2.Info("msg2")

	evt1 := <-handler.Events(0)
	evt2 := <-handler.Events(0)

	require.Contains(t, evt1.Properties, "g1")
	require.NotContains(t, evt1.Properties, "g2")

	require.Contains(t, evt2.Properties, "g2")
	require.NotContains(t, evt2.Properties, "g1")
}

// Expectation: WithGroup should return a new handler, not the same pointer.
func Test_SeqHandler_WithGroup_ReturnsNewHandler_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	h2 := handler.WithGroup("g").(*SeqHandler)

	require.NotSame(t, handler, h2)
}

// Expectation: WithGroup should allocate a new handlerGroups slice, not share the backing array.
func Test_SeqHandler_WithGroup_CopiesHandlerGroups_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	h2 := handler.WithGroup("g1").(*SeqHandler)
	h3 := handler.WithGroup("g2").(*SeqHandler)

	require.Empty(t, handler.handlerGroups, "parent should be unchanged")
	require.Equal(t, []string{"g1"}, h2.handlerGroups)
	require.Equal(t, []string{"g2"}, h3.handlerGroups)

	// Verify the backing arrays are different.
	h2.handlerGroups[0] = "mutated"
	require.Equal(t, []string{"g2"}, h3.handlerGroups, "h3 must not be affected by h2 mutation")
}

// Expectation: Chained WithGroup calls should each produce independent slices.
func Test_SeqHandler_WithGroup_ChainedCopiesIndependent_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	h2 := handler.WithGroup("a").(*SeqHandler)
	h3 := h2.WithGroup("b").(*SeqHandler)

	require.Empty(t, handler.handlerGroups)
	require.Equal(t, []string{"a"}, h2.handlerGroups)
	require.Equal(t, []string{"a", "b"}, h3.handlerGroups)

	h3.handlerGroups[0] = "mutated"
	require.Equal(t, "a", h2.handlerGroups[0], "h2 must be unaffected by h3 mutation")
}

// Expectation: The shared pointer should be the same across all WithGroup-derived handlers.
func Test_SeqHandler_WithGroup_SharesSharedPointer_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	h2 := handler.WithGroup("g").(*SeqHandler)

	require.Same(t, handler.shared, h2.shared)
}

// Expectation: nestInto should create intermediate maps and return the innermost one.
func Test_nestInto_CreatesIntermediateMaps_Success(t *testing.T) {
	t.Parallel()

	dst := make(map[string]any)
	inner := nestInto(dst, []string{"a", "b", "c"})
	inner["key"] = "value"

	a := dst["a"].(map[string]any)
	b := a["b"].(map[string]any)
	c := b["c"].(map[string]any)
	require.Equal(t, "value", c["key"])
}

// Expectation: nestInto with empty groups should return the original map.
func Test_nestInto_EmptyGroups_ReturnsSameMap_Success(t *testing.T) {
	t.Parallel()

	dst := make(map[string]any)
	result := nestInto(dst, nil)
	result["key"] = "value"

	require.Equal(t, "value", dst["key"])
}

// Expectation: nestInto should reuse existing intermediate maps rather than overwriting them.
func Test_nestInto_ReusesExistingMaps_Success(t *testing.T) {
	t.Parallel()

	dst := make(map[string]any)
	existing := map[string]any{"existing": true}
	dst["a"] = existing

	inner := nestInto(dst, []string{"a"})
	inner["new"] = true

	a := dst["a"].(map[string]any)
	require.True(t, a["existing"].(bool))
	require.True(t, a["new"].(bool))
}

// Expectation: A single group should create one nested map.
func Test_nestInto_SingleGroup_Success(t *testing.T) {
	t.Parallel()

	dst := make(map[string]any)
	inner := nestInto(dst, []string{"a"})
	inner["k"] = "v"

	child := dst["a"].(map[string]any)
	require.Equal(t, "v", child["k"])
}

// Expectation: Multiple groups should create a chain of nested maps.
func Test_nestInto_DeepNesting_Success(t *testing.T) {
	t.Parallel()

	dst := make(map[string]any)
	inner := nestInto(dst, []string{"a", "b", "c", "d", "e"})
	inner["leaf"] = true

	cur := dst
	for _, g := range []string{"a", "b", "c", "d", "e"} {
		next, ok := cur[g].(map[string]any)
		require.True(t, ok, "expected map at key %q", g)
		cur = next
	}
	require.True(t, cur["leaf"].(bool))
}

// Expectation: Calling nestInto twice with overlapping prefixes should merge, not overwrite.
func Test_nestInto_MergesExistingKeys_Success(t *testing.T) {
	t.Parallel()

	dst := make(map[string]any)

	inner1 := nestInto(dst, []string{"a", "b"})
	inner1["first"] = 1

	inner2 := nestInto(dst, []string{"a", "b"})
	inner2["second"] = 2

	ab := dst["a"].(map[string]any)["b"].(map[string]any)
	require.Equal(t, 1, ab["first"])
	require.Equal(t, 2, ab["second"])
}

// Expectation: Diverging paths should create separate branches under the same root.
func Test_nestInto_DivergingPaths_Success(t *testing.T) {
	t.Parallel()

	dst := make(map[string]any)

	nestInto(dst, []string{"a", "b"})["x"] = 1
	nestInto(dst, []string{"a", "c"})["y"] = 2

	a := dst["a"].(map[string]any)
	require.Equal(t, 1, a["b"].(map[string]any)["x"])
	require.Equal(t, 2, a["c"].(map[string]any)["y"])
}

// Expectation: Group names containing dots should be treated as literal keys, not split.
func Test_nestInto_DotsInGroupName_PreservedAsLiteralKey_Success(t *testing.T) {
	t.Parallel()

	dst := make(map[string]any)
	inner := nestInto(dst, []string{"a.b.c"})
	inner["k"] = "v"

	// The key should be the literal string "a.b.c", not nested a -> b -> c.
	child, ok := dst["a.b.c"].(map[string]any)
	require.True(t, ok, "expected literal dotted key")
	require.Equal(t, "v", child["k"])

	// Ensure no split happened.
	_, hasA := dst["a"]
	require.False(t, hasA, "should not have split on dots")
}

// Expectation: Group names with special characters should be preserved as-is.
func Test_nestInto_SpecialCharGroupNames_Success(t *testing.T) {
	t.Parallel()

	dst := make(map[string]any)
	names := []string{"with spaces", "with/slash", "with=equals", "日本語"}
	inner := nestInto(dst, names)
	inner["k"] = true

	cur := dst
	for _, n := range names {
		next, ok := cur[n].(map[string]any)
		require.True(t, ok, "expected map at key %q", n)
		cur = next
	}
	require.True(t, cur["k"].(bool))
}

// Expectation: If an existing key holds a non-map value, nestInto should overwrite it with a map.
func Test_nestInto_OverwritesNonMapValue_Success(t *testing.T) {
	t.Parallel()

	dst := map[string]any{"a": "not a map"}
	inner := nestInto(dst, []string{"a"})
	inner["k"] = "v"

	child, ok := dst["a"].(map[string]any)
	require.True(t, ok, "should have replaced string with map")
	require.Equal(t, "v", child["k"])
}

// Expectation: A simple string attr should pass through unchanged.
func Test_resolveAttr_SimpleString_PassThrough_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	a := slog.String("key", "value")
	resolved, ok := handler.resolveAttr(nil, a)

	require.True(t, ok)
	require.Equal(t, "key", resolved.Key)
	require.Equal(t, "value", resolved.Value.String())
}

// Expectation: An error value should be converted to its string representation.
func Test_resolveAttr_ErrorValue_ConvertedToString_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	a := slog.Any("err", stubError{"boom"})
	resolved, ok := handler.resolveAttr(nil, a)

	require.True(t, ok)
	require.Equal(t, slog.KindString, resolved.Value.Kind())
	require.Equal(t, "boom", resolved.Value.String())
}

// Expectation: ReplaceAttr that returns empty key should signal the attr should be dropped.
func Test_resolveAttr_ReplaceAttrDrops_ReturnsFalse_Success(t *testing.T) {
	t.Parallel()

	opts := &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == "drop" {
				return slog.Attr{}
			}

			return a
		},
	}

	_, handler := NewLogger("http://fake", WithHandlerOptions(opts), WithNoFlush())
	defer handler.Close()

	a := slog.String("drop", "me")
	_, ok := handler.resolveAttr(nil, a)

	require.False(t, ok)
}

// Expectation: ReplaceAttr should receive the correct groups slice.
func Test_resolveAttr_ReplaceAttrReceivesGroups_Success(t *testing.T) {
	t.Parallel()

	var receivedGroups []string

	opts := &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			receivedGroups = groups

			return a
		},
	}

	_, handler := NewLogger("http://fake", WithHandlerOptions(opts), WithNoFlush())
	defer handler.Close()

	groups := []string{"a", "b"}
	handler.resolveAttr(groups, slog.String("k", "v"))

	require.Equal(t, []string{"a", "b"}, receivedGroups)
}

// Expectation: ReplaceAttr should be able to rename a key.
func Test_resolveAttr_ReplaceAttrRenamesKey_Success(t *testing.T) {
	t.Parallel()

	opts := &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == "old" {
				a.Key = "new"
			}

			return a
		},
	}

	_, handler := NewLogger("http://fake", WithHandlerOptions(opts), WithNoFlush())
	defer handler.Close()

	a := slog.String("old", "val")
	resolved, ok := handler.resolveAttr(nil, a)

	require.True(t, ok)
	require.Equal(t, "new", resolved.Key)
}

// Expectation: Group-kind values should pass through without ReplaceAttr being called.
func Test_resolveAttr_GroupKind_SkipsReplaceAttr_Success(t *testing.T) {
	t.Parallel()

	called := false
	opts := &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			called = true

			return a
		},
	}

	_, handler := NewLogger("http://fake", WithHandlerOptions(opts), WithNoFlush())
	defer handler.Close()

	a := slog.Group("g", slog.String("inner", "v"))
	resolved, ok := handler.resolveAttr(nil, a)

	require.True(t, ok)
	require.Equal(t, slog.KindGroup, resolved.Value.Kind())
	require.False(t, called, "ReplaceAttr should not be called for group-kind attrs")
}

// Expectation: A LogValuer that resolves to a group should skip ReplaceAttr entirely.
func Test_resolveAttr_LogValuerResolvesToGroup_SkipsReplaceAttr_Success(t *testing.T) {
	t.Parallel()

	called := false
	opts := &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			called = true

			return a
		},
	}

	_, handler := NewLogger("http://fake", WithHandlerOptions(opts), WithNoFlush())
	defer handler.Close()

	// payload resolves to a GroupValue, so resolveAttr should return early
	// before reaching ReplaceAttr.
	a := slog.Any("p", payload{ID: 1, Name: "test"})
	resolved, ok := handler.resolveAttr(nil, a)

	require.True(t, ok)
	require.Equal(t, slog.KindGroup, resolved.Value.Kind())
	require.False(t, called, "ReplaceAttr must not be called for group-kind LogValuer result")
}

// Expectation: A non-group LogValuer should be resolved before ReplaceAttr receives it.
func Test_resolveAttr_NonGroupLogValuer_ResolvedBeforeReplaceAttr_Success(t *testing.T) {
	t.Parallel()

	var receivedKind slog.Kind
	opts := &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			receivedKind = a.Value.Kind()

			return a
		},
	}

	_, handler := NewLogger("http://fake", WithHandlerOptions(opts), WithNoFlush())
	defer handler.Close()

	// stringValuer implements LogValuer and resolves to KindString.
	// ReplaceAttr should see KindString, not KindLogValuer.
	a := slog.Any("p", stringValuer("hello"))
	resolved, ok := handler.resolveAttr(nil, a)

	require.True(t, ok)
	require.Equal(t, slog.KindString, receivedKind, "ReplaceAttr should see the resolved kind, not KindLogValuer")
	require.Equal(t, "hello", resolved.Value.String())
}

// Expectation: Nil ReplaceAttr should not cause a panic.
func Test_resolveAttr_NilReplaceAttr_NoPanic_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	require.NotPanics(t, func() {
		a, ok := handler.resolveAttr([]string{"g"}, slog.String("k", "v"))
		require.True(t, ok)
		require.Equal(t, "k", a.Key)
	})
}

// Expectation: Error conversion should happen after ReplaceAttr.
func Test_resolveAttr_ErrorConversionAfterReplace_Success(t *testing.T) {
	t.Parallel()

	var sawError bool
	opts := &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if _, ok := a.Value.Any().(error); ok {
				sawError = true
			}

			return a
		},
	}

	_, handler := NewLogger("http://fake", WithHandlerOptions(opts), WithNoFlush())
	defer handler.Close()

	a := slog.Any("err", stubError{"fail"})
	resolved, ok := handler.resolveAttr(nil, a)

	require.True(t, ok)
	require.True(t, sawError, "ReplaceAttr should see the error before conversion")
	require.Equal(t, slog.KindString, resolved.Value.Kind())
	require.Equal(t, "fail", resolved.Value.String())
}

// Expectation: An attr with an empty key and non-group value should be silently dropped.
func Test_resolveAndAddAttr_EmptyKeyNonGroup_Dropped_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	dst := make(map[string]any)
	handler.resolveAndAddAttr(dst, nil, slog.String("", "invisible"))

	require.Empty(t, dst)
}

// Expectation: An anonymous group (empty key, group kind) should inline its children.
func Test_resolveAndAddAttr_AnonymousGroup_InlinesChildren_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	dst := make(map[string]any)
	handler.resolveAndAddAttr(dst, nil, slog.Group("", slog.String("x", "1"), slog.Int("y", 2)))

	require.Equal(t, "1", dst["x"])
	require.Equal(t, int64(2), dst["y"])
}

// Expectation: A deeply nested anonymous group should still inline all children.
func Test_resolveAndAddAttr_NestedAnonymousGroups_InlinesAll_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	dst := make(map[string]any)
	inner := slog.Group("", slog.String("deep", "value"))
	outer := slog.Group("", inner, slog.Int("shallow", 1))
	handler.resolveAndAddAttr(dst, nil, outer)

	require.Equal(t, "value", dst["deep"])
	require.Equal(t, int64(1), dst["shallow"])
}

// Expectation: Named groups should create nested maps with their children inside.
func Test_resolveAndAddAttr_NamedGroup_CreatesNestedMap_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	dst := make(map[string]any)
	handler.resolveAndAddAttr(dst, nil, slog.Group("g", slog.String("k", "v")))

	g := dst["g"].(map[string]any)
	require.Equal(t, "v", g["k"])
}

// Expectation: Two named groups with the same key should merge their children.
func Test_resolveAndAddAttr_NamedGroupMerge_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	dst := make(map[string]any)
	handler.resolveAndAddAttr(dst, nil, slog.Group("g", slog.String("a", "1")))
	handler.resolveAndAddAttr(dst, nil, slog.Group("g", slog.String("b", "2")))

	g := dst["g"].(map[string]any)
	require.Equal(t, "1", g["a"])
	require.Equal(t, "2", g["b"])
}

// Expectation: A named group with a key that collides with a non-map value should overwrite it.
func Test_resolveAndAddAttr_NamedGroupOverwritesScalar_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	dst := map[string]any{"g": "scalar"}
	handler.resolveAndAddAttr(dst, nil, slog.Group("g", slog.String("k", "v")))

	g, ok := dst["g"].(map[string]any)
	require.True(t, ok, "should have replaced scalar with map")
	require.Equal(t, "v", g["k"])
}

// Expectation: Deeply nested named groups should produce deeply nested maps.
func Test_resolveAndAddAttr_DeepNamedGroups_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	dst := make(map[string]any)
	deep := slog.Group("l3", slog.String("leaf", "val"))
	mid := slog.Group("l2", deep)
	top := slog.Group("l1", mid)
	handler.resolveAndAddAttr(dst, nil, top)

	l1 := dst["l1"].(map[string]any)
	l2 := l1["l2"].(map[string]any)
	l3 := l2["l3"].(map[string]any)
	require.Equal(t, "val", l3["leaf"])
}

// Expectation: A named group containing dotted key names should preserve dots literally.
func Test_resolveAndAddAttr_DottedAttrKey_PreservedLiterally_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	dst := make(map[string]any)
	handler.resolveAndAddAttr(dst, nil, slog.String("a.b.c", "dotted"))

	require.Equal(t, "dotted", dst["a.b.c"])
	_, hasA := dst["a"]
	require.False(t, hasA)
}

// Expectation: A named group whose key contains dots should be preserved as a literal key.
func Test_resolveAndAddAttr_DottedGroupKey_PreservedLiterally_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake", WithNoFlush())
	defer handler.Close()

	dst := make(map[string]any)
	handler.resolveAndAddAttr(dst, nil, slog.Group("x.y", slog.String("k", "v")))

	g, ok := dst["x.y"].(map[string]any)
	require.True(t, ok, "dotted group key should be a literal map key")
	require.Equal(t, "v", g["k"])
}

// Expectation: ReplaceAttr dropping an attr inside a named group should exclude it.
func Test_resolveAndAddAttr_ReplaceAttrDropsInsideGroup_Success(t *testing.T) {
	t.Parallel()

	opts := &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == "drop" {
				return slog.Attr{}
			}

			return a
		},
	}

	_, handler := NewLogger("http://fake", WithHandlerOptions(opts), WithNoFlush())
	defer handler.Close()

	dst := make(map[string]any)
	handler.resolveAndAddAttr(dst, nil, slog.Group("g",
		slog.String("keep", "yes"),
		slog.String("drop", "no"),
	))

	g := dst["g"].(map[string]any)
	require.Equal(t, "yes", g["keep"])
	require.NotContains(t, g, "drop")
}

// Expectation: The function should return the correct Seq level string for each slog level.
func Test_convertLevel_Table(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		in       slog.Level
		expected string
	}{
		{
			name:     "debug",
			in:       slog.LevelDebug,
			expected: "Debug",
		},
		{
			name:     "info",
			in:       slog.LevelInfo,
			expected: "Information",
		},
		{
			name:     "warn",
			in:       slog.LevelWarn,
			expected: "Warning",
		},
		{
			name:     "error",
			in:       slog.LevelError,
			expected: "Error",
		},
		{
			name:     "unknown level defaults to information",
			in:       slog.Level(42),
			expected: "Information",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := convertLevel(tt.in)
			require.Equal(t, tt.expected, got)
		})
	}
}
