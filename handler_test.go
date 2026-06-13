package slogseq

import (
	"bytes"
	"context"
	"encoding/json"
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

// Expectation: The constructor should apply all provided options correctly.
func Test_NewSeqHandler_WithOptions_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://localhost:5341",
		WithAPIKey("test-key"),
		WithBatchSize(50),
		WithFlushInterval(5*time.Second),
		WithHandlerOptions(&slog.HandlerOptions{Level: slog.LevelWarn}),
	)

	require.Equal(t, "http://localhost:5341", handler.seqURL)
	require.Equal(t, "test-key", handler.apiKey)
	require.Equal(t, 50, handler.batchSize)
	require.Equal(t, 5*time.Second, handler.flushInterval)
	require.Equal(t, slog.LevelWarn, handler.options.Level.Level())

	_ = handler.Close()
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

// Expectation: Handle should send events with the correct message, level, and properties.
func Test_SeqHandler_Handle_CorrectProperties_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithWorkers(1),
		withNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)

	logger.Info("Hello, slog-seq!", "user", "alice", "count", 123)

	select {
	case evt := <-handler.workers[0].eventsCh:
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
		withNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger2 := logger.With("service", "testsvc")

	logger2.Info("WithAttrs test", "version", "1.2.3")

	select {
	case evt := <-handler.workers[0].eventsCh:
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
		withNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	grouped := logger.WithGroup("request").With("id", "1234").WithGroup("headers").With("Accept", "application/json")

	grouped.Info("Grouped log")

	select {
	case evt := <-handler.workers[0].eventsCh:
		request := evt.Properties["request"].(map[string]any)
		headers := request["headers"].(map[string]any)
		require.Equal(t, "1234", request["id"])
		require.Equal(t, "application/json", headers["Accept"])
	case <-time.After(2000 * time.Millisecond):
		t.Fatal("Timed out waiting for grouped event")
	}
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
		withNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)

	logger.Info("Hello, slog-seq!", "user", "alice", "count", 123)

	select {
	case evt := <-handler.workers[0].eventsCh:
		require.NotNil(t, evt.Properties["gosource"], "expected gosource to be set")
		source := evt.Properties["gosource"].(*slog.Source)
		require.NotEmpty(t, source.File)
		require.NotZero(t, source.Line)
		require.NotEmpty(t, source.Function)
		require.Contains(t, source.Function, "Test_SeqHandler_AddSource_IncludesSourceInfo_Success")
	case <-time.After(2000 * time.Millisecond):
		t.Fatal("Timed out waiting for log event in eventsCh")
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
		withNoFlush(),
	)
	defer handler.Close()

	ctx := context.Background()
	logger := slog.New(handler)
	logger.WithGroup("s").LogAttrs(ctx, slog.LevelInfo, "huba", slog.Int("a", 1), slog.Int("b", 2))
	logger.LogAttrs(ctx, slog.LevelInfo, "huba", slog.Group("s", slog.Int("a", 1), slog.Int("b", 2)))

	event1 := <-handler.workers[0].eventsCh
	event2 := <-handler.workers[0].eventsCh

	event1.Timestamp = event2.Timestamp
	require.Equal(t, event1, event2)
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
		withNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger.Info("Super secret info", "password", "2Fat2Fly")
	logger.WithGroup("secret_info").Info("Wohoo", "password", "secret")

	event1 := <-handler.workers[0].eventsCh
	event2 := <-handler.workers[0].eventsCh

	require.Equal(t, "*****", event1.Properties["password"])

	secretInfo := event2.Properties["secret_info"].(map[string]any)
	require.Equal(t, "*****", secretInfo["password"])
}

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

// Expectation: Anonymous groups should inline their attributes into the parent properties.
func Test_SeqHandler_Handle_AnonymousGroup_InlinesAttributes_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		withNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)

	logger.Info("anon-group-arg",
		slog.Any("", payload{ID: 42, Name: "keyname"}))

	logger.With("", payload{ID: 42, Name: "keyname"}).
		Info("anon-group-with")

	evt1 := <-handler.workers[0].eventsCh
	evt2 := <-handler.workers[0].eventsCh

	require.Equal(t, int64(42), evt1.Properties["id"])
	require.Equal(t, "keyname", evt1.Properties["name"])

	require.Equal(t, int64(42), evt2.Properties["id"])
	require.Equal(t, "keyname", evt2.Properties["name"])

	evt1.Timestamp = evt2.Timestamp
	require.Equal(t, evt1, evt2)
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
		withNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger.With("a", 1).WithGroup("g").With("b", 2).Info("test", "c", 3)

	<-handler.workers[0].eventsCh

	require.GreaterOrEqual(t, len(captured), 3, "expected at least 3 ReplaceAttr calls")
	require.Empty(t, captured[0], "expected no groups for 'a'")
	require.Equal(t, []string{"g"}, captured[1], "expected [g] for 'b'")
	require.Equal(t, []string{"g"}, captured[2], "expected [g] for 'c'")
}

// Expectation: All events should be received across multiple workers.
func Test_SeqHandler_Handle_MultipleWorkers_AllEventsReceived_Success(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(4),
		withNoFlush(),
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
			case <-handler.workers[w].eventsCh:
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
		withNoFlush(),
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
			case <-handler.workers[w].eventsCh:
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
		withNoFlush(),
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
			case <-handler.workers[w].eventsCh:
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
		withNoFlush(),
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
			case <-handler.workers[w].eventsCh:
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
		WithNonBlocking(true),
		withNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)

	for i := range maxWorkerEventBacklog + 500 {
		logger.Info("flood", "i", i)
	}

	count := 0
	for {
		select {
		case <-handler.workers[0].eventsCh:
			count++
		default:
			goto done
		}
	}
done:

	require.LessOrEqual(t, count, maxWorkerEventBacklog)
	require.NotZero(t, count, "expected some events to be received")
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
		withNoFlush(),
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
		WithNonBlocking(false),
		withNoFlush(),
	)

	logger := slog.New(handler)

	for range maxWorkerEventBacklog {
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
