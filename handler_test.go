package slogseq

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// TestNewSeqHandler tests constructing a new handler with various config.
func TestNewSeqHandler(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://localhost:5341",
		WithAPIKey("test-key"),
		WithBatchSize(50),
		WithFlushInterval(5*time.Second),
		WithHandlerOptions(&slog.HandlerOptions{Level: slog.LevelWarn}),
	)

	if handler.seqURL != "http://localhost:5341" {
		t.Errorf("expected seqURL to be http://localhost:5341, got %s", handler.seqURL)
	}
	if handler.apiKey != "test-key" {
		t.Errorf("expected apiKey to be test-key, got %s", handler.apiKey)
	}
	if handler.batchSize != 50 {
		t.Errorf("expected batchSize = 50, got %d", handler.batchSize)
	}
	if handler.flushInterval != 5*time.Second {
		t.Errorf("expected flushInterval = 5s, got %v", handler.flushInterval)
	}
	if handler.options.Level.Level() != slog.LevelWarn {
		t.Errorf("expected level = Warn, got %v", handler.options.Level)
	}

	// Clean up
	_ = handler.Close()
}

// TestSeqHandler_Handle checks that Handle() sends events with correct properties.
func TestSeqHandler_Handle(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithWorkers(1),
		withNoFlush(), // No flushing for this test.
	)
	defer handler.Close()

	logger := slog.New(handler)

	// Log something at Info level
	logger.Info("Hello, slog-seq!", "user", "alice", "count", 123)

	select {
	case evt := <-handler.workers[0].eventsCh:
		if evt.Message != "Hello, slog-seq!" {
			t.Errorf("Expected message 'Hello, slog-seq!', got '%s'", evt.Message)
		}
		if evt.Level != "Information" {
			t.Errorf("Expected level = Information, got '%s'", evt.Level)
		}
		if evt.Properties["user"] != "alice" {
			t.Errorf("Expected user=alice, got %v", evt.Properties["user"])
		}
		if evt.Properties["count"].(int64) != 123 {
			t.Errorf("Expected count=123, got %v", evt.Properties["count"])
		}
	case <-time.After(2000 * time.Millisecond):
		t.Error("Timed out waiting for log event in eventsCh")
	}
}

// TestSeqHandler_Enabled checks that level filtering via HandlerOptions works.
func TestSeqHandler_Enabled(t *testing.T) {
	t.Parallel()

	opts := &slog.HandlerOptions{Level: slog.LevelWarn}
	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithHandlerOptions(opts),
	)
	defer handler.Close()

	// Debug/Info should be disabled
	if handler.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("Debug level should be disabled")
	}
	if handler.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("Info level should be disabled")
	}
	// Warn and above should be enabled
	if !handler.Enabled(context.Background(), slog.LevelWarn) {
		t.Error("Warn level should be enabled")
	}
	if !handler.Enabled(context.Background(), slog.LevelError) {
		t.Error("Error level should be enabled")
	}
}

// TestSeqHandler_WithAttrs checks that WithAttrs merges attributes into subsequent logs.
func TestSeqHandler_WithAttrs(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithWorkers(1),
		withNoFlush(), // No flushing for this test.
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger2 := logger.With("service", "testsvc")

	logger2.Info("WithAttrs test", "version", "1.2.3")

	select {
	case evt := <-handler.workers[0].eventsCh:
		// Should have both service=testsvc and version=1.2.3
		if evt.Properties["service"] != "testsvc" {
			t.Errorf("Expected service=testsvc, got %v", evt.Properties["service"])
		}
		if evt.Properties["version"] != "1.2.3" {
			t.Errorf("Expected version=1.2.3, got %v", evt.Properties["version"])
		}
	case <-time.After(2000 * time.Millisecond):
		t.Error("Timed out waiting for WithAttrs event")
	}
}

// TestSeqHandler_WithGroup checks that WithGroup prefixes attribute keys.
func TestSeqHandler_WithGroup(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithWorkers(1),
		withNoFlush(), // No flushing for this test.
	)
	defer handler.Close()

	logger := slog.New(handler)
	grouped := logger.WithGroup("request").With("id", "1234").WithGroup("headers").With("Accept", "application/json")

	grouped.Info("Grouped log")

	select {
	case evt := <-handler.workers[0].eventsCh:
		// We expect keys to be "request.id" and "request.headers.Accept"
		request := evt.Properties["request"].(map[string]any)
		headers := request["headers"].(map[string]any)
		if request["id"] != "1234" {
			t.Errorf("Expected request.id=1234, got %v", request["id"])
		}
		if headers["Accept"] != "application/json" {
			t.Errorf("Expected request.headers.Accept=application/json, got %v", headers["Accept"])
		}
	case <-time.After(2000 * time.Millisecond):
		t.Error("Timed out waiting for grouped event")
	}
}

// TestSeqHandler_Close checks that Close() completes without error and presumably flushes.
func TestSeqHandler_Close(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
	)

	if err := handler.Close(); err != nil {
		t.Errorf("Close returned error: %v", err)
	}

	// Optionally, you might check that the background goroutine is done
	// but we can't do that directly without instrumentation or reflection.
}

// TestSeqHandler_convertLevel ensures level conversion matches expectations.
func TestSeqHandler_convertLevel(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in       slog.Level
		expected string
	}{
		{slog.LevelDebug, "Debug"},
		{slog.LevelInfo, "Information"},
		{slog.LevelWarn, "Warning"},
		{slog.LevelError, "Error"},
		{42, "Information"}, // Something out of range
	}

	for _, c := range cases {
		out := convertLevel(c.in)
		if out != c.expected {
			t.Errorf("convertLevel(%v) = %s, want %s", c.in, out, c.expected)
		}
	}
}

// TestSeqHandler_addSource ensures source information is added to log events.
func TestSeqHandler_addSource(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithSourceKey("gosource"),
		WithHandlerOptions(&slog.HandlerOptions{AddSource: true}),
		withNoFlush(), // No flushing for this test.
	)
	defer handler.Close()

	logger := slog.New(handler)

	logger.Info("Hello, slog-seq!", "user", "alice", "count", 123)

	select {
	case evt := <-handler.workers[0].eventsCh:
		if evt.Properties["gosource"] == nil {
			t.Error("Expected gosource to be set")
		}
		source := evt.Properties["gosource"].(*slog.Source)
		if source.File == "" {
			t.Error("Expected source file to be set")
		}
		if source.Line == 0 {
			t.Error("Expected source line to be set")
		}
		if source.Function == "" {
			t.Error("Expected source function to be set")
		}
		if !strings.Contains(source.Function, "TestSeqHandler_addSource") {
			t.Errorf("Expected source function to contain TestSeqHandler_addSource, got %s", source.Function)
		}
	case <-time.After(2000 * time.Millisecond):
		t.Error("Timed out waiting for log event in eventsCh")
	}
}

// TestSeqHandler_grouping ensures that grouping works as expected.
// test case from comments in slog.Handler.
func TestSeqHandler_grouping(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithWorkers(1),
		withNoFlush(), // No flushing for this test.
	)
	defer handler.Close()

	ctx := context.Background()
	logger := slog.New(handler)
	logger.WithGroup("s").LogAttrs(ctx, slog.LevelInfo, "huba", slog.Int("a", 1), slog.Int("b", 2))
	logger.LogAttrs(ctx, slog.LevelInfo, "huba", slog.Group("s", slog.Int("a", 1), slog.Int("b", 2)))

	event1 := <-handler.workers[0].eventsCh
	event2 := <-handler.workers[0].eventsCh

	if diff := cmp.Diff(event1, event2, cmpopts.IgnoreFields(CLEFEvent{}, "Timestamp")); diff != "" {
		t.Errorf("events differ: (-got +want)\n%s", diff)
	}
}

func TestSeqHandler_replaceAttr(t *testing.T) {
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
		withNoFlush(), // No flushing for this test.
	)
	defer handler.Close()

	logger := slog.New(handler)
	logger.Info("Super secret info", "password", "2Fat2Fly")
	logger.WithGroup("secret_info").Info("Wohoo", "password", "secret")

	event1 := <-handler.workers[0].eventsCh
	event2 := <-handler.workers[0].eventsCh

	if event1.Properties["password"] != "*****" {
		t.Errorf("Expected password=*****, got %v", event1.Properties["password"])
	}

	secretInfo := event2.Properties["secret_info"].(map[string]any)
	if secretInfo["password"] != "*****" {
		t.Errorf("Expected password=*****, got %v", secretInfo["password"])
	}
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

func TestSeqHandler_AnonymousGroup(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		withNoFlush(), // No flushing for this test.
	)
	defer handler.Close()

	logger := slog.New(handler)

	// 1. Argument-style anonymous group.
	logger.Info("anon-group-arg",
		slog.Any("", payload{ID: 42, Name: "keyname"}))

	// 2. With-style anonymous group.
	logger.With("", payload{ID: 42, Name: "keyname"}).
		Info("anon-group-with")

	evt1 := <-handler.workers[0].eventsCh
	evt2 := <-handler.workers[0].eventsCh

	// --- Assertions for the first event (argument style) -----------
	if got := evt1.Properties["id"]; got != int64(42) {
		t.Errorf("argument style: expected id=42, got %v", got)
	}
	if got := evt1.Properties["name"]; got != "keyname" {
		t.Errorf("argument style: expected name=arg, got %v", got)
	}

	// --- Assertions for the second event (With style) --------------
	if got := evt2.Properties["id"]; got != int64(42) {
		t.Errorf("With style: expected id=42, got %v", got)
	}
	if got := evt2.Properties["name"]; got != "keyname" {
		t.Errorf("With style: expected name=with, got %v", got)
	}

	// The two events should differ only in Timestamp and Message.
	if diff := cmp.Diff(evt1, evt2,
		cmpopts.IgnoreFields(CLEFEvent{}, "Timestamp", "Message"),
	); diff != "" {
		t.Errorf("events differ: (-arg +with)\n%s", diff)
	}
}

func TestSeqHandler_MultipleWorkers(t *testing.T) {
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

	// Collect all events from all workers
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

	if total != n {
		t.Errorf("expected %d events across workers, got %d", n, total)
	}
}

func TestSeqHandler_MultipleWorkersDistribution(t *testing.T) {
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

	// Check that work was distributed across workers, not all to one
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
		if c == 0 {
			t.Errorf("worker %d received no events", w)
		}
	}
}

func TestSeqHandler_ConcurrentHandleCalls(t *testing.T) {
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
	if total != expected {
		t.Errorf("expected %d events, got %d", expected, total)
	}
}

func TestSeqHandler_ConcurrentWithAttrsAndHandle(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(2),
		withNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)

	var wg sync.WaitGroup
	wg.Add(3)

	// Goroutine 1: log with base logger
	go func() {
		defer wg.Done()
		for i := range 50 {
			logger.Info("base", "i", i)
		}
	}()

	// Goroutine 2: derive with WithAttrs and log
	go func() {
		defer wg.Done()
		l := logger.With("service", "svc")
		for i := range 50 {
			l.Info("with-attrs", "i", i)
		}
	}()

	// Goroutine 3: derive with WithGroup and log
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

	if total != 150 {
		t.Errorf("expected 150 events, got %d", total)
	}
}

func TestSeqHandler_HandleAfterClose(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		withNoFlush(),
	)

	_ = handler.Close()

	// Should not panic
	logger := slog.New(handler)
	logger.Info("after close", "key", "value")
}

func TestSeqHandler_DoubleClose(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
	)

	if err := handler.Close(); err != nil {
		t.Errorf("first Close returned error: %v", err)
	}
	if err := handler.Close(); err != nil {
		t.Errorf("second Close returned error: %v", err)
	}
}

func TestSeqHandler_BlockingCloseUnblocksSenders(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithNonBlocking(false),
		withNoFlush(),
	)

	logger := slog.New(handler)

	// Fill the channel
	for range maxWorkerEventBacklog {
		logger.Info("fill")
	}

	// Start a goroutine that will block on send
	done := make(chan struct{})
	go func() {
		logger.Info("blocked")
		close(done)
	}()

	// Give it a moment to block
	time.Sleep(10 * time.Millisecond)

	// Close should unblock the sender
	_ = handler.Close()

	select {
	case <-done:
		// success - sender was unblocked
	case <-time.After(2 * time.Second):
		t.Error("blocked sender was not unblocked by Close")
	}
}

func TestSeqHandler_NonBlockingDropsOnFullChannel(t *testing.T) {
	t.Parallel()

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithNonBlocking(true),
		withNoFlush(),
	)
	defer handler.Close()

	logger := slog.New(handler)

	// Fill the channel beyond capacity
	for i := range maxWorkerEventBacklog + 500 {
		logger.Info("flood", "i", i)
	}

	// Drain and count
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

	if count > maxWorkerEventBacklog {
		t.Errorf("expected at most %d events, got %d", maxWorkerEventBacklog, count)
	}
	if count == 0 {
		t.Error("expected some events to be received")
	}
}
