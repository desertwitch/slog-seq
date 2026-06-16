package slogseq

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultBatchSize       = 50
	defaultBufferSize      = 1000
	defaultRetryBufferSize = 1000
	defaultFlushInterval   = 2 * time.Second
	defaultWorkerCount     = 1
	defaultSendBlocking    = false
	defaultDisableFlushing = false
)

var _ slog.Handler = (*SeqHandler)(nil)

type shared struct {
	// config (immutable after start, but no reason to copy)
	seqURL           string
	apiKey           string
	batchSize        int
	bufferSize       int
	retryBufferSize  int
	flushInterval    time.Duration
	disableTLSVerify bool
	workerCount      int
	blockingMode     bool
	noFlush          bool
	eventEnrichers   []func(context.Context, *CLEFEvent)

	// http client
	client *http.Client

	// concurrency
	next     atomic.Uint32
	workers  []worker
	workerWg sync.WaitGroup

	mu        sync.RWMutex
	closed    bool // through mu
	closeCh   chan struct{}
	closeOnce sync.Once

	// error handling
	errorHandlerFunc func(error)
}

type worker struct {
	eventsCh    chan CLEFEvent
	retryBuffer []CLEFEvent
}

// attrSet holds a set of attrs with the group path they belong under. The
// groups slice is a snapshot of handlerGroups at the time WithAttrs was called.
type attrSet struct {
	attrs  []slog.Attr
	groups []string
}

// NewLogger is a convenience function that creates a [SeqHandler] and wraps it
// in an [slog.Logger]. Refer to [NewSeqHandler] for teardown information.
func NewLogger(seqURL string, opts ...SeqOption) (*slog.Logger, *SeqHandler) {
	handler := NewSeqHandler(seqURL, opts...)

	return slog.New(handler), handler
}

// SeqHandler is an [slog.Handler] that sends structured log events to a Seq
// server using the CLEF (Compact Log Event Format) protocol. It supports
// batching, multiple workers, and asynchronous HTTP delivery.
//
// Create one using [NewSeqHandler]. Do not construct directly.
type SeqHandler struct {
	*shared

	handlerAttrs  []attrSet // current attr set, built up by WithAttr
	handlerGroups []string  // current group set, built up by WithGroup

	options   slog.HandlerOptions
	sourceKey string
}

// NewSeqHandler creates and starts a new [SeqHandler]. seqURL is the URL of the
// Seq server's CLEF ingestion endpoint. Derived handlers (via WithAttrs and
// WithGroup) share the same workers and connection. Close must be called on the
// original handler when no longer needed, rendering all (sub-)handlers unusable.
//
// See package documentation for all possible [SeqOption].
func NewSeqHandler(seqURL string, opts ...SeqOption) *SeqHandler {
	handler := &SeqHandler{
		shared: &shared{
			seqURL:  seqURL,
			closeCh: make(chan struct{}),

			bufferSize:      defaultBufferSize,
			retryBufferSize: defaultRetryBufferSize,
			batchSize:       defaultBatchSize,
			flushInterval:   defaultFlushInterval,
			workerCount:     defaultWorkerCount,
			noFlush:         defaultDisableFlushing,
			blockingMode:    defaultSendBlocking,
		},

		sourceKey: slog.SourceKey,
		options:   slog.HandlerOptions{},
	}

	for _, opt := range opts {
		handler = opt.apply(handler)
	}

	handler.start()

	return handler
}

func (h *SeqHandler) start() {
	if h.client == nil {
		h.client = newHTTPClient(h.disableTLSVerify)
	}
	if h.errorHandlerFunc == nil {
		h.errorHandlerFunc = func(_ error) {
			// By default we do nothing.
		}
	}

	h.workers = make([]worker, h.workerCount)
	for i := range h.workerCount {
		h.workers[i].eventsCh = make(chan CLEFEvent, h.bufferSize)

		h.workerWg.Add(1)
		go h.runBackgroundFlusher(&h.workers[i])
	}
}

// Ping checks whether the Seq server is reachable and in service by calling
// its /health endpoint. Uses the handler's configured HTTP client.
//
// Intended for startup checks before setting a constructed handler as default.
func (h *SeqHandler) Ping() error {
	u, err := url.Parse(h.seqURL)
	if err != nil {
		return fmt.Errorf("seq URL: %w", err)
	}
	u.Path = "/health"
	u.RawQuery = ""

	req, err := http.NewRequest(http.MethodGet, u.String(), nil) //nolint:noctx
	if err != nil {
		return fmt.Errorf("health request: %w", err)
	}
	if h.apiKey != "" {
		req.Header.Set("X-Seq-ApiKey", h.apiKey) //nolint:canonicalheader
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("health check: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check: status %d", resp.StatusCode)
	}

	return nil
}

// Enabled reports whether the handler is configured to log at the given level.
func (h *SeqHandler) Enabled(_ context.Context, l slog.Level) bool {
	if h.options.Level != nil {
		return l >= h.options.Level.Level()
	}

	return true
}

// SourceKey returns the key used for source location information when AddSource
// is enabled in the handler options.
//
// Deprecated: SourceKey is scheduled for removal in a future version, you
// should record a used SourceKey at [SeqHandler] construction time instead.
func (h *SeqHandler) SourceKey() string {
	return h.sourceKey
}

// Handle processes a log record, converting it to a CLEF event and dispatching
// it to a worker for asynchronous delivery to Seq.
func (h *SeqHandler) Handle(ctx context.Context, r slog.Record) error {
	levelString := convertLevel(r.Level)

	props := make(map[string]any, r.NumAttrs()+2) //nolint:mnd

	// Process handler (non-record) attrs from WithAttrs calls. Each entry
	// carries its own groups snapshot for correct nesting.
	for i := range h.handlerAttrs {
		ha := &h.handlerAttrs[i]
		dst := nestInto(props, ha.groups)

		for _, a := range ha.attrs {
			h.addResolvedAttr(dst, ha.groups, a)
		}
	}

	// Process record attrs and source under all handler (active) groups.
	if r.NumAttrs() > 0 || h.options.AddSource {
		recordDst := nestInto(props, h.handlerGroups)

		r.Attrs(func(a slog.Attr) bool {
			h.addResolvedAttr(recordDst, h.handlerGroups, a)

			return true
		})

		if h.options.AddSource {
			pc := r.PC
			caller := runtime.CallersFrames([]uintptr{pc})
			frame, _ := caller.Next()
			source := slog.Source{File: frame.File, Line: frame.Line, Function: frame.Function}

			h.addResolvedAttr(recordDst, h.handlerGroups, slog.Any(h.sourceKey, &source))
		}
	}

	// Split multi-line messages into a message (first line) and 'exception' (rest)
	msg := r.Message
	exception := ""
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		exception = msg[i+1:]
		msg = msg[:i]
	}

	event := CLEFEvent{
		Timestamp:  r.Time,
		Message:    msg,
		Exception:  exception,
		Level:      levelString,
		Properties: props,
	}

	for _, enrich := range h.eventEnrichers {
		enrich(ctx, &event)
	}

	h.HandleCLEFEvent(event)

	return nil
}

// HandleCLEFEvent dispatches a pre-built CLEF event to a worker for
// asynchronous delivery to Seq. This can be used by span processors or
// custom integrations that bypass slog.
func (h *SeqHandler) HandleCLEFEvent(event CLEFEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return
	}

	idx := h.next.Add(1) % uint32(len(h.workers)) //nolint:gosec
	if h.blockingMode {
		// blocking send
		select {
		case h.workers[idx].eventsCh <- event:
			// success
		case <-h.closeCh:
			// unblock on close, drop event
		}
	} else {
		// send to channel, drop if full
		select {
		case h.workers[idx].eventsCh <- event:
			// success
		default:
			// channel full, drop event
		}
	}
}

// WithAttrs returns a new handler with the given attributes added to every
// subsequent log event. The returned handler shares the same workers and
// connection as the original.
func (h *SeqHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h // no-op
	}

	resolved := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		a.Value = a.Value.Resolve()
		resolved[i] = a
	}

	h2 := *h
	h2.handlerAttrs = make([]attrSet, len(h.handlerAttrs)+1)
	copy(h2.handlerAttrs, h.handlerAttrs)
	h2.handlerAttrs[len(h2.handlerAttrs)-1] = attrSet{
		attrs:  resolved,
		groups: h.handlerGroups,
	}

	return &h2
}

// WithGroup returns a new handler that nests all subsequent attributes and
// record attributes under the given group name. The returned handler shares the
// same workers and connection as the original.
func (h *SeqHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h // no-op
	}

	h2 := *h
	h2.handlerGroups = make([]string, len(h.handlerGroups)+1)
	copy(h2.handlerGroups, h.handlerGroups)
	h2.handlerGroups[len(h2.handlerGroups)-1] = name

	return &h2
}

// Close shuts down the handler, draining all pending events and waiting for
// workers to finish. It is safe to call multiple times. Logging after Close
// will silently drop events. Note that this method will immediately render
// all derived handlers (WithGroup/WithAttrs) unusable as well, so should only
// be called once - usually on the shallowest handler on program teardown.
func (h *SeqHandler) Close() error {
	h.closeOnce.Do(func() {
		// Blocked senders hold read locks, so we release these first.
		// Otherwise we cannot acquire the write lock for "closed" below.
		close(h.closeCh)

		// We switch the bool to prevent new senders from entering.
		h.mu.Lock()
		h.closed = true
		h.mu.Unlock()

		// Once no more sends are guaranteed, we let the events drain.
		for i := range h.workerCount {
			close(h.workers[i].eventsCh)
		}

		// We wait until all events are drained and workers have left.
		h.workerWg.Wait()
	})

	return nil
}

// Events returns the event channel for the given worker index.
//
// Intended only for use in tests to inspect dispatched events.
func (h *SeqHandler) Events(workerIndex int) <-chan CLEFEvent {
	return h.workers[workerIndex].eventsCh
}

func (h *SeqHandler) addResolvedAttr(dst map[string]any, groups []string, a slog.Attr) {
	a, ok := h.resolveAttr(groups, a)
	if !ok {
		return
	}

	if a.Key == "" {
		if a.Value.Kind() == slog.KindGroup {
			// Anonymous group, inline
			for _, ga := range a.Value.Group() {
				h.addResolvedAttr(dst, groups, ga)
			}
		}

		// Non-group empty key: drop silently.
		return
	}

	if a.Value.Kind() == slog.KindGroup {
		// Named group:
		// Nest children into a sub-map, merging with any same-key existing map.
		groupMap, ok := dst[a.Key].(map[string]any)
		if !ok {
			groupMap = make(map[string]any)
			dst[a.Key] = groupMap
		}

		childGroups := append(groups, a.Key) //nolint:gocritic
		for _, ga := range a.Value.Group() {
			h.addResolvedAttr(groupMap, childGroups, ga)
		}

		return
	}

	// Regular value:
	dst[a.Key] = a.Value.Any()
}

// resolveAttr applies ReplaceAttr and converts error values.
func (h *SeqHandler) resolveAttr(groups []string, a slog.Attr) (slog.Attr, bool) {
	a.Value = a.Value.Resolve()

	if a.Value.Kind() == slog.KindGroup {
		return a, true
	}

	if h.options.ReplaceAttr != nil {
		a = h.options.ReplaceAttr(groups, a)
		if a.Key == "" {
			return a, false
		}
	}

	if v, ok := a.Value.Any().(error); ok {
		a.Value = slog.StringValue(v.Error())
	}

	return a, true
}

// nestInto navigates into nested sub-maps for the given group path, creating
// intermediate maps as needed. Returns the innermost map where attrs should be
// inserted. Dots in group names are preserved as literal map keys.
func nestInto(dst map[string]any, groups []string) map[string]any {
	for _, g := range groups {
		child, ok := dst[g].(map[string]any)
		if !ok {
			child = make(map[string]any)
			dst[g] = child
		}

		dst = child
	}

	return dst
}

func convertLevel(l slog.Level) string {
	switch l {
	case slog.LevelDebug:
		return CLEFLevelDebug.String()

	case slog.LevelInfo:
		return CLEFLevelInformation.String()

	case slog.LevelWarn:
		return CLEFLevelWarning.String()

	case slog.LevelError:
		return CLEFLevelError.String()

	default:
		return CLEFLevelInformation.String()
	}
}
