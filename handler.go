package slogseq

import (
	"context"
	"log/slog"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"slices"

	"go.opentelemetry.io/otel/trace"
)

type worker struct {
	eventsCh    chan CLEFEvent
	retryBuffer []CLEFEvent
	purgeTicker *time.Ticker
}

type shared struct {
	// config (immutable after start, but no reason to copy)
	seqURL           string
	apiKey           string
	batchSize        int
	flushInterval    time.Duration
	disableTLSVerify bool
	workerCount      int
	nonBlocking      bool
	noFlush          bool

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

type SeqHandler struct {
	*shared

	attrs     []slog.Attr
	groups    []string
	options   slog.HandlerOptions
	sourceKey string
}

func newSeqHandler(seqURL string) *SeqHandler {
	h := &SeqHandler{
		shared: &shared{
			seqURL:  seqURL,
			closeCh: make(chan struct{}),

			// sane defaults
			batchSize:     50,
			flushInterval: 2 * time.Second,
			workerCount:   1,
			nonBlocking:   true,
			noFlush:       false,
		},

		sourceKey: slog.SourceKey,
		options:   slog.HandlerOptions{},
	}

	return h
}

func (h *SeqHandler) start() {
	if h.client == nil {
		h.client = newHttpClient(h.disableTLSVerify)
	}
	if h.errorHandlerFunc == nil {
		h.errorHandlerFunc = func(err error) {
			// by default we do nothing
		}
	}

	h.workers = make([]worker, h.workerCount)

	// Start background workers
	for i := range h.workerCount {
		h.workers[i].eventsCh = make(chan CLEFEvent, 1000)

		h.workerWg.Add(1)
		go h.runBackgroundFlusher(&h.workers[i])
	}
}

func (h *SeqHandler) Handle(ctx context.Context, r slog.Record) error {
	// Convert slog.Level to text
	levelString := convertLevel(r.Level)

	spanCtx := trace.SpanContextFromContext(ctx)

	// Collect attributes into a map
	props := make(map[string]any)

	if h.options.AddSource {
		pc := r.PC
		caller := runtime.CallersFrames([]uintptr{pc})
		frame, _ := caller.Next()
		source := slog.Source{File: frame.File, Line: frame.Line, Function: frame.Function}

		if a, ok := h.resolveAttr(slog.Any(h.sourceKey, &source)); ok {
			h.addAttr(props, a)
		}
	}

	h.addAttrs(props, h.attrs)

	r.Attrs(func(a slog.Attr) bool {
		if a, ok := h.resolveAttr(a); ok {
			h.addAttr(props, a)
		}

		return true
	})

	// split multi-line messages into a message (first line) and 'exception' (rest)
	msg := strings.SplitN(r.Message, "\n", 2)

	var exception string
	if len(msg) == 1 {
		exception = ""
	} else {
		exception = msg[1]
	}

	// Create CLEF event
	event := CLEFEvent{
		Timestamp:  r.Time,
		Message:    msg[0],
		Exception:  exception,
		Level:      levelString,
		Properties: dottedToNested(props),
	}

	if spanCtx.IsValid() {
		event.TraceID = spanCtx.TraceID().String()
		event.SpanID = spanCtx.SpanID().String()
	}

	h.HandleCLEFEvent(event)

	return nil
}

func (h *SeqHandler) HandleCLEFEvent(event CLEFEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return
	}

	idx := h.next.Add(1) % uint32(len(h.workers))
	if h.nonBlocking {
		// send to channel, drop if full
		select {
		case h.workers[idx].eventsCh <- event:
			// success
		default:
			// channel full, drop event
		}
	} else {
		// blocking send
		select {
		case h.workers[idx].eventsCh <- event:
			// success
		case <-h.closeCh:
			// unblock on close, drop event
		}
	}
}

func (h *SeqHandler) Enabled(ctx context.Context, l slog.Level) bool {
	if h.options.Level != nil {
		return l >= h.options.Level.Level()
	}

	return true
}

func (h *SeqHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h2 := *h
	h2.attrs = slices.Clone(h.attrs)

	for _, a := range attrs {
		a.Value = a.Value.Resolve()

		if a.Key == "" {
			h2.attrs = append(h2.attrs, a)

			continue
		}

		if len(h2.groups) > 0 && a.Key != h2.sourceKey {
			a.Key = strings.Join(h2.groups, ".") + "." + a.Key
		}

		h2.attrs = append(h2.attrs, a)
	}

	return &h2
}

func (h *SeqHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h // no-op
	}

	h2 := *h
	h2.groups = slices.Clone(h.groups)
	h2.groups = append(h2.groups, name)

	return &h2
}

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

// SourceKey returns the key used when AddSource is enabled.
func (h *SeqHandler) SourceKey() string {
	return h.sourceKey
}

func (h *SeqHandler) resolveAttr(a slog.Attr) (slog.Attr, bool) {
	if h.options.ReplaceAttr != nil {
		a = h.options.ReplaceAttr(h.groups, a)
		if a.Key == "" {
			return a, false
		}
	}

	if len(h.groups) > 0 && a.Key != h.sourceKey {
		a.Key = strings.Join(h.groups, ".") + "." + a.Key
	}

	if v, ok := a.Value.Any().(error); ok {
		a.Value = slog.StringValue(v.Error())
	}

	return a, true
}

func (h *SeqHandler) addAttrs(dst map[string]any, attrs []slog.Attr) {
	for _, a := range attrs {
		h.addAttr(dst, a)
	}
}

func (h *SeqHandler) addAttr(dst map[string]any, a slog.Attr) {
	a.Value = a.Value.Resolve()

	if a.Key == "" {
		// Anonymous group, inline
		if a.Value.Kind() == slog.KindGroup {
			for _, ga := range a.Value.Group() {
				h.addAttr(dst, ga)
			}
		}

		return
	}

	switch a.Value.Kind() {
	case slog.KindGroup:
		groupMap, ok := dst[a.Key].(map[string]any)
		if !ok {
			groupMap = make(map[string]any)
			dst[a.Key] = groupMap
		}
		for _, ga := range a.Value.Group() {
			h.addAttr(groupMap, ga)
		}

	default:
		dst[a.Key] = a.Value.Any()
	}
}

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
