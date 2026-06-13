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

	"go.opentelemetry.io/otel/trace"
)

const (
	defaultBatchSize       = 50
	defaultFlushInterval   = 2 * time.Second
	defaultWorkerCount     = 1
	defaultSendBlocking    = false
	defaultDisableFlushing = false

	maxWorkerEventBacklog = 1000
)

var _ slog.Handler = (*SeqHandler)(nil)

type worker struct {
	eventsCh    chan CLEFEvent
	retryBuffer []CLEFEvent
	purgeTicker *time.Ticker
}

// groupOrAttrs holds either a group name or a list of slog.Attrs.
// This preserves the ordering of WithGroup and WithAttrs calls.
type groupOrAttrs struct {
	group string      // group name if non-empty
	attrs []slog.Attr // attrs if non-empty
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

	goas      []groupOrAttrs
	options   slog.HandlerOptions
	sourceKey string
}

func newSeqHandler(seqURL string) *SeqHandler {
	h := &SeqHandler{
		shared: &shared{
			seqURL:  seqURL,
			closeCh: make(chan struct{}),

			batchSize:     defaultBatchSize,
			flushInterval: defaultFlushInterval,
			workerCount:   defaultWorkerCount,
			nonBlocking:   !defaultSendBlocking,
			noFlush:       defaultDisableFlushing,
		},

		sourceKey: slog.SourceKey,
		options:   slog.HandlerOptions{},
	}

	return h
}

func (h *SeqHandler) start() {
	if h.client == nil {
		h.client = newHTTPClient(h.disableTLSVerify)
	}
	if h.errorHandlerFunc == nil {
		h.errorHandlerFunc = func(_ error) {
			// by default we do nothing
		}
	}

	h.workers = make([]worker, h.workerCount)

	// Start background workers
	for i := range h.workerCount {
		h.workers[i].eventsCh = make(chan CLEFEvent, maxWorkerEventBacklog)

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

	// Determine all active groups for source/ReplaceAttr context
	groups := h.activeGroups()

	// Process WithGroup/WithAttrs in order, for so-far seen groups.
	var groupsSoFar []string

	for _, goa := range h.goas {
		if goa.group != "" {
			groupsSoFar = append(groupsSoFar, goa.group)

			continue
		}
		for _, a := range goa.attrs {
			if h.options.ReplaceAttr != nil {
				a = h.options.ReplaceAttr(groupsSoFar, a)
				if a.Key == "" {
					continue
				}
			}

			if v, ok := a.Value.Any().(error); ok {
				a.Value = slog.StringValue(v.Error())
			}

			h.addAttr(props, a)
		}
	}

	// Process record attrs under all active groups
	r.Attrs(func(a slog.Attr) bool {
		if a, ok := h.resolveAttr(groups, a); ok {
			h.addAttr(props, a)
		}

		return true
	})

	// Process source last so it overwrites user-provided keys with reserved names.
	if h.options.AddSource {
		pc := r.PC
		caller := runtime.CallersFrames([]uintptr{pc})
		frame, _ := caller.Next()
		source := slog.Source{File: frame.File, Line: frame.Line, Function: frame.Function}

		if a, ok := h.resolveAttr(groups, slog.Any(h.sourceKey, &source)); ok {
			h.addAttr(props, a)
		}
	}

	// split multi-line messages into a message (first line) and 'exception' (rest)
	msg := strings.SplitN(r.Message, "\n", 2) //nolint:mnd

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

	idx := h.next.Add(1) % uint32(len(h.workers)) //nolint:gosec
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

func (h *SeqHandler) Enabled(_ context.Context, l slog.Level) bool {
	if h.options.Level != nil {
		return l >= h.options.Level.Level()
	}

	return true
}

func (h *SeqHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}

	// Resolve and prefix attrs with current groups
	groups := h.activeGroups()
	resolved := make([]slog.Attr, 0, len(attrs))

	for _, a := range attrs {
		a.Value = a.Value.Resolve()

		if a.Key == "" {
			resolved = append(resolved, a)

			continue
		}

		if len(groups) > 0 && a.Key != h.sourceKey {
			a.Key = strings.Join(groups, ".") + "." + a.Key
		}

		resolved = append(resolved, a)
	}

	return h.withGroupOrAttrs(groupOrAttrs{attrs: resolved})
}

func (h *SeqHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h // no-op
	}

	return h.withGroupOrAttrs(groupOrAttrs{group: name})
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

func (h *SeqHandler) withGroupOrAttrs(goa groupOrAttrs) *SeqHandler {
	h2 := *h
	h2.goas = make([]groupOrAttrs, len(h.goas)+1)
	copy(h2.goas, h.goas)
	h2.goas[len(h2.goas)-1] = goa

	return &h2
}

// activeGroups returns the group names currently in effect,
// collected from the goas slice.
func (h *SeqHandler) activeGroups() []string {
	var groups []string
	for _, goa := range h.goas {
		if goa.group != "" {
			groups = append(groups, goa.group)
		}
	}

	return groups
}

func (h *SeqHandler) resolveAttr(groups []string, a slog.Attr) (slog.Attr, bool) {
	if h.options.ReplaceAttr != nil {
		a = h.options.ReplaceAttr(groups, a)
		if a.Key == "" {
			return a, false
		}
	}

	if len(groups) > 0 && a.Key != h.sourceKey {
		a.Key = strings.Join(groups, ".") + "." + a.Key
	}

	if v, ok := a.Value.Any().(error); ok {
		a.Value = slog.StringValue(v.Error())
	}

	return a, true
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

	switch a.Value.Kind() { //nolint:exhaustive
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
