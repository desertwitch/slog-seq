package slogseq

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"
)

const (
	maxIdleConns          = 100
	dialTimeout           = 10 * time.Second
	idleConnTimeout       = 90 * time.Second
	tlsHandShakeTimeout   = 10 * time.Second
	expectContinueTimeout = 10 * time.Second
	requestTimeout        = 30 * time.Second
)

func newHTTPClient(skipVerify bool) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout: dialTimeout,
			}).DialContext,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: skipVerify, //nolint:gosec
			},
			MaxIdleConns:          maxIdleConns,
			IdleConnTimeout:       idleConnTimeout,
			TLSHandshakeTimeout:   tlsHandShakeTimeout,
			ExpectContinueTimeout: expectContinueTimeout,
		},
		Timeout: requestTimeout,
	}
}

func (h *SeqHandler) runFlusher(w *worker) {
	defer h.workerWg.Done()
	if h.noFlush { // Used in tests
		return
	}

	var tickerChan <-chan time.Time
	if h.flushInterval > 0 {
		ticker := time.NewTicker(h.flushInterval)
		defer ticker.Stop()
		tickerChan = ticker.C
	}

	events := make([]CLEFEvent, 0, h.batchSize)

	for {
		select {
		case e, ok := <-w.eventsCh:
			if !ok {
				h.flushBatch(w, &events)

				return
			}
			events = append(events, e)
			if len(events) >= h.batchSize {
				h.flushBatch(w, &events)
			}

		case <-tickerChan:
			h.flushBatch(w, &events)
		}
	}
}

func (h *SeqHandler) flushBatch(w *worker, events *[]CLEFEvent) {
	if len(w.retryBuffer) > 0 {
		leftover := h.sendEvents(w.retryBuffer)
		w.retryBuffer = leftover
	}

	if len(*events) > 0 {
		leftover := h.sendEvents(*events)
		if leftover != nil {
			w.retryBuffer = append(w.retryBuffer, leftover...)
		}

		*events = (*events)[:0]
	}

	if len(w.retryBuffer) > h.retryBufferSize {
		dropped := len(w.retryBuffer) - h.retryBufferSize
		w.retryBuffer = w.retryBuffer[dropped:]
		h.errorHandlerFunc(fmt.Errorf("dropping %d oldest events; retry buffer exceeded limit", dropped))
	}
}

func (h *SeqHandler) sendEvents(events []CLEFEvent) []CLEFEvent {
	if len(events) == 0 {
		return nil
	}

	var sb strings.Builder
	var tmp bytes.Buffer
	enc := json.NewEncoder(&tmp)
	for _, e := range events {
		tmp.Reset()
		clef := encodeEvent(e)
		if err := enc.Encode(clef); err != nil {
			h.errorHandlerFunc(fmt.Errorf("dropping unencodable event: %w", err))

			continue
		}
		sb.Write(tmp.Bytes())
	}
	if sb.Len() == 0 {
		return nil
	}

	req, err := http.NewRequest(http.MethodPost, h.seqURL, strings.NewReader(sb.String())) //nolint:noctx
	if err != nil {
		h.errorHandlerFunc(err)

		return events
	}
	req.Header.Set("Content-Type", "application/vnd.serilog.clef")
	if h.apiKey != "" {
		req.Header.Set("X-Seq-ApiKey", h.apiKey) //nolint:canonicalheader
	}

	resp, err := h.client.Do(req)
	if err != nil {
		h.errorHandlerFunc(err)

		return events
	}
	defer func() {
		// Drain the body to allow re-use of TCP connection.
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	// Both of these can poison the whole batch, so we split the batch until they're a single event.
	if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusRequestEntityTooLarge {
		if len(events) == 1 {
			h.errorHandlerFunc(errors.New("dropping single event; size exceeds Seq server limit"))

			return nil // dropped, not retryable
		}

		// Split batch in half and retry via recursion
		mid := len(events) / 2 //nolint:mnd
		leftover := h.sendEvents(events[:mid])
		rightover := h.sendEvents(events[mid:])

		return append(leftover, rightover...)
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		h.errorHandlerFunc(fmt.Errorf("seq server returned status code %d", resp.StatusCode))

		return events
	}

	return nil
}

func encodeEvent(e CLEFEvent) map[string]any {
	topLevel := make(map[string]any, len(e.Properties)+10) //nolint:mnd
	maps.Copy(topLevel, e.Properties)

	// Set reserved CLEF keys after copying properties to ensure they aren't overwritten
	topLevel["@t"] = e.Timestamp.Format(time.RFC3339Nano)
	topLevel["@m"] = e.Message
	topLevel["@l"] = e.Level

	if e.Exception != "" {
		topLevel["@x"] = e.Exception
	}
	if !e.SpanStart.IsZero() {
		topLevel["@st"] = e.SpanStart.Format(time.RFC3339Nano)
	}
	if e.TraceID != "" {
		topLevel["@tr"] = e.TraceID
	}
	if e.SpanID != "" {
		topLevel["@sp"] = e.SpanID
	}
	if e.ParentSpanID != "" {
		topLevel["@ps"] = e.ParentSpanID
	}
	if len(e.ResourceAttributes) > 0 {
		topLevel["@ra"] = dottedToNested(e.ResourceAttributes)
	}
	if e.SpanKind != "" {
		topLevel["@sk"] = e.SpanKind
	}

	return topLevel
}

// dottedToNested converts a flat map with dotted keys ("a.b.c") into a
// nested map structure. Used for ResourceAttributes encoding.
func dottedToNested(props map[string]any) map[string]any {
	out := make(map[string]any, len(props))

	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		path := strings.Split(k, ".")
		addNested(out, path, props[k])
	}

	return out
}

func addNested(dst map[string]any, path []string, val any) {
	if len(path) == 0 {
		return
	}

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
