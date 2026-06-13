package slogseq

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
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

func (h *SeqHandler) runBackgroundFlusher(w *worker) {
	defer h.workerWg.Done()
	if h.noFlush { // Used in tests
		return
	}

	ticker := time.NewTicker(h.flushInterval)
	defer ticker.Stop()

	purgeInterval := h.flushInterval * 60 //nolint:mnd
	w.purgeTicker = time.NewTicker(purgeInterval)
	defer w.purgeTicker.Stop()

	events := make([]CLEFEvent, 0, h.batchSize)

	for {
		select {
		case e, ok := <-w.eventsCh:
			if !ok {
				h.flushCurrentBatch(w, &events)

				return
			}
			events = append(events, e)
			if len(events) >= h.batchSize {
				h.flushCurrentBatch(w, &events)
			}

		case <-ticker.C:
			h.flushCurrentBatch(w, &events)

		case <-w.purgeTicker.C:
			// Purge events older than 5 minutes from retry buffer
			cutoff := time.Now().Add(-5 * time.Minute)
			h.purgeOldEvents(w, cutoff)
		}
	}
}

func (h *SeqHandler) flushCurrentBatch(w *worker, events *[]CLEFEvent) {
	if len(w.retryBuffer) > 0 {
		leftover := h.sendWithRetry(w.retryBuffer)
		w.retryBuffer = leftover
	}

	if len(*events) > 0 {
		leftover := h.sendWithRetry(*events)
		if leftover != nil {
			w.retryBuffer = append(w.retryBuffer, leftover...)
		}

		*events = (*events)[:0]
	}
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

func (h *SeqHandler) attemptSendBatch(events []CLEFEvent) []CLEFEvent {
	if len(events) == 0 {
		return nil
	}

	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	for _, e := range events {
		topLevel := encodeEvent(e)
		if err := enc.Encode(topLevel); err != nil {
			return events
		}
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

	if resp.StatusCode == http.StatusRequestEntityTooLarge {
		if len(events) == 1 {
			h.errorHandlerFunc(errors.New("dropping single event; size exceeds Seq server limit"))

			return nil // dropped, not retryable
		}

		// Split batch in half and retry via recursion
		mid := len(events) / 2 //nolint:mnd
		leftover := h.attemptSendBatch(events[:mid])
		rightover := h.attemptSendBatch(events[mid:])

		return append(leftover, rightover...)
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		h.errorHandlerFunc(fmt.Errorf("seq server returned status code %d", resp.StatusCode))

		return events
	}

	return nil
}

func (h *SeqHandler) sendWithRetry(events []CLEFEvent) []CLEFEvent {
	if len(events) == 0 {
		return nil
	}

	return h.attemptSendBatch(events)
}

func (h *SeqHandler) purgeOldEvents(w *worker, olderThan time.Time) {
	newBuf := w.retryBuffer[:0]

	for _, e := range w.retryBuffer {
		if e.Timestamp.After(olderThan) {
			newBuf = append(newBuf, e)
		}
	}

	purgedEvents := len(w.retryBuffer) - len(newBuf)
	if purgedEvents > 0 {
		h.errorHandlerFunc(fmt.Errorf("purged %d events from retry buffer older than %s", purgedEvents, olderThan.Format(time.RFC3339)))
	}

	w.retryBuffer = newBuf
}

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
