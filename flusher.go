package slogseq

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
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

// newHTTPClient creates an [http.Client] with connection pooling and timeouts
// suitable for batched log delivery. If skipVerify is true, TLS certificate
// validation is disabled.
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

// runFlusher is the per-worker loop that collects events from the channel and
// flushes them in batches, either when the batch is full or on a timer tick. On
// channel close it performs a final flush before exiting.
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
			if !ok { // Channel closed.
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

// flushBatch first retries any previously failed events, then sends the current
// batch. Leftover events from either are kept in the retry buffer, which is
// trimmed to retryBufferSize by dropping the oldest events.
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

// sendEvents encodes the events as newline-delimited CLEF JSON and POSTs them
// to Seq. On 400 or 413 it binary-splits the batch to isolate the offending
// event. Returns any events that should be retried, or nil on success.
func (h *SeqHandler) sendEvents(events []CLEFEvent) []CLEFEvent {
	if len(events) == 0 {
		return nil
	}

	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	for _, e := range events {
		if err := enc.Encode(e); err != nil {
			h.errorHandlerFunc(fmt.Errorf("dropping unencodable event: %w", err))

			continue
		}
	}
	if sb.Len() == 0 {
		return nil
	}

	req, err := http.NewRequest(http.MethodPost, h.seqURL, strings.NewReader(sb.String())) //nolint:noctx
	if err != nil {
		h.errorHandlerFunc(fmt.Errorf("http request: %w", err))

		return events
	}
	req.Header.Set("Content-Type", "application/vnd.serilog.clef")
	if h.apiKey != "" {
		req.Header.Set("X-Seq-ApiKey", h.apiKey) //nolint:canonicalheader
	}

	resp, err := h.client.Do(req)
	if err != nil {
		h.errorHandlerFunc(fmt.Errorf("http request: %w", err))

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
			h.errorHandlerFunc(fmt.Errorf("dropping oversized event; status code %d", resp.StatusCode))

			return nil // dropped, not retryable
		}

		// Split batch in half and retry via recursion:
		mid := len(events) / 2 //nolint:mnd
		leftover := h.sendEvents(events[:mid])
		rightover := h.sendEvents(events[mid:])

		return append(leftover, rightover...)
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		h.errorHandlerFunc(fmt.Errorf("http request: status code %d", resp.StatusCode))

		return events
	}

	return nil
}
