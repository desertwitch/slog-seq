package slogseq

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockTransport struct {
	RoundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.RoundTripFunc(req)
}

func GetHTTPClientMock(status int, msg string, f func()) *http.Client {
	transport := &mockTransport{
		RoundTripFunc: func(req *http.Request) (*http.Response, error) {
			f()

			return &http.Response{
				StatusCode: status,
				Body:       io.NopCloser(bytes.NewBufferString(msg)),
			}, nil
		},
	}

	return &http.Client{Transport: transport}
}

func TestRunBackgroundFlusher_BasicFlushOnBatchSize(t *testing.T) {
	t.Parallel()

	// Create a SeqHandler with small batchSize for easy testing.
	handler := &SeqHandler{
		shared: &shared{
			client:        GetHTTPClientMock(200, "ok", func() {}),
			seqURL:        "http://example.com",
			flushInterval: 100 * time.Hour, // large interval so it won't trigger unless forced
			batchSize:     2,               // flush after 2 events
			workerCount:   1,
			workers: []worker{{
				eventsCh:    make(chan CLEFEvent, 10),
				retryBuffer: make([]CLEFEvent, 0),
			}},
			errorHandlerFunc: func(err error) {},
		},
	}

	w := &handler.workers[0]
	handler.workerWg.Add(1)

	// Start runBackgroundFlusher in background
	go handler.runBackgroundFlusher(w)

	// Send 2 events (exactly batchSize); expect immediate flush
	e1 := CLEFEvent{Message: "event1", Timestamp: time.Now()}
	e2 := CLEFEvent{Message: "event2", Timestamp: time.Now()}

	w.eventsCh <- e1
	w.eventsCh <- e2

	// Close eventsCh, then wait
	close(w.eventsCh)
	handler.workerWg.Wait()

	// If we reached here, it means flush happened upon hitting batchSize (2).
	// We didn't explicitly check what was posted; for that, see the next test with a more elaborate mock.
	// Here we simply test we didn't panic and that code ran to completion.
	// Additional success checks can be done by counting calls, etc.

	// No leftover events expected in retryBuffer
	assert.Empty(t, w.retryBuffer, "retryBuffer should be empty after successful flush")
}

func TestRunBackgroundFlusher_FlushOnInterval(t *testing.T) {
	t.Parallel()

	// This test verifies that if fewer than batchSize events are in the channel,
	// the flush happens after flushInterval.

	// We'll keep flushInterval short so that we don't have to wait long.
	flushInterval := 50 * time.Millisecond

	// We'll track how many times our mock is called to ensure flush occurs.
	var callCount int

	handler := &SeqHandler{
		shared: &shared{
			client:        GetHTTPClientMock(200, "ok", func() { callCount++ }),
			seqURL:        "http://example.com",
			flushInterval: flushInterval,
			batchSize:     10, // large batchSize so it won't flush except on interval
		},
	}

	w := &worker{
		eventsCh: make(chan CLEFEvent, 10),
	}

	handler.workerWg.Add(1)

	go handler.runBackgroundFlusher(w)

	// Send 1 event (less than batchSize).
	w.eventsCh <- CLEFEvent{Message: "event1", Timestamp: time.Now()}

	// Wait a bit longer than flushInterval to ensure flush is triggered
	time.Sleep(2 * flushInterval)

	// Terminate
	close(w.eventsCh)
	handler.workerWg.Wait()

	// By now we expect at least 1 flush (callCount >= 1).
	// The exact number can vary if the background flusher loop ran more than once
	// but it should be at least 1.
	require.GreaterOrEqual(t, callCount, 1)
	assert.Empty(t, w.retryBuffer)
}

func TestRunBackgroundFlusher_RetryOnFailure(t *testing.T) {
	t.Parallel()

	// This test will simulate a first failure on sending batch,
	// then a subsequent success, ensuring retryBuffer is used.

	var attempts int
	handler := &SeqHandler{
		shared: &shared{
			client: &http.Client{
				Transport: &mockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						attempts++
						if attempts == 1 {
							return &http.Response{
								StatusCode: http.StatusInternalServerError,
								Body:       io.NopCloser(bytes.NewBufferString("error")),
							}, nil
						}

						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewBufferString("ok")),
						}, nil
					},
				},
			},
			seqURL:           "http://example.com",
			flushInterval:    100 * time.Hour,
			batchSize:        2,
			errorHandlerFunc: func(err error) {},
		},
	}

	w := &worker{
		eventsCh:    make(chan CLEFEvent, 10),
		retryBuffer: nil, // start empty
	}

	handler.workerWg.Add(1)

	go handler.runBackgroundFlusher(w)

	// Send 2 events -> triggers flush immediately (batchSize=2).
	e1 := CLEFEvent{Message: "fail1", Timestamp: time.Now()}
	e2 := CLEFEvent{Message: "fail2", Timestamp: time.Now()}

	w.eventsCh <- e1
	w.eventsCh <- e2

	// Give the background flusher a microsecond to process the batch trigger
	time.Sleep(10 * time.Millisecond)

	// Close and wait
	close(w.eventsCh)
	handler.workerWg.Wait()

	// We expect:
	//  - First attempt to send => 500 => both events remain in retryBuffer
	//  - Second attempt when flushCurrentBatch is called before exit => success => retryBuffer cleared

	// So there should have been 2 attempts total.
	assert.Equal(t, 2, attempts, "expected 2 attempts to send batch")
	// Retry buffer should be empty after the final success
	assert.Empty(t, w.retryBuffer)
}

func TestRunBackgroundFlusher_RetryBufferFlushedOnClose(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var sent []string

	handler := &SeqHandler{
		shared: &shared{
			client: &http.Client{
				Transport: &mockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						body, _ := io.ReadAll(req.Body)
						mu.Lock()
						sent = append(sent, string(body))
						mu.Unlock()

						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewReader(nil)),
						}, nil
					},
				},
			},
			seqURL:           "http://example.com",
			flushInterval:    100 * time.Hour,
			batchSize:        100,
			workerCount:      1,
			errorHandlerFunc: func(err error) {},
		},
	}

	w := &worker{
		eventsCh: make(chan CLEFEvent, 10),
		retryBuffer: []CLEFEvent{
			{Message: "retry1", Timestamp: time.Now()},
			{Message: "retry2", Timestamp: time.Now()},
		},
	}

	handler.workerWg.Add(1)
	go handler.runBackgroundFlusher(w)

	// Close immediately - no new events, just the retry buffer
	close(w.eventsCh)
	handler.workerWg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if len(sent) == 0 {
		t.Error("expected retry buffer to be flushed on close, but nothing was sent")
	}

	if len(w.retryBuffer) != 0 {
		t.Errorf("expected retry buffer to be empty after close, got %d events", len(w.retryBuffer))
	}
}

func TestAttemptSendBatch_SplitsOnRequestTooLarge(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var sentBatches []int

	handler := &SeqHandler{
		shared: &shared{
			client: &http.Client{
				Transport: &mockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						body, _ := io.ReadAll(req.Body)
						lines := strings.Count(strings.TrimSpace(string(body)), "\n") + 1

						mu.Lock()
						defer mu.Unlock()

						// Reject batches larger than 2 events
						if lines > 2 {
							return &http.Response{
								StatusCode: http.StatusRequestEntityTooLarge,
								Body:       io.NopCloser(bytes.NewReader(nil)),
							}, nil
						}

						sentBatches = append(sentBatches, lines)

						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewReader(nil)),
						}, nil
					},
				},
			},
			seqURL:           "http://example.com",
			errorHandlerFunc: func(err error) {},
		},
	}

	events := []CLEFEvent{
		{Message: "e1", Timestamp: time.Now()},
		{Message: "e2", Timestamp: time.Now()},
		{Message: "e3", Timestamp: time.Now()},
		{Message: "e4", Timestamp: time.Now()},
		{Message: "e5", Timestamp: time.Now()},
		{Message: "e6", Timestamp: time.Now()},
	}

	leftover := handler.attemptSendBatch(events)

	mu.Lock()
	defer mu.Unlock()

	if len(leftover) != 0 {
		t.Errorf("expected no leftover, got %d events", len(leftover))
	}

	// All events should have been sent in batches of 1 or 2
	total := 0
	for _, n := range sentBatches {
		if n > 2 {
			t.Errorf("batch of size %d should have been split", n)
		}
		total += n
	}

	if total != len(events) {
		t.Errorf("expected %d total events sent, got %d", len(events), total)
	}
}

func TestAttemptSendBatch_DropsOversizedSingleEvent(t *testing.T) {
	t.Parallel()

	var errorCalled bool

	handler := &SeqHandler{
		shared: &shared{
			client: &http.Client{
				Transport: &mockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: http.StatusRequestEntityTooLarge,
							Body:       io.NopCloser(bytes.NewReader(nil)),
						}, nil
					},
				},
			},
			seqURL: "http://example.com",
			errorHandlerFunc: func(err error) {
				errorCalled = true
			},
		},
	}

	events := []CLEFEvent{
		{Message: "huge event", Timestamp: time.Now()},
	}

	leftover := handler.attemptSendBatch(events)

	if len(leftover) != 0 {
		t.Errorf("expected single oversized event to be dropped, got %d leftover", len(leftover))
	}

	if !errorCalled {
		t.Error("expected error handler to be called when dropping oversized event")
	}
}

func TestAttemptSendBatch_PartialFailureOnSplit(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var attempts int

	handler := &SeqHandler{
		shared: &shared{
			client: &http.Client{
				Transport: &mockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						body, _ := io.ReadAll(req.Body)
						lines := strings.Count(strings.TrimSpace(string(body)), "\n") + 1

						mu.Lock()
						attempts++
						current := attempts
						mu.Unlock()

						// First call: 413 to trigger split
						// Second call (left half): success
						// Third call (right half): 500 server error
						switch {
						case current == 1:
							return &http.Response{
								StatusCode: http.StatusRequestEntityTooLarge,
								Body:       io.NopCloser(bytes.NewReader(nil)),
							}, nil
						case current == 2:
							return &http.Response{
								StatusCode: http.StatusOK,
								Body:       io.NopCloser(bytes.NewReader(nil)),
							}, nil
						default:
							_ = lines
							return &http.Response{
								StatusCode: http.StatusInternalServerError,
								Body:       io.NopCloser(bytes.NewReader(nil)),
							}, nil
						}
					},
				},
			},
			seqURL:           "http://example.com",
			errorHandlerFunc: func(err error) {},
		},
	}

	events := []CLEFEvent{
		{Message: "e1", Timestamp: time.Now()},
		{Message: "e2", Timestamp: time.Now()},
		{Message: "e3", Timestamp: time.Now()},
		{Message: "e4", Timestamp: time.Now()},
	}

	leftover := handler.attemptSendBatch(events)

	// Left half (e1, e2) succeeded, right half (e3, e4) failed with 500
	if len(leftover) != 2 {
		t.Errorf("expected 2 leftover events from failed right half, got %d", len(leftover))
	}

	if leftover[0].Message != "e3" || leftover[1].Message != "e4" {
		t.Errorf("expected leftover to be e3 and e4, got %s and %s", leftover[0].Message, leftover[1].Message)
	}
}

func TestPurgeOldEvents(t *testing.T) {
	t.Parallel()

	// Directly test purgeOldEvents, ensuring that events older than a certain cutoff are removed.
	now := time.Now()

	// Some events older than 5 minutes, some newer
	oldEvent := CLEFEvent{Message: "old", Timestamp: now.Add(-10 * time.Minute)}
	newEvent := CLEFEvent{Message: "new", Timestamp: now.Add(-1 * time.Minute)}

	handler := &SeqHandler{
		shared: &shared{
			workers:          []worker{{retryBuffer: []CLEFEvent{oldEvent, newEvent}}},
			errorHandlerFunc: func(err error) {},
		},
	}
	w := &handler.workers[0]

	cutoff := now.Add(-5 * time.Minute)
	handler.purgeOldEvents(w, cutoff)

	// We expect only the new event to remain (the old one is older than cutoff).
	require.Len(t, w.retryBuffer, 1, "expected only one event left in retryBuffer")
	assert.Equal(t, "new", w.retryBuffer[0].Message)
}

func TestNoFlushMode(t *testing.T) {
	t.Parallel()

	// If h.noFlush is set, runBackgroundFlusher should exit immediately.
	handler := &SeqHandler{
		shared: &shared{
			noFlush:       true,
			flushInterval: 1 * time.Millisecond,
		},
	}

	w := &worker{
		eventsCh: make(chan CLEFEvent, 1),
	}

	handler.workerWg.Add(1)

	go handler.runBackgroundFlusher(w)

	// Even if we send events, it should immediately return and do nothing.
	w.eventsCh <- CLEFEvent{Message: "test", Timestamp: time.Now()}

	// Close channels
	close(w.eventsCh)
	handler.workerWg.Wait()

	// Confirm that we never stored anything in retryBuffer
	assert.Nil(t, w.retryBuffer, "retryBuffer should remain nil/empty in noFlush mode")
}
