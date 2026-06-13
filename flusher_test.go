package slogseq

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

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

// Expectation: The flusher should flush events when the batch size is reached.
func Test_runBackgroundFlusher_FlushOnBatchSize_Success(t *testing.T) {
	t.Parallel()

	handler := &SeqHandler{
		shared: &shared{
			client:        GetHTTPClientMock(200, "ok", func() {}),
			seqURL:        "http://example.com",
			flushInterval: 100 * time.Hour,
			batchSize:     2,
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

	go handler.runBackgroundFlusher(w)

	w.eventsCh <- CLEFEvent{Message: "event1", Timestamp: time.Now()}
	w.eventsCh <- CLEFEvent{Message: "event2", Timestamp: time.Now()}

	close(w.eventsCh)
	handler.workerWg.Wait()

	require.Empty(t, w.retryBuffer, "retryBuffer should be empty after successful flush")
}

// Expectation: The flusher should flush events after the flush interval even if batch size is not reached.
func Test_runBackgroundFlusher_FlushOnInterval_Success(t *testing.T) {
	t.Parallel()

	flushInterval := 50 * time.Millisecond

	var callCount int

	handler := &SeqHandler{
		shared: &shared{
			client:        GetHTTPClientMock(200, "ok", func() { callCount++ }),
			seqURL:        "http://example.com",
			flushInterval: flushInterval,
			batchSize:     10,
		},
	}

	w := &worker{
		eventsCh: make(chan CLEFEvent, 10),
	}

	handler.workerWg.Add(1)

	go handler.runBackgroundFlusher(w)

	w.eventsCh <- CLEFEvent{Message: "event1", Timestamp: time.Now()}

	time.Sleep(2 * flushInterval)

	close(w.eventsCh)
	handler.workerWg.Wait()

	// By now we expect at least 1 flush (callCount >= 1).
	// The exact number can vary if the background flusher loop ran more than once
	// but it should be at least 1.
	require.GreaterOrEqual(t, callCount, 1)
	require.Empty(t, w.retryBuffer)
}

// Expectation: The flusher should retry failed batches and succeed on subsequent attempts.
func Test_runBackgroundFlusher_RetryOnFailure_Success(t *testing.T) {
	t.Parallel()

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
		retryBuffer: nil,
	}

	handler.workerWg.Add(1)

	go handler.runBackgroundFlusher(w)

	w.eventsCh <- CLEFEvent{Message: "fail1", Timestamp: time.Now()}
	w.eventsCh <- CLEFEvent{Message: "fail2", Timestamp: time.Now()}

	time.Sleep(10 * time.Millisecond)

	close(w.eventsCh)
	handler.workerWg.Wait()

	require.Equal(t, 2, attempts, "expected 2 attempts to send batch")
	require.Empty(t, w.retryBuffer)
}

// Expectation: The flusher should flush the retry buffer when the channel is closed.
func Test_runBackgroundFlusher_RetryBufferFlushedOnClose_Success(t *testing.T) {
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

	close(w.eventsCh)
	handler.workerWg.Wait()

	mu.Lock()
	defer mu.Unlock()

	require.NotEmpty(t, sent, "expected retry buffer to be flushed on close")
	require.Empty(t, w.retryBuffer, "expected retry buffer to be empty after close")
}

// Expectation: The flusher should exit immediately and not process events when noFlush is set.
func Test_runBackgroundFlusher_NoFlushModeExits_Success(t *testing.T) {
	t.Parallel()

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

	w.eventsCh <- CLEFEvent{Message: "test", Timestamp: time.Now()}

	close(w.eventsCh)
	handler.workerWg.Wait()

	require.Nil(t, w.retryBuffer, "retryBuffer should remain nil in noFlush mode")
}

// Expectation: The function should split oversized batches and send them in smaller chunks.
func Test_attemptSendBatch_SplitsOnRequestTooLarge_Success(t *testing.T) {
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

	require.Empty(t, leftover, "expected no leftover events")

	// All events should have been sent in batches of 1 or 2
	total := 0
	for _, n := range sentBatches {
		require.LessOrEqual(t, n, 2, "batch should have been split to at most 2")
		total += n
	}

	require.Equal(t, len(events), total, "expected all events to be sent")
}

// Expectation: The function should drop a single event that exceeds the server size limit.
func Test_attemptSendBatch_DropsOversizedSingleEvent_Success(t *testing.T) {
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

	require.Empty(t, leftover, "expected single oversized event to be dropped")
	require.True(t, errorCalled, "expected error handler to be called when dropping oversized event")
}

// Expectation: The function should return leftover events from the failed half of a split batch.
func Test_attemptSendBatch_PartialFail_ReturnsLeftover_Success(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var attempts int

	handler := &SeqHandler{
		shared: &shared{
			client: &http.Client{
				Transport: &mockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						_, _ = io.Copy(io.Discard, req.Body)

						mu.Lock()
						attempts++
						current := attempts
						mu.Unlock()

						// First call: 413 to trigger split
						// Second call (left half): success
						// Third call (right half): 500 server error
						switch current {
						case 1:
							return &http.Response{
								StatusCode: http.StatusRequestEntityTooLarge,
								Body:       io.NopCloser(bytes.NewReader(nil)),
							}, nil
						case 2:
							return &http.Response{
								StatusCode: http.StatusOK,
								Body:       io.NopCloser(bytes.NewReader(nil)),
							}, nil
						default:
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

	require.Len(t, leftover, 2, "expected 2 leftover events from failed right half")
	require.Equal(t, "e3", leftover[0].Message)
	require.Equal(t, "e4", leftover[1].Message)
}

// Expectation: The function should remove events older than the cutoff from the retry buffer.
func Test_purgeOldEvents_RemovesOldEvents_Success(t *testing.T) {
	t.Parallel()

	now := time.Now()
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

	require.Len(t, w.retryBuffer, 1, "expected only one event left in retryBuffer")
	require.Equal(t, "new", w.retryBuffer[0].Message)
}
