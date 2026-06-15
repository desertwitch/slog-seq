package slogseq

import (
	"bytes"
	"encoding/json"
	"errors"
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

// Expectation: newHTTPClient should return a non-nil client with the configured TLS setting.
func Test_newHTTPClient_ReturnsFunctionalClient_Success(t *testing.T) {
	t.Parallel()

	client := newHTTPClient(false)

	require.NotNil(t, client)
	require.Equal(t, requestTimeout, client.Timeout)

	transport, ok := client.Transport.(*http.Transport)
	require.True(t, ok)
	require.False(t, transport.TLSClientConfig.InsecureSkipVerify)
}

// Expectation: newHTTPClient with skipVerify should disable TLS verification.
func Test_newHTTPClient_SkipVerify_DisablesTLSVerification_Success(t *testing.T) {
	t.Parallel()

	client := newHTTPClient(true)

	transport, ok := client.Transport.(*http.Transport)
	require.True(t, ok)
	require.True(t, transport.TLSClientConfig.InsecureSkipVerify)
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

// Expectation: The flusher should handle rapid open/close without blocking.
func Test_runBackgroundFlusher_ImmediateClose_Success(t *testing.T) {
	t.Parallel()

	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(200, "ok", func() {}),
			seqURL:           "http://example.com",
			flushInterval:    time.Hour,
			batchSize:        100,
			errorHandlerFunc: func(_ error) {},
		},
	}

	w := &worker{
		eventsCh: make(chan CLEFEvent, 10),
	}

	handler.workerWg.Add(1)
	close(w.eventsCh)
	go handler.runBackgroundFlusher(w)

	done := make(chan struct{})
	go func() {
		handler.workerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("flusher did not exit after immediate close")
	}
}

// Expectation: The flusher should drain all remaining events from channel on close.
func Test_runBackgroundFlusher_DrainsOnClose_Success(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var totalSent int

	handler := &SeqHandler{
		shared: &shared{
			client: &http.Client{
				Transport: &mockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						body, _ := io.ReadAll(req.Body)
						lines := strings.Split(strings.TrimSpace(string(body)), "\n")
						mu.Lock()
						totalSent += len(lines)
						mu.Unlock()

						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewReader(nil)),
						}, nil
					},
				},
			},
			seqURL:           "http://example.com",
			flushInterval:    time.Hour,
			batchSize:        100,
			errorHandlerFunc: func(_ error) {},
		},
	}

	w := &worker{
		eventsCh: make(chan CLEFEvent, 100),
	}

	// Fill channel before starting flusher.
	for i := range 10 {
		w.eventsCh <- CLEFEvent{Message: "prefilled", Timestamp: time.Now(), Properties: map[string]any{"i": i}}
	}
	close(w.eventsCh)

	handler.workerWg.Add(1)
	go handler.runBackgroundFlusher(w)
	handler.workerWg.Wait()

	mu.Lock()
	defer mu.Unlock()

	require.Equal(t, 10, totalSent, "all prefilled events should be drained and sent")
}

// Expectation: The flusher should accumulate events until batch size before sending.
func Test_runBackgroundFlusher_BatchAccumulation_Success(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var batchSizes []int

	handler := &SeqHandler{
		shared: &shared{
			client: &http.Client{
				Transport: &mockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						body, _ := io.ReadAll(req.Body)
						lines := strings.Split(strings.TrimSpace(string(body)), "\n")
						mu.Lock()
						batchSizes = append(batchSizes, len(lines))
						mu.Unlock()

						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewReader(nil)),
						}, nil
					},
				},
			},
			seqURL:           "http://example.com",
			flushInterval:    time.Hour,
			batchSize:        5,
			errorHandlerFunc: func(_ error) {},
		},
	}

	w := &worker{
		eventsCh: make(chan CLEFEvent, 100),
	}

	handler.workerWg.Add(1)
	go handler.runBackgroundFlusher(w)

	// Send exactly 5 events to trigger one batch flush.
	for range 5 {
		w.eventsCh <- CLEFEvent{Message: "event", Timestamp: time.Now()}
	}

	// Give the flusher time to process.
	time.Sleep(50 * time.Millisecond)

	close(w.eventsCh)
	handler.workerWg.Wait()

	mu.Lock()
	defer mu.Unlock()

	require.NotEmpty(t, batchSizes)
	require.Equal(t, 5, batchSizes[0], "first batch should contain exactly batchSize events")
}

// Expectation: The purge ticker should purge old events from the retry buffer.
func Test_runBackgroundFlusher_PurgeTicker_PurgesOldEvents_Success(t *testing.T) {
	t.Parallel()

	flushInterval := 5 * time.Millisecond

	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(500, "error", func() {}),
			seqURL:           "http://example.com",
			flushInterval:    flushInterval,
			batchSize:        1,
			purgeUnsentAfter: 50 * time.Millisecond,
			errorHandlerFunc: func(_ error) {},
		},
	}

	w := &worker{
		eventsCh: make(chan CLEFEvent, 10),
	}

	handler.workerWg.Add(1)
	go handler.runBackgroundFlusher(w)

	// Send an event that will fail and land in the retry buffer.
	w.eventsCh <- CLEFEvent{
		Message:   "will-be-purged",
		Timestamp: time.Now().Add(-10 * time.Minute), // already old
	}

	// Wait for the flush to fail (event goes to retry buffer)
	// and then for the purge ticker to fire and remove it.
	time.Sleep(500 * time.Millisecond)

	close(w.eventsCh)
	handler.workerWg.Wait()

	require.Empty(t, w.retryBuffer, "old event should have been purged by the purge ticker")
}

// Expectation: The purge ticker should keep recent events in the retry buffer.
func Test_runBackgroundFlusher_PurgeTicker_KeepsRecentEvents_Success(t *testing.T) {
	t.Parallel()

	flushInterval := 5 * time.Millisecond

	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(500, "error", func() {}),
			seqURL:           "http://example.com",
			flushInterval:    flushInterval,
			batchSize:        1,
			purgeUnsentAfter: 10 * time.Minute,
			errorHandlerFunc: func(_ error) {},
		},
	}

	w := &worker{
		eventsCh: make(chan CLEFEvent, 10),
	}

	handler.workerWg.Add(1)
	go handler.runBackgroundFlusher(w)

	// Send an event with a recent timestamp - should survive purging.
	w.eventsCh <- CLEFEvent{
		Message:   "recent",
		Timestamp: time.Now(),
	}

	// Wait for flush to fail and purge ticker to fire.
	time.Sleep(500 * time.Millisecond)

	close(w.eventsCh)
	handler.workerWg.Wait()

	// The event failed to send so it should still be in the retry buffer,
	// and the purge ticker should not have removed it (purgeUnsentAfter is 10 minutes).
	require.NotEmpty(t, w.retryBuffer, "recent event should not be purged")
	require.Equal(t, "recent", w.retryBuffer[0].Message)
}

// Expectation: sendWithRetry with empty input should return nil.
func Test_sendWithRetry_EmptyInput_ReturnsNil_Success(t *testing.T) {
	t.Parallel()

	handler := &SeqHandler{
		shared: &shared{},
	}

	result := handler.sendWithRetry(nil)
	require.Nil(t, result)

	result = handler.sendWithRetry([]CLEFEvent{})
	require.Nil(t, result)
}

// Expectation: sendWithRetry should return nil on successful send.
func Test_sendWithRetry_SuccessfulSend_ReturnsNil_Success(t *testing.T) {
	t.Parallel()

	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(200, "ok", func() {}),
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) {},
		},
	}

	events := []CLEFEvent{
		{Message: "e1", Timestamp: time.Now()},
	}

	result := handler.sendWithRetry(events)
	require.Nil(t, result)
}

// Expectation: sendWithRetry should return events on failure.
func Test_sendWithRetry_FailedSend_ReturnsEvents_Success(t *testing.T) {
	t.Parallel()

	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(500, "error", func() {}),
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) {},
		},
	}

	events := []CLEFEvent{
		{Message: "e1", Timestamp: time.Now()},
	}

	result := handler.sendWithRetry(events)
	require.Len(t, result, 1)
}

// Expectation: flushCurrentBatch should send events and clear the slice.
func Test_flushCurrentBatch_SendsAndClearsEvents_Success(t *testing.T) {
	t.Parallel()

	var callCount int
	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(200, "ok", func() { callCount++ }),
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) {},
		},
	}

	w := &worker{}
	events := []CLEFEvent{
		{Message: "e1", Timestamp: time.Now()},
		{Message: "e2", Timestamp: time.Now()},
	}

	handler.flushCurrentBatch(w, &events)

	require.Empty(t, events)
	require.Equal(t, 1, callCount)
	require.Empty(t, w.retryBuffer)
}

// Expectation: flushCurrentBatch should flush retry buffer before current events.
func Test_flushCurrentBatch_FlushesRetryBufferFirst_Success(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var order []string

	handler := &SeqHandler{
		shared: &shared{
			client: &http.Client{
				Transport: &mockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						body, _ := io.ReadAll(req.Body)
						mu.Lock()
						if strings.Contains(string(body), "retry") {
							order = append(order, "retry")
						} else {
							order = append(order, "current")
						}
						mu.Unlock()

						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewReader(nil)),
						}, nil
					},
				},
			},
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) {},
		},
	}

	w := &worker{
		retryBuffer: []CLEFEvent{
			{Message: "retry event", Timestamp: time.Now()},
		},
	}
	events := []CLEFEvent{
		{Message: "current event", Timestamp: time.Now()},
	}

	handler.flushCurrentBatch(w, &events)

	mu.Lock()
	defer mu.Unlock()

	require.Equal(t, []string{"retry", "current"}, order)
	require.Empty(t, events)
	require.Empty(t, w.retryBuffer)
}

// Expectation: flushCurrentBatch with no events and no retry buffer should not make HTTP calls.
func Test_flushCurrentBatch_NothingToFlush_NoHTTPCalls_Success(t *testing.T) {
	t.Parallel()

	called := false
	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(200, "ok", func() { called = true }),
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) {},
		},
	}

	w := &worker{}
	events := []CLEFEvent{}

	handler.flushCurrentBatch(w, &events)

	require.False(t, called)
}

// Expectation: flushCurrentBatch should accumulate failed events into retry buffer.
func Test_flushCurrentBatch_FailedEvents_AccumulateInRetryBuffer_Success(t *testing.T) {
	t.Parallel()

	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(500, "error", func() {}),
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) {},
		},
	}

	w := &worker{}
	events := []CLEFEvent{
		{Message: "e1", Timestamp: time.Now()},
		{Message: "e2", Timestamp: time.Now()},
	}

	handler.flushCurrentBatch(w, &events)

	require.Empty(t, events, "events slice should be cleared even on failure")
	require.Len(t, w.retryBuffer, 2, "failed events should be in retry buffer")
}

// Expectation: flushCurrentBatch with only retry buffer events should flush them.
func Test_flushCurrentBatch_OnlyRetryBuffer_Flushed_Success(t *testing.T) {
	t.Parallel()

	var callCount int
	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(200, "ok", func() { callCount++ }),
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) {},
		},
	}

	w := &worker{
		retryBuffer: []CLEFEvent{
			{Message: "retry1", Timestamp: time.Now()},
		},
	}
	events := []CLEFEvent{}

	handler.flushCurrentBatch(w, &events)

	require.Equal(t, 1, callCount)
	require.Empty(t, w.retryBuffer)
}

// Expectation: flushCurrentBatch should append failed current events to existing retry buffer failures.
func Test_flushCurrentBatch_RetryBufferFailAndCurrentFail_BothAccumulate_Success(t *testing.T) {
	t.Parallel()

	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(500, "error", func() {}),
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) {},
		},
	}

	w := &worker{
		retryBuffer: []CLEFEvent{
			{Message: "old-retry", Timestamp: time.Now()},
		},
	}
	events := []CLEFEvent{
		{Message: "new-event", Timestamp: time.Now()},
	}

	handler.flushCurrentBatch(w, &events)

	require.Empty(t, events)
	require.Len(t, w.retryBuffer, 2, "both failed retry and failed current should be in buffer")
}

// Expectation: attemptSendBatch with empty input should return nil without making HTTP calls.
func Test_attemptSendBatch_EmptyInput_ReturnsNil_Success(t *testing.T) {
	t.Parallel()

	called := false
	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(200, "ok", func() { called = true }),
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) {},
		},
	}

	result := handler.attemptSendBatch(nil)
	require.Nil(t, result)
	require.False(t, called, "HTTP client should not be called for empty input")

	result = handler.attemptSendBatch([]CLEFEvent{})
	require.Nil(t, result)
	require.False(t, called)
}

// Expectation: attemptSendBatch should set Content-Type and API key headers.
func Test_attemptSendBatch_SetsHeaders_Success(t *testing.T) {
	t.Parallel()

	var capturedReq *http.Request

	handler := &SeqHandler{
		shared: &shared{
			client: &http.Client{
				Transport: &mockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						capturedReq = req

						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewReader(nil)),
						}, nil
					},
				},
			},
			seqURL:           "http://example.com",
			apiKey:           "my-api-key",
			errorHandlerFunc: func(_ error) {},
		},
	}

	events := []CLEFEvent{
		{Message: "e1", Timestamp: time.Now()},
	}

	handler.attemptSendBatch(events)

	require.NotNil(t, capturedReq)
	require.Equal(t, "application/vnd.serilog.clef", capturedReq.Header.Get("Content-Type"))
	require.Equal(t, "my-api-key", capturedReq.Header.Get("X-Seq-ApiKey"))
}

// Expectation: attemptSendBatch should omit API key header when apiKey is empty.
func Test_attemptSendBatch_EmptyAPIKey_NoHeader_Success(t *testing.T) {
	t.Parallel()

	var capturedReq *http.Request

	handler := &SeqHandler{
		shared: &shared{
			client: &http.Client{
				Transport: &mockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						capturedReq = req

						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewReader(nil)),
						}, nil
					},
				},
			},
			seqURL:           "http://example.com",
			apiKey:           "",
			errorHandlerFunc: func(_ error) {},
		},
	}

	events := []CLEFEvent{
		{Message: "e1", Timestamp: time.Now()},
	}

	handler.attemptSendBatch(events)

	require.NotNil(t, capturedReq)
	require.Empty(t, capturedReq.Header.Get("X-Seq-ApiKey"))
}

// Expectation: attemptSendBatch should send valid CLEF JSON lines in the request body.
func Test_attemptSendBatch_SendsValidCLEFBody_Success(t *testing.T) {
	t.Parallel()

	var capturedBody string

	handler := &SeqHandler{
		shared: &shared{
			client: &http.Client{
				Transport: &mockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						body, _ := io.ReadAll(req.Body)
						capturedBody = string(body)

						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewReader(nil)),
						}, nil
					},
				},
			},
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) {},
		},
	}

	now := time.Now()
	events := []CLEFEvent{
		{Message: "first", Level: "Information", Timestamp: now},
		{Message: "second", Level: "Warning", Timestamp: now},
	}

	handler.attemptSendBatch(events)

	lines := strings.Split(strings.TrimSpace(capturedBody), "\n")
	require.Len(t, lines, 2)

	for _, line := range lines {
		var m map[string]any
		err := json.Unmarshal([]byte(line), &m)
		require.NoError(t, err, "each line should be valid JSON")
		require.True(t, json.Valid([]byte(line)), "each line should be valid JSON")
		require.Contains(t, m, "@t")
		require.Contains(t, m, "@m")
		require.Contains(t, m, "@l")
	}
}

// Expectation: attemptSendBatch should return events and call error handler on HTTP client error.
func Test_attemptSendBatch_HTTPClientError_ReturnsEvents_Success(t *testing.T) {
	t.Parallel()

	var capturedErr error

	handler := &SeqHandler{
		shared: &shared{
			client: &http.Client{
				Transport: &mockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						return nil, errors.New("connection refused")
					},
				},
			},
			seqURL:           "http://example.com",
			errorHandlerFunc: func(err error) { capturedErr = err },
		},
	}

	events := []CLEFEvent{
		{Message: "e1", Timestamp: time.Now()},
	}

	result := handler.attemptSendBatch(events)

	require.Len(t, result, 1)
	require.Error(t, capturedErr)
	require.Contains(t, capturedErr.Error(), "connection refused")
}

// Expectation: attemptSendBatch should return events and call error handler on non-2xx status.
func Test_attemptSendBatch_Non2xxStatus_ReturnsEvents_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status int
	}{
		{name: "400 bad request", status: 400},
		{name: "401 unauthorized", status: 401},
		{name: "403 forbidden", status: 403},
		{name: "404 not found", status: 404},
		{name: "500 server error", status: 500},
		{name: "503 unavailable", status: 503},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var errCalled bool
			handler := &SeqHandler{
				shared: &shared{
					client:           GetHTTPClientMock(tt.status, "error", func() {}),
					seqURL:           "http://example.com",
					errorHandlerFunc: func(_ error) { errCalled = true },
				},
			}

			events := []CLEFEvent{
				{Message: "e1", Timestamp: time.Now()},
			}

			result := handler.attemptSendBatch(events)

			require.Len(t, result, 1, "events should be returned for status %d", tt.status)
			require.True(t, errCalled, "error handler should be called for status %d", tt.status)
		})
	}
}

// Expectation: attemptSendBatch should accept all 2xx status codes.
func Test_attemptSendBatch_All2xxStatuses_ReturnsNil_Success(t *testing.T) {
	t.Parallel()

	for status := 200; status <= 299; status++ {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()

			handler := &SeqHandler{
				shared: &shared{
					client:           GetHTTPClientMock(status, "ok", func() {}),
					seqURL:           "http://example.com",
					errorHandlerFunc: func(_ error) {},
				},
			}

			events := []CLEFEvent{
				{Message: "e1", Timestamp: time.Now()},
			}

			result := handler.attemptSendBatch(events)

			require.Nil(t, result)
		})
	}
}

// Expectation: attemptSendBatch should POST to the configured seqURL.
func Test_attemptSendBatch_PostsToConfiguredURL_Success(t *testing.T) {
	t.Parallel()

	var capturedURL string
	var capturedMethod string

	handler := &SeqHandler{
		shared: &shared{
			client: &http.Client{
				Transport: &mockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						capturedURL = req.URL.String()
						capturedMethod = req.Method

						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewReader(nil)),
						}, nil
					},
				},
			},
			seqURL:           "http://my-seq-server:5341/api/events/raw",
			errorHandlerFunc: func(_ error) {},
		},
	}

	events := []CLEFEvent{
		{Message: "e1", Timestamp: time.Now()},
	}

	handler.attemptSendBatch(events)

	require.Equal(t, "http://my-seq-server:5341/api/events/raw", capturedURL)
	require.Equal(t, "POST", capturedMethod)
}

// Expectation: attemptSendBatch should use POST method.
func Test_attemptSendBatch_UsesPostMethod_Success(t *testing.T) {
	t.Parallel()

	var capturedMethod string

	handler := &SeqHandler{
		shared: &shared{
			client: &http.Client{
				Transport: &mockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						capturedMethod = req.Method

						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewReader(nil)),
						}, nil
					},
				},
			},
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) {},
		},
	}

	events := []CLEFEvent{
		{Message: "e1", Timestamp: time.Now()},
	}

	handler.attemptSendBatch(events)

	require.Equal(t, http.MethodPost, capturedMethod)
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

// Expectation: attemptSendBatch should return events and call error handler when request creation fails.
func Test_attemptSendBatch_InvalidURL_ReturnsEvents_Success(t *testing.T) {
	t.Parallel()

	var capturedErr error
	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(200, "ok", func() {}),
			seqURL:           "://invalid-url",
			errorHandlerFunc: func(err error) { capturedErr = err },
		},
	}

	events := []CLEFEvent{
		{Message: "e1", Timestamp: time.Now()},
	}

	result := handler.attemptSendBatch(events)

	require.Len(t, result, 1)
	require.Error(t, capturedErr)
}

// Expectation: attemptSendBatch should return events when JSON encoding fails.
func Test_attemptSendBatch_JSONEncodeFailure_ReturnsEvents_Success(t *testing.T) {
	t.Parallel()

	called := false
	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(200, "ok", func() { called = true }),
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) {},
		},
	}

	events := []CLEFEvent{
		{
			Message:    "e1",
			Timestamp:  time.Now(),
			Properties: map[string]any{"bad": make(chan int)},
		},
	}

	result := handler.attemptSendBatch(events)

	require.Len(t, result, 1)
	require.False(t, called, "HTTP client should not be called when encoding fails")
}

// Expectation: purgeOldEvents with empty retry buffer should be a no-op.
func Test_purgeOldEvents_EmptyBuffer_NoOp_Success(t *testing.T) {
	t.Parallel()

	var errCalled bool
	handler := &SeqHandler{
		shared: &shared{
			errorHandlerFunc: func(_ error) { errCalled = true },
		},
	}

	w := &worker{retryBuffer: []CLEFEvent{}}

	handler.purgeOldEvents(w, time.Now())

	require.Empty(t, w.retryBuffer)
	require.False(t, errCalled, "error handler should not be called when nothing was purged")
}

// Expectation: purgeOldEvents should remove all events when all are older than cutoff.
func Test_purgeOldEvents_AllOld_RemovesAll_Success(t *testing.T) {
	t.Parallel()

	now := time.Now()
	handler := &SeqHandler{
		shared: &shared{
			errorHandlerFunc: func(_ error) {},
		},
	}

	w := &worker{
		retryBuffer: []CLEFEvent{
			{Message: "old1", Timestamp: now.Add(-10 * time.Minute)},
			{Message: "old2", Timestamp: now.Add(-20 * time.Minute)},
		},
	}

	handler.purgeOldEvents(w, now.Add(-5*time.Minute))

	require.Empty(t, w.retryBuffer)
}

// Expectation: purgeOldEvents should keep all events when all are newer than cutoff.
func Test_purgeOldEvents_AllNew_KeepsAll_Success(t *testing.T) {
	t.Parallel()

	now := time.Now()
	var errCalled bool
	handler := &SeqHandler{
		shared: &shared{
			errorHandlerFunc: func(_ error) { errCalled = true },
		},
	}

	w := &worker{
		retryBuffer: []CLEFEvent{
			{Message: "new1", Timestamp: now.Add(-1 * time.Minute)},
			{Message: "new2", Timestamp: now.Add(-2 * time.Minute)},
		},
	}

	handler.purgeOldEvents(w, now.Add(-5*time.Minute))

	require.Len(t, w.retryBuffer, 2)
	require.False(t, errCalled, "error handler should not be called when nothing was purged")
}

// Expectation: purgeOldEvents should call error handler with the count of purged events.
func Test_purgeOldEvents_CallsErrorHandlerWithCount_Success(t *testing.T) {
	t.Parallel()

	now := time.Now()
	var capturedErr error
	handler := &SeqHandler{
		shared: &shared{
			errorHandlerFunc: func(err error) { capturedErr = err },
		},
	}

	w := &worker{
		retryBuffer: []CLEFEvent{
			{Message: "old1", Timestamp: now.Add(-10 * time.Minute)},
			{Message: "old2", Timestamp: now.Add(-20 * time.Minute)},
			{Message: "new1", Timestamp: now.Add(-1 * time.Minute)},
		},
	}

	handler.purgeOldEvents(w, now.Add(-5*time.Minute))

	require.Error(t, capturedErr)
	require.Contains(t, capturedErr.Error(), "purged 2 events")
}

// Expectation: purgeOldEvents should keep events whose timestamp exactly equals the cutoff.
func Test_purgeOldEvents_ExactCutoff_Purged_Success(t *testing.T) {
	t.Parallel()

	cutoff := time.Now()
	handler := &SeqHandler{
		shared: &shared{
			errorHandlerFunc: func(_ error) {},
		},
	}

	w := &worker{
		retryBuffer: []CLEFEvent{
			{Message: "exact", Timestamp: cutoff},
		},
	}

	handler.purgeOldEvents(w, cutoff)

	// Timestamp.After(cutoff) is false when equal, so the event should be purged.
	require.Empty(t, w.retryBuffer, "event at exact cutoff should be purged (After is strictly greater)")
}

// Expectation: purgeOldEvents with nil retry buffer should not panic.
func Test_purgeOldEvents_NilBuffer_NoPanic_Success(t *testing.T) {
	t.Parallel()

	handler := &SeqHandler{
		shared: &shared{
			errorHandlerFunc: func(_ error) {},
		},
	}

	w := &worker{retryBuffer: nil}

	require.NotPanics(t, func() {
		handler.purgeOldEvents(w, time.Now())
	})
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

// Expectation: encodeEvent should set all required CLEF keys.
func Test_encodeEvent_RequiredKeys_Success(t *testing.T) {
	t.Parallel()

	now := time.Now()
	e := CLEFEvent{
		Timestamp: now,
		Message:   "hello",
		Level:     "Information",
	}

	m := encodeEvent(e)

	require.Equal(t, now.Format(time.RFC3339Nano), m["@t"])
	require.Equal(t, "hello", m["@m"])
	require.Equal(t, "Information", m["@l"])
}

// Expectation: encodeEvent should include exception when non-empty.
func Test_encodeEvent_WithException_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Error",
		Exception: "stack trace here",
	}

	m := encodeEvent(e)

	require.Equal(t, "stack trace here", m["@x"])
}

// Expectation: encodeEvent should omit exception when empty.
func Test_encodeEvent_EmptyException_Omitted_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
	}

	m := encodeEvent(e)

	_, hasX := m["@x"]
	require.False(t, hasX, "@x should be omitted when exception is empty")
}

// Expectation: encodeEvent should include trace and span IDs when set.
func Test_encodeEvent_TraceAndSpanID_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
		TraceID:   "abc123",
		SpanID:    "def456",
	}

	m := encodeEvent(e)

	require.Equal(t, "abc123", m["@tr"])
	require.Equal(t, "def456", m["@sp"])
}

// Expectation: encodeEvent should omit trace and span IDs when empty.
func Test_encodeEvent_EmptyTraceAndSpanID_Omitted_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
	}

	m := encodeEvent(e)

	_, hasTr := m["@tr"]
	_, hasSp := m["@sp"]
	require.False(t, hasTr)
	require.False(t, hasSp)
}

// Expectation: encodeEvent should include parent span ID when set.
func Test_encodeEvent_ParentSpanID_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp:    time.Now(),
		Message:      "msg",
		Level:        "Information",
		ParentSpanID: "parent123",
	}

	m := encodeEvent(e)

	require.Equal(t, "parent123", m["@ps"])
}

// Expectation: encodeEvent should omit parent span ID when empty.
func Test_encodeEvent_EmptyParentSpanID_Omitted_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
	}

	m := encodeEvent(e)

	_, hasPs := m["@ps"]
	require.False(t, hasPs)
}

// Expectation: encodeEvent should include span start when non-zero.
func Test_encodeEvent_SpanStart_Success(t *testing.T) {
	t.Parallel()

	start := time.Now().Add(-time.Second)
	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
		SpanStart: start,
	}

	m := encodeEvent(e)

	require.Equal(t, start.Format(time.RFC3339Nano), m["@st"])
}

// Expectation: encodeEvent should omit span start when zero.
func Test_encodeEvent_ZeroSpanStart_Omitted_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
	}

	m := encodeEvent(e)

	_, hasSt := m["@st"]
	require.False(t, hasSt)
}

// Expectation: encodeEvent should include span kind when set.
func Test_encodeEvent_SpanKind_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
		SpanKind:  "Server",
	}

	m := encodeEvent(e)

	require.Equal(t, "Server", m["@sk"])
}

// Expectation: encodeEvent should omit span kind when empty.
func Test_encodeEvent_EmptySpanKind_Omitted_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
	}

	m := encodeEvent(e)

	_, hasSk := m["@sk"]
	require.False(t, hasSk)
}

// Expectation: encodeEvent should copy properties into the top-level map.
func Test_encodeEvent_PropertiesCopied_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp:  time.Now(),
		Message:    "msg",
		Level:      "Information",
		Properties: map[string]any{"user": "alice", "count": 42},
	}

	m := encodeEvent(e)

	require.Equal(t, "alice", m["user"])
	require.Equal(t, 42, m["count"])
}

// Expectation: CLEF reserved keys should override user properties with the same name.
func Test_encodeEvent_ReservedKeysOverrideProperties_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp:  time.Now(),
		Message:    "real message",
		Level:      "Information",
		Properties: map[string]any{"@m": "fake message", "@l": "Fake"},
	}

	m := encodeEvent(e)

	require.Equal(t, "real message", m["@m"])
	require.Equal(t, "Information", m["@l"])
}

// Expectation: encodeEvent should include resource attributes when non-empty.
func Test_encodeEvent_ResourceAttributes_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp:          time.Now(),
		Message:            "msg",
		Level:              "Information",
		ResourceAttributes: map[string]any{"service.name": "myapp"},
	}

	m := encodeEvent(e)

	ra, ok := m["@ra"]
	require.True(t, ok, "expected @ra to be set")
	require.NotNil(t, ra)
}

// Expectation: encodeEvent should omit resource attributes when empty.
func Test_encodeEvent_EmptyResourceAttributes_Omitted_Success(t *testing.T) {
	t.Parallel()

	e := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "msg",
		Level:     "Information",
	}

	m := encodeEvent(e)

	_, hasRa := m["@ra"]
	require.False(t, hasRa)
}

// Expectation: dottedToNested should convert a flat dotted key to nested maps.
func Test_dottedToNested_SingleDottedKey_Success(t *testing.T) {
	t.Parallel()

	input := map[string]any{"a.b.c": "value"}
	result := dottedToNested(input)

	a := result["a"].(map[string]any)
	b := a["b"].(map[string]any)
	require.Equal(t, "value", b["c"])
}

// Expectation: dottedToNested should handle keys with no dots as top-level keys.
func Test_dottedToNested_NoDots_TopLevel_Success(t *testing.T) {
	t.Parallel()

	input := map[string]any{"simple": "value"}
	result := dottedToNested(input)

	require.Equal(t, "value", result["simple"])
}

// Expectation: dottedToNested should merge keys that share a common prefix.
func Test_dottedToNested_SharedPrefix_Merges_Success(t *testing.T) {
	t.Parallel()

	input := map[string]any{
		"service.name":    "myapp",
		"service.version": "1.0",
	}
	result := dottedToNested(input)

	service := result["service"].(map[string]any)
	require.Equal(t, "myapp", service["name"])
	require.Equal(t, "1.0", service["version"])
}

// Expectation: dottedToNested should handle deeply nested dotted keys.
func Test_dottedToNested_DeepNesting_Success(t *testing.T) {
	t.Parallel()

	input := map[string]any{"a.b.c.d.e": "deep"}
	result := dottedToNested(input)

	a := result["a"].(map[string]any)
	b := a["b"].(map[string]any)
	c := b["c"].(map[string]any)
	d := c["d"].(map[string]any)
	require.Equal(t, "deep", d["e"])
}

// Expectation: dottedToNested with empty input should return an empty map.
func Test_dottedToNested_EmptyInput_Success(t *testing.T) {
	t.Parallel()

	result := dottedToNested(map[string]any{})

	require.Empty(t, result)
}

// Expectation: dottedToNested should handle mixed dotted and non-dotted keys.
func Test_dottedToNested_MixedKeys_Success(t *testing.T) {
	t.Parallel()

	input := map[string]any{
		"flat":          "value1",
		"nested.key":    "value2",
		"nested.deep.k": "value3",
	}
	result := dottedToNested(input)

	require.Equal(t, "value1", result["flat"])

	nested := result["nested"].(map[string]any)
	require.Equal(t, "value2", nested["key"])

	deep := nested["deep"].(map[string]any)
	require.Equal(t, "value3", deep["k"])
}

// Expectation: dottedToNested should handle a key that is just a dot.
func Test_dottedToNested_SingleDot_Success(t *testing.T) {
	t.Parallel()

	input := map[string]any{".": "dotval"}
	result := dottedToNested(input)

	empty := result[""].(map[string]any)
	require.Equal(t, "dotval", empty[""])
}

// Expectation: dottedToNested should produce deterministic output with conflicting keys.
func Test_dottedToNested_ConflictingKeys_Deterministic_Success(t *testing.T) {
	t.Parallel()

	input := map[string]any{
		"a":   "scalar",
		"a.b": "nested",
	}

	// Run multiple times to verify determinism.
	for range 100 {
		result := dottedToNested(input)
		a, ok := result["a"].(map[string]any)
		require.True(t, ok, "a should be a map after conflict resolution")
		require.Equal(t, "nested", a["b"])
	}
}

// Expectation: addNested with empty path should be a no-op.
func Test_addNested_EmptyPath_NoOp_Success(t *testing.T) {
	t.Parallel()

	dst := make(map[string]any)
	addNested(dst, []string{}, "value")

	require.Empty(t, dst)
}

// Expectation: addNested with single-element path should set the value directly.
func Test_addNested_SingleElement_Success(t *testing.T) {
	t.Parallel()

	dst := make(map[string]any)
	addNested(dst, []string{"key"}, "value")

	require.Equal(t, "value", dst["key"])
}

// Expectation: addNested with multi-element path should create nested maps.
func Test_addNested_MultiElement_Success(t *testing.T) {
	t.Parallel()

	dst := make(map[string]any)
	addNested(dst, []string{"a", "b", "c"}, "deep")

	a := dst["a"].(map[string]any)
	b := a["b"].(map[string]any)
	require.Equal(t, "deep", b["c"])
}

// Expectation: addNested should merge into existing nested maps.
func Test_addNested_MergesExisting_Success(t *testing.T) {
	t.Parallel()

	dst := make(map[string]any)
	addNested(dst, []string{"a", "b"}, "first")
	addNested(dst, []string{"a", "c"}, "second")

	a := dst["a"].(map[string]any)
	require.Equal(t, "first", a["b"])
	require.Equal(t, "second", a["c"])
}

// Expectation: addNested should overwrite non-map value when path extends through it.
func Test_addNested_OverwritesNonMap_Success(t *testing.T) {
	t.Parallel()

	dst := map[string]any{"a": "scalar"}
	addNested(dst, []string{"a", "b"}, "nested")

	a := dst["a"].(map[string]any)
	require.Equal(t, "nested", a["b"])
}
