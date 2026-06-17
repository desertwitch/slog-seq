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
func Test_runFlusher_FlushOnBatchSize_Success(t *testing.T) {
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

	go handler.runFlusher(w)

	w.eventsCh <- CLEFEvent{Message: "event1", Timestamp: time.Now()}
	w.eventsCh <- CLEFEvent{Message: "event2", Timestamp: time.Now()}

	close(w.eventsCh)
	handler.workerWg.Wait()

	require.Empty(t, w.retryBuffer, "retryBuffer should be empty after successful flush")
}

// Expectation: The flusher should flush events after the flush interval even if batch size is not reached.
func Test_runFlusher_FlushOnInterval_Success(t *testing.T) {
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

	go handler.runFlusher(w)

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
func Test_runFlusher_RetryOnFailure_Success(t *testing.T) {
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
			retryBufferSize:  10,
		},
	}

	w := &worker{
		eventsCh:    make(chan CLEFEvent, 10),
		retryBuffer: nil,
	}

	handler.workerWg.Add(1)

	go handler.runFlusher(w)

	w.eventsCh <- CLEFEvent{Message: "fail1", Timestamp: time.Now()}
	w.eventsCh <- CLEFEvent{Message: "fail2", Timestamp: time.Now()}

	time.Sleep(10 * time.Millisecond)

	close(w.eventsCh)
	handler.workerWg.Wait()

	require.Equal(t, 2, attempts, "expected 2 attempts to send batch")
	require.Empty(t, w.retryBuffer)
}

// Expectation: The flusher should flush the retry buffer when the channel is closed.
func Test_runFlusher_RetryBufferFlushedOnClose_Success(t *testing.T) {
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
	go handler.runFlusher(w)

	close(w.eventsCh)
	handler.workerWg.Wait()

	mu.Lock()
	defer mu.Unlock()

	require.NotEmpty(t, sent, "expected retry buffer to be flushed on close")
	require.Empty(t, w.retryBuffer, "expected retry buffer to be empty after close")
}

// Expectation: The flusher should exit immediately and not process events when noFlush is set.
func Test_runFlusher_NoFlushModeExits_Success(t *testing.T) {
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
	go handler.runFlusher(w)

	w.eventsCh <- CLEFEvent{Message: "test", Timestamp: time.Now()}

	close(w.eventsCh)
	handler.workerWg.Wait()

	require.Nil(t, w.retryBuffer, "retryBuffer should remain nil in noFlush mode")
}

// Expectation: The flusher should handle rapid open/close without blocking.
func Test_runFlusher_ImmediateClose_Success(t *testing.T) {
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
	go handler.runFlusher(w)

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
func Test_runFlusher_DrainsOnClose_Success(t *testing.T) {
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
	go handler.runFlusher(w)
	handler.workerWg.Wait()

	mu.Lock()
	defer mu.Unlock()

	require.Equal(t, 10, totalSent, "all prefilled events should be drained and sent")
}

// Expectation: The flusher should accumulate events until batch size before sending.
func Test_runFlusher_BatchAccumulation_Success(t *testing.T) {
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
	go handler.runFlusher(w)

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

// Expectation: flushBatch should send events and clear the slice.
func Test_flushBatch_SendsAndClearsEvents_Success(t *testing.T) {
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

	handler.flushBatch(w, &events)

	require.Empty(t, events)
	require.Equal(t, 1, callCount)
	require.Empty(t, w.retryBuffer)
}

// Expectation: flushBatch should flush retry buffer before current events.
func Test_flushBatch_FlushesRetryBufferFirst_Success(t *testing.T) {
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

	handler.flushBatch(w, &events)

	mu.Lock()
	defer mu.Unlock()

	require.Equal(t, []string{"retry", "current"}, order)
	require.Empty(t, events)
	require.Empty(t, w.retryBuffer)
}

// Expectation: flushBatch with no events and no retry buffer should not make HTTP calls.
func Test_flushBatch_NothingToFlush_NoHTTPCalls_Success(t *testing.T) {
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

	handler.flushBatch(w, &events)

	require.False(t, called)
}

// Expectation: flushBatch should accumulate failed events into retry buffer.
func Test_flushBatch_FailedEvents_AccumulateInRetryBuffer_Success(t *testing.T) {
	t.Parallel()

	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(500, "error", func() {}),
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) {},
			retryBufferSize:  10,
		},
	}

	w := &worker{}
	events := []CLEFEvent{
		{Message: "e1", Timestamp: time.Now()},
		{Message: "e2", Timestamp: time.Now()},
	}

	handler.flushBatch(w, &events)

	require.Empty(t, events, "events slice should be cleared even on failure")
	require.Len(t, w.retryBuffer, 2, "failed events should be in retry buffer")
}

// Expectation: flushBatch with only retry buffer events should flush them.
func Test_flushBatch_OnlyRetryBuffer_Flushed_Success(t *testing.T) {
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

	handler.flushBatch(w, &events)

	require.Equal(t, 1, callCount)
	require.Empty(t, w.retryBuffer)
}

// Expectation: flushBatch should append failed current events to existing retry buffer failures.
func Test_flushBatch_RetryBufferFailAndCurrentFail_BothAccumulate_Success(t *testing.T) {
	t.Parallel()

	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(500, "error", func() {}),
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) {},
			retryBufferSize:  10,
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

	handler.flushBatch(w, &events)

	require.Empty(t, events)
	require.Len(t, w.retryBuffer, 2, "both failed retry and failed current should be in buffer")
}

// Expectation: flushBatch should cap the retry buffer at retryBufferSize.
func Test_flushBatch_RetryBufferCapped_Success(t *testing.T) {
	t.Parallel()

	var errMsg string
	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(500, "error", func() {}),
			seqURL:           "http://example.com",
			errorHandlerFunc: func(err error) { errMsg = err.Error() },
			retryBufferSize:  3,
		},
	}

	w := &worker{}
	events := []CLEFEvent{
		{Message: "e1", Timestamp: time.Now()},
		{Message: "e2", Timestamp: time.Now()},
		{Message: "e3", Timestamp: time.Now()},
		{Message: "e4", Timestamp: time.Now()},
		{Message: "e5", Timestamp: time.Now()},
	}

	handler.flushBatch(w, &events)

	require.Len(t, w.retryBuffer, 3, "retry buffer should be capped at retryBufferSize")
	require.Equal(t, "e3", w.retryBuffer[0].Message, "oldest events should be dropped")
	require.Equal(t, "e4", w.retryBuffer[1].Message)
	require.Equal(t, "e5", w.retryBuffer[2].Message)
	require.Contains(t, errMsg, "dropping 2 oldest events")
}

// Expectation: flushBatch should not drop events when retry buffer is within limit.
func Test_flushBatch_RetryBufferWithinLimit_NoDrop_Success(t *testing.T) {
	t.Parallel()

	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(500, "error", func() {}),
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) {},
			retryBufferSize:  10,
		},
	}

	w := &worker{}
	events := []CLEFEvent{
		{Message: "e1", Timestamp: time.Now()},
		{Message: "e2", Timestamp: time.Now()},
	}

	handler.flushBatch(w, &events)

	require.Len(t, w.retryBuffer, 2)

	require.Equal(t, "e1", w.retryBuffer[0].Message)
	require.Equal(t, "e2", w.retryBuffer[1].Message)
}

// Expectation: flushBatch should cap combined retry buffer and new failures.
func Test_flushBatch_RetryBufferCapsExistingPlusNew_Success(t *testing.T) {
	t.Parallel()

	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(500, "error", func() {}),
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) {},
			retryBufferSize:  3,
		},
	}

	w := &worker{
		retryBuffer: []CLEFEvent{
			{Message: "old1", Timestamp: time.Now()},
			{Message: "old2", Timestamp: time.Now()},
		},
	}
	events := []CLEFEvent{
		{Message: "new1", Timestamp: time.Now()},
		{Message: "new2", Timestamp: time.Now()},
	}

	handler.flushBatch(w, &events)

	require.Len(t, w.retryBuffer, 3)
	require.Equal(t, "old2", w.retryBuffer[0].Message, "oldest should be dropped first")
	require.Equal(t, "new1", w.retryBuffer[1].Message)
	require.Equal(t, "new2", w.retryBuffer[2].Message)
}

// Expectation: flushBatch should not trigger cap logic when retry buffer is exactly at limit.
func Test_flushBatch_RetryBufferExactlyAtLimit_NoDrop_Success(t *testing.T) {
	t.Parallel()

	var dropCalled bool
	handler := &SeqHandler{
		shared: &shared{
			client: GetHTTPClientMock(500, "error", func() {}),
			seqURL: "http://example.com",
			errorHandlerFunc: func(err error) {
				if strings.Contains(err.Error(), "dropping") {
					dropCalled = true
				}
			},
			retryBufferSize: 2,
		},
	}

	w := &worker{}
	events := []CLEFEvent{
		{Message: "e1", Timestamp: time.Now()},
		{Message: "e2", Timestamp: time.Now()},
	}

	handler.flushBatch(w, &events)

	require.Len(t, w.retryBuffer, 2)
	require.False(t, dropCalled, "should not drop when exactly at limit")
}

// Expectation: sendEvents with empty input should return nil without making HTTP calls.
func Test_sendEvents_EmptyInput_ReturnsNil_Success(t *testing.T) {
	t.Parallel()

	called := false
	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(200, "ok", func() { called = true }),
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) {},
		},
	}

	result := handler.sendEvents(nil)
	require.Nil(t, result)
	require.False(t, called, "HTTP client should not be called for empty input")

	result = handler.sendEvents([]CLEFEvent{})
	require.Nil(t, result)
	require.False(t, called)
}

// Expectation: sendEvents should set Content-Type and API key headers.
func Test_sendEvents_SetsHeaders_Success(t *testing.T) {
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

	handler.sendEvents(events)

	require.NotNil(t, capturedReq)
	require.Equal(t, "application/vnd.serilog.clef", capturedReq.Header.Get("Content-Type"))
	require.Equal(t, "my-api-key", capturedReq.Header.Get("X-Seq-ApiKey"))
}

// Expectation: sendEvents should omit API key header when apiKey is empty.
func Test_sendEvents_EmptyAPIKey_NoHeader_Success(t *testing.T) {
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

	handler.sendEvents(events)

	require.NotNil(t, capturedReq)
	require.Empty(t, capturedReq.Header.Get("X-Seq-ApiKey"))
}

// Expectation: sendEvents should send valid CLEF JSON lines in the request body.
func Test_sendEvents_SendsValidCLEFBody_Success(t *testing.T) {
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

	handler.sendEvents(events)

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

// Expectation: sendEvents should return events and call error handler on HTTP client error.
func Test_sendEvents_HTTPClientError_ReturnsEvents_Success(t *testing.T) {
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

	result := handler.sendEvents(events)

	require.Len(t, result, 1)
	require.Error(t, capturedErr)
	require.Contains(t, capturedErr.Error(), "connection refused")
}

// Expectation: sendEvents should return events and call error handler on non-2xx status.
func Test_sendEvents_Non2xxStatus_ReturnsEvents_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status int
	}{
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

			result := handler.sendEvents(events)

			require.Len(t, result, 1, "events should be returned for status %d", tt.status)
			require.True(t, errCalled, "error handler should be called for status %d", tt.status)
		})
	}
}

// Expectation: sendEvents should accept all 2xx status codes.
func Test_sendEvents_All2xxStatuses_ReturnsNil_Success(t *testing.T) {
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

			result := handler.sendEvents(events)

			require.Nil(t, result)
		})
	}
}

// Expectation: sendEvents should POST to the configured seqURL.
func Test_sendEvents_PostsToConfiguredURL_Success(t *testing.T) {
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

	handler.sendEvents(events)

	require.Equal(t, "http://my-seq-server:5341/api/events/raw", capturedURL)
	require.Equal(t, "POST", capturedMethod)
}

// Expectation: sendEvents should use POST method.
func Test_sendEvents_UsesPostMethod_Success(t *testing.T) {
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

	handler.sendEvents(events)

	require.Equal(t, http.MethodPost, capturedMethod)
}

// Expectation: The function should split oversized batches and send them in smaller chunks.
func Test_sendEvents_SplitsOnRequestTooLarge_Success(t *testing.T) {
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

	leftover := handler.sendEvents(events)

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
func Test_sendEvents_DropsOversizedSingleEvent_Success(t *testing.T) {
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

	leftover := handler.sendEvents(events)

	require.Empty(t, leftover, "expected single oversized event to be dropped")
	require.True(t, errorCalled, "expected error handler to be called when dropping oversized event")
}

// Expectation: The function should split oversized batches and send them in smaller chunks.
func Test_sendEvents_SplitsOnBadRequest_Success(t *testing.T) {
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
								StatusCode: http.StatusBadRequest,
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

	leftover := handler.sendEvents(events)

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

// Expectation: The function should drop a single event that is malformed.
func Test_sendEvents_DropsBadRequestSingleEvent_Success(t *testing.T) {
	t.Parallel()

	var errorCalled bool

	handler := &SeqHandler{
		shared: &shared{
			client: &http.Client{
				Transport: &mockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: http.StatusBadRequest,
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
		{Message: "malformed event", Timestamp: time.Now()},
	}

	leftover := handler.sendEvents(events)

	require.Empty(t, leftover, "expected single malformed event to be dropped")
	require.True(t, errorCalled, "expected error handler to be called when dropping malformed event")
}

// Expectation: The function should return leftover events from the failed half of a split batch.
func Test_sendEvents_PartialFail_ReturnsLeftover_Success(t *testing.T) {
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

	leftover := handler.sendEvents(events)

	require.Len(t, leftover, 2, "expected 2 leftover events from failed right half")
	require.Equal(t, "e3", leftover[0].Message)
	require.Equal(t, "e4", leftover[1].Message)
}

// Expectation: sendEvents should return events and call error handler when request creation fails.
func Test_sendEvents_InvalidURL_ReturnsEvents_Success(t *testing.T) {
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

	result := handler.sendEvents(events)

	require.Len(t, result, 1)
	require.Error(t, capturedErr)
}

// Expectation: sendEvents should drop unencodable events and send the rest.
func Test_sendEvents_DropsUnencodableEvent_SendsRest_Success(t *testing.T) {
	t.Parallel()

	var capturedBody string
	var errCount int

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
			errorHandlerFunc: func(_ error) { errCount++ },
		},
	}

	events := []CLEFEvent{
		{Message: "good1", Timestamp: time.Now()},
		{Message: "bad", Timestamp: time.Now(), Properties: map[string]any{"poison": make(chan int)}},
		{Message: "good2", Timestamp: time.Now()},
	}

	result := handler.sendEvents(events)

	require.Nil(t, result, "good events should have been sent successfully")
	require.Equal(t, 1, errCount, "error handler should be called once for the bad event")

	lines := strings.Split(strings.TrimSpace(capturedBody), "\n")
	require.Len(t, lines, 2, "only the two good events should be in the request")

	var first, second map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &first))
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &second))
	require.Equal(t, "good1", first["@m"])
	require.Equal(t, "good2", second["@m"])
}

// Expectation: sendEvents should return nil when all events are unencodable.
func Test_sendEvents_AllUnencodable_ReturnsNil_Success(t *testing.T) {
	t.Parallel()

	called := false
	errCount := 0
	handler := &SeqHandler{
		shared: &shared{
			client:           GetHTTPClientMock(200, "ok", func() { called = true }),
			seqURL:           "http://example.com",
			errorHandlerFunc: func(_ error) { errCount++ },
		},
	}

	events := []CLEFEvent{
		{Message: "bad1", Timestamp: time.Now(), Properties: map[string]any{"a": make(chan int)}},
		{Message: "bad2", Timestamp: time.Now(), Properties: map[string]any{"b": make(chan int)}},
	}

	result := handler.sendEvents(events)

	require.Nil(t, result, "all events dropped, nothing to retry")
	require.False(t, called, "HTTP client should not be called when no events survive encoding")
	require.Equal(t, 2, errCount, "error callback should have been called")
}
