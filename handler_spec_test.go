package slogseq

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/slogtest"
	"time"
)

func TestSlogtest(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var captured []string

	client := &http.Client{
		Transport: &mockTransport{
			RoundTripFunc: func(req *http.Request) (*http.Response, error) {
				body, _ := io.ReadAll(req.Body)

				mu.Lock()
				// Each line is a separate CLEF event
				for line := range strings.SplitSeq(strings.TrimSpace(string(body)), "\n") {
					if line != "" {
						captured = append(captured, line)
					}
				}
				mu.Unlock()

				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			},
		},
	}

	_, handler := NewLogger("http://fake",
		WithHTTPClient(client),
		WithBatchSize(1), // flush every event immediately
		WithFlushInterval(time.Millisecond),
	)

	err := slogtest.TestHandler(handler, func() []map[string]any {
		// Give the flusher time to send
		time.Sleep(50 * time.Millisecond)
		_ = handler.Close()

		mu.Lock()
		defer mu.Unlock()

		results := make([]map[string]any, 0, len(captured))
		for _, line := range captured {
			var m map[string]any

			if err := json.Unmarshal([]byte(line), &m); err != nil {
				t.Fatalf("failed to parse CLEF line: %v", err)
			}

			// slogtest expects standard keys
			parsed := make(map[string]any)
			for k, v := range m {
				switch k {
				case "@t":
					// Slog specification expects zero times to be omitted, but
					// CLEF specification requires even zero timestamps be sent.
					// We do this little dance to satisfy the slog test suite...
					if s, ok := v.(string); ok {
						t, err := time.Parse(time.RFC3339Nano, s)
						if err == nil && t.IsZero() {
							break // Omit it for this test.
						}
					}
					parsed["time"] = v
				case "@m":
					parsed["msg"] = v
				case "@l":
					parsed["level"] = v
				default:
					parsed[k] = v
				}
			}

			results = append(results, parsed)
		}

		return results
	})
	if err != nil {
		t.Error(err)
	}
}
