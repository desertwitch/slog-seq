// Package slogseq provides an [slog.Handler] that sends structured log events
// to a Seq server using the CLEF (Compact Log Event Format) protocol.
//
// Events are batched and delivered asynchronously over HTTP, so logging
// calls do not block on network I/O. Multiple workers can be configured
// for high-throughput workloads.
//
// For OpenTelemetry trace correlation and span forwarding, see the seqotel
// sub-package.
//
// Use [NewSeqHandler] to create a handler:
//
//	handler := slogseq.NewSeqHandler("http://seq:5341/ingest/clef",
//		slogseq.WithAPIKey("your-api-key"),
//		slogseq.WithBatchSize(50),
//	)
//	defer handler.Close()
//	slog.SetDefault(slog.New(handler))
package slogseq

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

// SeqOption is an option to configure a Seq handler.
type SeqOption interface {
	apply(handler *SeqHandler) *SeqHandler
}

type seqOptionFunc func(*SeqHandler) *SeqHandler

func (f seqOptionFunc) apply(h *SeqHandler) *SeqHandler {
	return f(h)
}

// WithAPIKey sets the API key for the Seq server. Default is empty (no
// authentication).
func WithAPIKey(apiKey string) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		h.apiKey = apiKey

		return h
	})
}

// WithBatchSize sets the number of events to batch before sending to Seq.
//
// Values less than 1 fall back to the default of 50.
func WithBatchSize(batchSize int) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		if batchSize < 1 {
			batchSize = defaultBatchSize
		}

		h.batchSize = batchSize

		return h
	})
}

// WithBufferSize sets the event channel capacity per worker. In non-blocking
// mode, events are dropped when the buffer is full. In blocking mode, Handle
// blocks until space is available.
//
// Values less than 1 fall back to the default of 1000.
func WithBufferSize(n int) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		if n < 1 {
			n = defaultBufferSize
		}

		h.bufferSize = n

		return h
	})
}

// WithRetryBufferSize sets the maximum number of failed events held for retry
// per worker. When full, the oldest events are dropped.
//
// Values less than 1 fall back to the default of 1000.
func WithRetryBufferSize(n int) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		if n < 1 {
			n = defaultRetryBufferSize
		}

		h.retryBufferSize = n

		return h
	})
}

// WithFlushInterval sets the interval at which to flush the batch, even if the
// batch size has not been reached.
//
// Values less than 1 fall back to the default of 2 seconds.
func WithFlushInterval(flushInterval time.Duration) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		if flushInterval < 1 {
			flushInterval = defaultFlushInterval
		}

		h.flushInterval = flushInterval

		return h
	})
}

// WithHandlerOptions sets the slog handler options.
func WithHandlerOptions(opts *slog.HandlerOptions) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		h.options = *opts

		return h
	})
}

// WithInsecure disables TLS certificate verification. Has no effect if
// WithHTTPClient is also set, since the custom client controls its own TLS
// configuration.
func WithInsecure() SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		h.disableTLSVerify = true

		return h
	})
}

// WithHTTPClient sets the HTTP client used for sending events to Seq. If not
// set, a default client is created with sensible timeouts (30s).
func WithHTTPClient(client *http.Client) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		h.client = client

		return h
	})
}

// WithGlobalAttrs sets attributes that are included in every log event emitted
// by this handler. LogValuer values are resolved eagerly at option time.
func WithGlobalAttrs(attrs ...slog.Attr) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		resolved := make([]slog.Attr, len(attrs))

		for i, a := range attrs {
			a.Value = a.Value.Resolve()
			resolved[i] = a
		}

		h.handlerAttrs = append(h.handlerAttrs, attrSet{
			attrs: resolved,
		})

		return h
	})
}

// WithSourceKey sets the key used for source location information when
// AddSource is enabled in the handler options. Default is slog.SourceKey
// ("source").
func WithSourceKey(key string) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		h.sourceKey = key

		return h
	})
}

// WithWorkers sets the number of background workers that send events to Seq.
// Consider increasing this if you have a very high volume of events.
//
// Values less than 1 fall back to the default of 1.
func WithWorkers(count int) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		if count < 1 {
			count = defaultWorkerCount
		}

		h.workerCount = count

		return h
	})
}

// WithNonBlocking controls whether Handle blocks when the worker channel is
// full. When true (the default), events are dropped silently. When false,
// Handle blocks until space is available or the handler is closed.
func WithNonBlocking(nonBlocking bool) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		h.nonBlocking = nonBlocking

		return h
	})
}

// WithErrorHandlerFunc sets a callback that is invoked when the handler
// encounters an error sending events to Seq. Default is a no-op.
func WithErrorHandlerFunc(fn func(error)) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		h.errorHandlerFunc = fn

		return h
	})
}

// WithEventEnricher adds a function that enriches each CLEF event with
// additional context before dispatch. Called during Handle with the log
// record's context and event pointer. Multiple enrichers run in the order
// they were added.
func WithEventEnricher(fn func(context.Context, *CLEFEvent)) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		if fn != nil {
			h.eventEnrichers = append(h.eventEnrichers, fn)
		}

		return h
	})
}

// WithNoFlush disables flushing.
//
// Intended only for use in tests to inspect dispatched events.
func WithNoFlush() SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		h.noFlush = true

		return h
	})
}
