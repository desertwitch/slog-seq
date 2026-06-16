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

// WithAPIKey sets the API key for the Seq server.
//
// If unset, no key is sent to the server (no authentication).
func WithAPIKey(apiKey string) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		h.apiKey = apiKey

		return h
	})
}

// WithBatchSize sets the number of events to batch before sending to Seq.
//
// If unset, or less than 1, the default is 50.
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
// If unset, or less than 1, the default is 1000.
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
// per worker. When full, the oldest unsent events (at this point usually
// retried several times) are dropped.
//
// If unset, or less than 1, the default is 1000.
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
// If unset, or less than zero, the default is 2 seconds.
// If zero, periodic flushing is disabled (and only [WithBatchSize] observed).
func WithFlushInterval(flushInterval time.Duration) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		if flushInterval < 0 {
			flushInterval = defaultFlushInterval
		}

		h.flushInterval = flushInterval

		return h
	})
}

// WithHandlerOptions sets [slog.HandlerOptions] on the handler.
func WithHandlerOptions(opts *slog.HandlerOptions) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		h.options = *opts

		return h
	})
}

// WithInsecure disables SSL certificate verification. Has no effect if
// WithHTTPClient is also set, the custom client controls its own configuration.
//
// If unset, the default is to allow only valid SSL certificates.
func WithInsecure() SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		h.disableTLSVerify = true

		return h
	})
}

// WithHTTPClient sets the HTTP client used for sending events to Seq.
//
// If unset, a default client is created with sensible timeouts (30s).
func WithHTTPClient(client *http.Client) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		h.client = client

		return h
	})
}

// WithGlobalAttrs sets [slog.Attr] that are included in every log event emitted
// by this handler. [slog.LogValuer] values are resolved eagerly at option time.
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

// WithSourceKey sets the [slog.SourceKey] used for source location information
// when AddSource is enabled in handler's given [slog.HandlerOptions] options.
//
// If unset, the default is "source".
func WithSourceKey(key string) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		h.sourceKey = key

		return h
	})
}

// WithWorkers sets the number of background workers that send events to Seq.
// Consider increasing this if you have a very high volume of events.
//
// If unset, or less than 1, the default is 1.
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
// full. When true, events are dropped silently. When false, Handle blocks until
// space becomes available or the handler is closed.
//
// If unset, the default is true (non-blocking) operation.
func WithNonBlocking(nonBlocking bool) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		h.nonBlocking = nonBlocking

		return h
	})
}

// WithErrorHandlerFunc sets a callback that is invoked when the handler
// encounters an error sending events to Seq.
//
// If unset, the default is a no-op.
func WithErrorHandlerFunc(fn func(error)) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		h.errorHandlerFunc = fn

		return h
	})
}

// WithEventEnricher adds a function that enriches each CLEF event with
// additional context before dispatch. Called during Handle with the log
// record's context and event pointer. Multiple enrichers run in the order
// they were added. Use this for custom integrations (such as tracing).
//
// If unset, the default is a no-op.
func WithEventEnricher(fn func(context.Context, *CLEFEvent)) SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		if fn != nil {
			h.eventEnrichers = append(h.eventEnrichers, fn)
		}

		return h
	})
}

// WithNoFlush disables flushing (workers exit immediately).
//
// Intended only for use in tests to inspect dispatched events.
func WithNoFlush() SeqOption {
	return seqOptionFunc(func(h *SeqHandler) *SeqHandler {
		h.noFlush = true

		return h
	})
}
