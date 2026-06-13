//nolint:mnd,sloglint
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"path"
	"time"

	slogseq "github.com/sokkalf/slog-seq" // import your library
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	tr "go.opentelemetry.io/otel/trace"
)

var (
	seqURL = flag.String("url", "http://localhost:5341/ingest/clef", "Seq ingestion URL")
	apiKey = flag.String("key", "", "Seq API key")
)

func main() {
	flag.Parse()
	if flag.NFlag() == 0 {
		flag.PrintDefaults()

		return
	}
	opts := &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == "password" {
				a.Value = slog.StringValue("*****")
			}
			if a.Key == "gosource" {
				// Replace the full path with just the file name
				s, ok := a.Value.Any().(*slog.Source)
				if ok {
					s.File = path.Base(s.File)
				}
			}

			return a
		},
	}

	seqLogger, handler := slogseq.NewLogger(*seqURL,
		slogseq.WithAPIKey(*apiKey),
		slogseq.WithHandlerOptions(opts),
		slogseq.WithBatchSize(50),
		slogseq.WithFlushInterval(2*time.Second),
		slogseq.WithGlobalAttrs(slog.String("service", "slog-seq"), slog.Float64("volume", 11.1)),
		slogseq.WithSourceKey("gosource"),
	)
	defer handler.Close()

	slog.SetDefault(seqLogger.With("app", "slog-seq").With("env", "dev").With("version", "1.0.0"))

	slog.Info("Hello from slog-seq test command!",
		"env", "dev",
		"version", "1.0.0")

	// gosource is overwritten by the AddSource option
	slog.Warn("This is a warning message", "huba", "fjall", "gosource", "notreallysource")

	multiLineMessage := "This is a multi-line message\n\nYep.\n\nWoo hoo.\nYea. It is.\n\n"

	slog.Info(multiLineMessage, "huba", "fjall")

	slog.Error("This is an error message", "huba", "fjall")

	slog.Debug("This is a debug message", "huba", "fjall", "password", "secret")
	grouped := slog.New(handler).WithGroup("request").With("id", "1234").WithGroup("headers").With("Accept", "application/json")

	grouped.Info("Grouped log", "password", "secret")

	spanProcessor := trace.NewSimpleSpanProcessor(&slogseq.LoggingSpanProcessor{Handler: handler})
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(spanProcessor), trace.WithSampler(trace.AlwaysSample()))
	tracer := tp.Tracer("example-tracer")
	ctx := context.Background()
	spanCtx, span := tracer.Start(ctx, "operation", tr.WithSpanKind(tr.SpanKindClient))
	span.AddEvent("Starting work")
	time.Sleep(500 * time.Millisecond)
	slog.InfoContext(spanCtx, "This is a span log message", "key", "value")
	spanCtx, subSpan := tracer.Start(spanCtx, "sub operation")
	subSpan.AddEvent("Sub operation started")
	time.Sleep(500 * time.Millisecond)
	subSpan.AddEvent("Sub operation completed", tr.WithAttributes(attribute.String("key", "value")))
	subSpan.End()
	span.AddEvent("Work done")
	slog.InfoContext(spanCtx, "All done!")
	span.End()

	errorTest := fmt.Errorf("this is an error: %w", errors.New("this is the cause"))
	slog.Error("This is an error message", "huba", "fjall", "error", errorTest)

	slog.New(handler).WithGroup("s").LogAttrs(ctx, slog.LevelDebug, "huba", slog.Int("a", 1), slog.Int("b", 2))
	slog.New(handler).LogAttrs(ctx, slog.LevelInfo, "huba", slog.Group("s", slog.Int("a", 1), slog.Int("b", 2)))

	slog.Debug("This is a debug message", "huba", "fjall", slog.Int("u", 42))
}
