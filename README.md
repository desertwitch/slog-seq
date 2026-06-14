>[!NOTE]
> This is a fork of `sokkalf/slog-seq` with my [changes](./CHANGELOG.md) and [patches](https://github.com/sokkalf/slog-seq/compare/master...desertwitch:slog-seq:master).  
> Feel free to use it, or the upstream, whichever suits your use case best.  
> It is maintained for my personal needs and not to compete with upstream.

<div>
    <a href="https://github.com/desertwitch/slog-seq/tags"><img alt="Release" src="https://img.shields.io/github/tag/desertwitch/slog-seq.svg"></a>
    <a href="https://go.dev/"><img alt="Go Version" src="https://img.shields.io/badge/Go-%3E%3D%201.25.0-%23007d9c"></a>
    <a href="https://pkg.go.dev/github.com/desertwitch/slog-seq"><img alt="Go Reference" src="https://pkg.go.dev/badge/github.com/desertwitch/slog-seq.svg"></a>
    <a href="https://goreportcard.com/report/github.com/desertwitch/slog-seq"><img alt="Go Report" src="https://goreportcard.com/badge/github.com/desertwitch/slog-seq"></a>
    <a href="./LICENSE"><img alt="License" src="https://img.shields.io/github/license/desertwitch/slog-seq"></a>
    <br>
    <a href="https://app.codecov.io/gh/desertwitch/slog-seq"><img alt="Codecov" src="https://codecov.io/github/desertwitch/slog-seq/graph/badge.svg?token=SLUM5DRVHR"></a>
    <a href="https://github.com/desertwitch/slog-seq/actions/workflows/golangci-lint.yml"><img alt="Lint" src="https://github.com/desertwitch/slog-seq/actions/workflows/golangci-lint.yml/badge.svg"></a>
    <a href="https://github.com/desertwitch/slog-seq/actions/workflows/golang-tests.yml"><img alt="Tests" src="https://github.com/desertwitch/slog-seq/actions/workflows/golang-tests.yml/badge.svg"></a>
</div>

# slog-seq

**slog-seq** is a library for sending logs to a [Seq](https://datalust.co/seq) server, as a handler for Go's structured logging [slog](https://go.dev/blog/slog). It also supports some trace functionality.

- [Installation](#installation)
- [Quick start](#quick-start)
- [HTTP client](#http-client)
- [Multiple workers](#multiple-workers)
- [Traces](#traces)
- [Benchmarks](#benchmarks)
- [A note on OpenTelemetry](#a-note-on-opentelemetry)
- [License](#license)


## Installation

```bash
go get github.com/desertwitch/slog-seq
```

## Quick start

It's pretty easy to get going.

```go
seqLogger, handler := slogseq.NewLogger("http://your-seq-server/ingest/clef",
    slogseq.WithAPIKey("your-api-key"),
    slogseq.WithBatchSize(50),
    slogseq.WithFlushInterval(2*time.Second),
)
defer handler.Close()

slog.SetDefault(seqLogger)
slog.Info("Hello, world!")
```

You can set some options, here are some examples:

```go
opts := &slog.HandlerOptions{
	Level:     slog.LevelInfo,  // minimum log level
	AddSource: true,            // show source file, line and function in log
	ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
		if a.Key == "password" {
			// mask passwords
			a.Value = slog.StringValue("*****")
		}
		if a.Key == slog.SourceKey {
			// Replace the full path with just the file name
			s := a.Value.Any().(*slog.Source)
			s.File = path.Base(s.File)
		}
		return a
	},
}
```

and then pass it to the `NewLogger` function with `slogseq.WithHandlerOptions(opts)`.

For the `AddSource` option, the default key used is `slog.SourceKey` ("source"), but you can change it by using `slogseq.WithSourceKey("your-key")` if this key is already used for something else.
If you log something else with this key when AddSource is enabled, it will be overwritten.

## HTTP client

If you need to disable TLS certificate verification, you can do so by using the option `slogseq.WithInsecure()`.

Alternatively, you can provide your own HTTP client by using the option `slogseq.WithHTTPClient(client)`.

## Multiple workers

You can set the number of workers that will send logs to the Seq server by using the option `slogseq.WithWorkers(n)`.

This can be useful if you have a high enough volume of logs to cause dropped messages.

## Traces

`LoggingSpanProcessor` implements a `trace.SpanProcessor` that sends spans to Seq using either `trace.NewSimpleSpanProcessor` or `trace.NewBatchSpanProcessor`, which behaves pretty much the same as slog-seq already handles batching.

Here is an example of how to use it:

```go
spanProcessor := trace.NewSimpleSpanProcessor(
    &slogseq.LoggingSpanProcessor{Handler: handler},
)

tp := trace.NewTracerProvider(
    trace.WithSpanProcessor(spanProcessor),
    trace.WithSampler(trace.AlwaysSample()),
)

tracer := tp.Tracer("example-tracer")

ctx, span := tracer.Start(context.Background(), "operation")
span.AddEvent("Starting work")
time.Sleep(500 * time.Millisecond)

slog.InfoContext(ctx, "This is a span log message", "key", "value")

ctx, subSpan := tracer.Start(ctx, "sub operation")
subSpan.AddEvent("Sub operation started")
time.Sleep(500 * time.Millisecond)
subSpan.AddEvent("Sub operation completed",
    tr.WithAttributes(attribute.String("key", "value")),
)
subSpan.End()

span.AddEvent("Work done")
slog.InfoContext(ctx, "All done!")
span.End()
```

![Seq with traces](./doc/seq_screenshot.png)

## Benchmarks

Benchmarks measure the hot path (log call through channel send).  
HTTP delivery is asynchronous and batched, so it does not block the caller.

**Measured using:** Go 1.26, Intel Core i5-12600K, 8-core VM.

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| Handle | 90 | 162 | 2 |
| Handle (parallel) | 189 | 447 | 6 |
| Handle + WithAttrs | 386 | 775 | 12 |
| Handle + WithGroups | 447 | 1021 | 10 |
| Handle + AddSource | 426 | 996 | 9 |
| Handle + ReplaceAttr | 348 | 720 | 10 |
| HandleCLEFEvent<sup>1</sup> | 30 | 35 | 0 |

<sub>**1**: Raw event dispatch after Handle preprocessing or directly by OTel.</sub>

Run `make benchmark` for full results.

## A note on OpenTelemetry

This package includes optional OpenTelemetry support for forwarding spans to
Seq. The OTel dependency is only used if you create a `LoggingSpanProcessor`. No
telemetry is collected or sent anywhere unless you explicitly configure it.

## License

MIT
