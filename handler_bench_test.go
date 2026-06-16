package slogseq

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"testing"
	"time"
)

func benchmarkHandler(b *testing.B) *SeqHandler {
	b.Helper()

	_, handler := NewLogger(
		"http://fake",
		WithHTTPClient(GetHTTPClientMock(200, "ok", func() {})),
		WithBatchSize(1),
		WithFlushInterval(time.Millisecond),
	)

	b.Cleanup(func() {
		_ = handler.Close()
	})

	return handler
}

func BenchmarkHandle(b *testing.B) {
	handler := benchmarkHandler(b)

	r := slog.NewRecord(
		time.Now(),
		slog.LevelInfo,
		"hello",
		0,
	)

	b.ReportAllocs()
	for b.Loop() {
		_ = handler.Handle(b.Context(), r)
	}
}

func BenchmarkHandle_Parallel(b *testing.B) {
	handler := benchmarkHandler(b)

	r := slog.NewRecord(
		time.Now(),
		slog.LevelInfo,
		"hello",
		0,
	)

	r.AddAttrs(
		slog.String("user", "bob"),
		slog.Int("status", 200),
		slog.String("method", "GET"),
	)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			_ = handler.Handle(ctx, r)
		}
	})
}

func BenchmarkHandle_WithAttrs(b *testing.B) {
	handler := benchmarkHandler(b)

	handler = handler.WithAttrs([]slog.Attr{
		slog.String("service", "api"),
		slog.String("env", "prod"),
		slog.String("version", "1.0"),
	}).(*SeqHandler)

	r := slog.NewRecord(
		time.Now(),
		slog.LevelInfo,
		"hello",
		0,
	)

	b.ReportAllocs()
	for b.Loop() {
		_ = handler.Handle(b.Context(), r)
	}
}

func BenchmarkHandle_ManyAttrs(b *testing.B) {
	handler := benchmarkHandler(b)

	r := slog.NewRecord(
		time.Now(),
		slog.LevelInfo,
		"hello",
		0,
	)

	r.AddAttrs(
		slog.String("k1", "v1"),
		slog.String("k2", "v2"),
		slog.String("k3", "v3"),
		slog.String("k4", "v4"),
		slog.String("k5", "v5"),
		slog.String("k6", "v6"),
		slog.String("k7", "v7"),
		slog.String("k8", "v8"),
		slog.String("k9", "v9"),
		slog.String("k10", "v10"),
	)

	b.ReportAllocs()
	for b.Loop() {
		_ = handler.Handle(context.Background(), r)
	}
}

func BenchmarkHandle_FlatAttrs(b *testing.B) {
	handler := benchmarkHandler(b)

	r := slog.NewRecord(
		time.Now(),
		slog.LevelInfo,
		"hello",
		0,
	)

	r.AddAttrs(
		slog.String("user", "bob"),
		slog.Int("status", 200),
		slog.String("method", "GET"),
		slog.Duration("duration", time.Millisecond),
	)

	b.ReportAllocs()
	for b.Loop() {
		_ = handler.Handle(b.Context(), r)
	}
}

func BenchmarkHandle_DottedAttrs(b *testing.B) {
	handler := benchmarkHandler(b)

	r := slog.NewRecord(
		time.Now(),
		slog.LevelInfo,
		"hello",
		0,
	)

	r.AddAttrs(
		slog.String("user.id", "123"),
		slog.String("user.name", "bob"),
		slog.Int("request.status", 200),
		slog.String("request.method", "GET"),
	)

	b.ReportAllocs()
	for b.Loop() {
		_ = handler.Handle(b.Context(), r)
	}
}

func BenchmarkHandle_ErrorAttr(b *testing.B) {
	handler := benchmarkHandler(b)

	r := slog.NewRecord(
		time.Now(),
		slog.LevelError,
		"request failed",
		0,
	)

	r.AddAttrs(
		slog.Any("err", errors.New("connection refused")),
		slog.Int("status", 500),
	)

	b.ReportAllocs()
	for b.Loop() {
		_ = handler.Handle(b.Context(), r)
	}
}

func BenchmarkHandle_ReplaceAttr(b *testing.B) {
	handler := benchmarkHandler(b)

	handler.options.ReplaceAttr = func(groups []string, a slog.Attr) slog.Attr {
		if a.Key == "password" {
			return slog.String("password", "***")
		}

		return a
	}

	r := slog.NewRecord(
		time.Now(),
		slog.LevelInfo,
		"hello",
		0,
	)

	r.AddAttrs(
		slog.String("user", "bob"),
		slog.String("password", "secret"),
	)

	b.ReportAllocs()
	for b.Loop() {
		_ = handler.Handle(b.Context(), r)
	}
}

func BenchmarkHandle_WithGroup(b *testing.B) {
	handler := benchmarkHandler(b)

	handler = handler.WithGroup("request").(*SeqHandler)

	r := slog.NewRecord(
		time.Now(),
		slog.LevelInfo,
		"hello",
		0,
	)

	r.AddAttrs(
		slog.Int("status", 200),
		slog.String("method", "GET"),
	)

	b.ReportAllocs()
	for b.Loop() {
		_ = handler.Handle(b.Context(), r)
	}
}

func BenchmarkHandle_TwoGroups(b *testing.B) {
	handler := benchmarkHandler(b)

	handler = handler.WithGroup("service").(*SeqHandler)
	handler = handler.WithGroup("request").(*SeqHandler)

	r := slog.NewRecord(
		time.Now(),
		slog.LevelInfo,
		"hello",
		0,
	)

	r.AddAttrs(
		slog.Int("status", 200),
		slog.String("method", "GET"),
	)

	b.ReportAllocs()
	for b.Loop() {
		_ = handler.Handle(b.Context(), r)
	}
}

func BenchmarkHandle_DeepGroups(b *testing.B) {
	handler := benchmarkHandler(b)

	handler = handler.WithGroup("service").(*SeqHandler)
	handler = handler.WithGroup("http").(*SeqHandler)
	handler = handler.WithGroup("request").(*SeqHandler)
	handler = handler.WithGroup("metadata").(*SeqHandler)

	r := slog.NewRecord(
		time.Now(),
		slog.LevelInfo,
		"hello",
		0,
	)

	r.AddAttrs(
		slog.String("user", "bob"),
	)

	b.ReportAllocs()
	for b.Loop() {
		_ = handler.Handle(b.Context(), r)
	}
}

func BenchmarkHandle_AnonymousGroup(b *testing.B) {
	handler := benchmarkHandler(b)

	r := slog.NewRecord(
		time.Now(),
		slog.LevelInfo,
		"hello",
		0,
	)

	r.AddAttrs(
		slog.Group("",
			slog.String("inlined_a", "val1"),
			slog.String("inlined_b", "val2"),
			slog.Int("inlined_c", 42),
		),
	)

	b.ReportAllocs()
	for b.Loop() {
		_ = handler.Handle(b.Context(), r)
	}
}

func BenchmarkHandle_WithGroupNoAttrs(b *testing.B) {
	handler := benchmarkHandler(b)

	handler = handler.WithGroup("service").(*SeqHandler)
	handler = handler.WithGroup("http").(*SeqHandler)

	r := slog.NewRecord(
		time.Now(),
		slog.LevelInfo,
		"hello",
		0,
	)

	b.ReportAllocs()
	for b.Loop() {
		_ = handler.Handle(b.Context(), r)
	}
}

func BenchmarkHandle_WithGroupsAndAttrs(b *testing.B) {
	handler := benchmarkHandler(b)

	handler = handler.WithGroup("service").(*SeqHandler)
	handler = handler.WithGroup("http").(*SeqHandler)

	handler = handler.WithAttrs([]slog.Attr{
		slog.String("app", "backend"),
		slog.String("version", "1.0"),
		slog.String("env", "prod"),
	}).(*SeqHandler)

	r := slog.NewRecord(
		time.Now(),
		slog.LevelInfo,
		"hello",
		0,
	)

	r.AddAttrs(
		slog.Int("status", 200),
		slog.String("method", "GET"),
		slog.String("path", "/users"),
	)

	b.ReportAllocs()
	for b.Loop() {
		_ = handler.Handle(b.Context(), r)
	}
}

func BenchmarkHandle_SlogGroup(b *testing.B) {
	handler := benchmarkHandler(b)

	r := slog.NewRecord(
		time.Now(),
		slog.LevelInfo,
		"hello",
		0,
	)

	r.AddAttrs(
		slog.Group(
			"user",
			slog.String("id", "123"),
			slog.String("name", "bob"),
		),
	)

	b.ReportAllocs()
	for b.Loop() {
		_ = handler.Handle(b.Context(), r)
	}
}

func BenchmarkHandle_NestedSlogGroups(b *testing.B) {
	handler := benchmarkHandler(b)

	r := slog.NewRecord(
		time.Now(),
		slog.LevelInfo,
		"hello",
		0,
	)

	r.AddAttrs(
		slog.Group(
			"request",
			slog.Group(
				"user",
				slog.String("id", "123"),
				slog.String("name", "bob"),
			),
			slog.Int("status", 200),
		),
	)

	b.ReportAllocs()
	for b.Loop() {
		_ = handler.Handle(b.Context(), r)
	}
}

func BenchmarkHandle_MultilineMessage(b *testing.B) {
	handler := benchmarkHandler(b)

	r := slog.NewRecord(
		time.Now(),
		slog.LevelError,
		"something failed\ngoroutine 1 [running]:\nmain.main()\n\t/app/main.go:42",
		0,
	)

	b.ReportAllocs()
	for b.Loop() {
		_ = handler.Handle(b.Context(), r)
	}
}

func BenchmarkHandle_AddSource(b *testing.B) {
	handler := benchmarkHandler(b)

	handler.options.AddSource = true
	pc, _, _, _ := runtime.Caller(0)

	r := slog.NewRecord(
		time.Now(),
		slog.LevelInfo,
		"hello",
		pc,
	)

	b.ReportAllocs()
	for b.Loop() {
		_ = handler.Handle(b.Context(), r)
	}
}

func BenchmarkHandleCLEFEvent(b *testing.B) {
	handler := benchmarkHandler(b)

	event := CLEFEvent{
		Timestamp: time.Now(),
		Message:   "hello",
		Level:     CLEFLevelInformation,
		Properties: map[string]any{
			"user":   "bob",
			"status": 200,
		},
	}

	b.ReportAllocs()
	for b.Loop() {
		handler.HandleCLEFEvent(event)
	}
}
