package logging

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"time"
)

var LogLevel = new(slog.LevelVar)

func ParseLevel(level string) (slog.Level, error) {
	switch level {
	case "debug":
		return slog.LevelDebug, nil
	case "error":
		return slog.LevelError, nil
	case "warn":
		return slog.LevelWarn, nil
	case "info":
		return slog.LevelInfo, nil
	}
	return slog.LevelInfo, fmt.Errorf("invalid log level: %s", level)
}

type LogFormat int

const (
	Text LogFormat = iota
	JSON
)

var writer io.Writer
var logHandler slog.Handler
var configured bool

func DestNone() {
	writer = io.Discard
}

func DestStdErr() {
	writer = os.Stderr
}

func DestFile(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	writer = file
	return nil
}

func FormatText() {
	logHandler = slog.NewTextHandler(writer, &slog.HandlerOptions{Level: LogLevel})
}

func FormatJSON() {
	logHandler = slog.NewJSONHandler(writer, &slog.HandlerOptions{Level: LogLevel})
}

func Configure() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	if writer == nil {
		DestNone()
	}

	if logHandler == nil {
		FormatJSON()
	}

	slog.SetDefault(slog.New(logHandler))

	configured = true
}

func Dispose() {
	if writer != nil {
		if file, ok := writer.(*os.File); ok {
			file.Close()
		}
	}
}

func DebugTimeElapsed(message string) func() {
	start := time.Now()
	return func() {
		if configured {
			slog.Debug(message, "ms", float64(time.Since(start).Microseconds())/1000)
		}
	}
}

func FatalError(ctx context.Context, msg string, err error) {
	if configured {
		slog.ErrorContext(ctx, msg, "err", err)
	}
	fmt.Printf("%s: %v\n", msg, err)
	os.Exit(1)
}
