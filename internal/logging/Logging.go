package logging

import (
	"log"
	"log/slog"
	"os"
	"time"
)

var LogLevel = new(slog.LevelVar)

func DebugTimeElapsed(message string) func() {
	start := time.Now()
	return func() {
		slog.Debug(message, "ms", float64(time.Since(start).Microseconds())/1000)
	}
}

func Configure() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	logHandler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: LogLevel})
	slog.SetDefault(slog.New(logHandler))
}
