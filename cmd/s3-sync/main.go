package main

import (
	"benritz/s3sync/internal/logging"
	"benritz/s3sync/internal/syncing"
)

func main() {
	logging.Configure()

	syncing.S3Sync()
}
