#!/bin/bash

DIST="dist"

# Clean dist
mkdir -p $DIST
rm -rf $DIST/*

# Build for linux/arm64
echo "Building for linux/arm64…"
env GOOS=linux GOARCH=arm64 go build -o $DIST/s3-sync-linux-arm64 cmd/s3-sync/main.go 

# Build for linux/amd64
echo "Building for linux/amd64…"
env GOOS=linux GOARCH=amd64 go build -o $DIST/s3-sync-linux-amd64 cmd/s3-sync/main.go 

# Build for darwin/arm64
echo "Building for darwin/arm64…"
env GOOS=darwin GOARCH=arm64 go build -o $DIST/s3-sync-macos-arm64 cmd/s3-sync/main.go 

# Build for darwin/amd64
echo "Building for darwin/amd64…"
env GOOS=darwin GOARCH=amd64 go build -o $DIST/s3-sync-macos-amd64 cmd/s3-sync/main.go 

# Build for windows/amd64
echo "Building for windows/amd64…"
env GOOS=windows GOARCH=amd64 go build -o $DIST/s3-sync-windows-amd64.exe cmd/s3-sync/main.go 

# Build for windows/arm64
echo "Building for windows/arm64…"
env GOOS=windows GOARCH=arm64 go build -o $DIST/s3-sync-windows-arm64.exe cmd/s3-sync/main.go 

echo "Build complete"