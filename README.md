# S3 sync with checksum tool 

A command-line utility for copying files between local filesystems and Amazon S3 buckets, or between S3 buckets along with adding checksums as metadata.

## Features

- Copy files/objects from local filesystem or S3 to S3
- Calculate full object checksums and store as S3 object metadata
- Supported hash algorithms: sha1, sha256, sha512, crc32, crc32c, md5

## Installation

### Building from source

```sh
# Clone the repository
git clone https://github.com/benritz/s3sync.git
cd s3sync

# Build for all platforms
./build.sh

# Or build for your specific platform
go build -o s3-sync cmd/s3-sync/main.go
```

## Usage

```sh
s3-sync [options] <source> <destination>
```

### Examples

```sh
# Sync a local directory to S3
s3-sync /path/to/local/dir s3://my-bucket/path/prefix

# Sync from S3 to another S3 bucket
s3-sync s3://source-bucket/path s3://dest-bucket/path

# Dry run (show what would happen without making changes)
s3-sync --dry-run /local/path s3://my-bucket/path
```

## Options

```
  -profile string          the AWS profile to use (default "default")
  -hash-algorithm string   the hash algorithm: sha1 (default), sha256, sha512, crc32, crc32c, md5
  -size-only               only check file size (default false)
  -inc-dir-markers         include directory markers (default false)
  -inc-hidden              include hidden files (default false)
  -storage-class string    the storage class for uploads: STANDARD, REDUCED_REDUNDANCY, STANDARD_IA,ONEZONE_IA, INTELLIGENT_TIERING, GLACIER, DEEP_ARCHIVE (default bucket setting)
  -dry-run                 dry run (default false)
  -concurrency int         the number of concurrent operations (default to available CPU cores)
  -max-part-size string    the maximum part size for multipart uploads (default 250MB)
  -log-level string        log level: none, error, warn, info, debug (default "none")
  -log-file string         log file (default stderr)
  -help                    print this help message
```

## Requirements

- Go 1.23.4 or later
- AWS credentials configured either through the AWS CLI (`aws configure`) or environment variables

## AWS Authentication

The tool uses the AWS SDK for Go, which supports the following methods for providing credentials:

1. Shared credentials file (`~/.aws/credentials`)
2. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
3. IAM roles for Amazon EC2 or ECS tasks

Use the `-profile` flag to specify which profile from your credentials file to use.

## License

MIT License