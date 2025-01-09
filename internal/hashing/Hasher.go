package hashing

import (
	"benritz/s3sync/internal/logging"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"runtime"
)

type Algorithm int

const (
	SHA1 Algorithm = iota
	SHA256
	SHA512
	CRC32
	CRC32C
	MD5
)

func (algorithm Algorithm) String() string {
	switch algorithm {
	case SHA1:
		return "sha1"
	case SHA256:
		return "sha256"
	case SHA512:
		return "sha512"
	case CRC32:
		return "crc32"
	case CRC32C:
		return "crc32c"
	case MD5:
		return "md5"
	default:
		return "unknown"
	}
}

func ParseAlgorithm(algorithm string) (Algorithm, error) {
	switch algorithm {
	case "sha1":
		return SHA1, nil
	case "sha256":
		return SHA256, nil
	case "sha512":
		return SHA512, nil
	case "crc32":
		return CRC32, nil
	case "crc32c":
		return CRC32C, nil
	case "md5":
		return MD5, nil
	default:
		return -1, fmt.Errorf("unsupported digest algorithm: %v", algorithm)
	}
}

func getHasher(algorithm Algorithm) (hash.Hash, error) {
	switch algorithm {
	case SHA1:
		return sha1.New(), nil
	case SHA256:
		return sha256.New(), nil
	case SHA512:
		return sha512.New(), nil
	case CRC32:
		return crc32.New(crc32.MakeTable(crc32.IEEE)), nil
	case CRC32C:
		return crc32.New(crc32.MakeTable(crc32.Castagnoli)), nil
	case MD5:
		return md5.New(), nil
	default:
		return nil, fmt.Errorf("unsupported digest algorithm: %v", algorithm)
	}
}

func GetHash(algorithm Algorithm, reader io.Reader) (string, error) {
	hasher, err := getHasher(algorithm)
	if err != nil {
		return "", err
	}

	if _, err := io.Copy(hasher, reader); err != nil {
		return "", err
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func getHashForFile(algorithm Algorithm, path string) (string, error) {
	if logging.LogLevel.Level() == slog.LevelDebug {
		defer logging.DebugTimeElapsed(fmt.Sprintf("getHashForFile %v %s", algorithm, path))()
	}

	reader, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer reader.Close()

	return GetHash(algorithm, reader)
}

type HashResult struct {
	Path      string
	Algorithm Algorithm
	Hash      string
	Error     error
}

type hashJob struct {
	Path      string
	Algorithm Algorithm
	Result    chan<- HashResult
}

type Hasher struct {
	queue          chan hashJob
	workerPoolSize int
}

func (h *Hasher) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-h.queue:
			if !ok {
				return
			}

			hash, err := getHashForFile(job.Algorithm, job.Path)

			job.Result <- HashResult{
				Path:      job.Path,
				Algorithm: job.Algorithm,
				Hash:      hash,
				Error:     err,
			}
		}
	}
}

func NewHasher(ctx context.Context) *Hasher {
	hasher := &Hasher{
		workerPoolSize: runtime.GOMAXPROCS(0),
		queue:          make(chan hashJob),
	}

	for i := 0; i < hasher.workerPoolSize; i++ {
		go hasher.worker(ctx)
	}

	return hasher
}

func (h *Hasher) Generate(path string, algorithm Algorithm, result chan<- HashResult) {
	h.queue <- hashJob{
		Path:      path,
		Algorithm: algorithm,
		Result:    result,
	}
}

func (h *Hasher) Close() {
	close(h.queue)
}
