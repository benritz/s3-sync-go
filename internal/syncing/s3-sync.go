package syncing

import (
	"benritz/s3sync/internal/hashing"
	"benritz/s3sync/internal/logging"
	"benritz/s3sync/internal/paths"
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
)

type hashAlgorithmFlag []string

func (flag *hashAlgorithmFlag) String() string {
	return fmt.Sprintf("%v", *flag)
}

func (flag *hashAlgorithmFlag) Set(value string) error {
	*flag = append(*flag, value)
	return nil
}

func S3Sync() {
	var hashAlgorithmFlags hashAlgorithmFlag

	profile := flag.String("profile", "default", "the AWS profile to use")
	sizeOnly := flag.Bool("sizeOnly", false, "only check file size")
	dryRun := flag.Bool("dryRun", false, "dry run")
	flag.Var(&hashAlgorithmFlags, "hashAlgorithm", "the hash algorithm, either sha1, sha256, sha512, crc32, crc32c or md5, defaults to sha1")
	concurrency := flag.Int("concurrency", 5, "the number of concurrent sync operations")
	debug := flag.Bool("debug", false, "debug logging")
	helpFlag := flag.Bool("help", false, "print this help message")
	flag.Parse()

	if len(hashAlgorithmFlags) == 0 {
		hashAlgorithmFlags = append(hashAlgorithmFlags, "sha1")
	}

	args := flag.Args()

	if len(args) != 2 || *helpFlag {
		log.Printf("Usage: %s <flags> <source> <destination>", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
		os.Exit(1)
	}

	var algorithms []hashing.Algorithm
	for _, algorithmName := range hashAlgorithmFlags {
		algorithm, err := hashing.ParseAlgorithm(algorithmName)
		if err != nil {
			log.Fatalf("%v", err)
		}
		algorithms = append(algorithms, algorithm)
	}

	maxConcurrency := runtime.GOMAXPROCS(0)
	if *concurrency < 1 || *concurrency > maxConcurrency {
		log.Fatalf("invalid concurrency: must be between 1 and %d, got %d", maxConcurrency, concurrency)
	}

	if *debug {
		logging.LogLevel.Set(slog.LevelDebug)
	}

	src, dst := args[0], args[1]

	srcPath, err := paths.ParseLocal(src)
	if err != nil {
		log.Fatalf("failed to parse source: %v", err)
	}

	dstPath, err := paths.ParseS3Path(dst)

	if err != nil {
		log.Fatalf("failed to parse destination: %v", err)
	}

	ctx := context.Background()

	slog.DebugContext(
		ctx,
		"arguments",
		"src", srcPath,
		"dst", dstPath,
		"algorithms", algorithms,
		"size only", *sizeOnly,
		"dry run", *dryRun,
	)

	syncer, err := NewSyncer(
		ctx,
		*profile,
		algorithms,
		*concurrency,
		*sizeOnly,
		*dryRun,
	)

	if err != nil {
		fmt.Printf("failed to create syncer: %v", err)
		log.Fatalf("failed to create syncer: %v", err)
	}

	result := make(chan *SyncResult)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ret, ok := <-result:
				if !ok {
					return
				}

				rel := ret.Path[len(srcPath.Base):]

				switch ret.Type {
				case Error:
					fmt.Printf("%s: failed to sync %v\n", rel, ret.Err)
				case Skip:
					fmt.Printf("%s: skipped\n", rel)
				case MetadataOnly:
					fmt.Printf("%s: updated metadata %v\n", rel, ret.MissingAlgorithms)
				case Upload:
					fmt.Printf("%s: uploaded\n", rel)
				}
			}
		}
	}()

	err = syncer.Sync(ctx, *srcPath, *dstPath, result)

	syncer.Close()
	close(result)

	if err != nil {
		fmt.Printf("sync failed: %v", err)
		slog.Error("sync failed", "err", err)
		os.Exit(1)
	}

	fmt.Println("sync complete")
	slog.Info("sync complete")
}
