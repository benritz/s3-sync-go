package syncing

import (
	"benritz/s3sync/internal/hashing"
	"benritz/s3sync/internal/logging"
	"benritz/s3sync/internal/paths"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
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
	ctx := context.Background()

	var hashAlgorithmFlags hashAlgorithmFlag

	profile := flag.String("profile", "default", "the AWS profile to use")
	sizeOnly := flag.Bool("sizeOnly", false, "only check file size")
	dryRun := flag.Bool("dryRun", false, "dry run")
	flag.Var(&hashAlgorithmFlags, "hashAlgorithm", "the hash algorithm: sha1 (default), sha256, sha512, crc32, crc32c, md5")
	concurrency := flag.Int("concurrency", 5, "the number of concurrent sync operations")
	logLevel := flag.String("logLevel", "none", "log level: none, error, warn, info, debug")
	logFile := flag.String("logFile", "", "log file")
	helpFlag := flag.Bool("help", false, "print this help message")
	flag.Parse()

	if len(hashAlgorithmFlags) == 0 {
		hashAlgorithmFlags = append(hashAlgorithmFlags, "sha1")
	}

	args := flag.Args()

	if len(args) != 2 || *helpFlag {
		fmt.Printf("Usage: %s <flags> <source> <destination>\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
		os.Exit(1)
	}

	var algorithms []hashing.Algorithm
	for _, algorithmName := range hashAlgorithmFlags {
		algorithm, err := hashing.ParseAlgorithm(algorithmName)
		if err != nil {
			logging.FatalError(ctx, "failed to parse algorithm", err)
		}
		algorithms = append(algorithms, algorithm)
	}

	maxConcurrency := runtime.GOMAXPROCS(0)
	if *concurrency < 1 || *concurrency > maxConcurrency {
		logging.FatalError(ctx, "invalid concurrency", fmt.Errorf("concurrency must be between 1 and %d, got %d", maxConcurrency, *concurrency))
	}

	if *logLevel == "none" {
		logging.DestNone()
	} else {
		level, err := logging.ParseLevel(*logLevel)
		if err != nil {
			logging.FatalError(ctx, "invalid log level", err)
		}
		logging.LogLevel.Set(level)

		logging.DestStdErr()
	}

	if logFile != nil && *logFile != "" {
		err := logging.DestFile(*logFile)
		if err != nil {
			logging.FatalError(ctx, "failed to open log file", err)
		}
	}

	logging.Configure()
	defer logging.Dispose()

	src, dst := args[0], args[1]

	srcRoot, err := paths.Parse(src)
	if err != nil {
		logging.FatalError(ctx, "failed to parse source", err)
	}

	dstRoot, err := paths.Parse(dst)
	if err != nil {
		logging.FatalError(ctx, "failed to parse destination", err)
	}

	if dstRoot.S3 == nil {
		logging.FatalError(ctx, "destination must be an S3 path", nil)
	}

	slog.DebugContext(
		ctx,
		"arguments",
		"src", srcRoot,
		"dst", dstRoot,
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
		logging.FatalError(ctx, "failed to create syncer", err)
	}

	result := make(chan *SyncResult)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case ret, ok := <-result:
				if !ok {
					return
				}

				rel := srcRoot.GetRel(ret.SrcPath.Path)

				switch ret.Type {
				case Error:
					fmt.Printf("%s: failed to sync %v\n", rel, ret.Err)
				case Skip:
					fmt.Printf("%s: skipped\n", rel)
				case MetadataOnly:
					fmt.Printf("%s: updated metadata %v\n", rel, ret.MissingAlgorithms)
				case Copied:
					fmt.Printf("%s: copied\n", rel)
				}
			}
		}
	}()

	err = syncer.Sync(ctx, srcRoot, dstRoot, result)

	syncer.Close()
	close(result)
	wg.Wait()

	if err != nil {
		logging.FatalError(ctx, "sync failed", err)
	}

	fmt.Println("sync complete")
	slog.Info("sync complete")
}
