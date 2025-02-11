package syncing

import (
	"benritz/s3sync/internal/hashing"
	"benritz/s3sync/internal/logging"
	"benritz/s3sync/internal/paths"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

// parse the part size string and return the size in bytes
// e.g. 5MB -> 5 * 1024 * 1024
func parsePartSize(s string) (int64, error) {
	s = strings.TrimSpace(s)

	var num, suffix string
	fraction := false

	for i, r := range s {
		if r == '.' {
			fraction = true
		} else if r < '0' || r > '9' {
			num = s[:i]
			suffix = strings.TrimSpace(s[i:])
			break
		}
	}

	if num == "" {
		num = s
		suffix = "B"
	} else {
		suffix = strings.ToUpper(suffix)
	}

	if fraction {
		value, err := strconv.ParseFloat(num, 64)
		if err != nil {
			return 0, err
		}

		switch suffix {
		case "", "B":
			// nothing to do
		case "KB", "K":
			value *= 1024
		case "MB", "M":
			value *= 1024 * 1024
		case "GB", "G":
			value *= 1024 * 1024 * 1024
		default:
			return 0, fmt.Errorf("invalid size format suffix: %s; use B, K, KB, M, MB, G or GB", suffix)
		}

		return int64(math.Ceil(value)), nil
	}

	value, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return 0, err
	}

	switch suffix {
	case "", "B":
		return value, nil
	case "KB", "K":
		return value << 10, nil
	case "MB", "M":
		return value << 10 << 10, nil
	case "GB", "G":
		return value << 10 << 10 << 10, nil
	default:
		return 0, fmt.Errorf("invalid size format suffix: %s; use B, K, KB, M, MB, G or GB", suffix)
	}
}

func S3Sync() {
	ctx := context.Background()

	var hashAlgorithmFlags hashAlgorithmFlag

	flag.Var(&hashAlgorithmFlags, "hashAlgorithm", "the hash algorithm: sha1 (default), sha256, sha512, crc32, crc32c, md5")
	profile := flag.String("profile", "default", "the AWS profile to use")
	sizeOnly := flag.Bool("sizeOnly", false, "only check file size (default false)")
	incHidden := flag.Bool("incHidden", false, "include hidden files (default false)")
	storageClass := flag.String("storageClass", "", "the storage class for uploads: STANDARD, REDUCED_REDUNDANCY, STANDARD_IA, ONEZONE_IA, INTELLIGENT_TIERING, GLACIER, DEEP_ARCHIVE (default bucket setting)")
	dryRun := flag.Bool("dryRun", false, "dry run (default false)")
	concurrency := flag.Int("concurrency", 5, "the number of concurrent operations")
	maxPartSize := flag.String("maxPartSize", "", "the maximum part size for multipart uploads, used when setting the checksum value for SHA checksum functions (default 250MB)")
	logLevel := flag.String("logLevel", "none", "log level: none, error, warn, info, debug")
	logFile := flag.String("logFile", "", "log file (default stderr)")
	helpFlag := flag.Bool("help", false, "print this help message")
	flag.Parse()

	if len(hashAlgorithmFlags) == 0 {
		hashAlgorithmFlags = append(hashAlgorithmFlags, "sha1")
	}

	args := flag.Args()

	// init logging
	if len(args) != 2 || *helpFlag {
		fmt.Printf("Usage: %s <flags> <source> <destination>\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
		os.Exit(1)
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

	// get options
	var syncerOptions []SyncerOption

	syncerOptions = append(syncerOptions, WithProfile(*profile))

	var algorithms []hashing.Algorithm
	for _, algorithmName := range hashAlgorithmFlags {
		algorithm, err := hashing.ParseAlgorithm(algorithmName)
		if err != nil {
			logging.FatalError(ctx, "failed to parse algorithm", err)
		}
		algorithms = append(algorithms, algorithm)
	}

	syncerOptions = append(syncerOptions, WithAlgorithms(algorithms))
	syncerOptions = append(syncerOptions, WithConcurrency(*concurrency))

	if *maxPartSize != "" {
		maxPartSizeBytes, err := parsePartSize(*maxPartSize)
		if err != nil {
			logging.FatalError(ctx, "invalid part size", err)
		}
		syncerOptions = append(syncerOptions, WithMaxPartSize(maxPartSizeBytes))
	}

	if *sizeOnly {
		syncerOptions = append(syncerOptions, WithSizeOnly())
	}

	if *dryRun {
		syncerOptions = append(syncerOptions, WithDryRun())
	}

	if *incHidden {
		syncerOptions = append(syncerOptions, WithIncHidden())
	}

	syncerOptions = append(syncerOptions, WithStorageClass(*storageClass))

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
		"aws profile", *profile,
		"dry run", *dryRun,
		"size only", *sizeOnly,
		"include hidden", *incHidden,
		"concurrency", *concurrency,
		"max part size", *maxPartSize,
		"log level", *logLevel,
		"log file", *logFile,
	)

	syncer, err := NewSyncer(ctx, syncerOptions...)

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

				switch ret.Outcome {
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
