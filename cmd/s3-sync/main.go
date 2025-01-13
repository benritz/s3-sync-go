package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"

	"benritz/s3sync/internal/hashing"
	"benritz/s3sync/internal/logging"
	"benritz/s3sync/internal/paths"
	"benritz/s3sync/internal/sync"
)

type digestAlgorithmFlag []string

func (flag *digestAlgorithmFlag) String() string {
	return fmt.Sprintf("%v", *flag)
}

func (flag *digestAlgorithmFlag) Set(value string) error {
	*flag = append(*flag, value)
	return nil
}

func main() {
	logging.Configure()

	var digestAlgorithmFlags digestAlgorithmFlag

	profile := flag.String("profile", "default", "the AWS profile to use")
	sizeOnly := flag.Bool("sizeOnly", false, "only check file size")
	dryRun := flag.Bool("dryRun", false, "dry run")
	debug := flag.Bool("debug", false, "debug logging")
	flag.Var(&digestAlgorithmFlags, "digestAlgorithm", "the digest algorithm, either sha1 or sha256")
	helpFlag := flag.Bool("help", false, "print this help message")
	flag.Parse()

	if len(digestAlgorithmFlags) == 0 {
		digestAlgorithmFlags = append(digestAlgorithmFlags, "sha1")
	}

	args := flag.Args()

	if len(args) != 2 || *helpFlag {
		log.Printf("Usage: %s <flags> <source> <destination>", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
		os.Exit(1)
	}

	var algorithms []hashing.Algorithm
	for _, algorithmName := range digestAlgorithmFlags {
		algorithm, err := hashing.ParseAlgorithm(algorithmName)
		if err != nil {
			log.Fatalf("%v", err)
		}
		algorithms = append(algorithms, algorithm)
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

	syncer, err := sync.NewSyncer(ctx, *profile, algorithms, *sizeOnly, *dryRun)

	if err != nil {
		fmt.Printf("failed to create syncer: %v", err)
		log.Fatalf("failed to create syncer: %v", err)
	}

	result := make(chan *sync.SyncResult)

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
				case sync.Error:
					fmt.Printf("%s: failed to sync %v\n", rel, ret.Err)
				case sync.Skip:
					fmt.Printf("%s: skipped\n", rel)
				case sync.MetadataOnly:
					fmt.Printf("%s: updated metadata %v\n", rel, ret.MissingAlgorithms)
				case sync.Upload:
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
