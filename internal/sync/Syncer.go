package sync

import (
	"benritz/s3sync/internal/hashing"
	"benritz/s3sync/internal/logging"
	"benritz/s3sync/internal/paths"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func getAwsConfig(ctx context.Context, profile string) (aws.Config, error) {
	if profile == "default" {
		return config.LoadDefaultConfig(ctx)
	}
	return config.LoadDefaultConfig(ctx, config.WithSharedConfigProfile(profile))
}

type SyncCheck int

const (
	None SyncCheck = iota
	SizeOnly
	ModifiedAndSize
)

func (syncCheck SyncCheck) String() string {
	switch syncCheck {
	case None:
		return "None"
	case SizeOnly:
		return "SizeOnly"
	case ModifiedAndSize:
		return "ModifiedAndSize"
	default:
		return "unknown"
	}
}

type SyncType int

const (
	Skip SyncType = iota
	Upload
	MetadataOnly
)

type SyncTypeResult struct {
	Type              SyncType
	MissingAlgorithms []hashing.Algorithm
	Metadata          map[string]string
}

func (s *Syncer) generateHashes(
	ctx context.Context,
	algorithms []hashing.Algorithm,
	path string,
	metadata map[string]string,
) error {
	result := make(chan hashing.HashResult, len(algorithms))

	for _, algorithm := range algorithms {
		s.hasher.Generate(path, algorithm, result)
	}

	completed := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case hashResult, ok := <-result:
			if !ok {
				return nil
			}

			if hashResult.Error != nil {
				return fmt.Errorf("failed to compute hash: %v", hashResult.Error)
			}

			metadata[hashResult.Algorithm.String()] = hashResult.Hash

			completed += 1
			if completed == len(algorithms) {
				close(result)
			}
		}
	}
}

type Syncer struct {
	Algorithms []hashing.Algorithm
	SizeOnly   bool
	DryRun     bool

	s3Client *s3.Client
	uploader *manager.Uploader
	hasher   *hashing.Hasher
}

func NewSyncer(
	ctx context.Context,
	profile string,
	algorithms []hashing.Algorithm,
	sizeOnly bool,
	dryRun bool,
) (*Syncer, error) {
	config, err := getAwsConfig(ctx, profile)
	if err != nil {
		return nil, fmt.Errorf("failed to get aws config: %v", err)
	}

	s3Client := s3.NewFromConfig(config)

	return &Syncer{
		Algorithms: algorithms,
		SizeOnly:   sizeOnly,
		DryRun:     dryRun,
		s3Client:   s3Client,
		uploader:   manager.NewUploader(s3Client),
		hasher:     hashing.NewHasher(ctx),
	}, nil
}

func (s *Syncer) Sync(ctx context.Context, srcPath paths.LocalPath, dstPath paths.S3Path) error {
	var syncCheck SyncCheck

	if s.SizeOnly {
		syncCheck = SizeOnly
	} else {
		syncCheck = ModifiedAndSize
	}

	if srcPath.Stat.IsDir() {
		// check for any objects under destination
		// we don't need to perform any sync checks if the destination is empty
		maxKeys := int32(1)
		ret, err := s.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(dstPath.Bucket),
			Prefix:  aws.String(dstPath.Prefix + "/"),
			MaxKeys: &maxKeys,
		})
		if err != nil {
			log.Fatalf("failed to list destination objects: %v", err)
		}

		if *ret.KeyCount == 0 {
			syncCheck = None
		}
	}

	err := filepath.WalkDir(srcPath.Path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			log.Printf("%s: error reading path %v", path, err)
			return nil
		}

		if d.IsDir() {
			// ignore - don't create directory marker in S3
			return nil
		}

		rel := strings.TrimPrefix(path, srcPath.Base)
		key := dstPath.Prefix + "/" + rel

		ret, err := s.syncToS3(
			ctx,
			dstPath.Bucket,
			key,
			path,
			d,
			syncCheck,
		)

		if err != nil {
			return err
		}

		switch ret.Type {
		case Skip:
			log.Printf("%s: skipped", path)
		case MetadataOnly:
			log.Printf("%s: updated metadata %v", path, ret.MissingAlgorithms)
		case Upload:
			log.Printf("%s: uploaded", path)
		}

		return nil
	})

	return err
}

func (s *Syncer) checkIfSyncNeeded(
	ctx context.Context,
	bucket, key, path string,
	entry fs.DirEntry,
	syncCheck SyncCheck,
) (*SyncTypeResult, error) {
	if syncCheck == None {
		return &SyncTypeResult{Type: Upload}, nil
	}

	objectInfo, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return &SyncTypeResult{Type: Upload}, nil
		}
		return nil, fmt.Errorf("failed to head object: %s %v", key, err)
	}

	fileInfo, err := entry.Info()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %s %v", path, err)
	}

	objectSize := objectInfo.ContentLength
	sizeMatch := objectSize != nil && fileInfo.Size() == *objectSize

	match := sizeMatch

	if match && syncCheck == ModifiedAndSize {
		lastModified := objectInfo.LastModified
		lastModifiedMatch := lastModified != nil &&
			(fileInfo.ModTime() == *lastModified || fileInfo.ModTime().Before(*lastModified))

		match = lastModifiedMatch
	}

	if match {
		missingAlgorithms := make([]hashing.Algorithm, 0)
		for _, algorithm := range s.Algorithms {
			if _, exists := objectInfo.Metadata[algorithm.String()]; !exists {
				missingAlgorithms = append(missingAlgorithms, algorithm)
			}
		}

		if len(missingAlgorithms) == 0 {
			return &SyncTypeResult{Type: Skip}, nil
		}

		return &SyncTypeResult{
			Type:              MetadataOnly,
			MissingAlgorithms: missingAlgorithms,
			Metadata:          objectInfo.Metadata,
		}, nil
	}

	return &SyncTypeResult{Type: Upload}, nil
}

func (s *Syncer) copyObject(ctx context.Context, path, bucket, key string, metadata map[string]string) error {
	if logging.LogLevel.Level() == slog.LevelDebug {
		defer logging.DebugTimeElapsed(fmt.Sprintf("copyObject %s s3://%s/%s %v", path, bucket, key, metadata))()
	}

	_, err := s.s3Client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		CopySource:        aws.String(bucket + "/" + key),
		Metadata:          metadata,
		MetadataDirective: types.MetadataDirectiveReplace,
	})

	if err != nil {
		return fmt.Errorf("failed to update metadata: %v", err)
	}

	return err
}

func (s *Syncer) uploadObject(ctx context.Context, path, bucket, key string, metadata map[string]string) error {
	if logging.LogLevel.Level() == slog.LevelDebug {
		defer logging.DebugTimeElapsed(fmt.Sprintf("uploadObject %s s3://%s/%s %v", path, bucket, key, metadata))()
	}

	reader, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer reader.Close()

	_, err = s.uploader.Upload(
		ctx,
		&s3.PutObjectInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			Body:     reader,
			Metadata: metadata,
		},
	)

	if err != nil {
		return fmt.Errorf("failed to upload: %v", err)
	}

	return nil
}

func (s *Syncer) syncToS3(
	ctx context.Context,
	bucket, key, path string,
	entry fs.DirEntry,
	checkMode SyncCheck,
) (*SyncTypeResult, error) {
	ret, err := s.checkIfSyncNeeded(
		ctx,
		bucket,
		key,
		path,
		entry,
		checkMode)

	if err != nil {
		return nil, err
	}

	if ret.Type == Skip {
		return ret, nil
	}

	if ret.Type == MetadataOnly {
		metadata := ret.Metadata
		s.generateHashes(ctx, ret.MissingAlgorithms, path, metadata)
		if !s.DryRun {
			err := s.copyObject(ctx, path, bucket, key, metadata)
			if err != nil {
				return nil, err
			}
		}
	} else {
		metadata := make(map[string]string)
		s.generateHashes(ctx, s.Algorithms, path, metadata)
		if !s.DryRun {
			err := s.uploadObject(ctx, path, bucket, key, metadata)
			if err != nil {
				return nil, err
			}
		}
	}

	return ret, nil
}
