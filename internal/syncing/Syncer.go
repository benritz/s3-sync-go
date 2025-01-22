package syncing

import (
	"benritz/s3sync/internal/hashing"
	"benritz/s3sync/internal/logging"
	"benritz/s3sync/internal/paths"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

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
	Copied
	MetadataOnly
	Error
)

type SyncResult struct {
	SrcPath           *paths.Path
	DstPath           *paths.Path
	Type              SyncType
	MissingAlgorithms []hashing.Algorithm
	Metadata          map[string]string
	Err               error
}

func NewSyncResult(dstPath, srcPath *paths.Path) *SyncResult {
	return &SyncResult{
		SrcPath: srcPath,
		DstPath: dstPath,
	}
}

func (r *SyncResult) Copied() *SyncResult {
	r.Type = Copied
	return r
}

func (r *SyncResult) Skip() *SyncResult {
	r.Type = Skip
	return r
}

func (r *SyncResult) MetadataOnly(missingAlgorithms []hashing.Algorithm, metadata map[string]string) *SyncResult {
	r.Type = MetadataOnly
	r.MissingAlgorithms = missingAlgorithms
	r.Metadata = metadata
	return r
}

func (r *SyncResult) Error(err error) *SyncResult {
	r.Type = Error
	r.Err = err
	return r
}

func (s *Syncer) generateHashes(
	ctx context.Context,
	algorithms []hashing.Algorithm,
	path *paths.Path,
	metadata map[string]string,
	useSrcMetadata bool,
) error {
	var localPath string
	if path.S3 != nil {
		// look for metadata in src object
		if useSrcMetadata {
			ret, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(path.Bucket),
				Key:    aws.String(path.Key),
			})

			if err != nil {
				return fmt.Errorf("failed to head object: %v", err)
			}

			// copy existing metadata including hashes
			for key, value := range ret.Metadata {
				metadata[key] = value
			}

			// remove any found algorithms
			var a []hashing.Algorithm

			for _, algorithm := range algorithms {
				if _, exists := metadata[algorithm.String()]; !exists {
					a = append(a, algorithm)
				}
			}

			if len(a) == 0 {
				return nil
			}

			algorithms = a
		}

		ret, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(path.Bucket),
			Key:    aws.String(path.Key),
		})

		if err != nil {
			return fmt.Errorf("failed to get object: %v", err)
		}

		defer ret.Body.Close()

		tmp, err := os.CreateTemp("", "prefix-*.txt")
		if err != nil {
			return fmt.Errorf("failed to create temp file: %v", err)
		}
		defer os.Remove(tmp.Name())

		written, err := io.Copy(tmp, ret.Body)
		if err != nil {
			tmp.Close()
			return fmt.Errorf("failed to copy object to temp file: %v", err)
		}
		if written != *ret.ContentLength {
			tmp.Close()
			return fmt.Errorf("failed to copy object to temp file, length mismatch: written=%v, length=%v", written, *ret.ContentLength)
		}

		if err := tmp.Close(); err != nil {
			return fmt.Errorf("failed to close temp file: %v", err)
		}

		localPath = tmp.Name()
	} else {
		localPath = path.Path
	}

	result := make(chan hashing.HashResult, len(algorithms))

	for _, algorithm := range algorithms {
		s.hasher.Generate(localPath, algorithm, result)
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

type syncJob struct {
	SrcPath   *paths.Path
	DstPath   *paths.Path
	SyncCheck SyncCheck
	Result    chan<- *SyncResult `json:"-"`
}

type Syncer struct {
	Algorithms []hashing.Algorithm
	SizeOnly   bool
	DryRun     bool

	s3Client  *s3.Client
	uploader  *manager.Uploader
	hasher    *hashing.Hasher
	queue     chan syncJob
	waitGroup sync.WaitGroup
}

func (s *Syncer) worker(ctx context.Context, id int) {
	s.waitGroup.Add(1)
	defer s.waitGroup.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-s.queue:
			if !ok {
				return
			}

			slog.DebugContext(
				ctx,
				"sync worker: got job",
				"workerId", id,
				"job", job,
			)

			ret := s.sync(
				ctx,
				job.SrcPath,
				job.DstPath,
				job.SyncCheck,
			)

			if logging.LogLevel.Level() == slog.LevelDebug {
				src := ret.SrcPath.Path
				dst := ret.DstPath.Path

				switch ret.Type {
				case Error:
					slog.DebugContext(
						ctx,
						"sync worker: failed to sync",
						"src", src,
						"dst", dst,
						"err", ret.Err,
					)
				case Skip:
					slog.DebugContext(
						ctx,
						"sync worker: skipped",
						"src", src,
						"dst", dst,
					)
				case MetadataOnly:
					slog.DebugContext(
						ctx,
						"sync worker: updated metadata",
						"src", src,
						"dst", dst,
						"missingAlgorithms", ret.MissingAlgorithms,
					)
				case Copied:
					slog.DebugContext(
						ctx,
						"sync worker: copied",
						"src", src,
						"dst", dst,
					)
				}
			}

			job.Result <- ret
		}
	}
}

func NewSyncer(
	ctx context.Context,
	profile string,
	algorithms []hashing.Algorithm,
	concurrency int,
	sizeOnly bool,
	dryRun bool,
) (*Syncer, error) {
	config, err := getAwsConfig(ctx, profile)
	if err != nil {
		return nil, fmt.Errorf("failed to get aws config: %v", err)
	}

	s3Client := s3.NewFromConfig(config)

	s := &Syncer{
		Algorithms: algorithms,
		SizeOnly:   sizeOnly,
		DryRun:     dryRun,
		s3Client:   s3Client,
		uploader:   manager.NewUploader(s3Client),
		hasher:     hashing.NewHasher(ctx, concurrency*len(algorithms)),
		queue:      make(chan syncJob),
	}

	for i := 0; i < concurrency; i++ {
		go s.worker(ctx, i)
	}

	return s, nil
}

func (s *Syncer) Close() {
	close(s.queue)
	s.waitGroup.Wait()
}

func (s *Syncer) Sync(
	ctx context.Context,
	srcRoot *paths.Path,
	dstRoot *paths.Path,
	result chan<- *SyncResult,
) error {
	if dstRoot.S3 == nil {
		return fmt.Errorf("destination path is not an S3 path")
	}

	var syncCheck SyncCheck

	if s.SizeOnly {
		syncCheck = SizeOnly
	} else {
		syncCheck = ModifiedAndSize
	}

	if srcRoot.S3 == nil {
		return s.syncFromLocal(
			ctx,
			srcRoot,
			dstRoot,
			syncCheck,
			result,
		)
	}

	s.syncFromS3(
		ctx,
		srcRoot,
		dstRoot,
		syncCheck,
		result,
	)

	return nil
}

func (s *Syncer) syncFromLocal(
	ctx context.Context,
	srcRoot *paths.Path,
	dstRoot *paths.Path,
	syncCheck SyncCheck,
	result chan<- *SyncResult,
) error {
	stat, err := os.Stat(srcRoot.Path)
	if err != nil {
		return fmt.Errorf("source path not found: %v", err)
	}

	if stat.IsDir() {
		// check for any objects under destination
		// we don't need to perform any sync checks if the destination is empty
		maxKeys := int32(1)

		ret, err := s.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(dstRoot.Bucket),
			Prefix:  aws.String(dstRoot.Key + "/"),
			MaxKeys: &maxKeys,
		})

		if err != nil {
			return fmt.Errorf("failed to check for destination objects: %v", err)
		}

		if *ret.KeyCount == 0 {
			syncCheck = None
		}
	}

	err = filepath.WalkDir(srcRoot.Path, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			// ignore - don't create directory marker in S3
			return nil
		}

		srcPath := &paths.Path{Path: path}

		rel := srcRoot.GetRel(path)
		dstPath := dstRoot.AppendRel(rel)

		syncResultError := func(err error) *SyncResult {
			ret := NewSyncResult(srcPath, dstPath)
			return ret.Error(err)
		}

		if err != nil {
			result <- syncResultError(fmt.Errorf("failed to read path: %v", err))
			return nil
		}

		fileInfo, err := d.Info()
		if err != nil {
			result <- syncResultError(fmt.Errorf("failed to get file info: %v", err))
			return nil
		}

		srcPath.PathInfo = paths.FromFileInfo(fileInfo)

		s.queue <- syncJob{
			SrcPath:   srcPath,
			DstPath:   dstPath,
			SyncCheck: syncCheck,
			Result:    result,
		}

		return nil
	})

	return err
}

func (s *Syncer) syncFromS3(
	ctx context.Context,
	srcRoot *paths.Path,
	dstRoot *paths.Path,
	syncCheck SyncCheck,
	result chan<- *SyncResult,
) error {
	var queuePath = func(path *paths.Path) {
		rel := srcRoot.GetRel(path.Path)
		dstPath := dstRoot.AppendRel(rel)

		s.queue <- syncJob{
			SrcPath:   path,
			DstPath:   dstPath,
			SyncCheck: syncCheck,
			Result:    result,
		}
	}

	// check if src path is an object or a directory
	ret, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(srcRoot.Bucket),
		Key:    aws.String(srcRoot.Key),
	})

	var isDir bool

	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			isDir = true
		} else {
			return fmt.Errorf("failed to head object: %s %v", srcRoot.Path, err)
		}
	} else {
		isDir = *ret.ContentLength == 0
	}

	if !isDir {
		srcPath := &paths.Path{
			Path: srcRoot.Path,
			S3:   srcRoot.S3,
			PathInfo: &paths.PathInfo{
				Size:    *ret.ContentLength,
				ModTime: *ret.LastModified,
			},
		}

		queuePath(srcPath)
	} else {
		// check for any objects under destination
		// we don't need to perform any sync checks if the destination is empty
		maxKeys := int32(1)

		ret, err := s.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(dstRoot.Bucket),
			Prefix:  aws.String(dstRoot.Key + "/"),
			MaxKeys: &maxKeys,
		})

		if err != nil {
			return fmt.Errorf("failed to check for destination objects: %v", err)
		}

		if *ret.KeyCount == 0 {
			syncCheck = None
		}

		paginator := s3.NewListObjectsV2Paginator(s.s3Client, &s3.ListObjectsV2Input{
			Bucket: aws.String(srcRoot.Bucket),
			Prefix: aws.String(srcRoot.Key + "/"),
		})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return fmt.Errorf("failed to check for destination objects: %v", err)
			}

			for _, obj := range page.Contents {
				srcPath := &paths.Path{
					Path: fmt.Sprintf("s3://%s/%s", srcRoot.Bucket, *obj.Key),
					S3: &paths.S3{
						Bucket: srcRoot.Bucket,
						Key:    *obj.Key,
					},
					PathInfo: &paths.PathInfo{
						Size:    *obj.Size,
						ModTime: *obj.LastModified,
					},
				}

				queuePath(srcPath)
			}
		}
	}

	return nil
}

func (s *Syncer) checkIfSyncNeeded(
	ctx context.Context,
	srcPath *paths.Path,
	dstPath *paths.Path,
	syncCheck SyncCheck,
) *SyncResult {
	ret := NewSyncResult(srcPath, dstPath)

	if syncCheck == None {
		return ret.Copied()
	}

	objectInfo, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(dstPath.Bucket),
		Key:    aws.String(dstPath.Key),
	})

	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return ret.Copied()
		}
		return ret.Error(fmt.Errorf("failed to head object: %s %v", dstPath.Path, err))
	}

	var match bool

	if srcPath.IsSame(dstPath) {
		match = true
	} else {
		if srcPath.PathInfo == nil {
			return ret.Error(fmt.Errorf("failed to get path info: %s", srcPath.Path))
		}

		objectSize := objectInfo.ContentLength
		sizeMatch := objectSize != nil && srcPath.Size == *objectSize

		match = sizeMatch

		if match && syncCheck == ModifiedAndSize {
			lastModified := objectInfo.LastModified
			lastModifiedMatch := lastModified != nil &&
				(srcPath.ModTime == *lastModified || srcPath.ModTime.Before(*lastModified))

			match = lastModifiedMatch
		}
	}

	if match {
		missingAlgorithms := make([]hashing.Algorithm, 0)
		for _, algorithm := range s.Algorithms {
			if _, exists := objectInfo.Metadata[algorithm.String()]; !exists {
				missingAlgorithms = append(missingAlgorithms, algorithm)
			}
		}

		if len(missingAlgorithms) == 0 {
			return ret.Skip()
		}

		return ret.MetadataOnly(missingAlgorithms, objectInfo.Metadata)
	}

	return ret.Copied()
}

func (s *Syncer) updateMetadata(ctx context.Context, dstPath *paths.Path, metadata map[string]string) error {
	if logging.LogLevel.Level() == slog.LevelDebug {
		defer logging.DebugTimeElapsed(fmt.Sprintf("updateMetadata %s %v", dstPath.Path, metadata))()
	}

	_, err := s.s3Client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:            aws.String(dstPath.Bucket),
		Key:               aws.String(dstPath.Key),
		CopySource:        aws.String(dstPath.Bucket + "/" + dstPath.Key),
		Metadata:          metadata,
		MetadataDirective: types.MetadataDirectiveReplace,
	})

	if err != nil {
		return fmt.Errorf("failed to update metadata: %v", err)
	}

	return err
}

func (s *Syncer) copyObject(
	ctx context.Context,
	srcPath *paths.Path,
	dstPath *paths.Path,
	metadata map[string]string,
) error {
	if logging.LogLevel.Level() == slog.LevelDebug {
		defer logging.DebugTimeElapsed(fmt.Sprintf("copyObject %s %s %v", srcPath.Path, dstPath.Path, metadata))()
	}

	_, err := s.s3Client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:            aws.String(dstPath.Bucket),
		Key:               aws.String(dstPath.Key),
		CopySource:        aws.String(srcPath.Bucket + "/" + srcPath.Key),
		Metadata:          metadata,
		MetadataDirective: types.MetadataDirectiveReplace,
	})

	if err != nil {
		return fmt.Errorf("failed to copy object: %v", err)
	}

	return err
}

func (s *Syncer) uploadObject(
	ctx context.Context,
	srcPath *paths.Path,
	dstPath *paths.Path,
	metadata map[string]string,
) error {
	if logging.LogLevel.Level() == slog.LevelDebug {
		defer logging.DebugTimeElapsed(fmt.Sprintf("uploadObject %s %s %v", srcPath.Path, dstPath.Path, metadata))()
	}

	reader, err := os.Open(srcPath.Path)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer reader.Close()

	_, err = s.uploader.Upload(
		ctx,
		&s3.PutObjectInput{
			Bucket:   aws.String(dstPath.Bucket),
			Key:      aws.String(dstPath.Key),
			Body:     reader,
			Metadata: metadata,
		},
	)

	if err != nil {
		return fmt.Errorf("failed to upload: %v", err)
	}

	return nil
}

func (s *Syncer) sync(
	ctx context.Context,
	srcPath, dstPath *paths.Path,
	checkMode SyncCheck,
) *SyncResult {
	ret := s.checkIfSyncNeeded(
		ctx,
		srcPath,
		dstPath,
		checkMode)

	if ret.Type == Error || ret.Type == Skip {
		return ret
	}

	if ret.Type == MetadataOnly {
		metadata := ret.Metadata

		s.generateHashes(
			ctx,
			ret.MissingAlgorithms,
			srcPath,
			metadata,
			!srcPath.IsSame(dstPath),
		)

		if !s.DryRun {
			err := s.updateMetadata(ctx, dstPath, metadata)
			if err != nil {
				return ret.Error(err)
			}
		}
	} else {
		metadata := make(map[string]string)

		s.generateHashes(
			ctx,
			s.Algorithms,
			srcPath,
			metadata,
			true,
		)

		if !s.DryRun {
			var err error

			if srcPath.S3 != nil {
				err = s.copyObject(ctx, srcPath, dstPath, metadata)
			} else {
				err = s.uploadObject(ctx, srcPath, dstPath, metadata)
			}

			if err != nil {
				return ret.Error(err)
			}
		}
	}

	return ret
}
