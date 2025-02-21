package syncing

import (
	"benritz/s3sync/internal/hashing"
	"benritz/s3sync/internal/logging"
	"benritz/s3sync/internal/paths"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

type SyncOutcome int

const (
	Skip SyncOutcome = iota
	Copied
	MetadataOnly
	Error
)

type SyncResult struct {
	SrcPath           *paths.Path
	DstPath           *paths.Path
	Outcome           SyncOutcome
	MissingAlgorithms []hashing.Algorithm
	Metadata          map[string]string
	Err               error
}

func NewSyncResult(srcPath, dstPath *paths.Path) *SyncResult {
	return &SyncResult{
		SrcPath: srcPath,
		DstPath: dstPath,
	}
}

func (r *SyncResult) Copied() *SyncResult {
	r.Outcome = Copied
	return r
}

func (r *SyncResult) Skip() *SyncResult {
	r.Outcome = Skip
	return r
}

func (r *SyncResult) MetadataOnly(missingAlgorithms []hashing.Algorithm, metadata map[string]string) *SyncResult {
	r.Outcome = MetadataOnly
	r.MissingAlgorithms = missingAlgorithms
	r.Metadata = metadata
	return r
}

func (r *SyncResult) Error(err error) *SyncResult {
	r.Outcome = Error
	r.Err = err
	return r
}

type SyncProgress struct {
	SrcPath  *paths.Path
	Progress int8
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
			},
				withRegion(path),
			)

			if err != nil {
				return fmt.Errorf("failed to head object: %v", err)
			}

			// copy existing metadata which may include hashes
			if ret.Metadata != nil {
				for key, value := range ret.Metadata {
					metadata[key] = value
				}
			}

			// remove any found hash algorithms
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
		},
			withRegion(path),
		)

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
	Result    chan<- *SyncResult   `json:"-"`
	Progress  chan<- *SyncProgress `json:"-"`
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
				job.Progress,
			)

			if logging.LogLevel.Level() == slog.LevelDebug {
				src := ret.SrcPath.Path
				dst := ret.DstPath.Path

				switch ret.Outcome {
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

			if job.Result != nil {
				job.Result <- ret
			}
		}
	}
}

// 5GB is the maximum part size for S3 uploads
var MaxPartSize = int64(5) << 10 << 10 << 10

type Syncer struct {
	algorithms   []hashing.Algorithm
	sizeOnly     bool
	dryRun       bool
	incHidden    bool
	storageClass *types.StorageClass
	maxPartSize  int64
	awsProfile   string
	concurrency  int
	s3Client     *s3.Client
	uploader     *manager.Uploader
	hasher       *hashing.Hasher
	queue        chan syncJob
	waitGroup    sync.WaitGroup
}

type SyncerOption func(*Syncer) error

func WithProfile(profile string) SyncerOption {
	return func(s *Syncer) error {
		profile = strings.TrimSpace(profile)
		if profile == "" {
			return fmt.Errorf("profile must not be empty")
		}
		s.awsProfile = profile
		return nil
	}
}

func WithAlgorithms(algorithms []hashing.Algorithm) SyncerOption {
	return func(s *Syncer) error {
		s.algorithms = algorithms
		return nil
	}
}

func WithConcurrency(concurrency int) SyncerOption {
	return func(s *Syncer) error {
		if concurrency < 1 {
			return fmt.Errorf("concurrency must be greater than 0")
		}
		if concurrency > runtime.GOMAXPROCS(0) {
			return fmt.Errorf("concurrency must not exceed %d", runtime.GOMAXPROCS(0))
		}
		s.concurrency = concurrency
		return nil
	}
}

func WithMaxPartSize(maxPartSize int64) SyncerOption {
	return func(s *Syncer) error {
		if maxPartSize < 1 {
			return fmt.Errorf("max part size must be greater than 0")
		}
		if maxPartSize > MaxPartSize {
			return fmt.Errorf("max part size must not exceed %d", MaxPartSize)
		}
		s.maxPartSize = maxPartSize
		return nil
	}
}

func WithSizeOnly() SyncerOption {
	return func(s *Syncer) error {
		s.sizeOnly = true
		return nil
	}
}

func WithDryRun() SyncerOption {
	return func(s *Syncer) error {
		s.dryRun = true
		return nil
	}
}

func WithIncHidden() SyncerOption {
	return func(s *Syncer) error {
		s.incHidden = true
		return nil
	}
}

func toStorageClass(storageClass string) (*types.StorageClass, error) {
	if storageClass == "" {
		return nil, nil
	}

	storageClass = strings.ToUpper(storageClass)

	values := types.StorageClass("").Values()

	for _, v := range values {
		if types.StorageClass(storageClass) == v {
			return &v, nil
		}
	}

	return nil, fmt.Errorf("invalid storage class: %s; %v", storageClass, values)
}

func WithStorageClass(name string) SyncerOption {
	return func(s *Syncer) error {
		storageClass, err := toStorageClass(name)
		if err != nil {
			return err
		}

		s.storageClass = storageClass
		return nil
	}
}

func NewSyncer(
	ctx context.Context,
	options ...SyncerOption,
) (*Syncer, error) {
	s := &Syncer{
		awsProfile:  "default",
		algorithms:  []hashing.Algorithm{hashing.SHA1},
		sizeOnly:    false,
		dryRun:      false,
		incHidden:   false,
		maxPartSize: int64(1024 * 1024 * 250),
		queue:       make(chan syncJob),
	}

	for _, option := range options {
		err := option(s)
		if err != nil {
			return nil, err
		}
	}

	config, err := getAwsConfig(ctx, s.awsProfile)
	if err != nil {
		return nil, fmt.Errorf("failed to get aws config: %v", err)
	}

	s.s3Client = s3.NewFromConfig(config)
	s.uploader = manager.NewUploader(s.s3Client)
	s.hasher = hashing.NewHasher(ctx, s.concurrency*len(s.algorithms))

	for i := 0; i < s.concurrency; i++ {
		go s.worker(ctx, i)
	}

	return s, nil
}

func (s *Syncer) Close() {
	close(s.queue)
	s.waitGroup.Wait()
}

func (s *Syncer) setBucketLocation(ctx context.Context, path *paths.Path) error {
	ret, err := s.s3Client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
		Bucket: aws.String(path.Bucket),
	})
	if err != nil {
		return fmt.Errorf("failed to get bucket location: %v", err)
	}

	path.S3.Location = string(ret.LocationConstraint)

	return nil
}

func (s *Syncer) Sync(
	ctx context.Context,
	srcRoot *paths.Path,
	dstRoot *paths.Path,
	result chan<- *SyncResult,
	progress chan<- *SyncProgress,
) error {
	if dstRoot.S3 == nil {
		return fmt.Errorf("destination path is not an S3 path")
	}

	var syncCheck SyncCheck

	if s.sizeOnly {
		syncCheck = SizeOnly
	} else {
		syncCheck = ModifiedAndSize
	}

	s.setBucketLocation(ctx, srcRoot)

	if srcRoot.S3 == nil {
		return s.syncFromLocal(
			ctx,
			srcRoot,
			dstRoot,
			syncCheck,
			result,
			progress,
		)
	}

	s.setBucketLocation(ctx, dstRoot)

	return s.syncFromS3(
		ctx,
		srcRoot,
		dstRoot,
		syncCheck,
		result,
		progress,
	)
}

func (s *Syncer) syncFromLocal(
	ctx context.Context,
	srcRoot *paths.Path,
	dstRoot *paths.Path,
	syncCheck SyncCheck,
	result chan<- *SyncResult,
	progress chan<- *SyncProgress,
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
		},
			withRegion(dstRoot),
		)

		if err != nil {
			return fmt.Errorf("failed to check for destination objects: %v", err)
		}

		if *ret.KeyCount == 0 {
			syncCheck = None
		}
	}

	err = filepath.WalkDir(srcRoot.Path, func(path string, d fs.DirEntry, err error) error {
		// ignore directories - don't create directory marker in S3
		if d.IsDir() {
			return nil
		}

		// optionally ignories hidden files
		if !s.incHidden && paths.IsHidden(path) {
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
			Progress:  progress,
		}

		return nil
	})

	return err
}

func withRegion(path *paths.Path) func(o *s3.Options) {
	return func(o *s3.Options) {
		var region string
		if path.S3.Location != "" {
			region = path.S3.Location
		} else {
			region = "us-east-1"
		}
		o.Region = region
	}
}

func (s *Syncer) syncFromS3(
	ctx context.Context,
	srcRoot *paths.Path,
	dstRoot *paths.Path,
	syncCheck SyncCheck,
	result chan<- *SyncResult,
	progress chan<- *SyncProgress,
) error {
	var queuePath = func(path *paths.Path) {
		rel := srcRoot.GetRel(path.Path)
		dstPath := dstRoot.AppendRel(rel)

		s.queue <- syncJob{
			SrcPath:   path,
			DstPath:   dstPath,
			SyncCheck: syncCheck,
			Result:    result,
			Progress:  progress,
		}
	}

	// check if src path is an object or a directory
	ret, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(srcRoot.Bucket),
		Key:    aws.String(srcRoot.Key),
	},
		withRegion(srcRoot),
	)

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
		},
			withRegion(dstRoot),
		)

		if err != nil {
			return fmt.Errorf("failed to check for destination objects: %s %v", dstRoot.Path, err)
		}

		if *ret.KeyCount == 0 {
			syncCheck = None
		}

		paginator := s3.NewListObjectsV2Paginator(s.s3Client, &s3.ListObjectsV2Input{
			Bucket: aws.String(srcRoot.Bucket),
			Prefix: aws.String(srcRoot.Key + "/"),
		})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx, withRegion(srcRoot))
			if err != nil {
				return fmt.Errorf("failed to list source objects: %v", err)
			}

			for _, obj := range page.Contents {
				srcPath := paths.FromS3Object(srcRoot, &obj)
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

	if srcPath.IsDir {
		return ret.Skip()
	}

	if syncCheck == None {
		return ret.Copied()
	}

	objectInfo, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(dstPath.Bucket),
		Key:    aws.String(dstPath.Key),
	},
		withRegion(dstPath),
	)

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
		for _, algorithm := range s.algorithms {
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
	},
		withRegion(dstPath),
	)

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
	},
		withRegion(dstPath),
	)

	if err != nil {
		return fmt.Errorf("failed to copy object: %v", err)
	}

	return err
}

func convertHexToBase64(hexStr string) (string, error) {
	binary, err := hex.DecodeString(hexStr)
	if err != nil {
		return "", err
	}
	ret := base64.StdEncoding.EncodeToString(binary)
	return ret, nil
}

func withPartSize(partSize *int64) func(*manager.Uploader) {
	return func(uploader *manager.Uploader) {
		if partSize != nil {
			uploader.PartSize = *partSize
		}
	}
}

func (s *Syncer) uploadObject(
	ctx context.Context,
	srcPath *paths.Path,
	dstPath *paths.Path,
	metadata map[string]string,
	progress chan<- *SyncProgress,
) error {
	if logging.LogLevel.Level() == slog.LevelDebug {
		defer logging.DebugTimeElapsed(fmt.Sprintf("uploadObject %s %s %v", srcPath.Path, dstPath.Path, metadata))()
	}

	reader, err := os.Open(srcPath.Path)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer reader.Close()

	var progressFn func(size int)

	if progress != nil {
		totalRead := 0.0
		progressFn = func(size int) {
			totalRead += float64(size)
			percent := totalRead / float64(srcPath.Size) * 100.0
			progress <- &SyncProgress{
				SrcPath:  srcPath,
				Progress: int8(math.Min(percent, 100.0)),
			}
		}
	} else {
		progressFn = func(size int) {}
	}

	input := s3.PutObjectInput{
		Bucket:   aws.String(dstPath.Bucket),
		Key:      aws.String(dstPath.Key),
		Body:     paths.NewProgressReader(reader, progressFn),
		Metadata: metadata,
	}

	if s.storageClass != nil {
		input.StorageClass = *s.storageClass
	}

	// add checksum to upload
	// s3 supports CRC32C, CRC32, SHA256, SHA1  (CRC-64/NVME is supported but not in the SDK)
	// only CRC algorithms support full object checksums
	var partSize *int64

	if hash, exists := metadata[hashing.CRC32C.String()]; exists {
		ret, err := convertHexToBase64(hash)
		if err == nil {
			input.ChecksumCRC32C = aws.String(ret)
		}
	} else if hash, exists := metadata[hashing.CRC32.String()]; exists {
		ret, err := convertHexToBase64(hash)
		if err == nil {
			input.ChecksumCRC32 = aws.String(ret)
		}
	} else if srcPath.PathInfo != nil && srcPath.PathInfo.Size <= s.maxPartSize {
		// multi-part uploads can only SHA checksum if the object size is less or equal to the part size
		partSize = &s.maxPartSize

		if hash, exists := metadata[hashing.SHA256.String()]; exists {
			ret, err := convertHexToBase64(hash)
			if err == nil {
				input.ChecksumSHA256 = aws.String(ret)
			}
		} else if hash, exists := metadata[hashing.SHA1.String()]; exists {
			ret, err := convertHexToBase64(hash)
			if err == nil {
				input.ChecksumSHA1 = aws.String(ret)
			}
		}
	}

	_, err = s.uploader.Upload(ctx, &input, withPartSize(partSize))

	if err != nil {
		return fmt.Errorf("failed to upload: %v", err)
	}

	return nil
}

func (s *Syncer) sync(
	ctx context.Context,
	srcPath, dstPath *paths.Path,
	checkMode SyncCheck,
	progress chan<- *SyncProgress,
) *SyncResult {
	ret := s.checkIfSyncNeeded(
		ctx,
		srcPath,
		dstPath,
		checkMode,
	)

	if ret.Outcome == Error || ret.Outcome == Skip {
		return ret
	}

	if ret.Outcome == MetadataOnly {
		metadata := ret.Metadata
		if metadata == nil {
			metadata = make(map[string]string)
		}

		s.generateHashes(
			ctx,
			ret.MissingAlgorithms,
			srcPath,
			metadata,
			!srcPath.IsSame(dstPath),
		)

		if !s.dryRun {
			err := s.updateMetadata(ctx, dstPath, metadata)
			if err != nil {
				return ret.Error(err)
			}
		}
	} else {
		metadata := make(map[string]string)

		s.generateHashes(
			ctx,
			s.algorithms,
			srcPath,
			metadata,
			true,
		)

		if !s.dryRun {
			var err error

			if srcPath.S3 != nil {
				err = s.copyObject(ctx, srcPath, dstPath, metadata)
			} else {
				err = s.uploadObject(ctx, srcPath, dstPath, metadata, progress)
			}

			if err != nil {
				return ret.Error(err)
			}
		}
	}

	return ret
}
