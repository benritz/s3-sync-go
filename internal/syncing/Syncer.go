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
	Copy
	UpdateMetadata
	UpdateStorageClass
	DirMarker
	Error
)

type SyncResult struct {
	SrcPath    *paths.Path
	DstPath    *paths.Path
	Outcome    SyncOutcome
	Algorithms []hashing.Algorithm
	Err        error
}

func NewSyncResult(srcPath, dstPath *paths.Path) *SyncResult {
	return &SyncResult{
		SrcPath: srcPath,
		DstPath: dstPath,
	}
}

func (r *SyncResult) Copy(algorithms []hashing.Algorithm) *SyncResult {
	r.Outcome = Copy
	r.Algorithms = algorithms
	return r
}

func (r *SyncResult) Skip() *SyncResult {
	r.Outcome = Skip
	return r
}

func (r *SyncResult) UpdateMetadata(algorithms []hashing.Algorithm) *SyncResult {
	r.Outcome = UpdateMetadata
	r.Algorithms = algorithms
	return r
}

func (r *SyncResult) UpdateStorageClass() *SyncResult {
	r.Outcome = UpdateStorageClass
	return r
}

func (r *SyncResult) DirMarker() *SyncResult {
	r.Outcome = DirMarker
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
			if err := s.headS3Path(ctx, path); err != nil {
				return err
			}

			// copy existing metadata which may include hashes
			if path.Metadata != nil {
				for key, value := range path.Metadata {
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

		tmp, err := os.CreateTemp("", "s3sync-*.tmp")
		if err != nil {
			return fmt.Errorf("failed to create temp file: %v", err)
		}

		cleanup := func(err error) error {
			tmp.Close()
			os.Remove(tmp.Name())
			return fmt.Errorf("failed to copy object to temp file: %v", err)
		}

		written, err := io.Copy(tmp, ret.Body)
		if err != nil {
			return cleanup(err)
		}

		if written != *ret.ContentLength {
			err := fmt.Errorf("length mismatch: written=%v, length=%v", written, *ret.ContentLength)
			return cleanup(err)
		}

		tmp.Close()

		path.LocalCopy = tmp.Name()
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
				case UpdateMetadata:
					slog.DebugContext(
						ctx,
						"sync worker: updated metadata",
						"src", src,
						"dst", dst,
						"missingAlgorithms", ret.Algorithms,
					)
				case Copy:
					slog.DebugContext(
						ctx,
						"sync worker: copied",
						"src", src,
						"dst", dst,
					)
				case DirMarker:
					slog.DebugContext(
						ctx,
						"sync worker: dir marker",
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
	algorithms    []hashing.Algorithm
	sizeOnly      bool
	dryRun        bool
	incDirMarkers bool
	incHidden     bool
	storageClass  *types.StorageClass
	maxPartSize   int64
	awsProfile    string
	concurrency   int
	s3Client      *s3.Client
	uploader      *manager.Uploader
	hasher        *hashing.Hasher
	queue         chan syncJob
	waitGroup     sync.WaitGroup
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

func WithIncDirMarkers() SyncerOption {
	return func(s *Syncer) error {
		s.incDirMarkers = true
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

	s.setBucketLocation(ctx, dstRoot)

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

	s.setBucketLocation(ctx, srcRoot)

	return s.syncFromS3(
		ctx,
		srcRoot,
		dstRoot,
		syncCheck,
		result,
		progress,
	)
}

func (s *Syncer) s3DirExists(ctx context.Context, path *paths.Path) (bool, error) {
	maxKeys := int32(2)
	prefix := path.Key + "/"

	ret, err := s.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(path.Bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: &maxKeys,
	},
		withRegion(path),
	)

	if err != nil {
		return false, fmt.Errorf("failed to list objects: %v", err)
	}

	if *ret.KeyCount == 0 {
		return false, nil
	}

	// check for directory marker
	if *ret.KeyCount == 1 &&
		!*ret.IsTruncated &&
		*ret.Contents[0].Key == prefix {
		return false, nil
	}

	return true, nil
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

	// we don't need to perform any sync checks if the destination is empty
	if stat.IsDir() {
		if exists, err := s.s3DirExists(ctx, dstRoot); err != nil {
			return err
		} else if !exists {
			syncCheck = None
		}
	}

	err = filepath.WalkDir(srcRoot.Path, func(path string, d fs.DirEntry, err error) error {
		// optionally ignore directories (don't create directory marker in S3)
		if !s.incDirMarkers && d.IsDir() {
			return nil
		}

		srcPath := &paths.Path{Path: path}

		// optionally ignories hidden files
		if !s.incHidden && paths.IsHidden(srcPath) {
			return nil
		}

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
	queue := func(srcPath, dstPath *paths.Path) {
		s.queue <- syncJob{
			SrcPath:   srcPath,
			DstPath:   dstPath,
			SyncCheck: syncCheck,
			Result:    result,
			Progress:  progress,
		}
	}

	// check if src root is an object or a directory
	if err := s.headS3Path(ctx, srcRoot); err != nil {
		return err
	}

	if srcRoot.PathInfo != nil && !srcRoot.IsDir {
		// single object
		// dst path can be a directory or a file
		// assume directory if no extension
		var dstPath *paths.Path
		dstExt := filepath.Ext(dstRoot.Path)
		if dstExt == "" {
			name := filepath.Base(srcRoot.Path)
			dstPath = dstRoot.AppendRel(name)
		} else {
			dstPath = dstRoot
		}

		queue(srcRoot, dstPath)
	} else {
		if exists, err := s.s3DirExists(ctx, dstRoot); err != nil {
			return err
		} else if !exists {
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

				// optionally ignore directories (don't create directory marker in S3)
				if !s.incDirMarkers && srcPath.IsDir {
					continue
				}

				// optionally ignories hidden files
				if !s.incHidden && paths.IsHidden(srcPath) {
					continue
				}

				rel := srcRoot.GetRel(srcPath.Path)
				dstPath := dstRoot.AppendRel(rel)

				queue(srcPath, dstPath)
			}
		}
	}

	return nil
}

// loads path info from S3 if needed
func (s *Syncer) headS3Path(ctx context.Context, path *paths.Path) error {
	// skip if path info already loaded
	if path.PathInfo != nil &&
		path.StorageClass != "" &&
		path.Metadata != nil {
		return nil
	}

	ret, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(path.Bucket),
		Key:    aws.String(path.Key),
	},
		withRegion(path),
	)

	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return nil
		}

		return fmt.Errorf("failed to head path: %s %v", path.Path, err)
	}

	if path.PathInfo == nil {
		path.PathInfo = &paths.PathInfo{
			Size:    *ret.ContentLength,
			ModTime: *ret.LastModified,
			IsDir:   *ret.ContentLength == 0 && strings.HasSuffix(path.Key, "/"),
		}
	}

	path.StorageClass = ret.StorageClass

	metadata := ret.Metadata
	if metadata == nil {
		metadata = make(map[string]string)
	}
	path.Metadata = metadata

	return nil
}

func (s *Syncer) checkIfSyncNeeded(
	ctx context.Context,
	srcPath *paths.Path,
	dstPath *paths.Path,
	syncCheck SyncCheck,
	algorithms []hashing.Algorithm,
) *SyncResult {
	ret := NewSyncResult(srcPath, dstPath)

	if !srcPath.IsDir && syncCheck == None {
		return ret.Copy(algorithms)
	}

	if err := s.headS3Path(ctx, dstPath); err != nil {
		return ret.Error(err)
	}

	// dst missing
	if dstPath.PathInfo == nil {
		if srcPath.IsDir {
			return ret.DirMarker()
		}

		return ret.Copy(algorithms)
	}

	if srcPath.IsDir {
		return ret.Skip()
	}

	var match bool

	if srcPath.IsSame(dstPath) {
		match = true
	} else {
		if srcPath.PathInfo == nil {
			return ret.Error(fmt.Errorf("failed to get path info: %s", srcPath.Path))
		}

		sizeMatch := srcPath.Size == dstPath.Size

		match = sizeMatch

		if match && syncCheck == ModifiedAndSize {
			match = (srcPath.ModTime == dstPath.ModTime || srcPath.ModTime.Before(dstPath.ModTime))
		}
	}

	if !match {
		return ret.Copy(algorithms)
	}

	missingAlgorithms := make([]hashing.Algorithm, 0)
	for _, algorithm := range algorithms {
		if _, exists := dstPath.Metadata[algorithm.String()]; !exists {
			missingAlgorithms = append(missingAlgorithms, algorithm)
		}
	}

	if len(missingAlgorithms) > 0 {
		return ret.UpdateMetadata(missingAlgorithms)
	}

	if s.storageClass == nil && srcPath.S3 != nil && dstPath.StorageClass != srcPath.StorageClass {
		dstPath.StorageClass = srcPath.StorageClass
		return ret.UpdateStorageClass()
	}

	if s.storageClass != nil && dstPath.StorageClass != *s.storageClass {
		return ret.UpdateStorageClass()
	}

	return ret.Skip()
}

func (s *Syncer) updateObject(ctx context.Context, dstPath *paths.Path, outcome SyncOutcome) error {
	if logging.LogLevel.Level() == slog.LevelDebug {
		defer logging.DebugTimeElapsed(fmt.Sprintf("updateObject %s %v", dstPath.Path, dstPath.Metadata))()
	}

	copySrc := dstPath.BucketUrlEncoded() + "/" + dstPath.KeyUrlEncoded()

	input := s3.CopyObjectInput{
		Bucket:            aws.String(dstPath.Bucket),
		Key:               aws.String(dstPath.Key),
		CopySource:        aws.String(copySrc),
		MetadataDirective: types.MetadataDirectiveCopy,
		StorageClass:      dstPath.StorageClass,
	}

	if outcome == UpdateMetadata {
		input.Metadata = dstPath.Metadata
		input.MetadataDirective = types.MetadataDirectiveReplace
	}

	if s.storageClass != nil {
		input.StorageClass = *s.storageClass
	}

	_, err := s.s3Client.CopyObject(ctx, &input, withRegion(dstPath))

	if err != nil {
		return fmt.Errorf("failed to update metadata: %v", err)
	}

	return err
}

func (s *Syncer) copyObject(
	ctx context.Context,
	srcPath *paths.Path,
	dstPath *paths.Path,
) error {
	if logging.LogLevel.Level() == slog.LevelDebug {
		defer logging.DebugTimeElapsed(fmt.Sprintf("copyObject %s %s %v", srcPath.Path, dstPath.Path, dstPath.Metadata))()
	}

	copySrc := srcPath.BucketUrlEncoded() + "/" + srcPath.KeyUrlEncoded()

	input := s3.CopyObjectInput{
		Bucket:            aws.String(dstPath.Bucket),
		Key:               aws.String(dstPath.Key),
		CopySource:        aws.String(copySrc),
		Metadata:          dstPath.Metadata,
		MetadataDirective: types.MetadataDirectiveReplace,
		StorageClass:      srcPath.StorageClass,
	}

	if s.storageClass != nil {
		input.StorageClass = *s.storageClass
	}

	_, err := s.s3Client.CopyObject(ctx, &input, withRegion(dstPath))

	if err != nil {
		return fmt.Errorf("failed to copy object: %v", err)
	}

	return err
}

func (s *Syncer) createDirMarker(
	ctx context.Context,
	path *paths.Path,
) error {
	if logging.LogLevel.Level() == slog.LevelDebug {
		defer logging.DebugTimeElapsed(fmt.Sprintf("dirMarker %s", path.Path))()
	}

	_, err := s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(path.Bucket),
		Key:    aws.String(path.Key),
		Body:   strings.NewReader(""),
	},
		withRegion(path),
	)

	if err != nil {
		return fmt.Errorf("failed to create dir marker: %v", err)
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

func (s *Syncer) reader(ctx context.Context, path *paths.Path) (io.ReadCloser, error) {
	if path.S3 == nil {
		return os.Open(path.Path)
	}

	if path.LocalCopy != "" {
		return os.Open(path.LocalCopy)
	}

	ret, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(path.Bucket),
		Key:    aws.String(path.Key),
	},
		withRegion(path),
	)

	if err != nil {
		return nil, err
	}

	return ret.Body, nil
}

func (s *Syncer) uploadObject(
	ctx context.Context,
	srcPath *paths.Path,
	dstPath *paths.Path,
	progress chan<- *SyncProgress,
) error {
	if logging.LogLevel.Level() == slog.LevelDebug {
		defer logging.DebugTimeElapsed(fmt.Sprintf("uploadObject %s %s %v", srcPath.Path, dstPath.Path, dstPath.Metadata))()
	}

	reader, err := s.reader(ctx, srcPath)

	if err != nil {
		return fmt.Errorf("failed to get reader: %s %v", srcPath.Path, err)
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
		Metadata: dstPath.Metadata,
	}

	if srcPath.S3 != nil {
		input.StorageClass = srcPath.StorageClass
	}
	if s.storageClass != nil {
		input.StorageClass = *s.storageClass
	}

	// add checksum if full object checksum is possible (object size <= part size)
	// s3 supports CRC32C, CRC32, CRC-64/NVME, SHA256, SHA1
	// only CRC algorithms support full object checksums
	// manager.Uploader does not support the ChecksumType "full object" param yet - https://aws.amazon.com/blogs/aws/introducing-default-data-integrity-protections-for-new-objects-in-amazon-s3/
	var partSize *int64

	if srcPath.PathInfo.Size <= s.maxPartSize {
		partSize = &s.maxPartSize

		if hash, exists := dstPath.Metadata[hashing.CRC32C.String()]; exists {
			if ret, _ := convertHexToBase64(hash); ret != "" {
				input.ChecksumCRC32C = aws.String(ret)
			}
		} else if hash, exists := dstPath.Metadata[hashing.CRC32.String()]; exists {
			if ret, _ := convertHexToBase64(hash); ret != "" {
				input.ChecksumCRC32 = aws.String(ret)
			}
		} else if hash, exists := dstPath.Metadata[hashing.SHA256.String()]; exists {
			if ret, _ := convertHexToBase64(hash); ret != "" {
				input.ChecksumSHA256 = aws.String(ret)
			}
		} else if hash, exists := dstPath.Metadata[hashing.SHA1.String()]; exists {
			if ret, _ := convertHexToBase64(hash); ret != "" {
				input.ChecksumSHA1 = aws.String(ret)
			}
		}
	}

	if _, err = s.uploader.Upload(ctx, &input, withPartSize(partSize)); err != nil {
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
	// manager.Uploader does not support the ChecksumType "full object" param yet, uncomment the following block when supported
	// source files greater than the maximum part size (limited to 5GB) must be uploaded in multiple parts
	// only CRC checksums support full object checksums
	// ensure a CRC algorithm is used
	// var algorithms []hashing.Algorithm

	// if srcPath.Size > s.maxPartSize &&
	// 	!(slices.Contains(s.algorithms, hashing.CRC32C) || slices.Contains(s.algorithms, hashing.CRC32)) {
	// 	algorithms = make([]hashing.Algorithm, len(s.algorithms))
	// 	copy(algorithms, s.algorithms)
	// 	algorithms = append(algorithms, hashing.CRC32C)
	// } else {
	// 	algorithms = s.algorithms
	// }

	algorithms := s.algorithms

	ret := s.checkIfSyncNeeded(
		ctx,
		srcPath,
		dstPath,
		checkMode,
		algorithms,
	)

	if ret.Outcome == Error || ret.Outcome == Skip {
		return ret
	}

	if ret.Outcome == DirMarker {
		if !s.dryRun {
			err := s.createDirMarker(ctx, dstPath)
			if err != nil {
				return ret.Error(err)
			}
		}

		return ret
	}

	if dstPath.Metadata == nil {
		dstPath.Metadata = make(map[string]string)
	}

	if len(ret.Algorithms) > 0 {
		err := s.generateHashes(
			ctx,
			ret.Algorithms,
			srcPath,
			dstPath.Metadata,
			!srcPath.IsSame(dstPath),
		)

		defer srcPath.RemoveLocalCopy()

		if err != nil {
			return ret.Error(err)
		}
	}

	if s.dryRun {
		return ret
	}

	var err error

	if (ret.Outcome == UpdateMetadata || ret.Outcome == UpdateStorageClass) && srcPath.Size <= MaxPartSize {
		err = s.updateObject(ctx, dstPath, ret.Outcome)
	} else if srcPath.S3 != nil && srcPath.Size <= MaxPartSize {
		err = s.copyObject(ctx, srcPath, dstPath)
	} else {
		err = s.uploadObject(ctx, srcPath, dstPath, progress)
	}

	if err != nil {
		return ret.Error(err)
	}

	return ret
}
