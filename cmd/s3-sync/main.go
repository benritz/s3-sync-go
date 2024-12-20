package main

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type digestAlgorithmFlag []string

func (flag *digestAlgorithmFlag) String() string {
	return fmt.Sprintf("%v", *flag)
}

func (flag *digestAlgorithmFlag) Set(value string) error {
	*flag = append(*flag, value)
	return nil
}

type localPath struct {
	path string
	base string
	stat os.FileInfo
}

func parseLocal(src string) (*localPath, error) {
	if strings.HasPrefix(src, "s3://") {
		return nil, fmt.Errorf("source must be a local path: %s", src)
	}

	src = filepath.ToSlash(src)

	if !filepath.IsAbs(src) {
		wd, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("failed to get current working directory: %v", err)
		}
		src = filepath.Join(wd, src)
	}

	stat, err := os.Stat(src)
	if err != nil {
		return nil, fmt.Errorf("source does not exist: %v", err)
	}

	var base string
	if stat.IsDir() {
		base = src
	} else {
		base = filepath.Dir(src)
	}
	base = base + "/"

	return &localPath{path: src, base: base, stat: stat}, nil
}

type s3Path struct {
	bucket string
	prefix string
}

func parseS3Path(path string) (*s3Path, error) {
	if !strings.HasPrefix(path, "s3://") {
		return nil, errors.New("not a s3 path")
	}

	path = strings.TrimPrefix(path, "s3://")
	parts := strings.SplitN(path, "/", 2)

	bucket := parts[0]

	var prefix string

	if len(parts) > 1 {
		prefix = parts[1]
		prefix = strings.TrimSuffix(prefix, "/")
	} else {
		prefix = ""
	}

	return &s3Path{bucket: bucket, prefix: prefix}, nil
}

type DigestAlgorithm int

const (
	SHA1 DigestAlgorithm = iota
	SHA256
	SHA512
	CRC32
	CRC32C
	MD5
)

func (algorithm DigestAlgorithm) String() string {
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

func parseDigestAlgorithm(algorithm string) (DigestAlgorithm, error) {
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

func getHasher(algorithm DigestAlgorithm) (hash.Hash, error) {
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

func getHash(algorithm DigestAlgorithm, reader io.Reader) (string, error) {
	hasher, err := getHasher(algorithm)
	if err != nil {
		return "", err
	}

	if _, err := io.Copy(hasher, reader); err != nil {
		return "", err
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

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
	MissingAlgorithms []DigestAlgorithm
	Metadata          map[string]string
}

func generateDigests(
	algorithms []DigestAlgorithm,
	reader io.ReadSeeker,
	metadata map[string]string,
) error {
	for _, algorithm := range algorithms {
		hash, err := getHash(algorithm, reader)
		if err != nil {
			return fmt.Errorf("failed to compute hash: %v", err)
		}
		metadata[algorithm.String()] = hash

		// reuse reader
		reader.Seek(0, io.SeekStart)
	}

	return nil
}

type Syncer struct {
	Algorithms []DigestAlgorithm
	SizeOnly   bool
	DryRun     bool

	s3Client *s3.Client
	uploader *manager.Uploader
}

func NewSyncer(
	ctx context.Context,
	profile string,
	algorithms []DigestAlgorithm,
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
	}, nil
}

func (s *Syncer) Sync(ctx context.Context, srcPath localPath, dstPath s3Path) error {
	var syncCheck SyncCheck

	if s.SizeOnly {
		syncCheck = SizeOnly
	} else {
		syncCheck = ModifiedAndSize
	}

	if srcPath.stat.IsDir() {
		// check for any objects under destination
		// we don't need to perform any sync checks if the destination is empty
		maxKeys := int32(1)
		ret, err := s.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(dstPath.bucket),
			Prefix:  aws.String(dstPath.prefix + "/"),
			MaxKeys: &maxKeys,
		})
		if err != nil {
			log.Fatalf("failed to list destination objects: %v", err)
		}

		if *ret.KeyCount == 0 {
			syncCheck = None
		}
	}

	err := filepath.WalkDir(srcPath.path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// ignore?
			return nil
		}

		if d.IsDir() {
			// ignore?
			// create directory marker in S3?
			return nil
		}

		rel := strings.TrimPrefix(path, srcPath.base)
		key := dstPath.prefix + "/" + rel

		ret, err := s.syncToS3(
			ctx,
			dstPath.bucket,
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
		var notFound *s3types.NotFound
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
		missingAlgorithms := make([]DigestAlgorithm, 0)
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

	reader, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer reader.Close()

	if ret.Type == MetadataOnly {
		metadata := ret.Metadata
		if metadata == nil {
			metadata = make(map[string]string)
		}

		generateDigests(s.Algorithms, reader, metadata)

		if !s.DryRun {
			_, err = s.s3Client.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:            aws.String(bucket),
				Key:               aws.String(key),
				CopySource:        aws.String(bucket + "/" + key),
				Metadata:          metadata,
				MetadataDirective: s3types.MetadataDirectiveReplace,
			})

			if err != nil {
				return nil, fmt.Errorf("failed to update metadata: %v", err)
			}
		}
	} else {
		metadata := make(map[string]string)

		generateDigests(s.Algorithms, reader, metadata)

		if !s.DryRun {
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
				return nil, fmt.Errorf("failed to upload: %v", err)
			}
		}
	}

	return ret, nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	var digestAlgorithmFlags digestAlgorithmFlag

	profile := flag.String("profile", "default", "the AWS profile to use")
	sizeOnly := flag.Bool("sizeOnly", false, "only check file size")
	dryRun := flag.Bool("dryRun", false, "dry run")
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

	var algorithms []DigestAlgorithm
	for _, algorithmName := range digestAlgorithmFlags {
		algorithm, err := parseDigestAlgorithm(algorithmName)
		if err != nil {
			log.Fatalf("%v", err)
		}
		algorithms = append(algorithms, algorithm)
	}

	src, dst := args[0], args[1]

	srcPath, err := parseLocal(src)
	if err != nil {
		log.Fatalf("failed to parse source: %v", err)
	}

	dstPath, err := parseS3Path(dst)

	if err != nil {
		log.Fatalf("failed to parse destination: %v", err)
	}

	ctx := context.Background()

	log.Printf("src: %v", srcPath)
	log.Printf("dst: %v", dstPath)
	log.Printf("algorithms: %v", algorithms)
	log.Printf("size only: %v", *sizeOnly)
	log.Printf("dry run: %v", *dryRun)

	syncer, err := NewSyncer(ctx, *profile, algorithms, *sizeOnly, *dryRun)

	if err != nil {
		log.Fatalf("failed to create syncer: %v", err)
	}

	syncer.Sync(ctx, *srcPath, *dstPath)

	if err != nil {
		log.Fatalf("sync failed: %v", err)
	}

	log.Printf("sync complete")
}
