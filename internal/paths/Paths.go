package paths

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type LocalPath struct {
	Path string
	Base string
	Stat os.FileInfo
}

func ParseLocal(src string) (*LocalPath, error) {
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

	return &LocalPath{Path: src, Base: base, Stat: stat}, nil
}

type S3Path struct {
	Bucket string
	Prefix string
}

func ParseS3Path(path string) (*S3Path, error) {
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

	return &S3Path{Bucket: bucket, Prefix: prefix}, nil
}
