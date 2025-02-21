package paths

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type S3 struct {
	Bucket   string
	Key      string
	Location string
}

func (p *S3) AppendRel(rel string) *S3 {
	return &S3{
		Bucket:   p.Bucket,
		Key:      filepath.Join(p.Key, rel),
		Location: p.Location,
	}
}

type PathInfo struct {
	Size    int64
	ModTime time.Time
}

func FromFileInfo(fi os.FileInfo) *PathInfo {
	return &PathInfo{
		Size:    fi.Size(),
		ModTime: fi.ModTime(),
	}
}

type Path struct {
	Path string
	*S3
	*PathInfo
}

func Parse(path string) (*Path, error) {
	if strings.HasPrefix(path, "s3://") {
		return parseS3(path)
	}

	return parseLocal(path)
}

func (p *Path) IsSame(other *Path) bool {
	return p.Path == other.Path
}

func (p *Path) GetRel(path string) string {
	rel := strings.TrimPrefix(path, p.Path)

	if rel == "" {
		return filepath.Base(path)
	}

	if rel != path {
		return rel[1:]
	}

	return path
}

func join(p1, p2 string) string {
	return fmt.Sprintf("%s/%s", strings.TrimSuffix(p1, "/"), strings.TrimPrefix(p2, "/"))
}

func (p *Path) AppendRel(rel string) *Path {
	path := &Path{Path: join(p.Path, rel)}

	if p.S3 != nil {
		path.S3 = p.S3.AppendRel(rel)
	}

	return path
}

func parseLocal(path string) (*Path, error) {
	path = filepath.ToSlash(path)

	if !filepath.IsAbs(path) {
		wd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		path = filepath.Join(wd, path)
	}

	return &Path{Path: path}, nil
}

func parseS3(path string) (*Path, error) {
	path = strings.TrimPrefix(path, "s3://")
	parts := strings.SplitN(path, "/", 2)

	bucket := parts[0]

	var key string

	if len(parts) > 1 {
		key = parts[1]
		key = strings.TrimSuffix(key, "/")
	} else {
		key = ""
	}

	return &Path{
		Path: fmt.Sprintf("s3://%s/%s", bucket, key),
		S3: &S3{
			Bucket: bucket,
			Key:    key,
		},
	}, nil
}

func IsHidden(path string) bool {
	hidden, err := isHidden(path)
	if err != nil {
		slog.Debug("failed to check hidden file", "path", path, "error", err)
	}
	return hidden
}
