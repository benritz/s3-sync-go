//go:build !windows

package paths

import (
	"path/filepath"
	"strings"
)

func isHidden(path string) (bool, error) {
	name := filepath.Base(path)
	return strings.HasPrefix(name, "."), nil
}
