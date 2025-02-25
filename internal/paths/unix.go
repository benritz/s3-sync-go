//go:build !windows

package paths

func isHiddenLocal(path string) (bool, error) {
	return isHiddenFileName(path), nil
}
