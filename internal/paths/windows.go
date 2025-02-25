//go:build windows

package paths

import (
	"golang.org/x/sys/windows"
)

func isHiddenLocal(path string) (bool, error) {
	pointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return false, err
	}
	attributes, err := windows.GetFileAttributes(pointer)
	if err != nil {
		return false, err
	}

	// Check if the file is hidden
	return attributes&windows.FILE_ATTRIBUTE_HIDDEN != 0, nil
}
