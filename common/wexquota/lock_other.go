//go:build !linux && !darwin

package wexquota

import "os"

func openLocked(dir string) (*os.File, error) { return nil, ErrDenied }
