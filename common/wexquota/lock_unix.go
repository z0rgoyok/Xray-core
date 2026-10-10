//go:build linux || darwin

package wexquota

import (
	"os"
	"path/filepath"
	"syscall"
)

func openLocked(dir string) (*os.File, error) {
	f, e := os.OpenFile(filepath.Join(dir, "ledger.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return nil, e
	}
	return f, nil
}
