// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package flowchassis

import (
	"errors"
	"os"
	"syscall"
)

type fileLock struct{ f *os.File }

// lockFile takes an exclusive lock on path, creating the file if needed. The
// operating system drops the lock when the process ends, however it ends.
func lockFile(path string) (*fileLock, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrLocked
		}
		return nil, err
	}
	return &fileLock{f: f}, nil
}

func (l *fileLock) unlock() {
	syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	l.f.Close()
}

// syncDir flushes a directory, so a file created or renamed in it survives a
// power cut.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}
