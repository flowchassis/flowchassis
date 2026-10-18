// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package flowchassis

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = kernel32.NewProc("LockFileEx")
	procUnlockFileEx = kernel32.NewProc("UnlockFileEx")
)

const (
	lockfileFailImmediately = 0x00000001
	lockfileExclusiveLock   = 0x00000002
	errorLockViolation      = syscall.Errno(33)
)

type fileLock struct{ f *os.File }

// lockFile takes an exclusive lock on path, creating the file if needed.
// Windows drops the lock when the process ends, however it ends.
func lockFile(path string) (*fileLock, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	var ol syscall.Overlapped
	r, _, e := procLockFileEx.Call(f.Fd(), lockfileExclusiveLock|lockfileFailImmediately, 0, 1, 0, uintptr(unsafe.Pointer(&ol)))
	if r == 0 {
		f.Close()
		if e == errorLockViolation {
			return nil, ErrLocked
		}
		return nil, e
	}
	return &fileLock{f: f}, nil
}

func (l *fileLock) unlock() {
	var ol syscall.Overlapped
	procUnlockFileEx.Call(l.f.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(&ol)))
	l.f.Close()
}

// syncDir does nothing on Windows, where directories can't be flushed this
// way and a rename is made durable by the file system.
func syncDir(string) {}

// openShared opens a file for reading and lets others delete or rename it
// meanwhile, so tools reading the journal don't block compaction.
func openShared(path string) (*os.File, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	h, err := syscall.CreateFile(p, syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(h), path), nil
}
