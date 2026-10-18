// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

//go:build !windows && (!unix || solaris || aix)

package flowchassis

import "os"

// On systems where this build has no file locking (Solaris, AIX, Plan 9,
// WebAssembly and others), Open doesn't guard the journal against a second
// process. Don't open one journal twice there.
type fileLock struct{}

func lockFile(string) (*fileLock, error) { return &fileLock{}, nil }

func (l *fileLock) unlock() {}

func syncDir(string) {}

func openShared(path string) (*os.File, error) { return os.Open(path) }
