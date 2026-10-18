// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

//go:build !unix && !windows

package flowchassis

// On systems with no file locking available here, Open doesn't guard the
// journal against a second process. Don't open one journal twice.
type fileLock struct{}

func lockFile(string) (*fileLock, error) { return &fileLock{}, nil }

func (l *fileLock) unlock() {}

func syncDir(string) {}
