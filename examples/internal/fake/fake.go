// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

// Package fake stands in for the outside services the examples call: a user
// directory, a storage provider, a mail provider, a cloud, a CA. Each one
// keeps a file of what it did, one line per effect, and honours idempotency
// keys the way a real provider's API does: a call with a key it has seen
// returns the first result and does nothing again.
package fake

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/flowchassis/flowchassis"
)

// Service is one fake outside service.
type Service struct {
	Name string
	// Fail, when set, is asked before every call; an error it returns is
	// returned instead of doing the work, the way a timeout or a refusal
	// would be.
	Fail func(c *flowchassis.Ctx, what string) error

	mu   sync.Mutex
	f    *os.File
	done map[string]string // idempotency key -> result
	n    int               // things created so far, for new IDs
	log  io.Writer
}

// Open opens the service's file in dir, reading back what it did before.
func Open(dir, name string, log io.Writer) (*Service, error) {
	path := filepath.Join(dir, name+".log")
	s := &Service{Name: name, done: map[string]string{}, log: log}
	if b, err := os.ReadFile(path); err == nil {
		sc := bufio.NewScanner(strings.NewReader(string(b)))
		for sc.Scan() {
			// key result what...
			parts := strings.SplitN(sc.Text(), " ", 3)
			if len(parts) == 3 {
				s.done[parts[0]] = parts[1]
				if parts[1] != "done" {
					s.n++
				}
			}
		}
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	s.f = f
	return s, nil
}

// Create does one piece of work that makes something new, under the step's
// idempotency key, and returns the new thing's ID. The effect is on disk
// before Create returns.
func (s *Service) Create(c *flowchassis.Ctx, what string) (string, error) {
	return s.call(c, what, true)
}

// Do does one piece of work under the step's idempotency key, such as a
// delete or a send. The effect is on disk before Do returns.
func (s *Service) Do(c *flowchassis.Ctx, what string) error {
	_, err := s.call(c, what, false)
	return err
}

func (s *Service) call(c *flowchassis.Ctx, what string, create bool) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.done[c.Key]; ok {
		s.printf("  %-10s %s -> %s (key seen before, nothing done again)\n", s.Name, what, id)
		return id, nil
	}
	if s.Fail != nil {
		if err := s.Fail(c, what); err != nil {
			s.printf("  %-10s %s -> error: %v\n", s.Name, what, err)
			return "", err
		}
	}
	id := "done"
	if create {
		s.n++
		id = fmt.Sprintf("%s-%d", s.Name, s.n)
	}
	if _, err := fmt.Fprintf(s.f, "%s %s %s\n", c.Key, id, what); err != nil {
		return "", err
	}
	if err := s.f.Sync(); err != nil {
		return "", err
	}
	s.done[c.Key] = id
	s.printf("  %-10s %s -> %s\n", s.Name, what, id)
	return id, nil
}

func (s *Service) printf(format string, args ...any) {
	if s.log != nil {
		fmt.Fprintf(s.log, format, args...)
	}
}

// Close closes the service's file.
func (s *Service) Close() error { return s.f.Close() }

// Effects reads a service's file in dir: one "key result what" line per
// effect, in the order they happened.
func Effects(dir, name string) []string {
	b, err := os.ReadFile(filepath.Join(dir, name+".log"))
	if err != nil {
		return nil
	}
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

// Dir returns dir, or a new temporary directory when dir is empty.
func Dir(dir string) (string, error) {
	if dir != "" {
		return dir, os.MkdirAll(dir, 0o755)
	}
	return os.MkdirTemp("", "flowchassis-example-")
}

// SyncWriter makes a writer safe for steps, services and servers that print
// from several goroutines at once.
func SyncWriter(w io.Writer) io.Writer { return &syncWriter{w: w} }

type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// Attempts counts the attempts a journal records for one step of one run:
// "begin" records, or "ubegin" for its undo. Tests use it to check that a
// fault really was retried, or really wasn't.
func Attempts(journal, run, step string, undo bool) (int, error) {
	recs, _, err := flowchassis.ReadJournal(journal)
	if err != nil {
		return 0, err
	}
	kind := "begin"
	if undo {
		kind = "ubegin"
	}
	n := 0
	for _, r := range recs {
		if r.Type == kind && r.Run == run && r.Step == step {
			n++
		}
	}
	return n, nil
}
