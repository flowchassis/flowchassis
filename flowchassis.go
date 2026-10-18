// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

// Package flowchassis is a small workflow execution kernel. It runs workflows
// made of named steps inside your own program, writes a checkpoint to a local
// journal file before and after every step, retries failed steps by a policy,
// undoes completed steps in reverse order when a run can't finish
// (compensation), and picks up every unfinished run where it stopped after a
// crash or restart.
//
// It needs no server and no database: the journal is one file next to your
// program. Steps run at least once. A step interrupted by a crash runs again
// after the restart, so a step with outside effects should pass Ctx.Key to the
// service it calls, or check for itself whether its work is already done.
package flowchassis

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Version is this release of FlowChassis. Raising it on main releases it: once
// the test workflow passes, the release workflow builds, checks, tags and
// publishes vX.Y.Z (see RELEASING.md).
const Version = "0.1.0"

// JournalFormat is the version of the journal file format this build writes
// and reads. It is stored in the file header.
const JournalFormat = 1

var (
	// ErrUnknownFlow is returned by Start for a flow that isn't registered.
	ErrUnknownFlow = errors.New("flowchassis: unknown flow")
	// ErrConflict is returned by Start when the run ID exists with another
	// flow or another input.
	ErrConflict = errors.New("flowchassis: run ID exists with a different flow or input")
	// ErrNotFound is returned for a run ID the journal doesn't hold.
	ErrNotFound = errors.New("flowchassis: run not found")
	// ErrFinished is returned by Cancel for a run that has already ended.
	ErrFinished = errors.New("flowchassis: run has already finished")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("flowchassis: engine is closed")
	// ErrLocked is returned by Open when another process has the journal open.
	ErrLocked = errors.New("flowchassis: journal is in use by another process")
	// ErrCorrupt is returned when the journal holds damage that isn't a torn
	// final write, so it can't be read safely.
	ErrCorrupt = errors.New("flowchassis: journal is corrupt")
	// ErrStopped is returned after a journal write failed. The engine stops
	// running steps rather than go on without a checkpoint. Close it and open
	// the journal again to resume from the last good checkpoint.
	ErrStopped = errors.New("flowchassis: engine stopped after a journal write failed")
)

// Status is where a run stands.
type Status string

const (
	// Pending: started, no step has begun yet (or its flow isn't registered
	// in this process yet).
	Pending Status = "pending"
	// Running: a step is under way or about to begin.
	Running Status = "running"
	// Retrying: a step failed and waits for its next attempt.
	Retrying Status = "retrying"
	// Compensating: the run can't finish and its completed steps are being
	// undone, newest first.
	Compensating Status = "compensating"
	// Completed: every step finished.
	Completed Status = "completed"
	// Compensated: the run failed or was canceled, and every completed step
	// with an Undo was undone.
	Compensated Status = "compensated"
	// Failed: an Undo kept failing, so the run stopped part way through
	// compensation and needs a person to look at it.
	Failed Status = "failed"
)

// Finished reports whether the status is final.
func (s Status) Finished() bool {
	return s == Completed || s == Compensated || s == Failed
}

// Flow is a workflow: a name and an ordered list of steps. By default a run
// goes from each step to the one after it; a step can choose another with
// Ctx.Next. The run completes after its last step, or after a step calls
// Ctx.Next("").
type Flow struct {
	Name  string
	Steps []Step
}

// Step is one unit of work.
type Step struct {
	// Name identifies the step in the journal. Keep names stable between
	// releases of your program: unfinished runs resume by name.
	Name string
	// Do performs the step. Returning nil completes it, and its changes to
	// the run's state are saved with the checkpoint. Returning an error
	// discards its state changes and retries the step under Retry.
	Do func(c *Ctx) error
	// Undo reverses the step during compensation. It sees the state as it
	// was when the step completed. Nil means the step needs no undoing.
	Undo func(c *Ctx) error
	// Retry sets how often Do and Undo are attempted. The zero value uses
	// Options.Retry.
	Retry RetryPolicy
}

// RetryPolicy sets attempts and the wait between them. The wait before
// attempt n+1 is Backoff * Multiplier^(n-1), capped at MaxBackoff.
type RetryPolicy struct {
	// MaxAttempts is the number of attempts, the first included. Zero takes
	// the default; 1 means no retries.
	MaxAttempts int
	// Backoff is the wait after the first failed attempt.
	Backoff time.Duration
	// MaxBackoff caps the wait. Zero means no cap.
	MaxBackoff time.Duration
	// Multiplier grows the wait after each failed attempt. Zero means 2.
	Multiplier float64
}

// DefaultRetry is used where neither the step nor Options set a policy.
var DefaultRetry = RetryPolicy{MaxAttempts: 3, Backoff: 100 * time.Millisecond, MaxBackoff: 30 * time.Second, Multiplier: 2}

func (p RetryPolicy) isZero() bool { return p == RetryPolicy{} }

func (p RetryPolicy) or(q RetryPolicy) RetryPolicy {
	if p.isZero() {
		return q
	}
	return p
}

func (p RetryPolicy) attempts() int {
	if p.MaxAttempts <= 0 {
		return DefaultRetry.MaxAttempts
	}
	return p.MaxAttempts
}

// delay is the wait after the given failed attempt (1 for the first).
func (p RetryPolicy) delay(attempt int) time.Duration {
	m := p.Multiplier
	if m <= 0 {
		m = 2
	}
	d := float64(p.Backoff)
	for i := 1; i < attempt; i++ {
		d *= m
		if p.MaxBackoff > 0 && d >= float64(p.MaxBackoff) {
			return p.MaxBackoff
		}
		if d > float64(365*24*time.Hour) {
			d = float64(365 * 24 * time.Hour)
			break
		}
	}
	if p.MaxBackoff > 0 && time.Duration(d) > p.MaxBackoff {
		return p.MaxBackoff
	}
	return time.Duration(d)
}

// Permanent marks an error as one retrying won't fix. A step that returns it
// fails at once, without further attempts.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// Ctx is what a step's Do or Undo gets: the context, which run and attempt
// this is, the run's input and its state.
type Ctx struct {
	context.Context
	// Run is the run ID.
	Run string
	// Flow is the flow's name.
	Flow string
	// Step is the step's name.
	Step string
	// Attempt counts attempts of this step in this run, from 1, across
	// restarts.
	Attempt int
	// Seq numbers the step's completions within the run, from 1. A step that
	// a run passes through twice (with Next) gets two numbers.
	Seq int
	// Key is a stable idempotency key for this step in this run: the same on
	// every attempt and after a restart. Pass it to outside services so that
	// a repeated call has no second effect. Undo gets its own key.
	Key string
	// Undoing is true inside Undo.
	Undoing bool

	input []byte
	state map[string][]byte
	next  *string
}

// Input returns the run's input, as given to Start.
func (c *Ctx) Input() []byte { return append([]byte(nil), c.input...) }

// Get returns a value from the run's state, and whether it is set.
func (c *Ctx) Get(key string) ([]byte, bool) {
	v, ok := c.state[key]
	return append([]byte(nil), v...), ok
}

// Set stores a value in the run's state. It is saved only if the step
// completes. In Undo it has no effect.
func (c *Ctx) Set(key string, value []byte) {
	if c.Undoing {
		return
	}
	c.state[key] = append([]byte(nil), value...)
}

// Delete removes a value from the run's state. It is saved only if the step
// completes. In Undo it has no effect.
func (c *Ctx) Delete(key string) {
	if c.Undoing {
		return
	}
	delete(c.state, key)
}

// Next chooses the step to run after this one completes. An empty name ends
// the run as completed. Without a call the run goes on to the following step.
func (c *Ctx) Next(step string) {
	if c.Undoing {
		return
	}
	c.next = &step
}

// RunInfo describes a run.
type RunInfo struct {
	ID      string            `json:"id"`
	Flow    string            `json:"flow"`
	Status  Status            `json:"status"`
	Step    string            `json:"step,omitempty"`    // the step under way or next, or being undone
	Attempt int               `json:"attempt,omitempty"` // attempts of that step so far
	Done    []string          `json:"done,omitempty"`    // completed steps, oldest first
	Undone  []string          `json:"undone,omitempty"`  // undone steps, in the order undone
	State   map[string][]byte `json:"state,omitempty"`
	Input   []byte            `json:"input,omitempty"`
	Error   string            `json:"error,omitempty"` // why it is retrying, compensating or failed
	Started time.Time         `json:"started"`
	Ended   time.Time         `json:"ended,omitempty"`
	RetryAt time.Time         `json:"retry_at,omitempty"`
}

func (i RunInfo) String() string {
	return fmt.Sprintf("%s %s %s", i.ID, i.Flow, i.Status)
}
