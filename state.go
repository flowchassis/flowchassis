// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

package flowchassis

import (
	"context"
	"fmt"
	"time"
)

// completion is one completed step of a run, with the state it left.
type completion struct {
	Step  string            `json:"step"`
	Seq   int               `json:"seq"`
	State map[string][]byte `json:"state,omitempty"`
}

// snapshot is a whole run, as compaction writes it.
type snapshot struct {
	Flow         string            `json:"flow"`
	Input        []byte            `json:"input,omitempty"`
	Started      time.Time         `json:"started"`
	Ended        time.Time         `json:"ended,omitempty"`
	Status       Status            `json:"status"`
	State        map[string][]byte `json:"state,omitempty"`
	Cursor       string            `json:"cursor,omitempty"`
	Attempt      int               `json:"attempt,omitempty"`
	RetryAt      time.Time         `json:"retry_at,omitempty"`
	Done         []completion      `json:"done,omitempty"`
	Undone       []string          `json:"undone,omitempty"`
	Compensating bool              `json:"compensating,omitempty"`
	UndoNext     int               `json:"undo_next,omitempty"`
	UAttempt     int               `json:"uattempt,omitempty"`
	Cancel       bool              `json:"cancel,omitempty"`
	Error        string            `json:"err,omitempty"`
}

// run is a run as the journal describes it so far, plus what the engine
// needs while running it. Only apply changes the journal part.
type run struct {
	id string
	snapshot

	// Engine bookkeeping, not in the journal.
	queued   bool
	active   bool
	timer    *time.Timer
	cancelFn context.CancelFunc
	finished chan struct{}
}

func newRun(id string) *run {
	return &run{id: id, finished: make(chan struct{}), snapshot: snapshot{UndoNext: -1}}
}

// apply moves a run on by one journal record. It is the only place run
// state changes, both when the journal is read back and while running, so
// the two can't drift apart. It returns the run, which is new for a start or
// snapshot record.
func apply(runs map[string]*run, rec *Record) (*run, error) {
	r := runs[rec.Run]
	switch rec.Type {
	case recStart:
		if r != nil {
			return nil, fmt.Errorf("%w: run %q started twice", ErrCorrupt, rec.Run)
		}
		r = newRun(rec.Run)
		r.Flow, r.Input, r.Started, r.Status = rec.Flow, rec.Input, rec.At, Pending
		r.State = map[string][]byte{}
		if rec.Next != nil {
			r.Cursor = *rec.Next
		}
		runs[rec.Run] = r
		return r, nil
	case recSnapshot:
		if rec.Snap == nil {
			return nil, fmt.Errorf("%w: empty snapshot of run %q", ErrCorrupt, rec.Run)
		}
		old := r
		r = newRun(rec.Run)
		if old != nil {
			r.finished = old.finished
		}
		r.snapshot = *rec.Snap
		if r.State == nil {
			r.State = map[string][]byte{}
		}
		runs[rec.Run] = r
		return r, nil
	}
	if r == nil {
		return nil, fmt.Errorf("%w: %s record for run %q, which never started", ErrCorrupt, rec.Type, rec.Run)
	}
	switch rec.Type {
	case recBegin:
		r.Status = Running
		r.Cursor = rec.Step
		r.Attempt = rec.Attempt
		r.RetryAt = time.Time{}
	case recDone:
		state := rec.State
		if state == nil {
			state = map[string][]byte{}
		}
		r.Done = append(r.Done, completion{Step: rec.Step, Seq: rec.Seq, State: state})
		r.State = state
		r.Cursor = ""
		if rec.Next != nil {
			r.Cursor = *rec.Next
		}
		r.Attempt = 0
		r.Error = ""
		r.RetryAt = time.Time{}
		r.Status = Running
	case recFail:
		r.Attempt = rec.Attempt
		r.Error = rec.Error
		if !rec.Final && rec.RetryAt != nil {
			r.RetryAt = *rec.RetryAt
			r.Status = Retrying
		}
	case recCancel:
		r.Cancel = true
	case recComp:
		r.Compensating = true
		r.Status = Compensating
		r.Error = rec.Error
		r.UndoNext = len(r.Done) - 1
		r.UAttempt = 0
		r.RetryAt = time.Time{}
	case recUBegin:
		r.Status = Compensating
		if i := r.indexOf(rec.Seq); i >= 0 {
			// Steps above it had no Undo and were passed over.
			r.UndoNext = i
		}
		r.UAttempt = rec.Attempt
		r.RetryAt = time.Time{}
	case recUDone:
		i := r.indexOf(rec.Seq)
		if i < 0 {
			return nil, fmt.Errorf("%w: run %q undid step %d, which it never completed", ErrCorrupt, rec.Run, rec.Seq)
		}
		r.Undone = append(r.Undone, r.Done[i].Step)
		r.UndoNext = i - 1
		r.UAttempt = 0
		r.RetryAt = time.Time{}
	case recUFail:
		r.UAttempt = rec.Attempt
		if !rec.Final && rec.RetryAt != nil {
			r.RetryAt = *rec.RetryAt
		}
		if rec.Final {
			r.Error = rec.Error
		}
	case recEnd:
		r.Status = rec.Status
		r.Ended = rec.At
		if rec.Error != "" {
			r.Error = rec.Error
		}
		r.RetryAt = time.Time{}
	default:
		return nil, fmt.Errorf("%w: unknown record type %q", ErrCorrupt, rec.Type)
	}
	return r, nil
}

func (r *run) indexOf(seq int) int {
	for i := len(r.Done) - 1; i >= 0; i-- {
		if r.Done[i].Seq == seq {
			return i
		}
	}
	return -1
}

func (r *run) info() RunInfo {
	i := RunInfo{
		ID: r.id, Flow: r.Flow, Status: r.Status, Error: r.Error,
		Started: r.Started, Ended: r.Ended, RetryAt: r.RetryAt,
		Input: clone(r.Input), State: cloneState(r.State),
		Undone: append([]string(nil), r.Undone...),
	}
	for _, c := range r.Done {
		i.Done = append(i.Done, c.Step)
	}
	switch {
	case r.Status.Finished():
	case r.Compensating:
		if r.UndoNext >= 0 && r.UndoNext < len(r.Done) {
			i.Step = r.Done[r.UndoNext].Step
		}
		i.Attempt = r.UAttempt
	default:
		i.Step = r.Cursor
		i.Attempt = r.Attempt
	}
	return i
}

func (r *run) snap() *snapshot {
	s := r.snapshot
	return &s
}

// Replay rebuilds the runs a list of journal records describes, in the order
// they started. It is how tools read a journal without an engine.
func Replay(recs []Record) ([]RunInfo, error) {
	runs := map[string]*run{}
	var order []string
	for i := range recs {
		_, existed := runs[recs[i].Run]
		if _, err := apply(runs, &recs[i]); err != nil {
			return nil, err
		}
		if !existed {
			order = append(order, recs[i].Run)
		}
	}
	out := make([]RunInfo, 0, len(order))
	for _, id := range order {
		out = append(out, runs[id].info())
	}
	return out, nil
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

func cloneState(s map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(s))
	for k, v := range s {
		out[k] = clone(v)
	}
	return out
}
