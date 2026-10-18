// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

package flowchassis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// Options configure an engine. The zero value works.
type Options struct {
	// Workers is how many runs execute at once. Zero means 4.
	Workers int
	// Retry is the policy for steps that don't set their own. Zero means
	// DefaultRetry.
	Retry RetryPolicy
	// Now returns the current time. Tests set it; nil means time.Now.
	Now func() time.Time
}

type flowDef struct {
	Flow
	byName map[string]*Step
	nextOf map[string]string
}

// Engine runs flows and keeps their journal. Open one per journal file; it
// starts running unfinished runs as soon as their flow is registered.
type Engine struct {
	opts  Options
	store Store
	lock  *fileLock

	// Journal writes. Records queue up and one writer at a time appends
	// every queued record and flushes them together (group commit), then
	// applies them in the order they were written. jmu is held shared for
	// each commit and exclusively by Compact and Close, so they see no write
	// under way.
	jmu      sync.RWMutex
	qmu      sync.Mutex
	queue    []*pending
	flushing bool
	smu      sync.Mutex // one Cancel at a time

	mu       sync.Mutex
	cond     *sync.Cond
	flows    map[string]*flowDef
	runs     map[string]*run
	starting map[string]chan struct{} // run IDs whose start record is being written
	order    []string
	ready    []*run
	closing  bool // no new work: Close was called or a write failed
	closed   bool // Close was called
	stopErr  error

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Open opens the journal at path, creating it if needed, takes its lock so
// no other process can open it, and returns an engine. Register your flows
// next: unfinished runs resume as soon as their flow is registered.
func Open(path string, opts Options) (*Engine, error) {
	lk, err := lockFile(path + ".lock")
	if err != nil {
		return nil, err
	}
	e, err := New(NewFileStore(path), opts)
	if err != nil {
		lk.unlock()
		return nil, err
	}
	e.lock = lk
	return e, nil
}

// New returns an engine over any Store. Open is the usual way in.
func New(store Store, opts Options) (*Engine, error) {
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.Retry.isZero() {
		opts.Retry = DefaultRetry
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	data, err := store.Load()
	if err != nil {
		store.Close()
		return nil, err
	}
	recs, _, torn, err := decodeAll(data)
	if err == nil && torn {
		err = fmt.Errorf("%w: the store returned a torn record", ErrCorrupt)
	}
	if err != nil {
		store.Close()
		return nil, err
	}
	e := &Engine{opts: opts, store: store, flows: map[string]*flowDef{}, runs: map[string]*run{}, starting: map[string]chan struct{}{}}
	e.cond = sync.NewCond(&e.mu)
	for i := range recs {
		_, existed := e.runs[recs[i].Run]
		r, err := apply(e.runs, &recs[i])
		if err != nil {
			store.Close()
			return nil, err
		}
		if !existed {
			e.order = append(e.order, r.id)
		}
	}
	for _, r := range e.runs {
		if r.Status.Finished() {
			close(r.finished)
		}
	}
	e.ctx, e.cancel = context.WithCancel(context.Background())
	for i := 0; i < opts.Workers; i++ {
		e.wg.Add(1)
		go e.worker()
	}
	return e, nil
}

// Register adds a flow. Runs of it already in the journal start or resume.
func (e *Engine) Register(f Flow) error {
	if f.Name == "" {
		return errors.New("flowchassis: a flow needs a name")
	}
	if len(f.Steps) == 0 {
		return fmt.Errorf("flowchassis: flow %q has no steps", f.Name)
	}
	d := &flowDef{Flow: f, byName: map[string]*Step{}, nextOf: map[string]string{}}
	for i := range f.Steps {
		s := &d.Steps[i]
		if s.Name == "" {
			return fmt.Errorf("flowchassis: step %d of flow %q has no name", i+1, f.Name)
		}
		if s.Do == nil {
			return fmt.Errorf("flowchassis: step %q of flow %q has no Do", s.Name, f.Name)
		}
		if _, dup := d.byName[s.Name]; dup {
			return fmt.Errorf("flowchassis: flow %q has two steps named %q", f.Name, s.Name)
		}
		d.byName[s.Name] = s
		if i+1 < len(f.Steps) {
			d.nextOf[s.Name] = f.Steps[i+1].Name
		} else {
			d.nextOf[s.Name] = ""
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closing {
		return e.closedErr()
	}
	if _, dup := e.flows[f.Name]; dup {
		return fmt.Errorf("flowchassis: flow %q is registered already", f.Name)
	}
	e.flows[f.Name] = d
	for _, id := range e.order {
		if r := e.runs[id]; r.Flow == f.Name {
			e.schedule(r)
		}
	}
	return nil
}

// Start begins a run of a flow with the given input. An empty id gets a
// random one. Start is idempotent: starting an id that exists, with the same
// flow and input, returns that run's id without starting another, so it is
// safe to call again after a crash. The same id with another flow or input
// is ErrConflict.
func (e *Engine) Start(flow, id string, input []byte) (string, error) {
	if id == "" {
		var b [12]byte
		if _, err := rand.Read(b[:]); err != nil {
			return "", err
		}
		id = hex.EncodeToString(b[:])
	}
	for {
		e.mu.Lock()
		if e.closing {
			e.mu.Unlock()
			return "", e.closedErr()
		}
		d := e.flows[flow]
		if d == nil {
			e.mu.Unlock()
			return "", fmt.Errorf("%w: %q", ErrUnknownFlow, flow)
		}
		if r := e.runs[id]; r != nil {
			same := r.Flow == flow && string(r.Input) == string(input)
			e.mu.Unlock()
			if !same {
				return "", fmt.Errorf("%w: %q", ErrConflict, id)
			}
			return id, nil
		}
		if wait, ok := e.starting[id]; ok {
			// Another Start of this id is being written: wait for it, then
			// compare with what it wrote.
			e.mu.Unlock()
			<-wait
			continue
		}
		wait := make(chan struct{})
		e.starting[id] = wait
		first := d.Steps[0].Name
		e.mu.Unlock()
		err := e.commit(&Record{Type: recStart, Run: id, Flow: flow, Input: clone(input), Next: &first})
		e.mu.Lock()
		delete(e.starting, id)
		close(wait)
		e.mu.Unlock()
		if err != nil {
			return "", err
		}
		return id, nil
	}
}

// Get returns a run.
func (e *Engine) Get(id string) (RunInfo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r := e.runs[id]
	if r == nil {
		return RunInfo{}, fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	return r.info(), nil
}

// List returns every run in the journal, in the order they started.
func (e *Engine) List() []RunInfo {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]RunInfo, 0, len(e.order))
	for _, id := range e.order {
		out = append(out, e.runs[id].info())
	}
	return out
}

// Wait blocks until the run finishes or ctx is done, and returns the run.
func (e *Engine) Wait(ctx context.Context, id string) (RunInfo, error) {
	e.mu.Lock()
	r := e.runs[id]
	if r == nil {
		e.mu.Unlock()
		return RunInfo{}, fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	ch := r.finished
	e.mu.Unlock()
	select {
	case <-ch:
		return e.Get(id)
	case <-ctx.Done():
		info, _ := e.Get(id)
		return info, ctx.Err()
	case <-e.ctx.Done():
		info, _ := e.Get(id)
		return info, e.closedErr()
	}
}

// Cancel asks a run to stop and undo its completed steps. A step under way
// sees its context canceled; if it still completes, it is undone too.
// Canceling a run that is compensating already changes nothing.
func (e *Engine) Cancel(id string) error {
	e.smu.Lock()
	defer e.smu.Unlock()
	e.mu.Lock()
	r := e.runs[id]
	switch {
	case e.closing:
		e.mu.Unlock()
		return e.closedErr()
	case r == nil:
		e.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrNotFound, id)
	case r.Status.Finished():
		e.mu.Unlock()
		return fmt.Errorf("%w: %q is %s", ErrFinished, id, r.Status)
	case r.Cancel || r.Compensating:
		e.mu.Unlock()
		return nil
	}
	e.mu.Unlock()
	if err := e.commit(&Record{Type: recCancel, Run: id}); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if r.cancelFn != nil {
		r.cancelFn()
	}
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
	e.schedule(r)
	return nil
}

// Compact rewrites the journal with one record per run, and leaves out runs
// that finished before the given time (a zero time keeps them all). It runs
// while the engine works; appends wait for it.
func (e *Engine) Compact(dropFinishedBefore time.Time) error {
	e.jmu.Lock()
	defer e.jmu.Unlock()
	e.mu.Lock()
	if e.closing {
		e.mu.Unlock()
		return e.closedErr()
	}
	var frames []byte
	var keep, drop []string
	for _, id := range e.order {
		r := e.runs[id]
		if r.Status.Finished() && !dropFinishedBefore.IsZero() && r.Ended.Before(dropFinishedBefore) {
			drop = append(drop, id)
			continue
		}
		f, err := encode(&Record{Type: recSnapshot, Run: id, At: e.opts.Now(), Snap: r.snap()})
		if err != nil {
			e.mu.Unlock()
			return err
		}
		frames = append(frames, f...)
		keep = append(keep, id)
	}
	e.mu.Unlock()
	if err := e.store.Replace(frames); err != nil {
		return err
	}
	e.mu.Lock()
	for _, id := range drop {
		delete(e.runs, id)
	}
	e.order = keep
	e.mu.Unlock()
	return nil
}

// Close stops the engine. Steps under way see their context canceled; a step
// that returns an error because of it is not counted as failed, and runs
// again when the journal is next opened. Close waits for them to return.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.closing = true
	e.cancel()
	e.cond.Broadcast()
	e.mu.Unlock()
	e.wg.Wait()
	e.mu.Lock()
	for _, r := range e.runs {
		if r.timer != nil {
			r.timer.Stop()
			r.timer = nil
		}
	}
	e.mu.Unlock()
	e.jmu.Lock()
	err := e.store.Close()
	e.jmu.Unlock()
	if e.lock != nil {
		e.lock.unlock()
		e.lock = nil
	}
	return err
}

// Err returns why the engine stopped, if a journal write failed.
func (e *Engine) Err() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stopErr
}

func (e *Engine) closedErr() error {
	if e.stopErr != nil {
		return e.stopErr
	}
	return ErrClosed
}

// pending is a record waiting to be written.
type pending struct {
	rec   *Record
	frame []byte
	done  chan error
}

// commit writes a record to the journal, flushed to disk, and then applies
// it to the run in memory. Records committed at the same time share one
// flush. It returns once the record is durable and applied.
func (e *Engine) commit(rec *Record) error {
	e.jmu.RLock()
	defer e.jmu.RUnlock()
	e.mu.Lock()
	if e.closed || e.stopErr != nil {
		err := e.closedErr()
		e.mu.Unlock()
		return err
	}
	e.mu.Unlock()
	rec.At = e.opts.Now().UTC()
	frame, err := encode(rec)
	if err != nil {
		return err
	}
	p := &pending{rec: rec, frame: frame, done: make(chan error, 1)}
	e.qmu.Lock()
	e.queue = append(e.queue, p)
	if e.flushing {
		e.qmu.Unlock()
		return <-p.done
	}
	e.flushing = true
	for {
		batch := e.queue
		e.queue = nil
		e.qmu.Unlock()
		e.flush(batch)
		e.qmu.Lock()
		if len(e.queue) == 0 {
			e.flushing = false
			e.qmu.Unlock()
			break
		}
	}
	return <-p.done
}

// flush appends a batch of records with one write and one flush, applies
// them in order, and tells each committer.
func (e *Engine) flush(batch []*pending) {
	var frames []byte
	if len(batch) == 1 {
		frames = batch[0].frame
	} else {
		for _, p := range batch {
			frames = append(frames, p.frame...)
		}
	}
	if err := e.store.Append(frames); err != nil {
		e.stop(err)
		err = e.Err()
		for _, p := range batch {
			p.done <- err
		}
		return
	}
	e.mu.Lock()
	for _, p := range batch {
		r, err := apply(e.runs, p.rec)
		if err != nil {
			// The engine only writes records apply accepts.
			e.mu.Unlock()
			panic(err)
		}
		switch p.rec.Type {
		case recStart:
			e.order = append(e.order, r.id)
			e.schedule(r)
		case recEnd:
			close(r.finished)
		}
	}
	e.mu.Unlock()
	for _, p := range batch {
		p.done <- nil
	}
}

// stop halts the engine after a journal write failed: nothing more is
// written, so the journal stays at its last good checkpoint.
func (e *Engine) stop(cause error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopErr == nil {
		e.stopErr = fmt.Errorf("%w: %v", ErrStopped, cause)
	}
	e.closing = true
	e.cancel()
	e.cond.Broadcast()
}

// schedule queues a run that can make progress, or sets a timer for one
// waiting to retry. The caller holds mu.
func (e *Engine) schedule(r *run) {
	if e.closing || r.active || r.queued || r.Status.Finished() || e.flows[r.Flow] == nil {
		return
	}
	// A canceled run doesn't sit out its retry wait: it goes straight to
	// compensation.
	if !r.RetryAt.IsZero() && !(r.Cancel && !r.Compensating) {
		if d := r.RetryAt.Sub(e.opts.Now()); d > 0 {
			if r.timer == nil {
				r.timer = time.AfterFunc(d, func() {
					e.mu.Lock()
					defer e.mu.Unlock()
					r.timer = nil
					e.schedule(r)
				})
			}
			return
		}
	}
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
	r.queued = true
	e.ready = append(e.ready, r)
	e.cond.Signal()
}

func (e *Engine) worker() {
	defer e.wg.Done()
	for {
		e.mu.Lock()
		for len(e.ready) == 0 && !e.closing {
			e.cond.Wait()
		}
		if e.closing {
			e.mu.Unlock()
			return
		}
		r := e.ready[0]
		e.ready = e.ready[1:]
		r.queued = false
		r.active = true
		e.mu.Unlock()

		for e.advance(r) {
		}

		e.mu.Lock()
		r.active = false
		e.schedule(r)
		e.mu.Unlock()
	}
}

// advance takes one action on a run: one attempt of a step or of an undo,
// or ending the run. It reports whether the run can go on at once.
func (e *Engine) advance(r *run) bool {
	e.mu.Lock()
	d := e.flows[r.Flow]
	if e.closing || d == nil || r.Status.Finished() {
		e.mu.Unlock()
		return false
	}
	if !r.RetryAt.IsZero() && r.RetryAt.After(e.opts.Now()) && !(r.Cancel && !r.Compensating) {
		e.mu.Unlock()
		return false
	}
	if r.Compensating {
		return e.undoOne(r, d)
	}
	return e.stepOne(r, d)
}

// stepOne runs one attempt of the run's current step. Called with mu held;
// returns with it released.
func (e *Engine) stepOne(r *run, d *flowDef) bool {
	if r.Cancel {
		e.mu.Unlock()
		return e.commit(&Record{Type: recComp, Run: r.id, Error: "canceled"}) == nil
	}
	if r.Cursor == "" {
		e.mu.Unlock()
		e.commit(&Record{Type: recEnd, Run: r.id, Status: Completed})
		return false
	}
	step := d.byName[r.Cursor]
	if step == nil {
		name := r.Cursor
		e.mu.Unlock()
		return e.failRun(r, name, r.Attempt, fmt.Errorf("flow %q has no step %q", r.Flow, name))
	}
	attempt, seq := r.Attempt+1, len(r.Done)+1
	ctx, cancel := context.WithCancel(e.ctx)
	c := &Ctx{
		Context: ctx, Run: r.id, Flow: r.Flow, Step: step.Name, Attempt: attempt, Seq: seq,
		Key:   r.id + "/" + step.Name + "/" + strconv.Itoa(seq),
		input: clone(r.Input), state: cloneState(r.State),
	}
	r.cancelFn = cancel
	e.mu.Unlock()
	defer cancel()

	if err := e.commit(&Record{Type: recBegin, Run: r.id, Step: step.Name, Seq: seq, Attempt: attempt}); err != nil {
		e.clearCancel(r)
		return false
	}
	err := call(step.Do, c)
	e.clearCancel(r)

	if err != nil && e.ctx.Err() != nil {
		// The engine is closing: leave the attempt open. It runs again
		// after the next Open.
		return false
	}
	if err == nil {
		next := d.nextOf[step.Name]
		if c.next != nil {
			next = *c.next
		}
		rec := &Record{Type: recDone, Run: r.id, Step: step.Name, Seq: seq, State: c.state, Next: &next}
		if _, encErr := encode(rec); encErr != nil {
			return e.failRun(r, step.Name, attempt, encErr)
		}
		return e.commit(rec) == nil
	}
	e.mu.Lock()
	canceled := r.Cancel
	e.mu.Unlock()
	policy := step.Retry.or(e.opts.Retry)
	if IsPermanent(err) || attempt >= policy.attempts() || canceled {
		return e.failRun(r, step.Name, attempt, err)
	}
	at := e.opts.Now().Add(policy.delay(attempt)).UTC()
	e.commit(&Record{Type: recFail, Run: r.id, Step: step.Name, Seq: seq, Attempt: attempt, Error: err.Error(), RetryAt: &at})
	return false
}

// failRun records a step's final failure and starts compensation.
func (e *Engine) failRun(r *run, step string, attempt int, err error) bool {
	msg := err.Error()
	if e.commit(&Record{Type: recFail, Run: r.id, Step: step, Attempt: attempt, Error: msg, Final: true}) != nil {
		return false
	}
	return e.commit(&Record{Type: recComp, Run: r.id, Error: fmt.Sprintf("step %s: %s", step, msg)}) == nil
}

// undoOne runs one attempt of the next undo. Called with mu held; returns
// with it released.
func (e *Engine) undoOne(r *run, d *flowDef) bool {
	i := r.UndoNext
	for i >= 0 {
		s := d.byName[r.Done[i].Step]
		if s == nil || s.Undo != nil {
			break
		}
		i--
	}
	if i < 0 {
		cause := r.Error
		e.mu.Unlock()
		e.commit(&Record{Type: recEnd, Run: r.id, Status: Compensated, Error: cause})
		return false
	}
	done := r.Done[i]
	step := d.byName[done.Step]
	if step == nil {
		e.mu.Unlock()
		e.commit(&Record{Type: recUFail, Run: r.id, Step: done.Step, Seq: done.Seq, Error: errNoUndo.Error(), Final: true})
		e.commit(&Record{Type: recEnd, Run: r.id, Status: Failed, Error: fmt.Sprintf("undo %s: %v", done.Step, errNoUndo)})
		return false
	}
	attempt := r.UAttempt + 1
	if i != r.UndoNext {
		attempt = 1
	}
	c := &Ctx{
		Context: e.ctx, Run: r.id, Flow: r.Flow, Step: step.Name, Attempt: attempt, Seq: done.Seq,
		Key:   r.id + "/" + step.Name + "/" + strconv.Itoa(done.Seq) + "/undo",
		input: clone(r.Input), state: cloneState(done.State), Undoing: true,
	}
	e.mu.Unlock()

	if err := e.commit(&Record{Type: recUBegin, Run: r.id, Step: step.Name, Seq: done.Seq, Attempt: attempt}); err != nil {
		return false
	}
	err := call(step.Undo, c)
	if err != nil && e.ctx.Err() != nil {
		return false
	}
	if err == nil {
		return e.commit(&Record{Type: recUDone, Run: r.id, Step: step.Name, Seq: done.Seq}) == nil
	}
	policy := step.Retry.or(e.opts.Retry)
	if IsPermanent(err) || attempt >= policy.attempts() {
		if e.commit(&Record{Type: recUFail, Run: r.id, Step: step.Name, Seq: done.Seq, Attempt: attempt, Error: err.Error(), Final: true}) != nil {
			return false
		}
		e.commit(&Record{Type: recEnd, Run: r.id, Status: Failed, Error: fmt.Sprintf("undo %s: %s", step.Name, err)})
		return false
	}
	at := e.opts.Now().Add(policy.delay(attempt)).UTC()
	e.commit(&Record{Type: recUFail, Run: r.id, Step: step.Name, Seq: done.Seq, Attempt: attempt, Error: err.Error(), RetryAt: &at})
	return false
}

func (e *Engine) clearCancel(r *run) {
	e.mu.Lock()
	r.cancelFn = nil
	e.mu.Unlock()
}

// call runs a step function, turning a panic into an error.
func call(f func(*Ctx) error, c *Ctx) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v", p)
		}
	}()
	return f(c)
}
