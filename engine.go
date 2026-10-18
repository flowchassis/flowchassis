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
	// Retry is the policy for steps that don't set their own, and fills the
	// fields a step's policy leaves at zero. Its own zero fields come from
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

	// Journal writes. Committers queue records; one writer goroutine takes
	// everything queued, checks it against the runs, appends it with one
	// write and one flush (group commit), applies it in order, and answers
	// each committer. jmu is held shared by every commit and exclusively by
	// Compact and Close, so they see no write under way.
	jmu    sync.RWMutex
	qmu    sync.Mutex
	queue  []*pending
	kick   chan struct{}
	wstop  chan struct{}
	wdone  chan struct{}
	cancMu sync.Mutex // one Cancel at a time

	mu       sync.Mutex
	cond     *sync.Cond
	flows    map[string]*flowDef
	runs     map[string]*run
	starting map[string]chan struct{} // run IDs whose start record is being written
	order    []string
	ready    []*run
	closing  bool // no new work: Close was called or a write failed
	closed   bool // Close has stopped the workers; no more writes
	stopErr  error

	closeOnce sync.Once
	closeErr  error

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
	opts.Retry = opts.Retry.or(DefaultRetry)
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
	e := &Engine{
		opts: opts, store: store,
		flows: map[string]*flowDef{}, runs: map[string]*run{}, starting: map[string]chan struct{}{},
		kick: make(chan struct{}, 1), wstop: make(chan struct{}), wdone: make(chan struct{}),
	}
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
		r.markFinished()
	}
	e.ctx, e.cancel = context.WithCancel(context.Background())
	go e.writer()
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
	d.Steps = append([]Step(nil), f.Steps...)
	for i := range d.Steps {
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
		if i+1 < len(d.Steps) {
			d.nextOf[s.Name] = d.Steps[i+1].Name
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
// is ErrConflict. A run that Compact dropped is forgotten, and its id can be
// started again.
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

// Wait blocks until the run finishes or ctx is done, and returns the run. A
// run that has finished is returned even after Close.
func (e *Engine) Wait(ctx context.Context, id string) (RunInfo, error) {
	e.mu.Lock()
	r := e.runs[id]
	if r == nil {
		e.mu.Unlock()
		return RunInfo{}, fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	ch := r.finished
	e.mu.Unlock()
	// Read the run only after the channel says it finished, so what is
	// returned is the finished run.
	get := func() RunInfo {
		e.mu.Lock()
		defer e.mu.Unlock()
		return r.info()
	}
	select {
	case <-ch:
		return get(), nil
	default:
	}
	select {
	case <-ch:
	case <-ctx.Done():
	case <-e.ctx.Done():
	}
	select {
	case <-ch:
		return get(), nil
	default:
	}
	info := get()
	if ctx.Err() != nil {
		return info, ctx.Err()
	}
	return info, e.closedErr()
}

// Cancel asks a run to stop and undo its completed steps. A step under way
// sees its context canceled; if it still completes, it is undone too.
// Canceling a run that is compensating already changes nothing.
func (e *Engine) Cancel(id string) error {
	e.cancMu.Lock()
	defer e.cancMu.Unlock()
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
	// The run can finish, or Compact drop it, before the record is written;
	// the writer checks again and answers ErrFinished or ErrNotFound.
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

// Compact rewrites the journal with a snapshot of each run, and leaves out
// runs that finished before the given time (a zero time keeps them all). It
// runs while the engine works; writes wait for it. A dropped run is
// forgotten: its ID can be started again.
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
	now := e.opts.Now().UTC()
	for _, id := range e.order {
		r := e.runs[id]
		if r.Status.Finished() && !dropFinishedBefore.IsZero() && r.Ended.Before(dropFinishedBefore) {
			drop = append(drop, id)
			continue
		}
		recs := []*Record{{Type: recSnapshot, Run: id, At: now, Snap: r.snap()}}
		for _, c := range r.Done {
			if len(c.State) > 0 {
				recs = append(recs, &Record{Type: recSState, Run: id, At: now, Seq: c.Seq, State: c.State})
			}
		}
		for _, rec := range recs {
			f, err := encode(rec)
			if err != nil {
				e.mu.Unlock()
				return fmt.Errorf("flowchassis: compacting run %q: %w", id, err)
			}
			frames = append(frames, f...)
		}
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

// Close stops the engine. Steps under way see their context canceled. A step
// that returns an error because of it is not counted as failed and its
// attempt doesn't count; it runs again when the journal is next opened. A
// step that completes anyway is recorded. Close waits for them to return.
func (e *Engine) Close() error {
	// The first call shuts down; any other, at the same time or later, waits
	// for it and returns its result.
	e.closeOnce.Do(func() { e.closeErr = e.shutdown() })
	return e.closeErr
}

func (e *Engine) shutdown() error {
	e.mu.Lock()
	e.closing = true
	e.cancel()
	e.cond.Broadcast()
	e.mu.Unlock()
	e.wg.Wait()

	e.mu.Lock()
	e.closed = true
	for _, r := range e.runs {
		if r.timer != nil {
			r.timer.Stop()
			r.timer = nil
		}
	}
	e.mu.Unlock()
	e.jmu.Lock() // every commit under way has returned
	close(e.wstop)
	<-e.wdone
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
// it to the run in memory. It returns once the record is durable and
// applied, or with the reason it wasn't written.
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
	e.qmu.Unlock()
	select {
	case e.kick <- struct{}{}:
	default:
	}
	return <-p.done
}

// writer is the one goroutine that writes the journal.
func (e *Engine) writer() {
	defer close(e.wdone)
	for {
		select {
		case <-e.kick:
		case <-e.wstop:
			return
		}
		for {
			e.qmu.Lock()
			batch := e.queue
			e.queue = nil
			e.qmu.Unlock()
			if len(batch) == 0 {
				break
			}
			e.flush(batch)
		}
	}
}

// flush checks a batch of records against the runs, appends the ones that
// still apply with one write and one flush, applies them in order, and
// answers each committer.
func (e *Engine) flush(batch []*pending) {
	e.mu.Lock()
	if e.stopErr != nil {
		err := e.stopErr
		e.mu.Unlock()
		for _, p := range batch {
			p.done <- err
		}
		return
	}
	// A record is written only if it still makes sense: a start for a new
	// run, anything else for a run that exists and hasn't finished, counting
	// the records ahead of it in the batch.
	started := map[string]bool{}
	ended := map[string]bool{}
	canceled := map[string]bool{}
	var ok []*pending
	var frames []byte
	for _, p := range batch {
		id := p.rec.Run
		r := e.runs[id]
		var err error
		switch {
		case p.rec.Type == recStart:
			if r != nil || started[id] {
				err = fmt.Errorf("%w: %q", ErrConflict, id)
			}
			started[id] = true
		case r == nil && !started[id]:
			err = fmt.Errorf("%w: %q", ErrNotFound, id)
		case ended[id] || (r != nil && r.Status.Finished()):
			err = fmt.Errorf("%w: %q", ErrFinished, id)
		case p.rec.Type == recEnd && p.rec.Status == Completed && (canceled[id] || (r != nil && r.Cancel)):
			// A cancel got in after the last step: the run compensates
			// instead. The worker sees the cancel on its next pass.
			err = errCanceledRun
		}
		if err != nil {
			p.done <- err
			continue
		}
		if endsRun(p.rec) {
			ended[id] = true
		}
		if p.rec.Type == recCancel {
			canceled[id] = true
		}
		ok = append(ok, p)
		frames = append(frames, p.frame...)
	}
	e.mu.Unlock()
	if len(ok) == 0 {
		return
	}
	if err := e.store.Append(frames); err != nil {
		e.stop(err)
		err = e.Err()
		for _, p := range ok {
			p.done <- err
		}
		return
	}
	e.mu.Lock()
	for i, p := range ok {
		r, err := apply(e.runs, p.rec)
		if err != nil {
			// Unreachable with the checks above: every record the engine
			// writes is one apply accepts for a run in this state. If it
			// ever happened the record is on disk and replay would refuse
			// it too, so stop rather than act on memory that no longer
			// matches the journal.
			e.mu.Unlock()
			e.stop(fmt.Errorf("applying a written record: %w", err))
			err = e.Err()
			for _, q := range ok[i:] {
				q.done <- err
			}
			return
		}
		if p.rec.Type == recStart {
			e.order = append(e.order, r.id)
			e.schedule(r)
		}
		r.markFinished()
		p.done <- nil
	}
	e.mu.Unlock()
}

// endsRun reports whether a record finishes its run.
func endsRun(rec *Record) bool {
	return rec.Type == recEnd || (rec.Type == recUFail && rec.Final)
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
		name, attempt := r.Cursor, r.Attempt
		e.mu.Unlock()
		return e.failRun(r, name, attempt, fmt.Errorf("flow %q has no step %q", r.Flow, name))
	}
	policy := step.Retry.or(e.opts.Retry)
	if r.Attempt >= policy.attempts() {
		// Every attempt so far was cut off by a crash: a step that brings the
		// process down doesn't get to loop forever.
		n := r.Attempt
		e.mu.Unlock()
		return e.failRun(r, step.Name, n, fmt.Errorf("gave up after %d attempts, the last of them cut off before it finished", n))
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

	if err != nil && e.ctx.Err() != nil && cutByClose(err) {
		// Close cut the attempt off. It doesn't count, and runs again after
		// the next Open.
		e.commit(&Record{Type: recYield, Run: r.id, Step: step.Name, Seq: seq, Attempt: attempt})
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
	if IsPermanent(err) || attempt >= policy.attempts() || canceled {
		return e.failRun(r, step.Name, attempt, err)
	}
	at := e.opts.Now().Add(policy.delay(attempt)).UTC()
	e.commit(&Record{Type: recFail, Run: r.id, Step: step.Name, Seq: seq, Attempt: attempt, Error: err.Error(), RetryAt: &at})
	return false
}

// failRun records a step's final failure, which starts compensation.
func (e *Engine) failRun(r *run, step string, attempt int, err error) bool {
	return e.commit(&Record{Type: recFail, Run: r.id, Step: step, Attempt: attempt, Error: err.Error(), Final: true}) == nil
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
		return false
	}
	prior := r.UAttempt
	if i != r.UndoNext {
		prior = 0
	}
	policy := step.Retry.or(e.opts.Retry)
	if prior >= policy.attempts() {
		e.mu.Unlock()
		e.commit(&Record{Type: recUFail, Run: r.id, Step: step.Name, Seq: done.Seq, Attempt: prior, Final: true,
			Error: fmt.Sprintf("gave up after %d attempts, the last of them cut off before it finished", prior)})
		return false
	}
	attempt := prior + 1
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
	if err != nil && e.ctx.Err() != nil && cutByClose(err) {
		e.commit(&Record{Type: recUYield, Run: r.id, Step: step.Name, Seq: done.Seq, Attempt: attempt})
		return false
	}
	if err == nil {
		return e.commit(&Record{Type: recUDone, Run: r.id, Step: step.Name, Seq: done.Seq}) == nil
	}
	if IsPermanent(err) || attempt >= policy.attempts() {
		e.commit(&Record{Type: recUFail, Run: r.id, Step: step.Name, Seq: done.Seq, Attempt: attempt, Error: err.Error(), Final: true})
		return false
	}
	at := e.opts.Now().Add(policy.delay(attempt)).UTC()
	e.commit(&Record{Type: recUFail, Run: r.id, Step: step.Name, Seq: done.Seq, Attempt: attempt, Error: err.Error(), RetryAt: &at})
	return false
}

// cutByClose reports whether a step's error came from its context being
// canceled, rather than from a failure of its own.
func cutByClose(err error) bool {
	return !IsPermanent(err) && errors.Is(err, context.Canceled)
}

// errCanceledRun is the writer's answer to a run ending as completed after
// a cancel was recorded.
var errCanceledRun = errors.New("flowchassis: run was canceled")

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
