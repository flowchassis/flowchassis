// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

package flowchassis

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fast is a retry policy short enough for tests.
var fast = RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond}

func newMem(t *testing.T, frames []byte) (*Engine, *MemStore) {
	t.Helper()
	s := NewMemStore(frames)
	e, err := New(s, Options{Retry: fast})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e, s
}

func wait(t *testing.T, e *Engine, id string) RunInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	info, err := e.Wait(ctx, id)
	if err != nil {
		t.Fatalf("waiting for %s: %v (status %s, step %s, error %q)", id, err, info.Status, info.Step, info.Error)
	}
	return info
}

// tape records what steps did, in order.
type tape struct {
	mu  sync.Mutex
	log []string
}

func (t *tape) add(s string) {
	t.mu.Lock()
	t.log = append(t.log, s)
	t.mu.Unlock()
}

func (t *tape) get() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.log...)
}

func simple(tp *tape, names ...string) Flow {
	f := Flow{Name: "f"}
	for _, n := range names {
		n := n
		f.Steps = append(f.Steps, Step{
			Name: n,
			Do:   func(c *Ctx) error { tp.add("do " + n); return nil },
			Undo: func(c *Ctx) error { tp.add("undo " + n); return nil },
		})
	}
	return f
}

func TestRunCompletes(t *testing.T) {
	e, _ := newMem(t, nil)
	tp := &tape{}
	if err := e.Register(simple(tp, "a", "b", "c")); err != nil {
		t.Fatal(err)
	}
	id, err := e.Start("f", "r1", []byte("in"))
	if err != nil || id != "r1" {
		t.Fatal(id, err)
	}
	info := wait(t, e, "r1")
	if info.Status != Completed {
		t.Fatalf("status %s", info.Status)
	}
	if got, want := tp.get(), []string{"do a", "do b", "do c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if !reflect.DeepEqual(info.Done, []string{"a", "b", "c"}) || len(info.Undone) != 0 {
		t.Fatalf("done %v undone %v", info.Done, info.Undone)
	}
	if string(info.Input) != "in" || info.Ended.IsZero() || info.Started.IsZero() {
		t.Fatalf("info %+v", info)
	}
}

func TestStateFlowsBetweenSteps(t *testing.T) {
	e, _ := newMem(t, nil)
	var seen []byte
	e.Register(Flow{Name: "f", Steps: []Step{
		{Name: "a", Do: func(c *Ctx) error { c.Set("x", append(c.Input(), '!')); c.Set("gone", []byte("1")); return nil }},
		{Name: "b", Do: func(c *Ctx) error { v, _ := c.Get("x"); seen = v; c.Delete("gone"); return nil }},
	}})
	e.Start("f", "r", []byte("hi"))
	info := wait(t, e, "r")
	if string(seen) != "hi!" {
		t.Fatalf("b saw %q", seen)
	}
	if _, ok := info.State["gone"]; ok || string(info.State["x"]) != "hi!" {
		t.Fatalf("state %v", info.State)
	}
}

func TestFailedAttemptDiscardsItsState(t *testing.T) {
	e, _ := newMem(t, nil)
	var second []byte
	var tries int32
	e.Register(Flow{Name: "f", Steps: []Step{{Name: "a", Do: func(c *Ctx) error {
		if atomic.AddInt32(&tries, 1) == 1 {
			c.Set("x", []byte("from the failed attempt"))
			return errors.New("boom")
		}
		second, _ = c.Get("x")
		return nil
	}}}})
	e.Start("f", "r", nil)
	if info := wait(t, e, "r"); info.Status != Completed {
		t.Fatal(info.Status)
	}
	if second != nil {
		t.Fatalf("the retry saw %q from the failed attempt", second)
	}
}

func TestNextChoosesTheStep(t *testing.T) {
	e, _ := newMem(t, nil)
	tp := &tape{}
	loops := 0
	e.Register(Flow{Name: "f", Steps: []Step{
		{Name: "a", Do: func(c *Ctx) error { tp.add("a"); return nil }},
		{Name: "b", Do: func(c *Ctx) error {
			tp.add(fmt.Sprintf("b%d", c.Seq))
			loops++
			if loops < 3 {
				c.Next("b")
			} else {
				c.Next("")
			}
			return nil
		}},
		{Name: "never", Do: func(c *Ctx) error { tp.add("never"); return nil }},
	}})
	e.Start("f", "r", nil)
	info := wait(t, e, "r")
	if got, want := tp.get(), []string{"a", "b2", "b3", "b4"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if info.Status != Completed || len(info.Done) != 4 {
		t.Fatalf("%+v", info)
	}
}

func TestNextToUnknownStepCompensates(t *testing.T) {
	e, _ := newMem(t, nil)
	tp := &tape{}
	e.Register(Flow{Name: "f", Steps: []Step{
		{Name: "a", Do: func(c *Ctx) error { c.Next("nowhere"); return nil }, Undo: func(c *Ctx) error { tp.add("undo a"); return nil }},
	}})
	e.Start("f", "r", nil)
	info := wait(t, e, "r")
	if info.Status != Compensated || !strings.Contains(info.Error, "nowhere") {
		t.Fatalf("%+v", info)
	}
	if got := tp.get(); !reflect.DeepEqual(got, []string{"undo a"}) {
		t.Fatal(got)
	}
}

func TestStartIsIdempotent(t *testing.T) {
	e, _ := newMem(t, nil)
	var runs int32
	e.Register(Flow{Name: "f", Steps: []Step{{Name: "a", Do: func(c *Ctx) error { atomic.AddInt32(&runs, 1); return nil }}}})
	e.Register(Flow{Name: "g", Steps: []Step{{Name: "a", Do: func(c *Ctx) error { return nil }}}})
	for i := 0; i < 3; i++ {
		if id, err := e.Start("f", "same", []byte("x")); err != nil || id != "same" {
			t.Fatal(id, err)
		}
	}
	wait(t, e, "same")
	if _, err := e.Start("f", "same", []byte("x")); err != nil {
		t.Fatalf("starting a finished run again: %v", err)
	}
	if runs != 1 {
		t.Fatalf("step ran %d times", runs)
	}
	if _, err := e.Start("f", "same", []byte("y")); !errors.Is(err, ErrConflict) {
		t.Fatalf("other input: %v", err)
	}
	if _, err := e.Start("g", "same", []byte("x")); !errors.Is(err, ErrConflict) {
		t.Fatalf("other flow: %v", err)
	}
	if _, err := e.Start("nope", "", nil); !errors.Is(err, ErrUnknownFlow) {
		t.Fatalf("unknown flow: %v", err)
	}
	id, err := e.Start("f", "", nil)
	if err != nil || len(id) != 24 {
		t.Fatalf("generated id %q: %v", id, err)
	}
	wait(t, e, id)
}

func TestRetryThenSucceed(t *testing.T) {
	e, _ := newMem(t, nil)
	var attempts []int
	var keys []string
	e.Register(Flow{Name: "f", Steps: []Step{{Name: "a", Do: func(c *Ctx) error {
		attempts = append(attempts, c.Attempt)
		keys = append(keys, c.Key)
		if c.Attempt < 3 {
			return errors.New("not yet")
		}
		return nil
	}}}})
	e.Start("f", "r", nil)
	info := wait(t, e, "r")
	if info.Status != Completed || !reflect.DeepEqual(attempts, []int{1, 2, 3}) {
		t.Fatalf("%s %v", info.Status, attempts)
	}
	if keys[0] != "r/a/1" || keys[1] != keys[0] || keys[2] != keys[0] {
		t.Fatalf("keys %v", keys)
	}
}

func TestRetryWaitsBackoff(t *testing.T) {
	e, _ := newMem(t, nil)
	var at []time.Time
	e.Register(Flow{Name: "f", Steps: []Step{{Name: "a",
		Retry: RetryPolicy{MaxAttempts: 3, Backoff: 40 * time.Millisecond, Multiplier: 2},
		Do: func(c *Ctx) error {
			at = append(at, time.Now())
			if c.Attempt < 3 {
				return errors.New("wait")
			}
			return nil
		}}}})
	e.Start("f", "r", nil)
	wait(t, e, "r")
	if len(at) != 3 {
		t.Fatal(len(at))
	}
	if d := at[1].Sub(at[0]); d < 40*time.Millisecond {
		t.Fatalf("first wait %v, want at least 40ms", d)
	}
	if d := at[2].Sub(at[1]); d < 80*time.Millisecond {
		t.Fatalf("second wait %v, want at least 80ms", d)
	}
}

func TestRetryingStatusIsVisible(t *testing.T) {
	e, _ := newMem(t, nil)
	e.Register(Flow{Name: "f", Steps: []Step{{Name: "a",
		Retry: RetryPolicy{MaxAttempts: 2, Backoff: time.Hour},
		Do:    func(c *Ctx) error { return errors.New("down") }}}})
	e.Start("f", "r", nil)
	deadline := time.Now().Add(5 * time.Second)
	for {
		info, _ := e.Get("r")
		if info.Status == Retrying {
			if info.Step != "a" || info.Attempt != 1 || info.Error != "down" || time.Until(info.RetryAt) < 59*time.Minute {
				t.Fatalf("%+v", info)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("never retrying: %+v", info)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestExhaustedRetriesCompensateInReverse(t *testing.T) {
	e, _ := newMem(t, nil)
	tp := &tape{}
	f := simple(tp, "a", "b", "c")
	f.Steps[2].Do = func(c *Ctx) error { tp.add(fmt.Sprintf("try c %d", c.Attempt)); return errors.New("c broke") }
	e.Register(f)
	e.Start("f", "r", nil)
	info := wait(t, e, "r")
	want := []string{"do a", "do b", "try c 1", "try c 2", "try c 3", "undo b", "undo a"}
	if got := tp.get(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if info.Status != Compensated || !reflect.DeepEqual(info.Undone, []string{"b", "a"}) || !strings.Contains(info.Error, "c broke") {
		t.Fatalf("%+v", info)
	}
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	e, _ := newMem(t, nil)
	var n int32
	e.Register(Flow{Name: "f", Steps: []Step{{Name: "a", Do: func(c *Ctx) error {
		atomic.AddInt32(&n, 1)
		return Permanent(errors.New("bad input"))
	}}}})
	e.Start("f", "r", nil)
	info := wait(t, e, "r")
	if n != 1 || info.Status != Compensated {
		t.Fatalf("ran %d times, %s", n, info.Status)
	}
	if !IsPermanent(Permanent(errors.New("x"))) || IsPermanent(errors.New("x")) || Permanent(nil) != nil {
		t.Fatal("Permanent / IsPermanent")
	}
}

func TestUndoSeesStateFromItsOwnStep(t *testing.T) {
	e, _ := newMem(t, nil)
	var seen []string
	var mu sync.Mutex
	undo := func(c *Ctx) error {
		v, _ := c.Get("v")
		mu.Lock()
		seen = append(seen, c.Step+"="+string(v)+" "+c.Key)
		mu.Unlock()
		c.Set("v", []byte("ignored"))
		return nil
	}
	e.Register(Flow{Name: "f", Steps: []Step{
		{Name: "a", Do: func(c *Ctx) error { c.Set("v", []byte("A")); return nil }, Undo: undo},
		{Name: "b", Do: func(c *Ctx) error { c.Set("v", []byte("B")); return nil }, Undo: undo},
		{Name: "c", Do: func(c *Ctx) error { return Permanent(errors.New("stop")) }},
	}})
	e.Start("f", "r", nil)
	info := wait(t, e, "r")
	want := []string{"b=B r/b/2/undo", "a=A r/a/1/undo"}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("got %v want %v", seen, want)
	}
	if string(info.State["v"]) != "B" {
		t.Fatalf("undo changed the state: %q", info.State["v"])
	}
}

func TestStepsWithoutUndoArePassedOver(t *testing.T) {
	e, _ := newMem(t, nil)
	tp := &tape{}
	f := simple(tp, "a", "b", "c")
	f.Steps[1].Undo = nil
	f.Steps = append(f.Steps, Step{Name: "d", Do: func(c *Ctx) error { return Permanent(errors.New("no")) }})
	e.Register(f)
	e.Start("f", "r", nil)
	info := wait(t, e, "r")
	if got, want := tp.get(), []string{"do a", "do b", "do c", "undo c", "undo a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if !reflect.DeepEqual(info.Undone, []string{"c", "a"}) {
		t.Fatal(info.Undone)
	}
}

func TestUndoRetriesThenFails(t *testing.T) {
	e, _ := newMem(t, nil)
	var n int32
	e.Register(Flow{Name: "f", Steps: []Step{
		{Name: "a", Do: func(c *Ctx) error { return nil }, Undo: func(c *Ctx) error { atomic.AddInt32(&n, 1); return errors.New("refund service down") }},
		{Name: "b", Do: func(c *Ctx) error { return Permanent(errors.New("no")) }},
	}})
	e.Start("f", "r", nil)
	info := wait(t, e, "r")
	if info.Status != Failed || n != 3 || !strings.Contains(info.Error, "refund service down") {
		t.Fatalf("%+v, %d undo attempts", info, n)
	}
	if len(info.Undone) != 0 {
		t.Fatal(info.Undone)
	}
}

func TestUndoRetrySucceeds(t *testing.T) {
	e, _ := newMem(t, nil)
	var attempts []int
	e.Register(Flow{Name: "f", Steps: []Step{
		{Name: "a", Do: func(c *Ctx) error { return nil }, Undo: func(c *Ctx) error {
			attempts = append(attempts, c.Attempt)
			if c.Attempt < 2 {
				return errors.New("later")
			}
			return nil
		}},
		{Name: "b", Do: func(c *Ctx) error { return Permanent(errors.New("no")) }},
	}})
	e.Start("f", "r", nil)
	if info := wait(t, e, "r"); info.Status != Compensated || !reflect.DeepEqual(attempts, []int{1, 2}) {
		t.Fatalf("%s %v", info.Status, attempts)
	}
}

func TestPanicIsAFailure(t *testing.T) {
	e, _ := newMem(t, nil)
	e.Register(Flow{Name: "f", Steps: []Step{{Name: "a", Do: func(c *Ctx) error {
		if c.Attempt == 1 {
			panic("oops")
		}
		return nil
	}}}})
	e.Start("f", "r", nil)
	if info := wait(t, e, "r"); info.Status != Completed {
		t.Fatal(info.Status)
	}
}

func TestCancelCompensates(t *testing.T) {
	e, _ := newMem(t, nil)
	tp := &tape{}
	entered := make(chan struct{})
	f := simple(tp, "a", "b")
	f.Steps[1].Do = func(c *Ctx) error {
		close(entered)
		<-c.Done()
		tp.add("b saw cancel")
		return c.Err()
	}
	e.Register(f)
	e.Start("f", "r", nil)
	<-entered
	if err := e.Cancel("r"); err != nil {
		t.Fatal(err)
	}
	if err := e.Cancel("r"); err != nil {
		t.Fatalf("second cancel: %v", err)
	}
	info := wait(t, e, "r")
	if info.Status != Compensated || info.Error != "step b: context canceled" {
		t.Fatalf("%+v", info)
	}
	if got, want := tp.get(), []string{"do a", "b saw cancel", "undo a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if err := e.Cancel("r"); !errors.Is(err, ErrFinished) {
		t.Fatal(err)
	}
	if err := e.Cancel("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestCancelWhileRetryWaiting(t *testing.T) {
	e, _ := newMem(t, nil)
	e.Register(Flow{Name: "f", Steps: []Step{{Name: "a",
		Retry: RetryPolicy{MaxAttempts: 5, Backoff: time.Hour},
		Do:    func(c *Ctx) error { return errors.New("down") }}}})
	e.Start("f", "r", nil)
	for {
		if info, _ := e.Get("r"); info.Status == Retrying {
			break
		}
		time.Sleep(time.Millisecond)
	}
	e.Cancel("r")
	start := time.Now()
	info := wait(t, e, "r")
	if info.Status != Compensated || time.Since(start) > 5*time.Second || info.Error != "canceled" {
		t.Fatalf("%+v after %v", info, time.Since(start))
	}
}

func TestCancelAfterStepCompletesUndoesIt(t *testing.T) {
	e, _ := newMem(t, nil)
	tp := &tape{}
	entered := make(chan struct{})
	release := make(chan struct{})
	f := simple(tp, "a", "b")
	f.Steps[0].Do = func(c *Ctx) error {
		close(entered)
		<-release
		tp.add("do a")
		return nil // completes even though canceled
	}
	e.Register(f)
	e.Start("f", "r", nil)
	<-entered
	e.Cancel("r")
	close(release)
	info := wait(t, e, "r")
	if got, want := tp.get(), []string{"do a", "undo a"}; !reflect.DeepEqual(got, want) || info.Status != Compensated {
		t.Fatalf("%v %s", got, info.Status)
	}
}

func TestListGetAndNotFound(t *testing.T) {
	e, _ := newMem(t, nil)
	tp := &tape{}
	e.Register(simple(tp, "a"))
	for _, id := range []string{"x", "y", "z"} {
		e.Start("f", id, nil)
		wait(t, e, id)
	}
	var ids []string
	for _, r := range e.List() {
		ids = append(ids, r.ID)
	}
	if !reflect.DeepEqual(ids, []string{"x", "y", "z"}) {
		t.Fatal(ids)
	}
	if _, err := e.Get("q"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := e.Wait(context.Background(), "q"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestWaitHonorsContext(t *testing.T) {
	e, _ := newMem(t, nil)
	e.Register(Flow{Name: "f", Steps: []Step{{Name: "a", Do: func(c *Ctx) error { <-c.Done(); return c.Err() }}}})
	e.Start("f", "r", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := e.Wait(ctx, "r"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestRegisterChecksFlows(t *testing.T) {
	e, _ := newMem(t, nil)
	ok := func(c *Ctx) error { return nil }
	bad := []Flow{
		{},
		{Name: "f"},
		{Name: "f", Steps: []Step{{Do: ok}}},
		{Name: "f", Steps: []Step{{Name: "a"}}},
		{Name: "f", Steps: []Step{{Name: "a", Do: ok}, {Name: "a", Do: ok}}},
	}
	for i, f := range bad {
		if err := e.Register(f); err == nil {
			t.Errorf("flow %d registered", i)
		}
	}
	if err := e.Register(Flow{Name: "f", Steps: []Step{{Name: "a", Do: ok}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Register(Flow{Name: "f", Steps: []Step{{Name: "a", Do: ok}}}); err == nil {
		t.Fatal("registered twice")
	}
}

func TestClosedEngine(t *testing.T) {
	e, _ := newMem(t, nil)
	tp := &tape{}
	e.Register(simple(tp, "a"))
	e.Close()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Start("f", "", nil); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := e.Cancel("x"); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := e.Register(Flow{Name: "g", Steps: []Step{{Name: "a", Do: func(*Ctx) error { return nil }}}}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := e.Compact(time.Time{}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestManyRunsAtOnce(t *testing.T) {
	s := NewMemStore(nil)
	e, err := New(s, Options{Workers: 8, Retry: fast})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	var count int64
	e.Register(Flow{Name: "f", Steps: []Step{
		{Name: "a", Do: func(c *Ctx) error { atomic.AddInt64(&count, 1); return nil }},
		{Name: "b", Do: func(c *Ctx) error {
			atomic.AddInt64(&count, 1)
			if c.Attempt == 1 && c.Run[len(c.Run)-1] == '7' {
				return errors.New("flaky")
			}
			return nil
		}},
	}})
	const n = 500
	for i := 0; i < n; i++ {
		if _, err := e.Start("f", fmt.Sprintf("r%03d", i), nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < n; i++ {
		if info := wait(t, e, fmt.Sprintf("r%03d", i)); info.Status != Completed {
			t.Fatal(info)
		}
	}
	if count != 2*n+n/10 {
		t.Fatalf("%d step calls, want %d", count, 2*n+n/10)
	}
}

func TestRetryPolicyDelay(t *testing.T) {
	p := RetryPolicy{Backoff: 100 * time.Millisecond, MaxBackoff: time.Second}
	want := []time.Duration{100, 200, 400, 800, 1000, 1000}
	for i, w := range want {
		if got := p.delay(i + 1); got != w*time.Millisecond {
			t.Errorf("attempt %d: %v, want %v", i+1, got, w*time.Millisecond)
		}
	}
	q := RetryPolicy{Backoff: time.Second, Multiplier: 3}
	if q.delay(3) != 9*time.Second || q.attempts() != DefaultRetry.MaxAttempts {
		t.Fatal(q.delay(3), q.attempts())
	}
	huge := RetryPolicy{Backoff: time.Hour}
	if d := huge.delay(10000); d <= 0 || d > 366*24*time.Hour {
		t.Fatalf("no overflow: %v", d)
	}
}

func TestOpenCreatesAndLocksJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	e, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, Options{}); !errors.Is(err, ErrLocked) {
		t.Fatalf("second open: %v", err)
	}
	e.Close()
	e2, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("open after close: %v", err)
	}
	e2.Close()
}

// countingStore counts appends and the records in them.
type countingStore struct {
	*MemStore
	mu      sync.Mutex
	appends int
}

func (c *countingStore) Append(frames []byte) error {
	c.mu.Lock()
	c.appends++
	c.mu.Unlock()
	return c.MemStore.Append(frames)
}

// TestGroupCommitSharesFlushes: records committed at the same time go out
// in fewer appends than records, every record lands, and the journal reads
// back to the same runs.
func TestGroupCommitSharesFlushes(t *testing.T) {
	cs := &countingStore{MemStore: NewMemStore(nil)}
	// Each append takes a moment, as a flush to disk does, so records queue
	// up behind it.
	cs.MemStore.FailAppend = func([]byte) error { time.Sleep(200 * time.Microsecond); return nil }
	e, err := New(cs, Options{Workers: 32})
	if err != nil {
		t.Fatal(err)
	}
	e.Register(Flow{Name: "f", Steps: []Step{
		{Name: "a", Do: func(c *Ctx) error { time.Sleep(time.Millisecond); return nil }},
		{Name: "b", Do: func(c *Ctx) error { return nil }},
	}})
	const n = 300
	var wg sync.WaitGroup
	for g := 0; g < 30; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := g; i < n; i += 30 {
				id := fmt.Sprintf("r%03d", i)
				if _, err := e.Start("f", id, nil); err != nil {
					t.Error(err)
					return
				}
				wait(t, e, id)
			}
		}(g)
	}
	wg.Wait()
	e.Close()
	recs, _, _, err := decodeAll(cs.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != n*6 {
		t.Fatalf("%d records, want %d", len(recs), n*6)
	}
	if cs.appends*2 > len(recs) {
		t.Fatalf("%d appends for %d records: fewer than two records a flush", cs.appends, len(recs))
	}
	t.Logf("%d records in %d appends", len(recs), cs.appends)
	runs, err := Replay(recs)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range runs {
		if r.Status != Completed {
			t.Fatal(r)
		}
	}
}

// TestConcurrentStartsOfOneID: many callers starting the same ID at once
// start one run, and all get its ID back.
func TestConcurrentStartsOfOneID(t *testing.T) {
	e, _ := newMem(t, nil)
	var calls int32
	e.Register(Flow{Name: "f", Steps: []Step{{Name: "a", Do: func(c *Ctx) error { atomic.AddInt32(&calls, 1); return nil }}}})
	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			input := []byte("same")
			if i == 49 {
				input = []byte("other")
			}
			id, err := e.Start("f", "one", input)
			if i == 49 {
				if err == nil {
					// It got in first; then the others conflict instead.
					return
				}
				if !errors.Is(err, ErrConflict) {
					errs <- err
				}
				return
			}
			if err != nil && !errors.Is(err, ErrConflict) {
				errs <- err
			}
			if err == nil && id != "one" {
				errs <- fmt.Errorf("got id %q", id)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	wait(t, e, "one")
	if calls != 1 || len(e.List()) != 1 {
		t.Fatalf("%d calls, %d runs", calls, len(e.List()))
	}
}
