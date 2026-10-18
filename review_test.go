// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

package flowchassis

// Tests for the defects an independent review found in the first draft of
// 0.1.0, each of which it reproduced. They stay as regression tests.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// journalOf encodes records as a store's frames.
func journalOf(t *testing.T, recs ...Record) []byte {
	t.Helper()
	var out []byte
	for i := range recs {
		recs[i].At = time.Now().UTC()
		f, err := encode(&recs[i])
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, f...)
	}
	return out
}

func strp(s string) *string { return &s }

// Cancel racing with Compact used to append a cancel record for a run the
// journal had dropped, panic, and leave a journal that never opened again.
func TestCancelRacingCompact(t *testing.T) {
	s := NewMemStore(nil)
	e, _ := New(s, Options{Workers: 8})
	e.Register(Flow{Name: "f", Steps: []Step{{Name: "a", Do: func(*Ctx) error { return nil }}}})
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				e.Compact(time.Now().Add(time.Hour))
			}
		}
	}()
	for i := 0; i < 400; i++ {
		id := fmt.Sprintf("r%d", i)
		if _, err := e.Start("f", id, nil); err != nil {
			t.Fatal(err)
		}
		if err := e.Cancel(id); err != nil && !errors.Is(err, ErrFinished) && !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	if e.Err() != nil {
		t.Fatal(e.Err())
	}
	e.Close()
	e2, err := New(NewMemStore(s.Bytes()), Options{})
	if err != nil {
		t.Fatalf("journal no longer opens: %v", err)
	}
	e2.Close()
}

// One flipped bit in an early record's length used to look like a torn
// tail, and opening the journal cut off everything after it.
func TestEarlyLengthDamageIsRefusedNotCut(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	e, _ := Open(path, Options{})
	e.Register(simple(&tape{}, "a", "b"))
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("r%d", i)
		e.Start("f", id, nil)
		wait(t, e, id)
	}
	e.Close()
	data, _ := os.ReadFile(path)
	first := int(binary.LittleEndian.Uint32(data[len(header):]))
	second := len(header) + frameSize + first
	data[second+2] ^= 0x01 // bit 16 of the second record's length
	os.WriteFile(path, data, 0o644)
	if _, err := Open(path, Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open gave %v", err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(after, data) {
		t.Fatalf("the journal was changed: %d bytes, was %d", len(after), len(data))
	}
	if _, _, err := ReadJournal(path); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

// A torn tail is cut off, and what was cut is kept beside the journal.
func TestTornTailIsKeptBeside(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	e, _ := Open(path, Options{})
	e.Register(simple(&tape{}, "a"))
	e.Start("f", "r", nil)
	wait(t, e, "r")
	e.Close()
	data, _ := os.ReadFile(path)
	cut := len(data) - 5
	os.WriteFile(path, data[:cut], 0o644)
	e, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	kept, err := os.ReadFile(path + ".torn")
	if err != nil {
		t.Fatal(err)
	}
	now, _ := os.ReadFile(path)
	if !bytes.Equal(append(now, kept...), data[:cut]) {
		t.Fatalf("journal (%d bytes) and .torn (%d bytes) don't add up to what was there (%d bytes)", len(now), len(kept), cut)
	}
}

// A crash between "failed for good" and "compensating" used to leave a run
// that ran its failed step again, and could complete. They are one record
// now: a journal that ends right after it compensates.
func TestFinalFailureSurvivesACrash(t *testing.T) {
	var calls int32
	f := Flow{Name: "f", Steps: []Step{
		{Name: "a", Do: func(*Ctx) error { return nil }, Undo: func(*Ctx) error { return nil }},
		{Name: "b", Do: func(*Ctx) error {
			if atomic.AddInt32(&calls, 1) == 1 {
				return Permanent(errors.New("card refused"))
			}
			return nil
		}},
	}}
	frames := journalOf(t,
		Record{Type: recStart, Run: "r", Flow: "f", Next: strp("a")},
		Record{Type: recBegin, Run: "r", Step: "a", Seq: 1, Attempt: 1},
		Record{Type: recDone, Run: "r", Step: "a", Seq: 1, Next: strp("b")},
		Record{Type: recBegin, Run: "r", Step: "b", Seq: 2, Attempt: 1},
		Record{Type: recFail, Run: "r", Step: "b", Attempt: 1, Error: "card refused", Final: true},
	)
	atomic.StoreInt32(&calls, 1)
	e, _ := New(NewMemStore(frames), Options{})
	defer e.Close()
	if info, _ := e.Get("r"); info.Status != Compensating {
		t.Fatalf("after replay: %s", info.Status)
	}
	e.Register(f)
	info := wait(t, e, "r")
	if info.Status != Compensated || atomic.LoadInt32(&calls) != 1 || info.Error != "step b: card refused" {
		t.Fatalf("%+v, b called %d more times", info, atomic.LoadInt32(&calls)-1)
	}
	// And the engine writes it as one record.
	s := NewMemStore(nil)
	e2, _ := New(s, Options{})
	atomic.StoreInt32(&calls, 0)
	e2.Register(f)
	e2.Start("f", "r", nil)
	wait(t, e2, "r")
	e2.Close()
	recs, _, _, _ := decodeAll(s.Bytes())
	for _, r := range recs {
		if r.Type == recComp {
			t.Fatal("a separate comp record was written after the final failure")
		}
	}
}

// The same for an undo that fails for good: the run ends failed in that
// record, and the undo isn't called again.
func TestFinalUndoFailureSurvivesACrash(t *testing.T) {
	var undos int32
	frames := journalOf(t,
		Record{Type: recStart, Run: "r", Flow: "f", Next: strp("a")},
		Record{Type: recBegin, Run: "r", Step: "a", Seq: 1, Attempt: 1},
		Record{Type: recDone, Run: "r", Step: "a", Seq: 1, Next: strp("b")},
		Record{Type: recBegin, Run: "r", Step: "b", Seq: 2, Attempt: 1},
		Record{Type: recFail, Run: "r", Step: "b", Attempt: 1, Error: "no", Final: true},
		Record{Type: recUBegin, Run: "r", Step: "a", Seq: 1, Attempt: 1},
		Record{Type: recUFail, Run: "r", Step: "a", Seq: 1, Attempt: 1, Error: "refund service down", Final: true},
	)
	e, _ := New(NewMemStore(frames), Options{})
	defer e.Close()
	e.Register(Flow{Name: "f", Steps: []Step{
		{Name: "a", Do: func(*Ctx) error { return nil }, Undo: func(*Ctx) error { atomic.AddInt32(&undos, 1); return nil }},
		{Name: "b", Do: func(*Ctx) error { return nil }},
	}})
	info := wait(t, e, "r")
	if info.Status != Failed || undos != 0 || info.Error != "undo a: refund service down" {
		t.Fatalf("%+v, undo called %d times", info, undos)
	}
}

// Attempts cut off by crashes count, so a step that brings the process down
// every time gives up at MaxAttempts and compensates, rather than loop.
func TestCrashLoopGivesUp(t *testing.T) {
	var calls, undos int32
	frames := journalOf(t,
		Record{Type: recStart, Run: "r", Flow: "f", Next: strp("a")},
		Record{Type: recBegin, Run: "r", Step: "a", Seq: 1, Attempt: 1},
		Record{Type: recDone, Run: "r", Step: "a", Seq: 1, Next: strp("b")},
		Record{Type: recBegin, Run: "r", Step: "b", Seq: 2, Attempt: 1},
		Record{Type: recBegin, Run: "r", Step: "b", Seq: 2, Attempt: 2},
	)
	e, _ := New(NewMemStore(frames), Options{})
	defer e.Close()
	e.Register(Flow{Name: "f", Steps: []Step{
		{Name: "a", Do: func(*Ctx) error { return nil }, Undo: func(*Ctx) error { atomic.AddInt32(&undos, 1); return nil }},
		{Name: "b", Retry: RetryPolicy{MaxAttempts: 2}, Do: func(*Ctx) error { atomic.AddInt32(&calls, 1); return nil }},
	}})
	info := wait(t, e, "r")
	if info.Status != Compensated || calls != 0 || undos != 1 || !strings.Contains(info.Error, "gave up after 2 attempts") {
		t.Fatalf("%+v: b called %d times, a undone %d", info, calls, undos)
	}
}

func TestUndoCrashLoopGivesUp(t *testing.T) {
	var undos int32
	frames := journalOf(t,
		Record{Type: recStart, Run: "r", Flow: "f", Next: strp("a")},
		Record{Type: recBegin, Run: "r", Step: "a", Seq: 1, Attempt: 1},
		Record{Type: recDone, Run: "r", Step: "a", Seq: 1, Next: strp("b")},
		Record{Type: recBegin, Run: "r", Step: "b", Seq: 2, Attempt: 1},
		Record{Type: recFail, Run: "r", Step: "b", Attempt: 1, Error: "no", Final: true},
		Record{Type: recUBegin, Run: "r", Step: "a", Seq: 1, Attempt: 1},
		Record{Type: recUBegin, Run: "r", Step: "a", Seq: 1, Attempt: 2},
	)
	e, _ := New(NewMemStore(frames), Options{})
	defer e.Close()
	e.Register(Flow{Name: "f", Steps: []Step{
		{Name: "a", Retry: RetryPolicy{MaxAttempts: 2}, Do: func(*Ctx) error { return nil }, Undo: func(*Ctx) error { atomic.AddInt32(&undos, 1); return nil }},
		{Name: "b", Do: func(*Ctx) error { return nil }},
	}})
	info := wait(t, e, "r")
	if info.Status != Failed || undos != 0 || !strings.Contains(info.Error, "gave up after 2 attempts") {
		t.Fatalf("%+v, undo called %d times", info, undos)
	}
}

// After a write fails, nothing queued behind it is written.
func TestNothingIsWrittenAfterAFailedWrite(t *testing.T) {
	s := NewMemStore(nil)
	var full atomic.Bool
	var after int32
	s.FailAppend = func([]byte) error {
		time.Sleep(100 * time.Microsecond)
		if full.Load() {
			if atomic.AddInt32(&after, 1) == 1 {
				return errors.New("no space left on device")
			}
			return nil // a disk that recovers must still not be written to
		}
		return nil
	}
	e, _ := New(s, Options{Workers: 16})
	defer e.Close()
	var steps int32
	e.Register(Flow{Name: "f", Steps: []Step{{Name: "a", Do: func(c *Ctx) error {
		if atomic.AddInt32(&steps, 1) == 40 {
			full.Store(true)
		}
		c.Next("a")
		return nil
	}}}})
	for i := 0; i < 16; i++ {
		e.Start("f", fmt.Sprintf("r%d", i), nil)
	}
	deadline := time.Now().Add(5 * time.Second)
	for e.Err() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if e.Err() == nil {
		t.Fatal("the engine didn't stop")
	}
	n := atomic.LoadInt32(&steps)
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt32(&after); got != 1 {
		t.Fatalf("%d appends reached the store after the failed one", got-1)
	}
	if atomic.LoadInt32(&steps) > n+16 {
		t.Fatalf("steps kept running after the engine stopped: %d then %d", n, steps)
	}
}

// A caller of Start isn't kept writing other runs' records: the writer has
// its own goroutine.
func TestStartIsNotHeldByBusyRuns(t *testing.T) {
	s := NewMemStore(nil)
	s.FailAppend = func([]byte) error { time.Sleep(500 * time.Microsecond); return nil }
	e, _ := New(s, Options{Workers: 4})
	defer e.Close()
	var stop atomic.Bool
	e.Register(Flow{Name: "loop", Steps: []Step{{Name: "a", Do: func(c *Ctx) error {
		if !stop.Load() {
			c.Next("a")
		} else {
			c.Next("")
		}
		return nil
	}}}})
	e.Register(Flow{Name: "f", Steps: []Step{{Name: "a", Do: func(*Ctx) error { return nil }}}})
	e.Start("loop", "busy1", nil)
	e.Start("loop", "busy2", nil)
	time.Sleep(50 * time.Millisecond)
	var worst time.Duration
	for i := 0; i < 20; i++ {
		t0 := time.Now()
		if _, err := e.Start("f", fmt.Sprintf("q%d", i), nil); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(t0); d > worst {
			worst = d
		}
	}
	stop.Store(true)
	if worst > 250*time.Millisecond {
		t.Fatalf("a Start took %v", worst)
	}
}

// Wait on a finished run returns it, not ErrClosed, after Close.
func TestWaitOnAFinishedRunAfterClose(t *testing.T) {
	e, _ := newMem(t, nil)
	e.Register(simple(&tape{}, "a"))
	e.Start("f", "r", nil)
	wait(t, e, "r")
	e.Close()
	for i := 0; i < 200; i++ {
		info, err := e.Wait(context.Background(), "r")
		if err != nil || info.Status != Completed {
			t.Fatalf("call %d: %v %s", i, err, info.Status)
		}
	}
}

// A step that completes while Close waits for it is recorded, and doesn't
// run again.
func TestCloseRecordsAStepThatCompletes(t *testing.T) {
	s := NewMemStore(nil)
	var calls int32
	entered := make(chan struct{})
	f := Flow{Name: "f", Steps: []Step{
		{Name: "a", Do: func(c *Ctx) error {
			if atomic.AddInt32(&calls, 1) == 1 {
				close(entered)
				<-c.Done()
			}
			return nil // completes anyway
		}},
		{Name: "b", Do: func(*Ctx) error { return nil }},
	}}
	e, _ := New(s, Options{})
	e.Register(f)
	e.Start("f", "r", nil)
	<-entered
	e.Close()
	e2, _ := New(NewMemStore(s.Bytes()), Options{})
	defer e2.Close()
	e2.Register(f)
	if info := wait(t, e2, "r"); info.Status != Completed || calls != 1 {
		t.Fatalf("%s, a ran %d times", info.Status, calls)
	}
}

// Compaction keeps each step's state in a record of its own, so a run whose
// steps each left several megabytes still compacts, and its undos still see
// their own state.
func TestCompactionWithLargeStates(t *testing.T) {
	s := NewMemStore(nil)
	big := func(c byte) []byte { return bytes.Repeat([]byte{c}, 5<<20) }
	var mu sync.Mutex
	var seen []string
	undo := func(c *Ctx) error {
		v, _ := c.Get(c.Step)
		mu.Lock()
		seen = append(seen, fmt.Sprintf("%s:%d:%c", c.Step, len(v), v[0]))
		mu.Unlock()
		return nil
	}
	gate := make(chan struct{})
	f := Flow{Name: "f", Steps: []Step{
		{Name: "a", Do: func(c *Ctx) error { c.Set("a", big('A')); return nil }, Undo: undo},
		{Name: "b", Do: func(c *Ctx) error { c.Set("b", big('B')); return nil }, Undo: undo},
		{Name: "c", Do: func(c *Ctx) error { c.Delete("a"); c.Delete("b"); c.Set("c", big('C')); return nil }, Undo: undo},
		{Name: "d", Do: func(c *Ctx) error {
			select {
			case <-gate:
				return Permanent(errors.New("no"))
			case <-c.Done():
				return c.Err()
			}
		}},
	}}
	e, _ := New(s, Options{})
	e.Register(f)
	e.Start("f", "r", nil)
	for {
		if info, _ := e.Get("r"); info.Step == "d" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err := e.Compact(time.Time{}); err != nil {
		t.Fatal(err)
	}
	e.Close()
	e2, err := New(NewMemStore(s.Bytes()), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	close(gate)
	e2.Register(f)
	info := wait(t, e2, "r")
	want := []string{"c:5242880:C", "b:5242880:B", "a:5242880:A"}
	if info.Status != Compensated || !reflect.DeepEqual(seen, want) {
		t.Fatalf("%s %v", info.Status, seen)
	}
}

// A crash while a new journal's header was written leaves a journal that
// opens as empty, keeping the stray bytes beside it.
func TestHeaderCutShortOpens(t *testing.T) {
	for name, b := range map[string][]byte{"part of the header": header[:4], "zeros": make([]byte, 8)} {
		path := filepath.Join(t.TempDir(), "j")
		os.WriteFile(path, b, 0o644)
		e, err := Open(path, Options{})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		e.Register(simple(&tape{}, "a"))
		e.Start("f", "r", nil)
		wait(t, e, "r")
		e.Close()
		if _, err := os.Stat(path + ".torn"); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if runs := mustRead(t, path); runs["r"].Status != Completed {
			t.Fatal(name, runs)
		}
	}
}

// A step's policy fills the fields it leaves at zero from the defaults, so
// RetryPolicy{MaxAttempts: 5} still waits between attempts.
func TestRetryPolicyFieldsDefaultOneByOne(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 5}.or(DefaultRetry)
	if p.MaxAttempts != 5 || p.Backoff != DefaultRetry.Backoff || p.Multiplier != DefaultRetry.Multiplier || p.MaxBackoff != 0 {
		t.Fatalf("%+v", p)
	}
	if (RetryPolicy{}).or(DefaultRetry) != DefaultRetry {
		t.Fatal("a zero policy should be the default")
	}
}
