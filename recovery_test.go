// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

package flowchassis

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestResumeAfterClose closes an engine with a step under way and opens the
// journal again: the run resumes at that step, which runs again as the same
// attempt (Close doesn't use one up), and the earlier steps don't run again.
func TestResumeAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	tp := &tape{}
	entered := make(chan struct{})
	f := simple(tp, "a", "b", "c")
	first := true
	f.Steps[1].Do = func(c *Ctx) error {
		if first {
			first = false
			close(entered)
			<-c.Done()
			return c.Err()
		}
		tp.add("do b attempt " + itoa(c.Attempt))
		return nil
	}
	e, err := Open(path, Options{Retry: fast})
	if err != nil {
		t.Fatal(err)
	}
	e.Register(f)
	e.Start("f", "r", []byte("in"))
	<-entered
	e.Close()

	info := mustRead(t, path)["r"]
	if info.Status != Running || info.Step != "b" || info.Attempt != 0 {
		t.Fatalf("journal after close: %+v", info)
	}

	e2, err := Open(path, Options{Retry: fast})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if info, _ := e2.Get("r"); info.Status != Running {
		t.Fatalf("before register: %s", info.Status)
	}
	e2.Register(f)
	info = wait(t, e2, "r")
	if info.Status != Completed {
		t.Fatal(info.Status)
	}
	if got, want := tp.get(), []string{"do a", "do b attempt 1", "do c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

// TestRunsWaitForTheirFlow: runs in the journal whose flow isn't registered
// stay as they are until it is.
func TestRunsWaitForTheirFlow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	tp := &tape{}
	e, _ := Open(path, Options{})
	e.Register(simple(tp, "a"))
	block := make(chan struct{})
	e.Register(Flow{Name: "slow", Steps: []Step{{Name: "a", Do: func(c *Ctx) error {
		select {
		case <-block:
		case <-c.Done():
		}
		return c.Err()
	}}}})
	e.Start("slow", "s", nil)
	e.Close()
	close(block)

	e2, _ := Open(path, Options{})
	defer e2.Close()
	e2.Register(simple(tp, "a"))
	time.Sleep(30 * time.Millisecond)
	if info, _ := e2.Get("s"); info.Status != Pending && info.Status != Running {
		t.Fatalf("ran without its flow: %s", info.Status)
	}
	e2.Register(Flow{Name: "slow", Steps: []Step{{Name: "a", Do: func(c *Ctx) error { return nil }}}})
	if info := wait(t, e2, "s"); info.Status != Completed {
		t.Fatal(info.Status)
	}
}

// TestRetryDelaySurvivesRestart: a run waiting to retry keeps waiting after
// a restart, until the time the journal recorded.
func TestRetryDelaySurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	var calls []time.Time
	f := Flow{Name: "f", Steps: []Step{{Name: "a",
		Retry: RetryPolicy{MaxAttempts: 2, Backoff: 300 * time.Millisecond},
		Do: func(c *Ctx) error {
			calls = append(calls, time.Now())
			if c.Attempt == 1 {
				return errors.New("later")
			}
			return nil
		}}}}
	e, _ := Open(path, Options{})
	e.Register(f)
	e.Start("f", "r", nil)
	for {
		if info, _ := e.Get("r"); info.Status == Retrying {
			break
		}
		time.Sleep(time.Millisecond)
	}
	e.Close()
	e2, _ := Open(path, Options{})
	defer e2.Close()
	e2.Register(f)
	wait(t, e2, "r")
	if len(calls) != 2 || calls[1].Sub(calls[0]) < 300*time.Millisecond {
		t.Fatalf("%d calls, gap %v", len(calls), calls[len(calls)-1].Sub(calls[0]))
	}
}

// TestCompensationResumesAfterRestart: closing during an undo leaves the
// undo to run again on the next open, and the steps undone already aren't
// undone twice.
func TestCompensationResumesAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	tp := &tape{}
	entered := make(chan struct{})
	block := true
	f := simple(tp, "a", "b", "c")
	f.Steps[0].Undo = func(c *Ctx) error {
		if block {
			block = false
			close(entered)
			<-c.Done()
			return c.Err()
		}
		tp.add("undo a attempt " + itoa(c.Attempt))
		return nil
	}
	f.Steps = append(f.Steps, Step{Name: "d", Do: func(c *Ctx) error { return Permanent(errors.New("no")) }})
	e, _ := Open(path, Options{})
	e.Register(f)
	e.Start("f", "r", nil)
	<-entered
	e.Close()
	if info := mustRead(t, path)["r"]; info.Status != Compensating || info.Step != "a" {
		t.Fatalf("%+v", info)
	}
	e2, _ := Open(path, Options{})
	defer e2.Close()
	e2.Register(f)
	info := wait(t, e2, "r")
	want := []string{"do a", "do b", "do c", "undo c", "undo b", "undo a attempt 1"}
	if got := tp.get(); !reflect.DeepEqual(got, want) || info.Status != Compensated {
		t.Fatalf("got %v want %v (%s)", got, want, info.Status)
	}
}

// TestCancelSurvivesRestart: a cancel recorded before a crash is honored
// after it.
func TestCancelSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	tp := &tape{}
	entered := make(chan struct{})
	f := simple(tp, "a", "b")
	gate := true
	f.Steps[1].Do = func(c *Ctx) error {
		if gate {
			gate = false
			close(entered)
			<-c.Done()
			return c.Err()
		}
		tp.add("b ran again")
		return nil
	}
	s := NewMemStore(nil)
	e, _ := New(s, Options{})
	e.Register(f)
	e.Start("f", "r", nil)
	<-entered
	// Write the cancel record as Cancel would, then stop as a crash would,
	// before the step sees it.
	e.commit(&Record{Type: recCancel, Run: "r"})
	e.Close()

	e2, _ := New(NewMemStore(s.Bytes()), Options{})
	defer e2.Close()
	e2.Register(f)
	info := wait(t, e2, "r")
	if got := tp.get(); info.Status != Compensated || !reflect.DeepEqual(got, []string{"do a", "undo a"}) {
		t.Fatalf("%s %v", info.Status, got)
	}
	_ = path
}

// TestTornWriteAtEveryByte cuts the journal at every byte of its last
// record. Each cut opens, drops the torn record, and shows the run as it
// was before that record; the next append lands cleanly after the cut.
func TestTornWriteAtEveryByte(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "j")
	tp := &tape{}
	e, _ := Open(path, Options{})
	e.Register(simple(tp, "a", "b"))
	e.Start("f", "r", []byte("input"))
	wait(t, e, "r")
	e.Close()
	full, _ := os.ReadFile(path)
	recs, _, err := ReadJournal(path)
	if err != nil || len(recs) < 4 {
		t.Fatal(len(recs), err)
	}
	// Where the last record starts.
	last, _ := encode(&recs[len(recs)-1])
	cut0 := len(full) - len(last)
	if !bytes.Equal(full[cut0:], last) {
		t.Fatal("re-encoding the last record differs from the file")
	}
	for cut := cut0; cut < len(full); cut++ {
		p := filepath.Join(dir, "cut")
		os.WriteFile(p, full[:cut], 0o644)
		got, torn, err := ReadJournal(p)
		if err != nil || len(got) != len(recs)-1 || (cut > cut0) != torn {
			t.Fatalf("cut at %d: %d records, torn %v, %v", cut, len(got), torn, err)
		}
		e, err := Open(p, Options{})
		if err != nil {
			t.Fatalf("cut at %d: %v", cut, err)
		}
		if info, _ := e.Get("r"); info.Status == Completed {
			t.Fatalf("cut at %d: the end record survived", cut)
		}
		e.Register(simple(&tape{}, "a", "b"))
		if info := wait(t, e, "r"); info.Status != Completed {
			t.Fatalf("cut at %d: %s", cut, info.Status)
		}
		e.Close()
		if _, torn, err := ReadJournal(p); torn || err != nil {
			t.Fatalf("cut at %d: after recovery torn %v, %v", cut, torn, err)
		}
	}
}

// TestZeroFilledTail: a file system can leave zeros after a crash; they are
// treated as a torn write.
func TestZeroFilledTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	e, _ := Open(path, Options{})
	e.Register(simple(&tape{}, "a"))
	e.Start("f", "r", nil)
	wait(t, e, "r")
	e.Close()
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	f.Write(make([]byte, 4096))
	f.Close()
	e, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if info, _ := e.Get("r"); info.Status != Completed {
		t.Fatal(info.Status)
	}
	e.Close()
}

// TestCorruptionIsRefused: damage before the end is not a torn write, and
// the journal isn't opened rather than quietly lose what follows.
func TestCorruptionIsRefused(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "j")
	e, _ := Open(path, Options{})
	e.Register(simple(&tape{}, "a", "b"))
	for _, id := range []string{"r1", "r2", "r3"} {
		e.Start("f", id, nil)
		wait(t, e, id)
	}
	e.Close()
	full, _ := os.ReadFile(path)
	cases := map[string]func([]byte){
		"flipped payload byte": func(b []byte) { b[len(header)+20] ^= 0xff },
		"flipped checksum":     func(b []byte) { b[len(header)+5] ^= 0x01 },
		"huge length":          func(b []byte) { b[len(header)+3] = 0x7f },
		"zero length":          func(b []byte) { copy(b[len(header):], []byte{0, 0, 0, 0}) },
	}
	for name, damage := range cases {
		b := append([]byte(nil), full...)
		damage(b)
		p := filepath.Join(dir, "bad")
		os.WriteFile(p, b, 0o644)
		if _, err := Open(p, Options{}); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: Open gave %v", name, err)
		}
		if _, _, err := ReadJournal(p); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: ReadJournal gave %v", name, err)
		}
		os.Remove(p + ".lock")
	}
	other := filepath.Join(dir, "other")
	os.WriteFile(other, []byte("hello world, this is some text"), 0o644)
	if _, err := Open(other, Options{}); !errors.Is(err, ErrCorrupt) {
		t.Errorf("not a journal: %v", err)
	}
	p := filepath.Join(dir, "future")
	os.WriteFile(p, []byte("FLOWCJ99"), 0o644)
	if _, err := Open(p, Options{}); err == nil || !strings.Contains(err.Error(), "isn't supported") {
		t.Errorf("future format: %v", err)
	}
}

// TestCompaction: compacting keeps every unfinished run exactly as it was,
// drops old finished runs on request, and runs carry on after reopening.
func TestCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	tp := &tape{}
	block := make(chan struct{})
	f := Flow{Name: "f", Steps: []Step{
		{Name: "a", Do: func(c *Ctx) error { c.Set("k", []byte(c.Run)); return nil }, Undo: func(c *Ctx) error { tp.add("undo a " + c.Run); return nil }},
		{Name: "b", Do: func(c *Ctx) error {
			if strings.HasPrefix(c.Run, "slow") {
				select {
				case <-block:
				case <-c.Done():
					return c.Err()
				}
			}
			if strings.HasPrefix(c.Run, "fail") {
				return Permanent(errors.New("no"))
			}
			return nil
		}},
	}}
	e, _ := Open(path, Options{})
	e.Register(f)
	for _, id := range []string{"done1", "done2", "fail1"} {
		e.Start("f", id, nil)
		wait(t, e, id)
	}
	e.Start("f", "slow1", []byte("x"))
	for {
		if info, _ := e.Get("slow1"); info.Step == "b" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	before, _ := os.Stat(path)
	if err := e.Compact(time.Time{}); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(path)
	if after.Size() >= before.Size() {
		t.Fatalf("compaction grew the journal: %d to %d", before.Size(), after.Size())
	}
	if len(e.List()) != 4 {
		t.Fatal(len(e.List()))
	}
	// Appends after compaction go to the new file.
	e.Start("f", "done3", nil)
	wait(t, e, "done3")
	if err := e.Compact(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, r := range e.List() {
		ids = append(ids, r.ID)
	}
	if !reflect.DeepEqual(ids, []string{"slow1"}) {
		t.Fatalf("after dropping finished runs: %v", ids)
	}
	e.Close()
	runs := mustRead(t, path)
	if len(runs) != 1 || runs["slow1"].Step != "b" || string(runs["slow1"].State["k"]) != "slow1" {
		t.Fatalf("%+v", runs)
	}
	e2, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	close(block)
	e2.Register(f)
	if info := wait(t, e2, "slow1"); info.Status != Completed || string(info.Input) != "x" {
		t.Fatalf("%+v", info)
	}
	// A run compacted mid-compensation can still undo its steps.
	e2.Start("f", "fail2", nil)
	wait(t, e2, "fail2")
	if got := tp.get(); !reflect.DeepEqual(got, []string{"undo a fail1", "undo a fail2"}) {
		t.Fatal(got)
	}
}

// TestCompactionMidCompensation compacts a run while it is part way
// through undoing its steps, then finishes it from the compacted journal.
func TestCompactionMidCompensation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	tp := &tape{}
	entered := make(chan struct{})
	gate := true
	f := simple(tp, "a", "b", "c")
	f.Steps[0].Undo = func(c *Ctx) error {
		if gate {
			gate = false
			close(entered)
			<-c.Done()
			return c.Err()
		}
		v, _ := c.Get("seen")
		tp.add("undo a " + string(v))
		return nil
	}
	f.Steps[0].Do = func(c *Ctx) error { c.Set("seen", []byte("A")); tp.add("do a"); return nil }
	f.Steps = append(f.Steps, Step{Name: "d", Do: func(c *Ctx) error { return Permanent(errors.New("no")) }})
	e, _ := Open(path, Options{})
	e.Register(f)
	e.Start("f", "r", nil)
	<-entered
	if err := e.Compact(time.Time{}); err != nil {
		t.Fatal(err)
	}
	e.Close()
	e2, _ := Open(path, Options{})
	defer e2.Close()
	e2.Register(f)
	info := wait(t, e2, "r")
	if got, want := tp.get(), []string{"do a", "do b", "do c", "undo c", "undo b", "undo a A"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if !reflect.DeepEqual(info.Undone, []string{"c", "b", "a"}) {
		t.Fatal(info.Undone)
	}
}

// TestWriteFailureStopsTheEngine: when the journal can't be written, the
// engine runs nothing more, so it never acts on a step it couldn't record.
// Reopening resumes from the last good record.
func TestWriteFailureStopsTheEngine(t *testing.T) {
	s := NewMemStore(nil)
	var full atomic.Bool
	s.FailAppend = func([]byte) error {
		if full.Load() {
			return errors.New("no space left on device")
		}
		return nil
	}
	tp := &tape{}
	f := simple(tp, "a", "b", "c")
	f.Steps[1].Do = func(c *Ctx) error { tp.add("do b"); full.Store(true); return nil }
	e, _ := New(s, Options{})
	e.Register(f)
	e.Start("f", "r", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := e.Wait(ctx, "r")
	if !errors.Is(err, ErrStopped) || !errors.Is(e.Err(), ErrStopped) || !strings.Contains(e.Err().Error(), "no space") {
		t.Fatalf("wait: %v, Err: %v", err, e.Err())
	}
	if _, err := e.Start("f", "r2", nil); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	e.Close()
	if got := tp.get(); !reflect.DeepEqual(got, []string{"do a", "do b"}) {
		t.Fatalf("ran on after the failed write: %v", got)
	}
	// The journal has b begun but not done: b runs again after reopening.
	e2, _ := New(NewMemStore(s.Bytes()), Options{})
	defer e2.Close()
	e2.Register(simple(tp, "a", "b", "c"))
	if info := wait(t, e2, "r"); info.Status != Completed {
		t.Fatal(info.Status)
	}
	if got := tp.get(); !reflect.DeepEqual(got, []string{"do a", "do b", "do b", "do c"}) {
		t.Fatal(got)
	}
}

// TestOversizedStateFailsTheStep: a step that leaves more state than a
// record can hold fails for good, instead of stopping the engine.
func TestOversizedStateFailsTheStep(t *testing.T) {
	e, _ := newMem(t, nil)
	e.Register(Flow{Name: "f", Steps: []Step{{Name: "a", Do: func(c *Ctx) error {
		c.Set("big", make([]byte, MaxRecord))
		return nil
	}}}})
	e.Start("f", "r", nil)
	info := wait(t, e, "r")
	if info.Status != Compensated || !strings.Contains(info.Error, "over the") {
		t.Fatalf("%+v", info)
	}
	if e.Err() != nil {
		t.Fatal(e.Err())
	}
}

// TestJournalIsReadableWhileOpen: tools can read the journal while an
// engine has it open and locked.
func TestJournalIsReadableWhileOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	e, _ := Open(path, Options{})
	defer e.Close()
	e.Register(simple(&tape{}, "a"))
	e.Start("f", "r", nil)
	wait(t, e, "r")
	if runs := mustRead(t, path); runs["r"].Status != Completed {
		t.Fatal(runs)
	}
}

func TestReplayRejectsNonsense(t *testing.T) {
	cases := [][]Record{
		{{Type: recDone, Run: "x"}},
		{{Type: recStart, Run: "x"}, {Type: recStart, Run: "x"}},
		{{Type: recStart, Run: "x"}, {Type: "what", Run: "x"}},
		{{Type: recStart, Run: "x"}, {Type: recUDone, Run: "x", Seq: 4}},
		{{Type: recSnapshot, Run: "x"}},
	}
	for i, recs := range cases {
		if _, err := Replay(recs); !errors.Is(err, ErrCorrupt) {
			t.Errorf("case %d: %v", i, err)
		}
	}
}

func mustRead(t *testing.T, path string) map[string]RunInfo {
	t.Helper()
	recs, _, err := ReadJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	runs, err := Replay(recs)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]RunInfo{}
	for _, r := range runs {
		out[r.ID] = r
	}
	return out
}

func itoa(n int) string { return string(rune('0' + n)) }
