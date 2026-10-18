// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

// Package demo is the crash test behind FlowChassis's main promise. An order
// flow (reserve stock, charge the card, write a receipt, ship, notify) writes
// every outside effect to a ledger file, standing in for the services a real
// flow would call. A worker process runs a batch of orders; a supervisor
// kills it with SIGKILL (TerminateProcess on Windows) at random moments and
// starts it again, until the worker gets through the batch. Then Verify
// checks every run against the ledger.
package demo

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/flowchassis/flowchassis"
)

const (
	JournalName = "orders.journal"
	LedgerName  = "effects.ledger"
	FlowName    = "order"
)

// Steps of the order flow, in order. Receipt has no Undo: compensation passes
// over it.
var Steps = []string{"reserve", "charge", "receipt", "ship", "notify"}

// RunID names the i-th order of a batch, from 1.
func RunID(i int) string { return fmt.Sprintf("order-%04d", i) }

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// ShipFails reports whether the order is one whose shipping is refused, so
// its run must end compensated. About one order in four.
func ShipFails(run string) bool { return hash(run)%4 == 0 }

// ChargeFlaky reports whether the order's card charge fails on its first two
// attempts, so its run must retry. About one order in five.
func ChargeFlaky(run string) bool { return hash(run)%5 == 1 }

// Ledger is an append-only file of effect lines: "do STEP RUN ATTEMPT" or
// "undo STEP RUN ATTEMPT". Each line is written with one append and flushed
// before the step returns.
type Ledger struct{ f *os.File }

// OpenLedger opens the ledger for appends.
func OpenLedger(path string) (*Ledger, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &Ledger{f: f}, nil
}

func (l *Ledger) write(kind string, c *flowchassis.Ctx) error {
	if _, err := fmt.Fprintf(l.f, "%s %s %s %d\n", kind, c.Step, c.Run, c.Attempt); err != nil {
		return err
	}
	return l.f.Sync()
}

// Close closes the ledger.
func (l *Ledger) Close() error { return l.f.Close() }

// Effect is one ledger line.
type Effect struct {
	Kind, Step, Run string
	Attempt         int
}

// ReadLedger reads the ledger. A last line cut short by a kill is skipped.
func ReadLedger(path string) ([]Effect, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Effect
	lines := strings.Split(string(data), "\n")
	for _, line := range lines[:len(lines)-1] { // the part after the last newline is unfinished
		var e Effect
		if _, err := fmt.Sscanf(line, "%s %s %s %d", &e.Kind, &e.Step, &e.Run, &e.Attempt); err != nil {
			return nil, fmt.Errorf("ledger line %q: %v", line, err)
		}
		out = append(out, e)
	}
	return out, nil
}

func pauseFor(c *flowchassis.Ctx, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-c.Done():
		return c.Err()
	}
}

// OrderFlow returns the order flow, writing its effects to l. Each step
// pauses for pause before its effect, which widens the windows a kill can
// land in.
func OrderFlow(l *Ledger, pause time.Duration) flowchassis.Flow {
	do := func(step string, extra func(c *flowchassis.Ctx) error) func(c *flowchassis.Ctx) error {
		return func(c *flowchassis.Ctx) error {
			if err := pauseFor(c, pause); err != nil {
				return err
			}
			if extra != nil {
				if err := extra(c); err != nil {
					return err
				}
			}
			if err := l.write("do", c); err != nil {
				return err
			}
			c.Set(step, []byte(c.Key))
			return nil
		}
	}
	undo := func(step string) func(c *flowchassis.Ctx) error {
		return func(c *flowchassis.Ctx) error {
			if _, ok := c.Get(step); !ok {
				return flowchassis.Permanent(fmt.Errorf("undo %s sees no %s in its state", step, step))
			}
			if err := pauseFor(c, pause); err != nil {
				return err
			}
			return l.write("undo", c)
		}
	}
	retry := flowchassis.RetryPolicy{MaxAttempts: 4, Backoff: 2 * time.Millisecond, MaxBackoff: 20 * time.Millisecond}
	return flowchassis.Flow{Name: FlowName, Steps: []flowchassis.Step{
		{Name: "reserve", Do: do("reserve", nil), Undo: undo("reserve"), Retry: retry},
		{Name: "charge", Do: do("charge", func(c *flowchassis.Ctx) error {
			if ChargeFlaky(c.Run) && c.Attempt <= 2 {
				return errors.New("card network timed out")
			}
			return nil
		}), Undo: undo("charge"), Retry: retry},
		{Name: "receipt", Do: do("receipt", nil), Retry: retry},
		{Name: "ship", Do: do("ship", func(c *flowchassis.Ctx) error {
			if ShipFails(c.Run) {
				return flowchassis.Permanent(errors.New("carrier refused the address"))
			}
			return nil
		}), Undo: undo("ship"), Retry: retry},
		{Name: "notify", Do: do("notify", nil), Retry: retry},
	}}
}

// Worker opens the journal in dir, starts orders 1 to n (Start is
// idempotent, so after a restart it only starts the ones still missing),
// and waits for all of them to finish.
func Worker(dir string, n, workers int, pause time.Duration) error {
	l, err := OpenLedger(filepath.Join(dir, LedgerName))
	if err != nil {
		return err
	}
	defer l.Close()
	e, err := flowchassis.Open(filepath.Join(dir, JournalName), flowchassis.Options{Workers: workers})
	if err != nil {
		return err
	}
	defer e.Close()
	if err := e.Register(OrderFlow(l, pause)); err != nil {
		return err
	}
	for i := 1; i <= n; i++ {
		if _, err := e.Start(FlowName, RunID(i), []byte(fmt.Sprintf(`{"order":%d}`, i))); err != nil {
			return err
		}
	}
	for i := 1; i <= n; i++ {
		if _, err := e.Wait(context.Background(), RunID(i)); err != nil {
			return err
		}
	}
	return e.Close()
}

// Report is what a crash test found. Every number in it is counted from the
// journal and the ledger after the last process exited.
type Report struct {
	Runs         int      `json:"runs"`
	Completed    int      `json:"completed"`
	Compensated  int      `json:"compensated"`
	Failed       int      `json:"failed"`
	Unfinished   int      `json:"unfinished"`
	Kills        int      `json:"kills"`
	Workers      int      `json:"workers"` // runs a worker process executes at once
	Processes    int      `json:"processes"`
	Effects      int      `json:"effects"`           // ledger lines
	Repeated     int      `json:"repeated"`          // effects done again after a kill landed between the effect and its checkpoint
	RepeatedUndo int      `json:"repeated_undos"`    // the part of Repeated that were undos
	Interrupted  int      `json:"interrupted_steps"` // step attempts a kill cut off: begun in the journal, never done or failed
	InterruptedU int      `json:"interrupted_undos"` // the same for undo attempts
	Retries      int      `json:"retries"`           // failed attempts that were retried
	JournalBytes int64    `json:"journal_bytes"`     // journal size at the end
	Records      int      `json:"journal_records"`   // journal records at the end
	Seconds      float64  `json:"seconds"`
	Checks       []string `json:"checks"`
	Problems     []string `json:"problems"`
	OK           bool     `json:"ok"`
	Version      string   `json:"flowchassis"`
	Go           string   `json:"go"`
	OS           string   `json:"os"`
	Arch         string   `json:"arch"`
	CPUs         int      `json:"cpus"`
}

// Supervise runs worker processes made by cmd in dir until one exits 0,
// killing each of the first kills processes after a random delay between
// minDelay and maxDelay. It returns the number of kills and processes.
func Supervise(cmd func() *exec.Cmd, kills int, minDelay, maxDelay time.Duration, seed int64, logf func(string, ...any)) (killed, processes int, err error) {
	rng := rand.New(rand.NewSource(seed))
	for {
		c := cmd()
		c.Stderr = os.Stderr
		if err := c.Start(); err != nil {
			return killed, processes, err
		}
		processes++
		exited := make(chan error, 1)
		go func() { exited <- c.Wait() }()
		if killed < kills {
			delay := minDelay + time.Duration(rng.Int63n(int64(maxDelay-minDelay)+1))
			select {
			case err := <-exited:
				if err == nil {
					return killed, processes, nil
				}
				return killed, processes, fmt.Errorf("worker process %d failed: %v", processes, err)
			case <-time.After(delay):
				c.Process.Kill()
				<-exited
				killed++
				if logf != nil && killed%10 == 0 {
					logf("killed %d worker processes", killed)
				}
				continue
			}
		}
		select {
		case err := <-exited:
			if err != nil {
				return killed, processes, fmt.Errorf("worker process %d failed: %v", processes, err)
			}
			return killed, processes, nil
		case <-time.After(10 * time.Minute):
			c.Process.Kill()
			return killed, processes, errors.New("the last worker process didn't finish in 10 minutes")
		}
	}
}

// Verify checks the journal and ledger in dir after a batch of n orders,
// and fills in the report.
func Verify(dir string, n int, r *Report) {
	r.Runs = n
	r.Version = flowchassis.Version
	r.Go, r.OS, r.Arch, r.CPUs = runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU()
	problem := func(format string, args ...any) {
		if len(r.Problems) < 50 {
			r.Problems = append(r.Problems, fmt.Sprintf(format, args...))
		}
	}
	check := func(name string, before int) {
		if len(r.Problems) == before {
			r.Checks = append(r.Checks, name)
		}
	}

	jpath := filepath.Join(dir, JournalName)
	recs, torn, err := flowchassis.ReadJournal(jpath)
	if err != nil {
		problem("reading the journal: %v", err)
		return
	}
	if torn {
		problem("the journal ends in a torn write after the last process exited cleanly")
	}
	if st, err := os.Stat(jpath); err == nil {
		r.JournalBytes = st.Size()
	}
	r.Records = len(recs)
	runs, err := flowchassis.Replay(recs)
	if err != nil {
		problem("replaying the journal: %v", err)
		return
	}
	effects, err := ReadLedger(filepath.Join(dir, LedgerName))
	if err != nil {
		problem("reading the ledger: %v", err)
		return
	}
	r.Effects = len(effects)
	open := map[string]string{} // run -> type of its attempt under way
	for i := range recs {
		rec := &recs[i]
		switch rec.Type {
		case "fail":
			if !rec.Final {
				r.Retries++
			}
			delete(open, rec.Run)
		case "begin", "ubegin":
			if t, ok := open[rec.Run]; ok {
				if t == "begin" {
					r.Interrupted++
				} else {
					r.InterruptedU++
				}
			}
			open[rec.Run] = rec.Type
		case "done", "udone", "ufail":
			delete(open, rec.Run)
		}
	}

	byRun := map[string]RunInfoLite{}
	for _, info := range runs {
		byRun[info.ID] = RunInfoLite{Status: string(info.Status), Done: info.Done, Undone: info.Undone}
	}

	p := len(r.Problems)
	if len(runs) != n {
		problem("the journal holds %d runs, not %d", len(runs), n)
	}
	for i := 1; i <= n; i++ {
		id := RunID(i)
		info, ok := byRun[id]
		if !ok {
			problem("%s is missing from the journal", id)
			continue
		}
		switch info.Status {
		case "completed":
			r.Completed++
		case "compensated":
			r.Compensated++
		case "failed":
			r.Failed++
		default:
			r.Unfinished++
		}
	}
	check("every order started, is in the journal once, and finished", p)

	p = len(r.Problems)
	for i := 1; i <= n; i++ {
		id := RunID(i)
		want := "completed"
		if ShipFails(id) {
			want = "compensated"
		}
		if got := byRun[id].Status; got != want {
			problem("%s ended %s, not %s", id, got, want)
		}
	}
	check("each order ended the way its own rules say: shipped orders completed, refused ones compensated", p)

	// What the ledger says happened, per run and step: distinct effects
	// (what an idempotent service would keep) and how many lines (attempts
	// that got as far as the effect).
	type key struct{ kind, step, run string }
	count := map[key]int{}
	order := map[key]int{} // position of the last line, to check undo came after do
	for i, e := range effects {
		k := key{e.Kind, e.Step, e.Run}
		count[k]++
		order[k] = i
	}
	for k, c := range count {
		if c > 1 {
			r.Repeated += c - 1
			if k.kind == "undo" {
				r.RepeatedUndo += c - 1
			}
		}
	}

	p = len(r.Problems)
	for i := 1; i <= n; i++ {
		id := RunID(i)
		info := byRun[id]
		var wantDo, wantUndo []string
		if ShipFails(id) {
			wantDo = []string{"reserve", "charge", "receipt"}
			wantUndo = []string{"charge", "reserve"}
		} else {
			wantDo = Steps
		}
		for _, s := range Steps {
			has := count[key{"do", s, id}] > 0
			if has != contains(wantDo, s) {
				problem("%s: ledger has do %s = %v, want %v", id, s, has, !has)
			}
			undone := count[key{"undo", s, id}] > 0
			if undone != contains(wantUndo, s) {
				problem("%s: ledger has undo %s = %v, want %v", id, s, undone, !undone)
			}
			if undone && order[key{"undo", s, id}] < order[key{"do", s, id}] {
				problem("%s: undo %s was written before its do", id, s)
			}
		}
		if !equal(info.Done, wantDo) {
			problem("%s: journal says done %v, want %v", id, info.Done, wantDo)
		}
		if !equal(info.Undone, wantUndo) {
			problem("%s: journal says undone %v, want %v", id, info.Undone, wantUndo)
		}
	}
	check("every effect the journal records as done is in the ledger, and every ledger effect is in the journal", p)
	check("compensation undid exactly the completed steps with an undo, newest first, after they were done", p)

	p = len(r.Problems)
	// Within one run, effects must follow the flow's order: no step's first
	// effect before an earlier step's first effect.
	first := map[key]int{}
	for i, e := range effects {
		k := key{e.Kind, e.Step, e.Run}
		if _, ok := first[k]; !ok {
			first[k] = i
		}
	}
	for i := 1; i <= n; i++ {
		id := RunID(i)
		last := -1
		for _, s := range Steps {
			pos, ok := first[key{"do", s, id}]
			if !ok {
				break
			}
			if pos < last {
				problem("%s: %s happened before the step ahead of it", id, s)
			}
			last = pos
		}
	}
	check("steps of each run happened in flow order", p)

	p = len(r.Problems)
	w := r.Workers
	if w < 1 {
		w = 1
	}
	if r.Repeated > r.Kills*w {
		problem("%d repeated effects from %d kills of processes running %d orders at once: more than one per order a kill interrupted", r.Repeated, r.Kills, w)
	}
	check("an effect is repeated only when a kill interrupted its step, at most once per order in flight", p)

	r.OK = len(r.Problems) == 0 && r.Unfinished == 0 && r.Failed == 0
}

// RunInfoLite is the part of a run Verify looks at.
type RunInfoLite struct {
	Status string
	Done   []string
	Undone []string
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Summary is a one-line account of a report.
func (r *Report) Summary() string {
	verdict := "PASS"
	if !r.OK {
		verdict = "FAIL"
	}
	return fmt.Sprintf("%s: %d runs (%d completed, %d compensated, %d failed, %d unfinished) through %d kills and %d processes; kills cut off %d step and %d undo attempts; %d effects, %d repeated after a kill; %d checks passed, %d problems",
		verdict, r.Runs, r.Completed, r.Compensated, r.Failed, r.Unfinished, r.Kills, r.Processes, r.Interrupted, r.InterruptedU, r.Effects, r.Repeated, len(r.Checks), len(r.Problems))
}

// Expected counts how many of n orders should complete and be compensated.
func Expected(n int) (completed, compensated int) {
	for i := 1; i <= n; i++ {
		if ShipFails(RunID(i)) {
			compensated++
		} else {
			completed++
		}
	}
	return
}
