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

// MaxAttempts is the order flow's attempt limit for each step and undo.
const MaxAttempts = 10

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

// Ledger is an append-only file of effect lines: "do STEP RUN ATTEMPT",
// "undo STEP RUN ATTEMPT", or "try ship RUN ATTEMPT" for a shipping attempt
// that is about to be refused. Each line is written with one append and flushed
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
	// Attempts cut off by kills count toward MaxAttempts, so it leaves room
	// for several kills landing in one step on top of the flaky charge's two
	// failures.
	retry := flowchassis.RetryPolicy{MaxAttempts: MaxAttempts, Backoff: 2 * time.Millisecond, MaxBackoff: 20 * time.Millisecond}
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
				// The attempt leaves a trace before it fails for good, so the
				// checks can see if a step that failed for good ever runs
				// again.
				if err := l.write("try", c); err != nil {
					return err
				}
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
	GaveUp       int      `json:"gave_up"`           // runs whose step kept being cut off until MaxAttempts, so they compensated
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
	byRun := map[string]flowchassis.RunInfo{}
	for _, info := range runs {
		byRun[info.ID] = info
	}

	// Every attempt the journal records, with how it ended: done, failed,
	// failed for good, yielded to a shutdown, or cut off (a kill landed
	// before it ended).
	type attempt struct {
		kind, step string // kind is "do" or "undo"
		number     int
		end        string
		effects    int
	}
	attempts := map[string][]*attempt{}
	open := map[string]*attempt{}
	// Runs where one step was cut off by kills MaxAttempts times: the engine
	// gives up and compensates, as the contract says it must.
	gaveUp := map[string]bool{}
	gaveUpStep := map[string]string{} // the step it gave up on
	p := len(r.Problems)
	for i := range recs {
		rec := &recs[i]
		cur := open[rec.Run]
		switch rec.Type {
		case "begin", "ubegin":
			if cur != nil {
				cur.end = "cut"
				if cur.kind == "do" {
					r.Interrupted++
				} else {
					r.InterruptedU++
				}
			}
			kind := "do"
			if rec.Type == "ubegin" {
				kind = "undo"
			}
			for _, a := range attempts[rec.Run] {
				if a.kind == kind && a.step == rec.Step && a.end == "final" {
					problem("%s: %s %s began again after failing for good", rec.Run, kind, rec.Step)
				}
			}
			if rec.Attempt > MaxAttempts {
				problem("%s: %s %s attempt %d, over the limit of %d", rec.Run, kind, rec.Step, rec.Attempt, MaxAttempts)
			}
			a := &attempt{kind: kind, step: rec.Step, number: rec.Attempt}
			attempts[rec.Run] = append(attempts[rec.Run], a)
			open[rec.Run] = a
		case "done", "udone", "fail", "ufail", "yield", "uyield":
			if rec.Final && strings.HasPrefix(rec.Error, "gave up after") {
				gaveUp[rec.Run] = true
				gaveUpStep[rec.Run] = rec.Step
			}
			if cur == nil {
				// A final failure without an attempt is a crash loop giving
				// up; anything else is out of place.
				if (rec.Type == "fail" || rec.Type == "ufail") && rec.Final {
					break
				}
				problem("%s: %s record with no attempt under way", rec.Run, rec.Type)
				break
			}
			switch {
			case rec.Type == "done" || rec.Type == "udone":
				cur.end = "done"
			case rec.Type == "yield" || rec.Type == "uyield":
				cur.end = "yield"
			case rec.Final:
				cur.end = "final"
			default:
				cur.end = "fail"
				r.Retries++
			}
			delete(open, rec.Run)
		}
	}
	if len(open) > 0 {
		problem("%d runs still have an attempt under way", len(open))
	}
	// Within one step, attempt numbers only go up, except that an attempt
	// yielded to a shutdown is given back.
	for id, list := range attempts {
		last := map[string]*attempt{}
		for _, a := range list {
			k := a.kind + " " + a.step
			if prev := last[k]; prev != nil && a.number <= prev.number && prev.end != "yield" && prev.end != "done" {
				problem("%s: %s attempt %d after attempt %d", id, k, a.number, prev.number)
			}
			last[k] = a
		}
	}
	check("attempt numbers only go up, stay within the limit, and nothing runs again after failing for good", p)

	p = len(r.Problems)
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
		case flowchassis.Completed:
			r.Completed++
		case flowchassis.Compensated:
			r.Compensated++
		case flowchassis.Failed:
			r.Failed++
		default:
			r.Unfinished++
		}
	}
	check("every order started, is in the journal once, and finished", p)

	p = len(r.Problems)
	for i := 1; i <= n; i++ {
		id := RunID(i)
		want := flowchassis.Completed
		if ShipFails(id) || gaveUp[id] {
			want = flowchassis.Compensated
		}
		if got := byRun[id].Status; got != want {
			problem("%s ended %s, not %s", id, got, want)
		}
	}
	r.GaveUp = len(gaveUp)
	check("each order ended the way its own rules say: shipped orders completed, refused ones and crash loops that gave up compensated", p)

	// Every ledger line belongs to an attempt the journal recorded: same
	// run, step and attempt number. Each attempt leaves at most one line.
	p = len(r.Problems)
	type key struct{ kind, step, run string }
	lines := map[key][]*attempt{}
	for _, e := range effects {
		kind := e.Kind
		if kind == "try" {
			kind = "do"
		}
		var match *attempt
		for _, a := range attempts[e.Run] {
			if a.kind == kind && a.step == e.Step && a.number == e.Attempt {
				match = a // the last one with that number, if a yield repeated it
			}
		}
		if match == nil {
			problem("%s: ledger has %s %s attempt %d, which the journal never began", e.Run, e.Kind, e.Step, e.Attempt)
			continue
		}
		match.effects++
		if match.effects > 1 {
			problem("%s: %s %s attempt %d left %d effects", e.Run, e.Kind, e.Step, e.Attempt, match.effects)
		}
		k := key{e.Kind, e.Step, e.Run}
		lines[k] = append(lines[k], match)
	}
	check("every effect in the ledger belongs to an attempt the journal recorded, one effect at most per attempt", p)

	// An effect repeats only when a kill cut its attempt off after the effect
	// and before the checkpoint: every line but the last of each effect must
	// belong to a cut-off attempt.
	p = len(r.Problems)
	for k, list := range lines {
		for _, a := range list[:len(list)-1] {
			r.Repeated++
			if k.kind == "undo" {
				r.RepeatedUndo++
			}
			if a.end != "cut" && a.end != "yield" {
				problem("%s: %s %s repeated, though its attempt %d ended %q", k.run, k.kind, k.step, a.number, a.end)
			}
		}
	}
	check("each repeated effect comes from an attempt a kill cut off between the effect and its checkpoint", p)

	p = len(r.Problems)
	for i := 1; i <= n; i++ {
		id := RunID(i)
		info := byRun[id]
		var wantDo, wantUndo []string
		refused := ShipFails(id)
		switch {
		case gaveUp[id]:
			// It stopped wherever the crash loop was: what it completed is
			// done, and the completed steps that have an undo are undone.
			wantDo = info.Done
			refused = refused && contains(info.Done, "receipt") && len(lines[key{"try", "ship", id}]) > 0
			for i := len(info.Done) - 1; i >= 0; i-- {
				if info.Done[i] == "reserve" || info.Done[i] == "charge" || info.Done[i] == "ship" {
					wantUndo = append(wantUndo, info.Done[i])
				}
			}
		case refused:
			wantDo = []string{"reserve", "charge", "receipt"}
			wantUndo = []string{"charge", "reserve"}
		default:
			wantDo = Steps
		}
		for _, s := range Steps {
			has := len(lines[key{"do", s, id}]) > 0
			// A step given up on never completed, so it isn't undone, but
			// attempts that kills cut off after their effect leave lines.
			if has && gaveUp[id] && s == gaveUpStep[id] {
				continue
			}
			if has != contains(wantDo, s) {
				problem("%s: ledger has do %s = %v, want %v", id, s, has, !has)
			}
			undone := len(lines[key{"undo", s, id}]) > 0
			if undone != contains(wantUndo, s) {
				problem("%s: ledger has undo %s = %v, want %v", id, s, undone, !undone)
			}
		}
		if tried := len(lines[key{"try", "ship", id}]) > 0; tried != refused {
			problem("%s: ledger has a refused shipping attempt = %v, want %v", id, tried, refused)
		}
		if !equal(info.Done, wantDo) {
			problem("%s: journal says done %v, want %v", id, info.Done, wantDo)
		}
		if !equal(info.Undone, wantUndo) {
			problem("%s: journal says undone %v, want %v", id, info.Undone, wantUndo)
		}
	}
	check("the ledger and the journal agree on what each order did and undid", p)

	p = len(r.Problems)
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
		for _, s := range []string{"charge", "reserve"} {
			u, ok := first[key{"undo", s, id}]
			if ok && u < first[key{"do", s, id}] {
				problem("%s: undo %s came before its do", id, s)
			}
		}
		if u, ok := first[key{"undo", "reserve", id}]; ok && u < first[key{"undo", "charge", id}] {
			problem("%s: reserve was undone before charge", id)
		}
	}
	check("steps happened in flow order, and undos newest first after their steps", p)

	r.OK = len(r.Problems) == 0 && r.Unfinished == 0 && r.Failed == 0
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
