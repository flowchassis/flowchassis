// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

package flowchassis_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/flowchassis/flowchassis"
	"github.com/flowchassis/flowchassis/internal/demo"
)

// The test binary doubles as the worker process the crash tests kill.
func TestMain(m *testing.M) {
	if dir := os.Getenv("FLOWCHASSIS_TEST_WORKER"); dir != "" {
		n, _ := strconv.Atoi(os.Getenv("FLOWCHASSIS_TEST_RUNS"))
		w, _ := strconv.Atoi(os.Getenv("FLOWCHASSIS_TEST_WORKERS"))
		p, _ := time.ParseDuration(os.Getenv("FLOWCHASSIS_TEST_PAUSE"))
		if err := demo.Worker(dir, n, w, p); err != nil {
			fmt.Fprintln(os.Stderr, "worker:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// crash runs a batch of orders in worker processes, kills the first kills
// of them at random moments, and checks every run against the ledger.
func crash(t *testing.T, runs, kills, workers int, pause time.Duration, seed int64) *demo.Report {
	t.Helper()
	dir := t.TempDir()
	start := time.Now()
	killed, procs, err := demo.Supervise(func() *exec.Cmd {
		c := exec.Command(os.Args[0], "-test.run=^$")
		c.Env = append(os.Environ(),
			"FLOWCHASSIS_TEST_WORKER="+dir,
			"FLOWCHASSIS_TEST_RUNS="+strconv.Itoa(runs),
			"FLOWCHASSIS_TEST_WORKERS="+strconv.Itoa(workers),
			"FLOWCHASSIS_TEST_PAUSE="+pause.String())
		return c
	}, kills, 5*time.Millisecond, 120*time.Millisecond, seed, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := &demo.Report{Kills: killed, Processes: procs, Workers: workers}
	r.Seconds = time.Since(start).Seconds()
	demo.Verify(dir, runs, r)
	t.Log(r.Summary())
	for _, p := range r.Problems {
		t.Error(p)
	}
	if !r.OK {
		t.Fatal("crash test failed")
	}
	if killed < kills {
		t.Fatalf("only %d of %d kills landed before the batch finished; lengthen the pause", killed, kills)
	}
	// Verify has checked each order's status against its own rules. A run
	// whose step kills cut off MaxAttempts times gives up and compensates,
	// which the contract requires; on a machine where processes start
	// slowly (Windows) kills can keep landing on the same few orders.
	wantDone, _ := demo.Expected(runs)
	if r.Completed+r.Compensated+r.Failed != runs || r.Failed != r.GaveUpUndo || r.Completed < wantDone-r.GaveUp-r.GaveUpUndo {
		t.Fatalf("%d completed, %d compensated, %d failed; %d gave up on a step, %d on an undo; want %d completed less those that gave up, and failed only from undos that gave up",
			r.Completed, r.Compensated, r.Failed, r.GaveUp, r.GaveUpUndo, wantDone)
	}
	if r.GaveUp > 0 || r.GaveUpUndo > 0 {
		t.Logf("%d orders gave up on a step and compensated, %d gave up on an undo and failed, after %d attempts cut off by kills", r.GaveUp, r.GaveUpUndo, demo.MaxAttempts)
	}
	// The journal checks clean with the reader tools use.
	if _, torn, err := flowchassis.ReadJournal(dir + "/" + demo.JournalName); err != nil || torn {
		t.Fatalf("journal: torn %v, %v", torn, err)
	}
	return r
}

// TestKilledWorkerOneAtATime: one order at a time, so each kill lands in a
// known run, in a step, an undo, or between them.
func TestKilledWorkerOneAtATime(t *testing.T) {
	crash(t, 60, 40, 1, 20*time.Millisecond, 1)
}

// TestKilledWorkerManyAtOnce: eight orders in flight in each process, so a
// kill interrupts several runs at once.
func TestKilledWorkerManyAtOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	crash(t, 300, 60, 8, 30*time.Millisecond, 2)
}

// TestKilledWorkerDuringCompensation uses a long pause, so kills land inside
// undos, and checks that some did: the journal shows undo attempts that
// began and never finished.
func TestKilledWorkerDuringCompensation(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	cut := 0
	for seed := int64(3); seed < 9 && cut == 0; seed++ {
		cut = crash(t, 40, 40, 2, 40*time.Millisecond, seed).InterruptedU
	}
	if cut == 0 {
		t.Fatal("no kill landed inside an undo in six batches")
	}
}

// A crash loop inside an undo: a step is cut off by kills MaxAttempts times,
// the run gives up and compensates, and then the undo is cut off
// MaxAttempts times too. The engine ends the run failed, as the contract
// says, and the crash demo's checks must accept that rather than call it a
// problem. (Seen once on a slow Windows runner: 1 order in 300.)
func TestVerifyAcceptsAnUndoCrashLoop(t *testing.T) {
	dir := t.TempDir()
	run := demo.RunID(1)
	if demo.ShipFails(run) {
		t.Fatal("pick an order whose shipping isn't refused")
	}
	s := flowchassis.NewFileStore(filepath.Join(dir, demo.JournalName))
	if _, err := s.Load(); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	charge := "charge"
	var recs []*flowchassis.Record
	add := func(r flowchassis.Record) { r.Run, r.At = run, now; recs = append(recs, &r) }
	add(flowchassis.Record{Type: "start", Flow: demo.FlowName, Input: []byte(run)})
	add(flowchassis.Record{Type: "begin", Step: "reserve", Seq: 1, Attempt: 1})
	add(flowchassis.Record{Type: "done", Step: "reserve", Seq: 1, Next: &charge})
	for a := 1; a <= demo.MaxAttempts; a++ {
		add(flowchassis.Record{Type: "begin", Step: "charge", Seq: 2, Attempt: a})
	}
	add(flowchassis.Record{Type: "fail", Step: "charge", Attempt: demo.MaxAttempts, Final: true,
		Error: "gave up after 10 attempts, the last of them cut off before it finished"})
	for a := 1; a <= demo.MaxAttempts; a++ {
		add(flowchassis.Record{Type: "ubegin", Step: "reserve", Seq: 1, Attempt: a})
	}
	add(flowchassis.Record{Type: "ufail", Step: "reserve", Seq: 1, Attempt: demo.MaxAttempts, Final: true,
		Error: "gave up after 10 attempts, the last of them cut off before it finished"})
	var frames []byte
	for _, r := range recs {
		b, err := flowchassis.EncodeRecord(r)
		if err != nil {
			t.Fatal(err)
		}
		frames = append(frames, b...)
	}
	if err := s.Append(frames); err != nil {
		t.Fatal(err)
	}
	s.Close()
	// Kills landed after some effects: a charge and an undone reservation.
	ledger := "do reserve " + run + " 1\ndo charge " + run + " 3\nundo reserve " + run + " 4\n"
	if err := os.WriteFile(filepath.Join(dir, demo.LedgerName), []byte(ledger), 0o644); err != nil {
		t.Fatal(err)
	}

	var r demo.Report
	demo.Verify(dir, 1, &r)
	if !r.OK || r.Failed != 1 || r.GaveUp != 1 || r.GaveUpUndo != 1 {
		t.Fatalf("OK %v, failed %d, gave up %d, gave up on an undo %d; problems: %v", r.OK, r.Failed, r.GaveUp, r.GaveUpUndo, r.Problems)
	}

	// And a failed run with no undo crash loop behind it is still a problem.
	recs = recs[:len(recs)-1]
	recs = append(recs, &flowchassis.Record{Type: "ufail", Run: run, At: now, Step: "reserve", Seq: 1, Attempt: demo.MaxAttempts, Final: true, Error: "refused"})
	frames = nil
	for _, rec := range recs {
		b, _ := flowchassis.EncodeRecord(rec)
		frames = append(frames, b...)
	}
	os.Remove(filepath.Join(dir, demo.JournalName))
	s = flowchassis.NewFileStore(filepath.Join(dir, demo.JournalName))
	s.Load()
	s.Append(frames)
	s.Close()
	r = demo.Report{}
	demo.Verify(dir, 1, &r)
	if r.OK {
		t.Fatal("a run that failed for another reason passed the checks")
	}
}
