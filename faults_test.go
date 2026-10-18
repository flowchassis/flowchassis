// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

package flowchassis_test

import (
	"fmt"
	"os"
	"os/exec"
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
	if r.Completed+r.Compensated != runs || r.Completed < wantDone-r.GaveUp {
		t.Fatalf("%d completed, %d compensated, %d gave up; want %d completed less those that gave up", r.Completed, r.Compensated, r.GaveUp, wantDone)
	}
	if r.GaveUp > 0 {
		t.Logf("%d orders gave up after %d attempts cut off by kills, and compensated", r.GaveUp, demo.MaxAttempts)
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
