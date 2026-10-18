// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

// Command flowchassis inspects FlowChassis journals and runs the crash demo.
//
//	flowchassis version
//	flowchassis runs JOURNAL [-status S] [-json]
//	flowchassis show JOURNAL RUN [-json]
//	flowchassis check JOURNAL
//	flowchassis compact JOURNAL [-drop-finished-before DURATION]
//	flowchassis demo [-runs N] [-kills N] [-workers N] [-pause D] [-seed N] [-dir DIR]
//
// runs, show and check read the journal without locking it, so they work
// while a program has it open.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"github.com/flowchassis/flowchassis"
	"github.com/flowchassis/flowchassis/internal/demo"
)

const usage = `flowchassis inspects FlowChassis journals and runs the crash demo.

  flowchassis version
  flowchassis runs JOURNAL [-status S] [-json]      list the runs in a journal
  flowchassis show JOURNAL RUN [-json]              one run: state and history
  flowchassis check JOURNAL                         verify a journal record by record
                                                    (exit 0 good, 1 damaged, 3 torn final write)
  flowchassis compact JOURNAL [-drop-finished-before DURATION]
                                                    rewrite a journal with one record per run
  flowchassis demo [-runs N] [-kills N] [-workers N] [-pause D] [-seed N] [-dir DIR]
                                                    kill worker processes mid-flow, then check every run

runs, show and check don't lock the journal; compact does, so it waits for
no one and fails if a program has the journal open.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	cmd, args := args[0], args[1:]
	var err error
	switch cmd {
	case "version", "-version", "--version":
		fmt.Fprintf(stdout, "flowchassis %s (journal format %02d, %s, %s/%s)\n", flowchassis.Version, flowchassis.JournalFormat, runtime.Version(), runtime.GOOS, runtime.GOARCH)
		return 0
	case "help", "-h", "-help", "--help":
		fmt.Fprint(stdout, usage)
		return 0
	case "runs":
		err = cmdRuns(args, stdout)
	case "show":
		err = cmdShow(args, stdout)
	case "check":
		return cmdCheck(args, stdout, stderr)
	case "compact":
		err = cmdCompact(args, stdout)
	case "demo":
		return cmdDemo(args, stdout, stderr)
	case "demo-worker":
		err = cmdDemoWorker(args)
	default:
		fmt.Fprintf(stderr, "flowchassis: unknown command %q\n\n%s", cmd, usage)
		return 2
	}
	if err != nil {
		fmt.Fprintf(stderr, "flowchassis %s: %v\n", cmd, err)
		var u usageError
		if errors.As(err, &u) {
			return 2
		}
		return 1
	}
	return 0
}

type usageError struct{ msg string }

func (u usageError) Error() string { return u.msg }

// parse reads flags given before or after the positional arguments.
func parse(fs *flag.FlagSet, args []string) ([]string, error) {
	fs.SetOutput(io.Discard)
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, usageError{err.Error()}
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos, nil
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
}

func load(path string) ([]flowchassis.Record, []flowchassis.RunInfo, bool, error) {
	recs, torn, err := flowchassis.ReadJournal(path)
	if err != nil {
		return nil, nil, false, err
	}
	runs, err := flowchassis.Replay(recs)
	return recs, runs, torn, err
}

func cmdRuns(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("runs", flag.ContinueOnError)
	status := fs.String("status", "", "only runs with this status")
	asJSON := fs.Bool("json", false, "print JSON")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usageError{"usage: flowchassis runs JOURNAL [-status S] [-json]"}
	}
	_, runs, _, err := load(pos[0])
	if err != nil {
		return err
	}
	var out []flowchassis.RunInfo
	for _, r := range runs {
		if *status == "" || string(r.Status) == *status {
			out = append(out, r)
		}
	}
	if *asJSON {
		return writeJSON(stdout, out)
	}
	w := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "RUN\tFLOW\tSTATUS\tSTEP\tATTEMPT\tSTARTED\tERROR")
	for _, r := range out {
		attempt := ""
		if r.Attempt > 0 {
			attempt = strconv.Itoa(r.Attempt)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", r.ID, r.Flow, r.Status, r.Step, attempt, r.Started.Local().Format("2006-01-02 15:04:05"), shorten(r.Error, 60))
	}
	return w.Flush()
}

func cmdShow(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("show", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "print JSON")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return usageError{"usage: flowchassis show JOURNAL RUN [-json]"}
	}
	recs, runs, _, err := load(pos[0])
	if err != nil {
		return err
	}
	id := pos[1]
	var info *flowchassis.RunInfo
	for i := range runs {
		if runs[i].ID == id {
			info = &runs[i]
		}
	}
	if info == nil {
		return fmt.Errorf("%w: %q", flowchassis.ErrNotFound, id)
	}
	var history []flowchassis.Record
	for _, r := range recs {
		if r.Run == id {
			history = append(history, r)
		}
	}
	if *asJSON {
		return writeJSON(stdout, struct {
			Run     flowchassis.RunInfo  `json:"run"`
			History []flowchassis.Record `json:"history"`
		}{*info, history})
	}
	fmt.Fprintf(stdout, "run      %s\nflow     %s\nstatus   %s\n", info.ID, info.Flow, info.Status)
	if info.Step != "" {
		fmt.Fprintf(stdout, "step     %s (attempt %d)\n", info.Step, info.Attempt)
	}
	if !info.RetryAt.IsZero() {
		fmt.Fprintf(stdout, "retry at %s\n", info.RetryAt.Local().Format(time.RFC3339))
	}
	if info.Error != "" {
		fmt.Fprintf(stdout, "error    %s\n", info.Error)
	}
	fmt.Fprintf(stdout, "started  %s\n", info.Started.Local().Format(time.RFC3339))
	if !info.Ended.IsZero() {
		fmt.Fprintf(stdout, "ended    %s\n", info.Ended.Local().Format(time.RFC3339))
	}
	if len(info.Done) > 0 {
		fmt.Fprintf(stdout, "done     %s\n", strings.Join(info.Done, ", "))
	}
	if len(info.Undone) > 0 {
		fmt.Fprintf(stdout, "undone   %s\n", strings.Join(info.Undone, ", "))
	}
	fmt.Fprintf(stdout, "input    %s\n", printable(info.Input))
	if len(info.State) > 0 {
		fmt.Fprintln(stdout, "state")
		keys := make([]string, 0, len(info.State))
		for k := range info.State {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(stdout, "  %s = %s\n", k, printable(info.State[k]))
		}
	}
	fmt.Fprintln(stdout, "history")
	for _, r := range history {
		fmt.Fprintf(stdout, "  %s  %s\n", r.At.Local().Format("15:04:05.000"), describe(r))
	}
	return nil
}

func describe(r flowchassis.Record) string {
	switch r.Type {
	case "start":
		return fmt.Sprintf("started %s", r.Flow)
	case "begin":
		return fmt.Sprintf("%s: attempt %d began", r.Step, r.Attempt)
	case "done":
		next := "end"
		if r.Next != nil && *r.Next != "" {
			next = *r.Next
		}
		return fmt.Sprintf("%s: done, next %s", r.Step, next)
	case "fail":
		if r.Final {
			return fmt.Sprintf("%s: attempt %d failed for good, compensating: %s", r.Step, r.Attempt, r.Error)
		}
		return fmt.Sprintf("%s: attempt %d failed, retry at %s: %s", r.Step, r.Attempt, r.RetryAt.Local().Format("15:04:05.000"), r.Error)
	case "yield":
		return fmt.Sprintf("%s: attempt %d cut off by shutdown, not counted", r.Step, r.Attempt)
	case "uyield":
		return fmt.Sprintf("undo %s: attempt %d cut off by shutdown, not counted", r.Step, r.Attempt)
	case "sstate":
		return "state of a completed step, written by compaction"
	case "cancel":
		return "cancel asked for"
	case "comp":
		return fmt.Sprintf("compensating: %s", r.Error)
	case "ubegin":
		return fmt.Sprintf("undo %s: attempt %d began", r.Step, r.Attempt)
	case "udone":
		return fmt.Sprintf("undo %s: done", r.Step)
	case "ufail":
		if r.Final {
			return fmt.Sprintf("undo %s: attempt %d failed for good, run failed: %s", r.Step, r.Attempt, r.Error)
		}
		return fmt.Sprintf("undo %s: attempt %d failed, retry at %s: %s", r.Step, r.Attempt, r.RetryAt.Local().Format("15:04:05.000"), r.Error)
	case "end":
		if r.Error != "" {
			return fmt.Sprintf("ended %s (%s)", r.Status, r.Error)
		}
		return fmt.Sprintf("ended %s", r.Status)
	case "snapshot":
		return "snapshot written by compaction"
	}
	return r.Type
}

func cmdCheck(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: flowchassis check JOURNAL")
		return 2
	}
	path := args[0]
	st, err := os.Stat(path)
	if err != nil {
		fmt.Fprintf(stderr, "flowchassis check: %v\n", err)
		return 1
	}
	recs, runs, torn, err := load(path)
	if err != nil {
		fmt.Fprintf(stdout, "%s: %d bytes: NOT OK: %v\n", path, st.Size(), err)
		return 1
	}
	count := map[flowchassis.Status]int{}
	for _, r := range runs {
		count[r.Status]++
	}
	fmt.Fprintf(stdout, "%s: %d bytes, %d records, %d runs\n", path, st.Size(), len(recs), len(runs))
	for _, s := range []flowchassis.Status{flowchassis.Pending, flowchassis.Running, flowchassis.Retrying, flowchassis.Compensating, flowchassis.Completed, flowchassis.Compensated, flowchassis.Failed} {
		if count[s] > 0 {
			fmt.Fprintf(stdout, "  %-13s %d\n", s, count[s])
		}
	}
	if torn {
		fmt.Fprintln(stdout, "WARNING: the journal ends in a torn write. Every whole record before it has a good checksum. Opening the journal")
		fmt.Fprintln(stdout, "cuts the torn bytes off and keeps them in JOURNAL.torn. A program writing to the journal as this ran can cause it too.")
		return 3
	}
	fmt.Fprintln(stdout, "OK: every record has a good checksum")
	return 0
}

func cmdCompact(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("compact", flag.ContinueOnError)
	drop := fs.Duration("drop-finished-before", 0, "leave out runs that finished longer ago than this, such as 720h")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usageError{"usage: flowchassis compact JOURNAL [-drop-finished-before DURATION]"}
	}
	path := pos[0]
	before, err := os.Stat(path)
	if err != nil {
		return err
	}
	// No flows are registered, so nothing runs while the engine is open.
	e, err := flowchassis.Open(path, flowchassis.Options{Workers: 1})
	if err != nil {
		return err
	}
	n := len(e.List())
	var cutoff time.Time
	if *drop > 0 {
		cutoff = time.Now().Add(-*drop)
	}
	if err := e.Compact(cutoff); err != nil {
		e.Close()
		return err
	}
	kept := len(e.List())
	if err := e.Close(); err != nil {
		return err
	}
	after, err := os.Stat(path)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "%s: %d bytes to %d bytes, %d runs kept, %d dropped\n", path, before.Size(), after.Size(), kept, n-kept)
	return nil
}

func cmdDemo(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	fs.SetOutput(stderr)
	runs := fs.Int("runs", 200, "orders to run")
	kills := fs.Int("kills", 40, "worker processes to kill")
	workers := fs.Int("workers", 4, "orders each worker process runs at once")
	pause := fs.Duration("pause", 20*time.Millisecond, "pause inside each step, to widen the windows a kill lands in")
	seed := fs.Int64("seed", 1, "seed for the kill timing")
	dir := fs.String("dir", "", "directory for the journal and ledger (default: a new temporary one, removed after)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *runs < 1 || *workers < 1 || *kills < 0 {
		fmt.Fprintln(stderr, "flowchassis demo: -runs and -workers must be at least 1, and -kills at least 0")
		return 2
	}
	self, err := os.Executable()
	if err != nil {
		fmt.Fprintf(stderr, "flowchassis demo: %v\n", err)
		return 1
	}
	work := *dir
	if work == "" {
		work, err = os.MkdirTemp("", "flowchassis-demo-")
		if err != nil {
			fmt.Fprintf(stderr, "flowchassis demo: %v\n", err)
			return 1
		}
		defer os.RemoveAll(work)
	} else if err := os.MkdirAll(work, 0o755); err != nil {
		fmt.Fprintf(stderr, "flowchassis demo: %v\n", err)
		return 1
	}
	if entries, _ := os.ReadDir(work); len(entries) > 0 {
		fmt.Fprintf(stderr, "flowchassis demo: %s isn't empty\n", work)
		return 1
	}
	fmt.Fprintf(stderr, "running %d orders, killing %d worker processes along the way\n", *runs, *kills)
	start := time.Now()
	killed, procs, err := demo.Supervise(func() *exec.Cmd {
		return exec.Command(self, "demo-worker", work, strconv.Itoa(*runs), strconv.Itoa(*workers), pause.String())
	}, *kills, 5*time.Millisecond, 120*time.Millisecond, *seed, func(f string, a ...any) { fmt.Fprintf(stderr, f+"\n", a...) })
	rep := &demo.Report{Kills: killed, Processes: procs, Workers: *workers}
	if err != nil {
		rep.Problems = append(rep.Problems, err.Error())
	}
	rep.Seconds = time.Since(start).Seconds()
	demo.Verify(work, *runs, rep)
	if err != nil {
		rep.OK = false
	}
	writeJSON(stdout, rep)
	fmt.Fprintln(stderr, rep.Summary())
	if !rep.OK {
		return 1
	}
	return 0
}

func cmdDemoWorker(args []string) error {
	if len(args) != 4 {
		return usageError{"usage: flowchassis demo-worker DIR RUNS WORKERS PAUSE"}
	}
	n, err1 := strconv.Atoi(args[1])
	w, err2 := strconv.Atoi(args[2])
	p, err3 := time.ParseDuration(args[3])
	if err := errors.Join(err1, err2, err3); err != nil {
		return usageError{err.Error()}
	}
	return demo.Worker(filepath.Clean(args[0]), n, w, p)
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func printable(b []byte) string {
	if b == nil {
		return "(none)"
	}
	if utf8.Valid(b) && !strings.ContainsFunc(string(b), func(r rune) bool { return r < 0x20 && r != '\t' }) {
		return shorten(string(b), 200)
	}
	return fmt.Sprintf("(%d bytes of binary)", len(b))
}

func shorten(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return string(r[:n-1]) + "…"
}
