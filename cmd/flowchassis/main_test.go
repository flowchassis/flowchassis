// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/flowchassis/flowchassis"
	"github.com/flowchassis/flowchassis/internal/demo"
)

// The demo starts its own executable as the worker; in tests that is the
// test binary, which answers to demo-worker here.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "demo-worker" {
		os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

func cli(args ...string) (int, string, string) {
	var out, errOut bytes.Buffer
	code := run(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

// journal makes a journal with a completed, a compensated and a retrying
// run.
func journal(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "orders.journal")
	e, err := flowchassis.Open(path, flowchassis.Options{})
	if err != nil {
		t.Fatal(err)
	}
	e.Register(flowchassis.Flow{Name: "order", Steps: []flowchassis.Step{
		{Name: "reserve", Do: func(c *flowchassis.Ctx) error { c.Set("sku", []byte("A-42")); return nil }, Undo: func(*flowchassis.Ctx) error { return nil }},
		{Name: "charge",
			Retry: flowchassis.RetryPolicy{MaxAttempts: 3, Backoff: time.Hour},
			Do: func(c *flowchassis.Ctx) error {
				switch c.Run {
				case "bad":
					return flowchassis.Permanent(errors.New("card declined"))
				case "slow":
					return errors.New("bank timeout")
				}
				return nil
			}},
	}})
	for _, id := range []string{"good", "bad", "slow"} {
		e.Start("order", id, []byte(`{"order":"`+id+`"}`))
	}
	e.Wait(t.Context(), "good")
	e.Wait(t.Context(), "bad")
	for {
		if info, _ := e.Get("slow"); info.Status == flowchassis.Retrying {
			break
		}
		time.Sleep(time.Millisecond)
	}
	e.Close()
	return path
}

func TestVersionAndUsage(t *testing.T) {
	code, out, _ := cli("version")
	if code != 0 || !strings.HasPrefix(out, "flowchassis "+flowchassis.Version+" (journal format 01") {
		t.Fatalf("%d %q", code, out)
	}
	if code, _, errOut := cli(); code != 2 || !strings.Contains(errOut, "flowchassis runs") {
		t.Fatal(code, errOut)
	}
	if code, _, _ := cli("nope"); code != 2 {
		t.Fatal(code)
	}
	if code, out, _ := cli("help"); code != 0 || !strings.Contains(out, "compact") {
		t.Fatal(code)
	}
}

func TestRuns(t *testing.T) {
	path := journal(t)
	code, out, _ := cli("runs", path)
	if code != 0 {
		t.Fatal(code)
	}
	for _, want := range []string{"good  ", "completed", "bad", "compensated", "card declined", "slow", "retrying", "charge"} {
		if !strings.Contains(out, want) {
			t.Errorf("runs output lacks %q:\n%s", want, out)
		}
	}
	code, out, _ = cli("runs", "-status", "retrying", "-json", path)
	var runs []flowchassis.RunInfo
	if code != 0 || json.Unmarshal([]byte(out), &runs) != nil || len(runs) != 1 || runs[0].ID != "slow" {
		t.Fatalf("%d %s", code, out)
	}
	if code, _, _ := cli("runs"); code != 2 {
		t.Fatal(code)
	}
	if code, _, _ := cli("runs", filepath.Join(t.TempDir(), "missing")); code != 1 {
		t.Fatal(code)
	}
}

func TestShow(t *testing.T) {
	path := journal(t)
	code, out, _ := cli("show", path, "bad")
	if code != 0 {
		t.Fatal(code)
	}
	for _, want := range []string{"status   compensated", "done     reserve", "undone   reserve", `input    {"order":"bad"}`, "sku = A-42",
		"charge: attempt 1 failed for good: card declined", "undo reserve: done", "ended compensated"} {
		if !strings.Contains(out, want) {
			t.Errorf("show lacks %q:\n%s", want, out)
		}
	}
	code, out, _ = cli("show", path, "slow")
	if code != 0 || !strings.Contains(out, "retry at") || !strings.Contains(out, "step     charge (attempt 1)") {
		t.Fatalf("%d\n%s", code, out)
	}
	code, out, _ = cli("show", "-json", path, "good")
	var v struct {
		Run     flowchassis.RunInfo
		History []flowchassis.Record
	}
	if code != 0 || json.Unmarshal([]byte(out), &v) != nil || v.Run.Status != flowchassis.Completed || len(v.History) != 6 {
		t.Fatalf("%d %s", code, out)
	}
	if code, _, errOut := cli("show", path, "nobody"); code != 1 || !strings.Contains(errOut, "not found") {
		t.Fatal(code, errOut)
	}
}

func TestCheck(t *testing.T) {
	path := journal(t)
	code, out, _ := cli("check", path)
	if code != 0 || !strings.Contains(out, "every checksum good") || !strings.Contains(out, "retrying      1") {
		t.Fatalf("%d %s", code, out)
	}
	data, _ := os.ReadFile(path)
	torn := filepath.Join(t.TempDir(), "torn")
	os.WriteFile(torn, data[:len(data)-3], 0o644)
	if code, out, _ := cli("check", torn); code != 0 || !strings.Contains(out, "torn write") {
		t.Fatalf("%d %s", code, out)
	}
	bad := filepath.Join(t.TempDir(), "bad")
	data[20] ^= 0xff
	os.WriteFile(bad, data, 0o644)
	if code, out, _ := cli("check", bad); code != 1 || !strings.Contains(out, "NOT OK") {
		t.Fatalf("%d %s", code, out)
	}
	if code, _, _ := cli("check"); code != 2 {
		t.Fatal(code)
	}
}

func TestCompact(t *testing.T) {
	path := journal(t)
	code, out, _ := cli("compact", path)
	if code != 0 || !strings.Contains(out, "3 runs kept, 0 dropped") {
		t.Fatalf("%d %s", code, out)
	}
	code, out, _ = cli("compact", path, "-drop-finished-before", "1ns")
	if code != 0 || !strings.Contains(out, "1 runs kept, 2 dropped") {
		t.Fatalf("%d %s", code, out)
	}
	code, out, _ = cli("runs", path)
	if code != 0 || !strings.Contains(out, "slow") || strings.Contains(out, "good") {
		t.Fatalf("%d %s", code, out)
	}
	// compact takes the lock, so it refuses a journal a program has open.
	e, err := flowchassis.Open(path, flowchassis.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if code, _, errOut := cli("compact", path); code != 1 || !strings.Contains(errOut, "in use") {
		t.Fatal(code, errOut)
	}
	if code, _, _ := cli("compact", path, "-drop-finished-before", "soon"); code != 2 {
		t.Fatal(code)
	}
}

func TestDemo(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "demo")
	code, out, errOut := cli("demo", "-runs", "20", "-kills", "5", "-workers", "2", "-pause", "10ms", "-dir", dir)
	if code != 0 {
		t.Fatalf("%d\n%s\n%s", code, out, errOut)
	}
	var r demo.Report
	if err := json.Unmarshal([]byte(out), &r); err != nil {
		t.Fatal(err)
	}
	if !r.OK || r.Runs != 20 || r.Kills != 5 || r.Completed+r.Compensated != 20 || len(r.Checks) != 6 {
		t.Fatalf("%+v", r)
	}
	if !strings.Contains(errOut, "PASS: 20 runs") {
		t.Fatal(errOut)
	}
	// The journal it leaves checks clean, and a second demo won't reuse it.
	if code, out, _ := cli("check", filepath.Join(dir, demo.JournalName)); code != 0 || !strings.Contains(out, "20 runs") {
		t.Fatal(code, out)
	}
	if code, _, errOut := cli("demo", "-dir", dir); code != 1 || !strings.Contains(errOut, "isn't empty") {
		t.Fatal(code, errOut)
	}
	if code, _, _ := cli("demo", "-runs", "0"); code != 2 {
		t.Fatal(code)
	}
}
