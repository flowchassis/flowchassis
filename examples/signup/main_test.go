// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"io"
	"strings"
	"testing"

	"github.com/flowchassis/flowchassis"
	"github.com/flowchassis/flowchassis/examples/internal/fake"
)

func TestSignup(t *testing.T) {
	dir := t.TempDir()
	runs, err := run(dir, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]flowchassis.Status{
		"signup-ana": flowchassis.Completed,
		"signup-ben": flowchassis.Completed,
		"signup-cy":  flowchassis.Compensated,
	}
	for _, r := range runs {
		if r.Status != want[r.ID] {
			t.Errorf("%s: %s, want %s (%s)", r.ID, r.Status, want[r.ID], r.Error)
		}
	}

	users := strings.Join(fake.Effects(dir, "users"), "\n")
	for _, s := range []string{"create user cy@example.com", "delete user users-3"} {
		if !strings.Contains(users, s) {
			t.Errorf("users log has no %q:\n%s", s, users)
		}
	}
	if strings.Contains(users, "activate user users-3") {
		t.Error("Cy's account was activated")
	}
	if n := len(fake.Effects(dir, "storage")); n != 2 {
		t.Errorf("storage did %d things, want 2 buckets (none for Cy)", n)
	}
	if n := len(fake.Effects(dir, "mail")); n != 2 {
		t.Errorf("%d welcome emails, want 2: Ben's timeouts are retried, and sent once", n)
	}

	// Running the program again over the same journal starts nothing new and
	// calls nothing: every signup is found finished.
	before := len(fake.Effects(dir, "users")) + len(fake.Effects(dir, "storage")) + len(fake.Effects(dir, "mail"))
	if _, err := run(dir, io.Discard); err != nil {
		t.Fatal(err)
	}
	after := len(fake.Effects(dir, "users")) + len(fake.Effects(dir, "storage")) + len(fake.Effects(dir, "mail"))
	if after != before {
		t.Errorf("a second run made %d new calls, want none", after-before)
	}
}
