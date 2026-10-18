// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/flowchassis/flowchassis"
	"github.com/flowchassis/flowchassis/examples/internal/fake"
)

func TestWebhook(t *testing.T) {
	dir := t.TempDir()
	runs, recv, err := run(dir, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]flowchassis.Status{
		"evt-1001-acme":    flowchassis.Completed,
		"evt-1001-globex":  flowchassis.Compensated,
		"evt-1001-initech": flowchassis.Compensated,
	}
	for _, r := range runs {
		if r.Status != want[r.ID] {
			t.Errorf("%s: %s, want %s (%s)", r.ID, r.Status, want[r.ID], r.Error)
		}
	}
	// Acme: two 503s and a 200, which also means the signature checked out.
	// Globex: one 410, no retries. Initech: all six attempts.
	for path, n := range map[string]int{"/acme": 3, "/globex": 1, "/initech": 6} {
		if got := recv.Hits(path); got != n {
			t.Errorf("%s got %d requests, want %d", path, got, n)
		}
	}
	dead := strings.Join(fake.Effects(dir, "deadletter"), "\n")
	if !strings.Contains(dead, "evt-1001-globex") || !strings.Contains(dead, "evt-1001-initech") || strings.Contains(dead, "acme") {
		t.Errorf("dead-letter queue:\n%s", dead)
	}
	// The signing secrets never reach the journal.
	recs, _, err := flowchassis.ReadJournal(filepath.Join(dir, "webhook.journal"))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if strings.Contains(string(r.Input), "secret") {
			t.Fatalf("a signing secret is in the journal: %s", r.Input)
		}
	}
	outbox := strings.Join(fake.Effects(dir, "outbox"), "\n")
	if strings.Count(outbox, " delivered evt") != 1 || !strings.Contains(outbox, "delivered evt-1001-acme") {
		t.Errorf("outbox:\n%s", outbox)
	}
}
