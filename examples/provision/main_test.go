// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"io"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/flowchassis/flowchassis"
	"github.com/flowchassis/flowchassis/examples/internal/fake"
)

func TestProvision(t *testing.T) {
	dir := t.TempDir()
	runs, err := run(dir, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]flowchassis.RunInfo{}
	for _, r := range runs {
		byID[r.ID] = r
	}

	shop := byID["shop.example.com"]
	if shop.Status != flowchassis.Completed {
		t.Fatalf("shop: %s (%s)", shop.Status, shop.Error)
	}

	blog := byID["blog.example.net"]
	if blog.Status != flowchassis.Compensated || !slices.Equal(blog.Undone, []string{"add_dns", "create_vm"}) {
		t.Fatalf("blog: %s, undone %v; want compensated, undone [add_dns create_vm]", blog.Status, blog.Undone)
	}

	legacy := byID["legacy.example.org"]
	if legacy.Status != flowchassis.Failed || len(legacy.Undone) != 0 {
		t.Fatalf("legacy: %s, undone %v; want failed with nothing undone", legacy.Status, legacy.Undone)
	}
	if string(legacy.State["vm"]) != "compute-3" || string(legacy.State["dns"]) != "dns-3" {
		t.Errorf("legacy state %v: a person needs the VM and record IDs", legacy.State)
	}

	// The faults really fired: three health checks while the VM booted, one
	// try at each refused certificate, three at legacy's failing undo.
	j := filepath.Join(dir, "provision.journal")
	for _, c := range []struct {
		run, step string
		undo      bool
		want      int
	}{
		{"shop.example.com", "health_check", false, 3},
		{"blog.example.net", "issue_cert", false, 1},
		{"legacy.example.org", "issue_cert", false, 1},
		{"legacy.example.org", "add_dns", true, 3},
		{"legacy.example.org", "create_vm", true, 0},
	} {
		n, err := fake.Attempts(j, c.run, c.step, c.undo)
		if err != nil {
			t.Fatal(err)
		}
		if n != c.want {
			t.Errorf("%s %s (undo %v): %d attempts, want %d", c.run, c.step, c.undo, n, c.want)
		}
	}

	compute := strings.Join(fake.Effects(dir, "compute"), "\n")
	if !strings.Contains(compute, "destroy compute-2") {
		t.Error("blog's VM wasn't destroyed")
	}
	if strings.Contains(compute, "destroy compute-1") || strings.Contains(compute, "destroy compute-3") {
		t.Error("a VM was destroyed that should still be there")
	}
	// The health check waited for the VM to boot: two failures, then done.
	if n := strings.Count(compute, " GET https://"); n != 1 {
		t.Errorf("%d successful health checks logged, want 1", n)
	}
	if shop.State == nil || string(shop.State["cert"]) == "" {
		t.Error("shop has no certificate in its state")
	}
	// The blog record was removed; legacy's couldn't be.
	dns := fake.Effects(dir, "dns")
	if len(dns) != 4 || !strings.Contains(dns[2], "remove dns-2") {
		t.Errorf("dns log %v", dns)
	}
}
