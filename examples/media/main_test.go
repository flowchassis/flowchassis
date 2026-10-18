// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/flowchassis/flowchassis"
	"github.com/flowchassis/flowchassis/examples/internal/fake"
)

// The test runs the example in a child process (this test binary, told by
// the environment to act as the program), kills it while it transcodes, then
// runs it again in this process and checks that it carried on.
func TestMain(m *testing.M) {
	if dir := os.Getenv("FLOWCHASSIS_MEDIA_CHILD"); dir != "" {
		run(dir, 40*time.Millisecond, io.Discard)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func segmentFiles(dir, run string) []string {
	names, _ := filepath.Glob(filepath.Join(dir, "segments", run, "*.ts"))
	return names
}

func TestMediaKilledWhileTranscoding(t *testing.T) {
	dir := t.TempDir()
	child := exec.Command(os.Args[0], "-test.run=^$")
	child.Env = append(os.Environ(), "FLOWCHASSIS_MEDIA_CHILD="+dir)
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	// Kill it with SIGKILL (TerminateProcess on Windows) once three segments
	// of the keynote are on disk.
	deadline := time.Now().Add(20 * time.Second)
	for len(segmentFiles(dir, "keynote.mov")) < 3 {
		if time.Now().After(deadline) {
			child.Process.Kill()
			t.Fatal("the child never got to three segments")
		}
		time.Sleep(5 * time.Millisecond)
	}
	child.Process.Kill()
	child.Wait()
	beforeKill := len(segmentFiles(dir, "keynote.mov"))
	if beforeKill == 8 {
		t.Fatal("the child finished before it was killed; nothing was tested")
	}
	if n := len(fake.Effects(dir, "cdn")); n == 2 {
		t.Fatal("both videos were published before the kill")
	}

	var out strings.Builder
	runs, err := run(dir, time.Millisecond, &out)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range runs {
		if r.Status != flowchassis.Completed {
			t.Fatalf("%s: %s (%s)", r.ID, r.Status, r.Error)
		}
	}
	if !strings.Contains(out.String(), "keynote.mov was cut off at transcode") {
		t.Errorf("the second run didn't resume the keynote mid-transcode:\n%s", out.String())
	}
	// It carried on: segments finished before the kill weren't redone.
	// Only the one that was cut off runs again, so the first segment the
	// second run transcodes is at or after the last one on disk.
	first := ""
	for _, l := range strings.Split(out.String(), "\n") {
		if f := strings.Fields(l); len(f) > 2 && f[0] == "transcode" && f[1] == "keynote.mov" {
			first = l
			break
		}
	}
	if first == "" {
		t.Fatalf("the second run transcoded nothing for the keynote:\n%s", out.String())
	}
	var n int
	if _, err := fmt.Sscanf(first[strings.Index(first, "segment"):], "segment %d of", &n); err != nil {
		t.Fatalf("can't read %q: %v", first, err)
	}
	// beforeKill segment files were on disk. The last of them may have been
	// written just before its checkpoint, so the second run starts at that
	// one (1-based number beforeKill) or the one after it.
	if n != beforeKill && n != beforeKill+1 {
		t.Errorf("%d segments were on disk at the kill, but the second run started the keynote at segment %d", beforeKill, n)
	}
	for _, v := range videos {
		if n := len(segmentFiles(dir, v.Name)); n != v.Segments {
			t.Errorf("%s: %d segment files, want %d", v.Name, n, v.Segments)
		}
	}
	// Each video is published exactly once, with all its segments.
	cdn := fake.Effects(dir, "cdn")
	if len(cdn) != 2 || !strings.Contains(strings.Join(cdn, "\n"), "publish keynote.mov, 8 segments") {
		t.Errorf("cdn log %v", cdn)
	}
	if n := len(fake.Effects(dir, "storage")); n != 4 {
		t.Errorf("storage did %d things, want 4: two originals, two thumbnails", n)
	}
}
