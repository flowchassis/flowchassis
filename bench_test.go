// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

package flowchassis

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// benchRuns runs b.N runs of a three-step flow with empty steps against a
// journal file, with the given number of workers and as many callers, so the
// time is the engine's own: checkpoints written and flushed to disk. Each run
// writes 8 records: start, begin and done for each step, and end.
func benchRuns(b *testing.B, workers int) {
	path := filepath.Join(b.TempDir(), "j")
	e, err := Open(path, Options{Workers: workers})
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()
	nop := func(*Ctx) error { return nil }
	e.Register(Flow{Name: "f", Steps: []Step{{Name: "a", Do: nop}, {Name: "b", Do: nop}, {Name: "c", Do: nop}}})
	b.ResetTimer()
	// As many callers as workers each start their share of runs and wait
	// for them, as request handlers would.
	var wg sync.WaitGroup
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := g; i < b.N; i += workers {
				id := fmt.Sprintf("r%d", i)
				if _, err := e.Start("f", id, nil); err != nil {
					b.Error(err)
					return
				}
				if _, err := e.Wait(context.Background(), id); err != nil {
					b.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(b.N*3)/b.Elapsed().Seconds(), "steps/s")
}

func BenchmarkRunsOneWorker(b *testing.B)    { benchRuns(b, 1) }
func BenchmarkRunsEightWorkers(b *testing.B) { benchRuns(b, 8) }
func BenchmarkRunsSixtyFour(b *testing.B)    { benchRuns(b, 64) }
