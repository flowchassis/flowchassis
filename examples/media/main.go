// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

// Media: store an uploaded video, transcode it, make a thumbnail, publish it.
// Transcoding is the long part, so it goes one segment per step: the step
// does one segment and, until the last, sends the run back to itself with
// Ctx.Next. Every finished segment is a checkpoint. Kill the program while it
// transcodes and run it again with the same -dir: it carries on at the
// segment it was on, not from the start.
//
//	go run ./examples/media
//	(press Ctrl-C while it transcodes, then run the same command again)
//
// To kill it from another terminal instead, build it first, so the signal
// reaches the program itself and not the go command:
//
//	go build -o media ./examples/media && ./media
//	kill -9 <its pid>
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/flowchassis/flowchassis"
	"github.com/flowchassis/flowchassis/examples/internal/fake"
)

// Video is a run's input.
type Video struct {
	Name     string `json:"name"`
	Segments int    `json:"segments"`
}

// Media holds the outside services and the transcoder's settings.
type Media struct {
	Storage, CDN *fake.Service
	Dir          string        // segments go in Dir/segments/RUN/
	SegmentTime  time.Duration // how long one segment takes
	Out          io.Writer
}

func mediaFlow(m Media) flowchassis.Flow {
	video := func(c *flowchassis.Ctx) Video {
		var v Video
		json.Unmarshal(c.Input(), &v)
		return v
	}
	return flowchassis.Flow{Name: "media", Steps: []flowchassis.Step{
		{
			Name: "store_original",
			Do: func(c *flowchassis.Ctx) error {
				id, err := m.Storage.Create(c, "store original "+video(c).Name)
				if err != nil {
					return err
				}
				c.Set("original", []byte(id))
				return nil
			},
		},
		{
			Name: "transcode",
			Do: func(c *flowchassis.Ctx) error {
				v := video(c)
				seg := 0
				if b, ok := c.Get("segment"); ok {
					seg, _ = strconv.Atoi(string(b))
				}
				fmt.Fprintf(m.Out, "  %-10s %s segment %d of %d (attempt %d)\n", "transcode", v.Name, seg+1, v.Segments, c.Attempt)
				if err := m.transcode(c, seg); err != nil {
					return err
				}
				c.Set("segment", []byte(strconv.Itoa(seg+1)))
				if seg+1 < v.Segments {
					c.Next("transcode") // the next segment, after this checkpoint
				}
				return nil
			},
			// Plenty of attempts: a crash cuts off one, and a long job may
			// be killed more than once.
			Retry: flowchassis.RetryPolicy{MaxAttempts: 10, Backoff: 100 * time.Millisecond},
		},
		{
			Name: "thumbnail",
			Do: func(c *flowchassis.Ctx) error {
				id, err := m.Storage.Create(c, "thumbnail for "+video(c).Name)
				if err != nil {
					return err
				}
				c.Set("thumbnail", []byte(id))
				return nil
			},
		},
		{
			Name: "publish",
			Do: func(c *flowchassis.Ctx) error {
				v := video(c)
				return m.CDN.Do(c, fmt.Sprintf("publish %s, %d segments", v.Name, v.Segments))
			},
		},
	}}
}

// transcode stands in for the real work on one segment: it takes
// SegmentTime and writes the segment's file. Writing the same segment again
// replaces the file, so a segment cut off by a crash is simply done again.
func (m Media) transcode(c *flowchassis.Ctx, seg int) error {
	select {
	case <-time.After(m.SegmentTime):
	case <-c.Done(): // Close or Cancel: stop, this attempt is retried
		return c.Err()
	}
	dir := filepath.Join(m.Dir, "segments", c.Run)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data := fmt.Sprintf("%s segment %d, attempt %d, pid %d\n", c.Run, seg, c.Attempt, os.Getpid())
	return writeFileSync(filepath.Join(dir, fmt.Sprintf("%03d.ts", seg)), []byte(data))
}

func writeFileSync(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

var videos = []Video{
	{"keynote.mov", 8},
	{"product-demo.mov", 6},
}

func run(dir string, segmentTime time.Duration, out io.Writer) ([]flowchassis.RunInfo, error) {
	out = fake.SyncWriter(out)
	m := Media{Dir: dir, SegmentTime: segmentTime, Out: out}
	var err error
	if m.Storage, err = fake.Open(dir, "storage", out); err != nil {
		return nil, err
	}
	defer m.Storage.Close()
	if m.CDN, err = fake.Open(dir, "cdn", out); err != nil {
		return nil, err
	}
	defer m.CDN.Close()

	e, err := flowchassis.Open(filepath.Join(dir, "media.journal"), flowchassis.Options{Workers: 2})
	if err != nil {
		return nil, err
	}
	defer e.Close()
	// Before Register, runs of this flow in the journal wait as they were
	// left, so this reports where the last process got to.
	for _, v := range videos {
		if r, err := e.Get(v.Name); err == nil && !r.Status.Finished() {
			fmt.Fprintf(out, "%s was cut off at %s; carrying on (%s of %d segments done)\n", v.Name, r.Step, orZero(r.State["segment"]), v.Segments)
		}
	}
	if err := e.Register(mediaFlow(m)); err != nil {
		return nil, err
	}

	// Start both. After a crash, Start finds each run in the journal (the
	// engine has already resumed it) and returns its ID.
	for _, v := range videos {
		b, _ := json.Marshal(v)
		if _, err := e.Start("media", v.Name, b); err != nil {
			return nil, err
		}
	}
	var runs []flowchassis.RunInfo
	for _, v := range videos {
		r, err := e.Wait(context.Background(), v.Name)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(out, "%s => %s\n", r.ID, r.Status)
		runs = append(runs, r)
	}
	return runs, nil
}

func orZero(b []byte) string {
	if len(b) == 0 {
		return "0"
	}
	return string(b)
}

func main() {
	defaultDir := filepath.Join(os.TempDir(), "flowchassis-media-example")
	dir := flag.String("dir", defaultDir, "where to keep the journal, the segments and the fake services' logs; delete it to start over")
	segmentTime := flag.Duration("segment-time", 700*time.Millisecond, "how long one segment takes to transcode")
	flag.Parse()
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("Working in %s\n", *dir)
	if _, err := run(*dir, *segmentTime, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	j := filepath.Join(*dir, "media.journal")
	fmt.Printf("\nAll published. See a run's history with: flowchassis show %s keynote.mov\nDelete %s to start over.\n", j, *dir)
}
