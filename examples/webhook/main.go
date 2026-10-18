// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

// Webhook: deliver an event to a customer's endpoint. Queue it, sign it,
// POST it, mark it delivered. A failed POST is tried again after a wait that
// doubles each time. When delivery fails for good, compensation moves the
// event to the dead-letter queue.
//
// Three customers get an order.paid event, served by a real HTTP server in
// this program. Acme's endpoint answers 503 twice and then takes it. Globex
// answers 410 Gone, which means stop, so that event is dead-lettered at once.
// Initech's endpoint is down and answers 500 every time, so its event is
// dead-lettered after six attempts.
//
//	go run ./examples/webhook
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/flowchassis/flowchassis"
	"github.com/flowchassis/flowchassis/examples/internal/fake"
)

// Delivery is a run's input.
type Delivery struct {
	Customer string          `json:"customer"`
	URL      string          `json:"url"`
	Secret   string          `json:"secret"`
	Event    json.RawMessage `json:"event"`
}

func webhookFlow(outbox, deadLetter *fake.Service, client *http.Client) flowchassis.Flow {
	delivery := func(c *flowchassis.Ctx) Delivery {
		var d Delivery
		json.Unmarshal(c.Input(), &d)
		return d
	}
	return flowchassis.Flow{Name: "webhook", Steps: []flowchassis.Step{
		{
			Name: "queue",
			Do: func(c *flowchassis.Ctx) error {
				return outbox.Do(c, "queue "+c.Run)
			},
			// Compensation for a delivery that failed for good.
			Undo: func(c *flowchassis.Ctx) error {
				return deadLetter.Do(c, "dead-letter "+c.Run)
			},
		},
		{
			Name: "sign",
			Do: func(c *flowchassis.Ctx) error {
				d := delivery(c)
				mac := hmac.New(sha256.New, []byte(d.Secret))
				mac.Write(d.Event)
				c.Set("signature", []byte("sha256="+hex.EncodeToString(mac.Sum(nil))))
				return nil
			},
		},
		{
			Name: "post",
			Do: func(c *flowchassis.Ctx) error {
				d := delivery(c)
				sig, _ := c.Get("signature")
				req, err := http.NewRequestWithContext(c, "POST", d.URL, bytes.NewReader(d.Event))
				if err != nil {
					return flowchassis.Permanent(err)
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Webhook-Signature", string(sig))
				// The same on every attempt, so the receiver can drop a
				// delivery it has already processed.
				req.Header.Set("Webhook-Id", c.Run)
				resp, err := client.Do(req)
				if err != nil {
					return err // network trouble: try again
				}
				resp.Body.Close()
				switch {
				case resp.StatusCode < 300:
					return nil
				case resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500:
					return fmt.Errorf("endpoint answered %s", resp.Status)
				default:
					// 400, 404, 410: the endpoint won't take it, ever.
					return flowchassis.Permanent(fmt.Errorf("endpoint answered %s", resp.Status))
				}
			},
			Retry: flowchassis.RetryPolicy{MaxAttempts: 6, Backoff: 50 * time.Millisecond, Multiplier: 2, MaxBackoff: time.Second},
		},
		{
			Name: "mark_delivered",
			Do: func(c *flowchassis.Ctx) error {
				return outbox.Do(c, "delivered "+c.Run)
			},
		},
	}}
}

// Receivers is the customers' side: one HTTP server, three endpoints.
type Receivers struct {
	mu    sync.Mutex
	hits  map[string]int       // requests per endpoint
	first map[string]time.Time // each endpoint's first request
	out   io.Writer
}

func (rc *Receivers) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.hits[r.URL.Path]++
	n := rc.hits[r.URL.Path]
	id := r.Header.Get("Webhook-Id")
	if n == 1 {
		rc.first[r.URL.Path] = time.Now()
	}
	at := time.Since(rc.first[r.URL.Path]).Round(10 * time.Millisecond)
	answer := func(code int) {
		fmt.Fprintf(rc.out, "  %-10s request %d for %s at +%v -> %d %s\n", r.URL.Path, n, id, at, code, http.StatusText(code))
		w.WriteHeader(code)
	}
	// Check the signature the way a customer would, with the shared secret.
	body, _ := io.ReadAll(r.Body)
	mac := hmac.New(sha256.New, []byte("secret-"+r.URL.Path[1:]))
	mac.Write(body)
	if !hmac.Equal([]byte(r.Header.Get("Webhook-Signature")), []byte("sha256="+hex.EncodeToString(mac.Sum(nil)))) {
		answer(http.StatusUnauthorized)
		return
	}
	switch r.URL.Path {
	case "/acme":
		if n <= 2 {
			answer(http.StatusServiceUnavailable)
			return
		}
		answer(http.StatusOK)
	case "/globex":
		answer(http.StatusGone)
	default:
		answer(http.StatusInternalServerError)
	}
}

func run(dir string, out io.Writer) ([]flowchassis.RunInfo, *Receivers, error) {
	out = fake.SyncWriter(out)
	// The customers' endpoints, on a local port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	recv := &Receivers{hits: map[string]int{}, first: map[string]time.Time{}, out: out}
	srv := &http.Server{Handler: recv}
	go srv.Serve(ln)
	defer srv.Close()
	base := "http://" + ln.Addr().String()

	outbox, err := fake.Open(dir, "outbox", out)
	if err != nil {
		return nil, nil, err
	}
	defer outbox.Close()
	deadLetter, err := fake.Open(dir, "deadletter", out)
	if err != nil {
		return nil, nil, err
	}
	defer deadLetter.Close()

	e, err := flowchassis.Open(filepath.Join(dir, "webhook.journal"), flowchassis.Options{Workers: 1})
	if err != nil {
		return nil, nil, err
	}
	defer e.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	if err := e.Register(webhookFlow(outbox, deadLetter, client)); err != nil {
		return nil, nil, err
	}

	var runs []flowchassis.RunInfo
	for _, customer := range []string{"acme", "globex", "initech"} {
		id := "evt-1001-" + customer
		d, _ := json.Marshal(Delivery{
			Customer: customer,
			URL:      base + "/" + customer,
			Secret:   "secret-" + customer,
			Event:    json.RawMessage(`{"type":"order.paid","order":"1001","amount":4200}`),
		})
		fmt.Fprintf(out, "%s:\n", id)
		if _, err := e.Start("webhook", id, d); err != nil {
			return nil, nil, err
		}
		r, err := e.Wait(context.Background(), id)
		if err != nil {
			return nil, nil, err
		}
		fmt.Fprintf(out, "  => %s", r.Status)
		if r.Error != "" {
			fmt.Fprintf(out, " (%s)", r.Error)
		}
		fmt.Fprintln(out)
		runs = append(runs, r)
	}
	return runs, recv, nil
}

func main() {
	dirFlag := flag.String("dir", "", "where to keep the journal and the queues' logs (default: a new temporary directory)")
	flag.Parse()
	dir, err := fake.Dir(*dirFlag)
	if err == nil {
		_, _, err = run(dir, os.Stdout)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	j := filepath.Join(dir, "webhook.journal")
	fmt.Printf("\nJournal: %s\nSee the retries and their times with: flowchassis show %s evt-1001-initech\n", j, j)
}
