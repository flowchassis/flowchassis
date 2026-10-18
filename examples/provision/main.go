// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

// Provision: bring up a site on a new server. Create the VM, point DNS at
// it, get a TLS certificate, then wait until it answers its health check.
//
// Three sites are set up. shop.example.com works, after a few health checks
// while the VM boots. The CA refuses blog.example.net, so its DNS record is
// removed and its VM destroyed, newest first. legacy.example.org is refused
// too, but the DNS provider's API is down and the record can't be removed:
// that run stops as failed, with the VM still there, and waits for a person.
//
//	go run ./examples/provision
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/flowchassis/flowchassis"
	"github.com/flowchassis/flowchassis/examples/internal/fake"
)

// Site is a run's input.
type Site struct {
	Domain string `json:"domain"`
	Size   string `json:"size"`
}

// Cloud holds the outside services the flow calls.
type Cloud struct {
	Compute, DNS, CA *fake.Service
}

func provisionFlow(cloud Cloud) flowchassis.Flow {
	site := func(c *flowchassis.Ctx) Site {
		var s Site
		json.Unmarshal(c.Input(), &s)
		return s
	}
	get := func(c *flowchassis.Ctx, key string) string {
		v, _ := c.Get(key)
		return string(v)
	}
	return flowchassis.Flow{Name: "provision", Steps: []flowchassis.Step{
		{
			Name: "create_vm",
			Do: func(c *flowchassis.Ctx) error {
				vm, err := cloud.Compute.Create(c, "create "+site(c).Size+" VM for "+site(c).Domain)
				if err != nil {
					return err
				}
				c.Set("vm", []byte(vm))
				return nil
			},
			Undo: func(c *flowchassis.Ctx) error {
				return cloud.Compute.Do(c, "destroy "+get(c, "vm"))
			},
		},
		{
			Name: "add_dns",
			Do: func(c *flowchassis.Ctx) error {
				rec, err := cloud.DNS.Create(c, "A record "+site(c).Domain+" pointing at "+get(c, "vm"))
				if err != nil {
					return err
				}
				c.Set("dns", []byte(rec))
				return nil
			},
			Undo: func(c *flowchassis.Ctx) error {
				return cloud.DNS.Do(c, "remove "+get(c, "dns"))
			},
			// Also the policy for the undo: three tries to remove the
			// record, then the run stops and waits for a person.
			Retry: flowchassis.RetryPolicy{MaxAttempts: 3, Backoff: 50 * time.Millisecond},
		},
		{
			Name: "issue_cert",
			Do: func(c *flowchassis.Ctx) error {
				cert, err := cloud.CA.Create(c, "certificate for "+site(c).Domain)
				if errors.Is(err, errCAARefused) {
					// A DNS policy won't change on a retry.
					return flowchassis.Permanent(err)
				}
				if err != nil {
					return err
				}
				c.Set("cert", []byte(cert))
				return nil
			},
			Undo: func(c *flowchassis.Ctx) error {
				return cloud.CA.Do(c, "revoke "+get(c, "cert"))
			},
		},
		{
			Name: "health_check",
			Do: func(c *flowchassis.Ctx) error {
				return cloud.Compute.Do(c, "GET https://"+site(c).Domain+"/health")
			},
			// A VM takes a while to boot: keep checking, waiting longer
			// each time, for up to about half a minute.
			Retry: flowchassis.RetryPolicy{MaxAttempts: 12, Backoff: 100 * time.Millisecond, Multiplier: 2, MaxBackoff: 5 * time.Second},
		},
	}}
}

// faults: VMs answer their health check from the third try. The CA refuses
// the .net and .org sites for good. The DNS API is down for legacy.
func faults(cloud Cloud) {
	cloud.Compute.Fail = func(c *flowchassis.Ctx, what string) error {
		if c.Step == "health_check" && c.Attempt <= 2 {
			return errors.New("connection refused, still booting")
		}
		return nil
	}
	cloud.CA.Fail = func(c *flowchassis.Ctx, what string) error {
		if !c.Undoing && !strings.HasSuffix(what, ".com") {
			return errCAARefused
		}
		return nil
	}
	cloud.DNS.Fail = func(c *flowchassis.Ctx, what string) error {
		if c.Undoing && c.Run == "legacy.example.org" {
			return errors.New("DNS provider API: 503 service unavailable")
		}
		return nil
	}
}

// errCAARefused is the CA's answer when the domain's CAA record names
// another CA.
var errCAARefused = errors.New("CAA record forbids this CA for the domain")

var sites = []Site{
	{"shop.example.com", "small"},
	{"blog.example.net", "small"},
	{"legacy.example.org", "large"},
}

func run(dir string, out io.Writer) ([]flowchassis.RunInfo, error) {
	out = fake.SyncWriter(out)
	var cloud Cloud
	for name, p := range map[string]**fake.Service{"compute": &cloud.Compute, "dns": &cloud.DNS, "ca": &cloud.CA} {
		svc, err := fake.Open(dir, name, out)
		if err != nil {
			return nil, err
		}
		defer svc.Close()
		*p = svc
	}
	faults(cloud)

	e, err := flowchassis.Open(filepath.Join(dir, "provision.journal"), flowchassis.Options{Workers: 1})
	if err != nil {
		return nil, err
	}
	defer e.Close()
	if err := e.Register(provisionFlow(cloud)); err != nil {
		return nil, err
	}

	var runs []flowchassis.RunInfo
	for _, s := range sites {
		b, _ := json.Marshal(s)
		fmt.Fprintf(out, "%s:\n", s.Domain)
		if _, err := e.Start("provision", s.Domain, b); err != nil {
			return nil, err
		}
		r, err := e.Wait(context.Background(), s.Domain)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(out, "  => %s", r.Status)
		if r.Error != "" {
			fmt.Fprintf(out, " (%s)", r.Error)
		}
		fmt.Fprintln(out)
		if r.Status == flowchassis.Failed {
			fmt.Fprintf(out, "     left for a person: %s\n", stateLine(r))
		}
		runs = append(runs, r)
	}
	return runs, nil
}

// stateLine lists what a run still holds: the IDs its completed steps
// saved, which a person needs to clean up by hand.
func stateLine(r flowchassis.RunInfo) string {
	var parts []string
	for _, k := range []string{"vm", "dns", "cert"} {
		if v, ok := r.State[k]; ok {
			parts = append(parts, k+"="+string(v))
		}
	}
	return strings.Join(parts, " ")
}

func main() {
	dirFlag := flag.String("dir", "", "where to keep the journal and the fake services' logs (default: a new temporary directory)")
	flag.Parse()
	dir, err := fake.Dir(*dirFlag)
	if err == nil {
		_, err = run(dir, os.Stdout)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	j := filepath.Join(dir, "provision.journal")
	fmt.Printf("\nJournal: %s\nSee the failed run's history with: flowchassis show %s legacy.example.org\n", j, j)
}
