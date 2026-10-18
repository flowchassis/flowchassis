// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

// Signup: create the account, give it storage, send the welcome email, then
// switch it on. Three people sign up. Ana's goes straight through. Ben's
// welcome email hits a mail provider that times out twice, so that step is
// retried. Cy picks a plan the storage provider can't serve, so his account
// is deleted again and nothing is left behind.
//
//	go run ./examples/signup
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
	"time"

	"github.com/flowchassis/flowchassis"
	"github.com/flowchassis/flowchassis/examples/internal/fake"
)

// Signup is a run's input.
type Signup struct {
	Email string `json:"email"`
	Plan  string `json:"plan"`
}

// Services are the outside systems the flow calls.
type Services struct {
	Users, Storage, Mail *fake.Service
}

func signupFlow(s Services) flowchassis.Flow {
	input := func(c *flowchassis.Ctx) Signup {
		var in Signup
		json.Unmarshal(c.Input(), &in)
		return in
	}
	return flowchassis.Flow{Name: "signup", Steps: []flowchassis.Step{
		{
			Name: "create_user",
			Do: func(c *flowchassis.Ctx) error {
				id, err := s.Users.Create(c, "create user "+input(c).Email)
				if err != nil {
					return err
				}
				c.Set("user", []byte(id)) // saved with the checkpoint
				return nil
			},
			Undo: func(c *flowchassis.Ctx) error {
				id, _ := c.Get("user")
				return s.Users.Do(c, "delete user "+string(id))
			},
		},
		{
			Name: "provision_storage",
			Do: func(c *flowchassis.Ctx) error {
				id, err := s.Storage.Create(c, "create bucket, plan "+input(c).Plan)
				if errors.Is(err, errNoRegion) {
					// Retrying won't help: compensate now.
					return flowchassis.Permanent(err)
				}
				if err != nil {
					return err // anything else: try again
				}
				c.Set("bucket", []byte(id))
				return nil
			},
			Undo: func(c *flowchassis.Ctx) error {
				id, _ := c.Get("bucket")
				return s.Storage.Do(c, "delete bucket "+string(id))
			},
		},
		{
			// No Undo: an email can't be unsent.
			Name: "send_welcome",
			Do: func(c *flowchassis.Ctx) error {
				return s.Mail.Do(c, "send welcome email to "+input(c).Email)
			},
			Retry: flowchassis.RetryPolicy{MaxAttempts: 5, Backoff: 100 * time.Millisecond},
		},
		{
			Name: "activate",
			Do: func(c *flowchassis.Ctx) error {
				id, _ := c.Get("user")
				return s.Users.Do(c, "activate user "+string(id))
			},
		},
	}}
}

// faults makes Ben's mail time out twice and refuses Cy's plan for good.
func faults(s Services) {
	s.Mail.Fail = func(c *flowchassis.Ctx, what string) error {
		if c.Run == "signup-ben" && c.Attempt <= 2 {
			return errors.New("mail provider timed out")
		}
		return nil
	}
	s.Storage.Fail = func(c *flowchassis.Ctx, what string) error {
		if c.Run == "signup-cy" && !c.Undoing {
			return fmt.Errorf("%w enterprise-eu", errNoRegion)
		}
		return nil
	}
}

// errNoRegion is the storage provider's answer for a plan no region offers.
var errNoRegion = errors.New("no storage region offers plan")

var signups = []Signup{
	{"ana@example.com", "free"},
	{"ben@example.com", "pro"},
	{"cy@example.com", "enterprise-eu"},
}

func run(dir string, out io.Writer) ([]flowchassis.RunInfo, error) {
	out = fake.SyncWriter(out)
	var s Services
	for name, p := range map[string]**fake.Service{"users": &s.Users, "storage": &s.Storage, "mail": &s.Mail} {
		svc, err := fake.Open(dir, name, out)
		if err != nil {
			return nil, err
		}
		defer svc.Close()
		*p = svc
	}
	faults(s)

	e, err := flowchassis.Open(filepath.Join(dir, "signup.journal"), flowchassis.Options{Workers: 1})
	if err != nil {
		return nil, err
	}
	defer e.Close()
	if err := e.Register(signupFlow(s)); err != nil {
		return nil, err
	}

	var runs []flowchassis.RunInfo
	for _, in := range signups {
		b, _ := json.Marshal(in)
		id := "signup-" + in.Email[:len(in.Email)-len("@example.com")]
		fmt.Fprintf(out, "%s:\n", id)
		// Starting the same ID again finds the existing run, so a retried
		// HTTP request can't sign someone up twice.
		if _, err := e.Start("signup", id, b); err != nil {
			return nil, err
		}
		r, err := e.Wait(context.Background(), id)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(out, "  => %s", r.Status)
		if r.Error != "" {
			fmt.Fprintf(out, " (%s)", r.Error)
		}
		fmt.Fprintln(out)
		runs = append(runs, r)
	}
	return runs, nil
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
	fmt.Printf("\nJournal: %s\nSee one run's history with: flowchassis show %s signup-cy\n",
		filepath.Join(dir, "signup.journal"), filepath.Join(dir, "signup.journal"))
}
