// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

package flowchassis_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/flowchassis/flowchassis"
)

// An order flow: reserve stock, charge the card, ship. When shipping is
// refused, the charge is refunded and the stock released, newest first.
func Example() {
	dir, _ := os.MkdirTemp("", "example")
	defer os.RemoveAll(dir)

	e, err := flowchassis.Open(filepath.Join(dir, "orders.journal"), flowchassis.Options{})
	if err != nil {
		panic(err)
	}
	defer e.Close()

	err = e.Register(flowchassis.Flow{Name: "order", Steps: []flowchassis.Step{
		{
			Name: "reserve",
			Do: func(c *flowchassis.Ctx) error {
				fmt.Println("reserve stock for", string(c.Input()))
				c.Set("reservation", []byte("R-1001"))
				return nil
			},
			Undo: func(c *flowchassis.Ctx) error {
				r, _ := c.Get("reservation")
				fmt.Println("release reservation", string(r))
				return nil
			},
		},
		{
			Name: "charge",
			Do: func(c *flowchassis.Ctx) error {
				// c.Key is the same on every attempt: an idempotency key
				// for the payment provider.
				fmt.Println("charge card, idempotency key", c.Key)
				return nil
			},
			Undo: func(c *flowchassis.Ctx) error {
				fmt.Println("refund, idempotency key", c.Key)
				return nil
			},
			Retry: flowchassis.RetryPolicy{MaxAttempts: 5, Backoff: 200 * time.Millisecond},
		},
		{
			Name: "ship",
			Do: func(c *flowchassis.Ctx) error {
				return flowchassis.Permanent(errors.New("carrier refused the address"))
			},
		},
	}})
	if err != nil {
		panic(err)
	}

	id, _ := e.Start("order", "order-1001", []byte("order 1001"))
	run, _ := e.Wait(context.Background(), id)
	fmt.Println(run.Status, "-", run.Error)
	fmt.Println("undone:", run.Undone)

	// Output:
	// reserve stock for order 1001
	// charge card, idempotency key order-1001/charge/2
	// refund, idempotency key order-1001/charge/2/undo
	// release reservation R-1001
	// compensated - step ship: carrier refused the address
	// undone: [charge reserve]
}
