# FlowChassis

A small workflow execution kernel for Go. It runs your workflows inside your
own program, keeps a checkpoint of every step in one journal file, retries
what fails, undoes what can't finish, and picks every run back up where it
stopped after a crash.

No server. No database. No other modules: just the Go standard library and a
file next to your program.

[flowchassis.com](https://flowchassis.com) · Apache 2.0 · Go 1.24 or later

```
go get github.com/flowchassis/flowchassis@latest
```

## The problem it solves

Your program takes an order. It reserves the stock, charges the card, books
the courier. Then the process dies, between the charge and the courier.

What now? The card was charged. Nobody shipped anything. When the program
comes back up, it has no idea how far it got.

The usual fixes are a status column and a pile of retry code, which work until
the day they don't, or a workflow server such as Temporal, which is a whole
system to run beside yours. FlowChassis sits in between. It is a library. You
describe the order as steps, and it writes down where each run is before and
after every step. After a crash it reads that back and carries on. If a run
can't finish, it runs each finished step's undo (refund the card, release the
stock) in reverse order. People call that pattern a saga, and the undo part
compensation.

The graphical builders, dashboards and hosted services are left to other
products. This is the part underneath them: the chassis.

## How it works, in plain words

1. You write a **flow**: a name and a list of **steps**. Each step has a `Do`
   function, and optionally an `Undo` that reverses it.
2. You **start** a run of the flow with an ID and some input.
3. Before a step runs, FlowChassis writes a line to the journal: "run 42,
   step charge, attempt 1, starting". After it succeeds, another: "run 42,
   charge done, here is the run's state, next is ship". Each line is flushed
   to disk before anything else happens.
4. If a step fails, it is tried again after a wait that grows each time. You
   choose how many attempts.
5. If a step keeps failing, or fails in a way you mark as permanent, the run
   **compensates**: the `Undo` of every completed step runs, newest first.
6. If the process dies at any point, the next start reads the journal and
   every unfinished run continues from its last checkpoint. A step that was
   under way when the process died runs again.

That last point matters. Steps run **at least once**. Every step gets a
`Key` that stays the same across attempts and restarts: pass it to the
payment provider or the courier as an idempotency key, and a repeated call
has no second effect.

## Quick start

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/flowchassis/flowchassis"
)

func main() {
	e, err := flowchassis.Open("orders.journal", flowchassis.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()

	err = e.Register(flowchassis.Flow{Name: "order", Steps: []flowchassis.Step{
		{
			Name: "reserve",
			Do: func(c *flowchassis.Ctx) error {
				c.Set("reservation", []byte("R-1001")) // saved with the checkpoint
				return nil
			},
			Undo: func(c *flowchassis.Ctx) error {
				r, _ := c.Get("reservation") // the state as reserve left it
				fmt.Println("release", string(r))
				return nil
			},
		},
		{
			Name: "charge",
			Do: func(c *flowchassis.Ctx) error {
				return chargeCard(c, c.Key) // same Key on every attempt
			},
			Undo: func(c *flowchassis.Ctx) error {
				return refund(c, c.Key)
			},
			Retry: flowchassis.RetryPolicy{MaxAttempts: 5},
		},
		{
			Name: "ship",
			Do: func(c *flowchassis.Ctx) error {
				if refused {
					return flowchassis.Permanent(errors.New("address refused"))
				}
				return nil
			},
		},
	}})
	if err != nil {
		log.Fatal(err)
	}

	// Starting the same ID again returns the same run, so this is safe to
	// repeat after a crash.
	id, err := e.Start("order", "order-1001", []byte(`{"sku":"A-42"}`))
	if err != nil {
		log.Fatal(err)
	}
	run, _ := e.Wait(context.Background(), id)
	fmt.Println(run.Status) // completed, or compensated if ship was refused
}
```

`Open` takes a lock on the journal, so a second process can't open it by
mistake. Runs already in the journal resume as soon as you `Register` their
flow. There is no separate recovery call.

## Examples

Four complete programs in [examples](examples), each a common job with the
outside services faked so it runs anywhere. Each prints what it calls and
what happens, keeps its journal in a directory you can inspect with
`flowchassis show`, and has a test that CI runs on every change.

| Example | What it shows | Run it |
|---|---|---|
| [signup](examples/signup) | An account, its storage and a welcome email. A flaky mail provider is retried; a plan the storage can't serve deletes the account again | `go run ./examples/signup` |
| [provision](examples/provision) | A VM, a DNS record, a certificate and a health check that waits for boot. A refused certificate tears down DNS and the VM; an undo that keeps failing leaves the run `failed` for a person | `go run ./examples/provision` |
| [media](examples/media) | A long transcode done one segment per step with `Ctx.Next`. Kill it mid-way and run it again: it carries on at the segment it was on | `go run ./examples/media` |
| [webhook](examples/webhook) | Signed deliveries to a real local HTTP server, with doubling waits between attempts. 410 Gone stops at once; an endpoint that stays down is dead-lettered after six tries | `go run ./examples/webhook` |

The fake services in [examples/internal/fake](examples/internal/fake) keep a
log of what they did and honour idempotency keys, as real providers' APIs do:
a call with a key they've seen does nothing again.

## Reference

### Engine

| Call | What it does |
|---|---|
| `Open(path, Options)` | Opens or creates the journal file and locks it |
| `New(store, Options)` | The same over any `Store`, such as `NewMemStore` for tests |
| `Register(Flow)` | Adds a flow. Its runs in the journal start or resume |
| `Start(flow, id, input)` | Starts a run. An empty id gets a random one. Same id, flow and input: returns the existing run. Same id, anything else: `ErrConflict` |
| `Wait(ctx, id)` | Blocks until the run finishes |
| `Get(id)`, `List()` | A run, or all of them in start order, as `RunInfo` |
| `Cancel(id)` | Stops a run and undoes its completed steps. A step under way sees its context canceled |
| `Compact(before)` | Rewrites the journal with one record per run, leaving out runs that finished before `before` |
| `Close()` | Stops. A step interrupted by `Close` is not counted as failed and runs again on the next open |
| `Err()` | Why the engine stopped, if a journal write failed |

`Options` has `Workers` (runs executing at once, default 4), `Retry` (the
policy for steps that don't set one) and `Now` (for tests).

### Steps

A `Step` has a `Name`, a `Do`, an optional `Undo` and an optional `Retry`.

- **Name** identifies the step in the journal. Keep names stable between
  releases of your program, because unfinished runs resume by name.
- **Do** returns nil to complete. Its changes to the state are saved with the
  checkpoint. An error throws those changes away and retries.
- **Undo** reverses the step during compensation. It sees the state exactly
  as the step left it, even if later steps changed it. A step with no `Undo`
  is passed over.
- **Retry** is a `RetryPolicy`: `MaxAttempts` (the first included),
  `Backoff` (the first wait), `Multiplier` and `MaxBackoff`. Fields left at
  zero come from `Options.Retry`, then from the default of 3 attempts from
  100 ms, doubling, except `MaxBackoff`, where zero means no cap. Undo uses
  the same policy.

Inside a step, `*Ctx` gives you:

| | |
|---|---|
| `c` itself | A `context.Context`, canceled by `Cancel` or `Close` |
| `c.Input()` | The run's input |
| `c.Get`, `c.Set`, `c.Delete` | The run's state: named byte values, saved only when the step completes |
| `c.Next(name)` | Go to another step after this one. `c.Next("")` ends the run as completed |
| `c.Key` | Idempotency key: `run/step/seq`, plus `/undo` inside an Undo |
| `c.Attempt` | This attempt's number, counted across restarts |
| `c.Run`, `c.Flow`, `c.Step`, `c.Seq`, `c.Undoing` | Where you are |

`flowchassis.Permanent(err)` marks an error that retrying won't fix. The step
fails at once and the run compensates.

### Statuses

| Status | Meaning |
|---|---|
| `pending` | Started; no step has begun, or the flow isn't registered in this process yet |
| `running` | A step is under way or about to begin |
| `retrying` | A step failed and waits for its next attempt; `RetryAt` says when |
| `compensating` | Undoing completed steps, newest first |
| `completed` | Every step finished |
| `compensated` | The run failed or was canceled, and every completed step with an `Undo` was undone |
| `failed` | An `Undo` kept failing. The run stopped part way through compensation and needs a person |

### What happens when

- **The process is killed during a step.** On the next start the step runs
  again, attempt number one higher. Steps before it don't. Killed attempts
  count, so a step that crashes the process every time gives up at
  `MaxAttempts` and the run compensates.
- **`Close` is called during a step.** The step's context is canceled. If it
  returns the context's error, the attempt is given back and the step runs
  again on the next open as the same attempt. If it completes anyway, or
  fails for a reason of its own, that is recorded.
- **It is killed during an undo.** That undo runs again. Undos already done
  don't.
- **It is killed while a step waits to retry.** The wait goes on from the time
  in the journal.
- **The last write was torn by a crash or power cut.** The torn record is cut
  off on open, kept in `JOURNAL.torn`, and the run carries on from the record
  before it. That includes a group commit that reached the disk only in part.
  A tear counts as one only if nothing whole follows it and it looks like an
  unfinished write.
- **The journal is damaged anywhere else.** `Open` refuses it with
  `ErrCorrupt`, and leaves the file as it is, rather than drop what comes
  after. `flowchassis check` says where.
- **The disk is full, or a write fails.** The engine stops running steps and
  `Err` says why. It never acts on a step it couldn't record. Fix the disk,
  open the journal again, and runs resume from the last good record.

[docs/CONTRACT.md](docs/CONTRACT.md) has these rules in full, and
[docs/JOURNAL.md](docs/JOURNAL.md) the file format.

## The flowchassis command

```
flowchassis runs orders.journal                 every run: status, step, attempt, error
flowchassis runs orders.journal -status failed  just the ones that need a person
flowchassis show orders.journal order-1001      one run: state, input and full history
flowchassis check orders.journal                verify every record's checksum (exit 0, 1 damaged, 3 torn)
flowchassis compact orders.journal -drop-finished-before 720h
flowchassis demo                                the crash demo, described below
```

`runs`, `show` and `check` read the journal without locking it, so they work
while your program has it open. Add `-json` to `runs` and `show` for scripts.

Ready-built binaries for Linux, macOS and Windows are on the
[releases page](https://github.com/flowchassis/flowchassis/releases/latest).

## How it was tested

The promise is recoverable execution, so most of the testing kills things.

**The crash demo.** `flowchassis demo` runs a batch of orders through a
five-step flow in a worker process: reserve, charge, receipt, ship, notify.
Every step writes its effect to a ledger file, standing in for the outside
services a real flow would call. A supervisor kills the worker with SIGKILL
at random moments (TerminateProcess on Windows) and starts it again, over and
over. When a worker finally gets through the batch, every order is checked
against the ledger. About one order in four has its shipping refused and must
end compensated. About one in five has a card charge that fails twice first.

At its defaults, 200 orders went through 40 kills, which cut off 136 step
attempts and 14 undo attempts in the middle. Every order ended the way its
own rules said: 149 completed, 51 compensated, none lost, none left
unfinished. 4 effects were repeated, and the checks trace each one to a step
that a kill cut off after its effect and before its checkpoint, which is what
at-least-once means and why `Key` exists. A larger run of 1,000 orders through
200 kills, 16 in flight at once, came out the same: 750 completed, 250
compensated, 46 of 5,296 effects repeated, each traced the same way, and 0
problems across seven checks.

**Torn writes.** A test cuts the journal at every byte of its last record.
Every cut opens cleanly, keeps the cut bytes in `JOURNAL.torn`, and resumes
from the record before.

**Damage.** Flipped bytes, bad checksums and impossible lengths before the end
of the file are refused, and the file is left as it was, rather than skipped.
That includes one flipped bit in an early record's length, which a looser rule
would mistake for a torn tail.

**A full disk.** A store that starts failing mid-run stops the engine before
the next step. Nothing queued behind the failed write is written, and the run
resumes correctly once the journal opens again.

**An independent review.** Before the first release, a separate review read
the code against [the contract](docs/CONTRACT.md) and tried to break it. It
found real defects in that draft: a cancel racing with compaction that could
crash the program and leave a journal that wouldn't open, a narrow crash
window that let a step that had failed for good run again, a step that crashed
the process every time looping forever, and the flipped-bit case above, among
others. All are fixed, and each has a test that reproduces it
([review_test.go](review_test.go)). A second review then checked the fixes
themselves and found more: two `Close` calls after a failed write could panic,
a `Cancel` landing just before a run's end record could return success while
the run completed, and `Wait` could hand back a run a moment before it
finished. Those are fixed and tested too. The crash demo's checks were tightened at
the same time: every ledger line must now match an attempt in the journal, and
every repeated effect must come from an attempt a kill cut off. Planting the
step-runs-again defect back into the engine on purpose made the demo report
27 problems.

**The rest.** Every call and status has tests, as do retries and backoff,
cancel during a step and during a retry wait, compensation that resumes after
a restart, compaction in the middle of a compensation and with large states,
many callers starting the same ID at once, and group commit. 72 test
functions run on Linux, macOS and Windows in CI, and under the race detector
on Linux.

**Speed.** Every checkpoint is flushed to disk before its step goes on, so
speed depends on the disk. Records written at the same moment share one
flush, written by one writer goroutine. On the 2-CPU cloud machine the results
were recorded on, empty steps ran at about 1,800 a second with one worker,
6,900 with eight and 26,000 with 64.

All of this is recorded by `sh scripts/record-tests.sh` into
[test/results](test/results), and every number here comes from those files.

## Size

The library is about 2,000 lines of Go with no dependencies outside the
standard library. The `flowchassis` binary is 2.8 MB, static and stripped.

## What it doesn't do

- **One process per journal.** It runs inside one program on one machine.
  Several programs need several journals.
- **Steps run at least once, not exactly once.** Use `Key` with the services
  you call.
- **Compaction forgets.** A finished run that `Compact` drops is gone, and its
  ID can be started again. Keep finished runs for as long as a caller might
  retry a `Start`.
- **The state is for small things.** IDs, references, amounts. Each step's
  state is written whole with its checkpoint, up to 16 MB a record.
- **No graphical designer, dashboard or server.** That's the point.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
Copyright FlowChassis.com 2026.
