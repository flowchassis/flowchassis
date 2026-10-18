# Notes for Claude

This file says where the next session starts and what comes next. What comes
next is decided with the owner before it goes here, and work starts only when
the owner says go.

## Releases

Releases are automated. To release, raise `Version` in `flowchassis.go` and
push to `main`; never ask the owner to publish one. [RELEASING.md](RELEASING.md)
has the procedure and the checks to run afterwards.

## Where things stand

v0.1.0 is the first release (October 2026): Apache 2.0, CI on Linux, macOS and
Windows, and automated releases with smoke-tested binaries for Linux, macOS and
Windows.

FlowChassis is a workflow execution kernel: a Go library with no dependencies
outside the standard library. Flows of named steps; a checkpoint before and
after every step in one append-only journal file (CRC-32C framed JSON records,
flushed to disk, group commit); retries with backoff; compensation (undo of
completed steps, newest first); cancel; resume after a crash; compaction; a
file lock against a second process. `cmd/flowchassis` inspects journals (runs,
show, check, compact) and runs the crash demo, which kills real worker
processes. Read [docs/CONTRACT.md](docs/CONTRACT.md) before changing behavior;
[docs/JOURNAL.md](docs/JOURNAL.md) is the file format.

Code map: `flowchassis.go` (types, errors, retry policy, Ctx), `journal.go`
(framing, torn-tail rule, FileStore, MemStore, ReadJournal), `state.go` (the
run state machine: `apply` is the only place run state changes, used both on
replay and while running), `engine.go` (scheduling, step and undo execution,
group commit, Start, Cancel, Compact, Close), `lock_*.go` (flock, LockFileEx),
`internal/demo` (the order flow, ledger, supervisor and checks behind the crash
tests and `flowchassis demo`), `examples/` (four runnable programs, signup,
provision, media and webhook, each with a test, over fake services in
`examples/internal/fake` that log their effects and honour idempotency keys;
the site's case studies quote their code and output, so keep the two in step).

Out of scope unless the owner decides otherwise: several processes sharing one
journal, a server or network API, a graphical builder or dashboard, exactly-once
steps.

## Test results

`sh scripts/record-tests.sh` runs every test, the race detector, the release
plan's cases, the benchmarks, both crash demos (the default and 1,000 orders
through 200 kills) and the smoke check on a static binary, measures sizes, and
writes them to `test/results/`. Every number on the site and in the README
comes from those files; record again after any change that could move them.
It needs Go and Python 3.

## Site

FlowChassis.com is in `photozeitgeist/flowchassis.com` (October 2026): the Hugo
Launch template, unchanged, with content and config values only. Pages: about,
contact, quick start, download, reference. Posts: the launch, a plain-language
explainer of workflow kernels and sagas, how it was tested, the name, and four
case studies tagged "case studies" (one per example program), linked from the
Case Studies menu entry.
Images are drawn into static/images. Every number comes from `test/results/`;
when a recording changes a number, update the site too.

## Next

Nothing is decided. Agree it with the owner before it goes here.
