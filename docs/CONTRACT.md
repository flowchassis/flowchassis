# The contract

What FlowChassis promises, and what it leaves to you. Code that changes any
of this changes the contract: update this file, the README and the tests
together.

## Durability

1. A record is on disk before the engine acts on it. `Start` returns after the
   run's start record is flushed. A step's `Do` is called only after its begin
   record is flushed. A step counts as completed only once its done record is
   flushed, and the next step begins after that.
2. Records written at the same moment by different runs share one write and
   one flush (group commit). Each caller still waits for its own record to be
   flushed.
3. If a write or flush fails, the engine stops. It runs no more steps, `Start`
   and `Cancel` return `ErrStopped`, and `Err` says why. Nothing in memory gets
   ahead of the journal. Opening the journal again resumes from its last good
   record.
4. A record cut short by a crash at the end of the journal is removed when the
   journal is opened. Damage anywhere else makes `Open` fail with `ErrCorrupt`.
   The engine never skips damaged records to reach later ones.

## Execution

5. Steps run at least once. A step whose begin record is in the journal and
   whose done or failed record isn't was interrupted, and runs again after the
   next `Open`, with `Attempt` one higher. Steps already done don't run again.
6. `Ctx.Key` is `run/step/seq` for a step and `run/step/seq/undo` for its
   undo. It is the same on every attempt and after every restart, and differs
   between steps, between two passes of a run through the same step, and
   between runs.
7. A step's state changes are kept only if it completes. A failed attempt's
   changes are thrown away, so the next attempt starts from the state the
   previous step left.
8. One run executes one step at a time. Different runs execute in parallel,
   up to `Options.Workers`.
9. A panic in a step is caught and treated as that attempt failing.

## Retries

10. A failed attempt is retried after `Backoff * Multiplier^(n-1)`, capped at
    `MaxBackoff`, until `MaxAttempts` attempts have been made. The time of the
    next attempt is in the journal, so a restart doesn't shorten or reset the
    wait.
11. An error wrapped with `Permanent` isn't retried.
12. Attempts count across restarts. An attempt interrupted by a crash counts.
    An attempt interrupted by `Close`, where the step returns an error because
    its context was canceled, counts as begun but is not recorded as failed.

## Compensation

13. A run compensates when a step fails for good (attempts used up, a
    `Permanent` error, or a `Next` to a step the flow doesn't have), or when it
    is canceled.
14. Compensation calls `Undo` for each completed step, newest first. Steps
    with no `Undo` are passed over. The step that failed isn't undone, because
    it never completed: a step that fails part way must leave nothing behind,
    or clean up after itself.
15. `Undo` sees the input and the state exactly as its own step left them. Its
    changes to the state are ignored.
16. An undo is retried by the same rules as its step. If it fails for good, the
    run ends `failed` with the error, and compensation stops there. A `failed`
    run needs a person.
17. A step that completes after the run was canceled is undone with the rest.

## Starting and canceling

18. `Start` with an ID that exists returns that ID if the flow and input match,
    and `ErrConflict` if not. It never starts a second run with the same ID.
    Many callers starting one ID at once get one run.
19. `Cancel` writes a cancel record before it returns, so a cancel survives a
    crash. A run waiting to retry goes to compensation at once. A finished run
    gives `ErrFinished`.

## Flows and versions

20. Runs resume by flow name and step name. A run whose flow isn't registered
    waits, unchanged, until it is. A run whose current step was renamed or
    removed fails for good and compensates. A completed step whose name is gone
    from the flow can't be undone, so the run ends `failed`.
21. The journal records the next step when a step completes, so changing the
    order of steps affects runs only from their next step on.

## Files and processes

22. `Open` takes an exclusive lock on `JOURNAL.lock`. A second `Open` of the
    same journal, from any process, fails with `ErrLocked`. The operating
    system releases the lock when the process ends, however it ends.
23. `ReadJournal`, and the `runs`, `show` and `check` commands built on it,
    read without the lock and may see a torn final record from a write under
    way, which they leave out.
24. `Compact` writes a new journal beside the old one, flushes it, and moves it
    over the old one. A crash during compaction leaves one or the other whole.
