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
3. If a write or flush fails, the engine stops. Nothing more is written, not
   even records already queued behind the failed one. It runs no more steps,
   `Start` and `Cancel` return `ErrStopped`, and `Err` says why. Nothing in
   memory gets ahead of the journal. Opening the journal again resumes from
   its last good record.
4. A write torn by a crash at the end of the journal is cut off when the
   journal is opened, and the cut bytes are kept in `JOURNAL.torn`. A tear is
   recognized only when nothing whole follows it: less than a frame header,
   zeros, or the start of a frame that runs to the end of the file, with no
   checksum-valid frame anywhere after. Anything else is damage, and `Open`
   fails with `ErrCorrupt` without changing the file. The engine never skips
   damaged records to reach later ones. (A power cut that leaves a later part
   of the last write on disk but not an earlier part looks like damage, and is
   refused rather than guessed at.) A journal whose header a crash cut short,
   or left as zeros, opens as empty, with the stray bytes kept the same way.
5. The writer checks every record against the runs before writing it: a
   record for a run that has finished, or that compaction dropped, is never
   written. `Cancel` answers `ErrFinished` or `ErrNotFound` in that case.

## Execution

6. Steps run at least once. A step whose begin record is in the journal and
   whose done or failed record isn't was interrupted, and runs again after the
   next `Open`, with `Attempt` one higher. Steps already done don't run again.
   A step that completes while `Close` waits for it is recorded as done.
7. `Ctx.Key` is `run/step/seq` for a step and `run/step/seq/undo` for its
   undo. It is the same on every attempt and after every restart, and differs
   between steps, between two passes of a run through the same step, and
   between runs.
8. A step's state changes are kept only if it completes. A failed attempt's
   changes are thrown away, so the next attempt starts from the state the
   previous step left.
9. One run executes one step at a time. Different runs execute in parallel,
   up to `Options.Workers`.
10. A panic in a step is caught and treated as that attempt failing.

## Retries

11. A failed attempt is retried after `Backoff * Multiplier^(n-1)`, capped at
    `MaxBackoff`, until `MaxAttempts` attempts have been made. The time of the
    next attempt is in the journal, so a restart doesn't shorten or reset the
    wait. A policy's zero fields come from `Options.Retry` and then
    `DefaultRetry`, except `MaxBackoff`, where zero means no cap.
12. An error wrapped with `Permanent` isn't retried.
13. Attempts count across restarts. An attempt cut off by a crash counts, so a
    step that brings the process down every time gives up at `MaxAttempts`
    and the run compensates, without the step running again. An attempt cut
    off by `Close`, where the step returns an error because its context was
    canceled, is given back with a yield record and doesn't count.

## Compensation

14. A run compensates when a step fails for good (attempts used up, a
    `Permanent` error, or a `Next` to a step the flow doesn't have), or when it
    is canceled. Failing for good and starting compensation are one record,
    so no crash can fall between them.
15. Compensation calls `Undo` for each completed step, newest first. Steps
    with no `Undo` are passed over. The step that failed isn't undone, because
    it never completed: a step that fails part way must leave nothing behind,
    or clean up after itself.
16. `Undo` sees the input and the state exactly as its own step left them. Its
    changes to the state are ignored.
17. An undo is retried by the same rules as its step. If it fails for good, the
    run ends `failed` with the error, in the same record, and compensation
    stops there. A `failed` run needs a person.
18. A step that completes after the run was canceled is undone with the rest.

## Starting and canceling

19. `Start` with an ID that exists returns that ID if the flow and input match,
    and `ErrConflict` if not. It never starts a second run with an ID the
    journal holds. Many callers starting one ID at once get one run. A run that
    `Compact` dropped is no longer held, so its ID can be started again: keep
    finished runs as long as callers might retry a `Start`.
20. `Cancel` writes a cancel record before it returns, so a cancel survives a
    crash. A run waiting to retry goes to compensation at once. A finished run
    gives `ErrFinished`.
21. `Wait` returns a finished run even after `Close`.

## Flows and versions

22. Runs resume by flow name and step name. A run whose flow isn't registered
    waits, unchanged, until it is. A run whose current step was renamed or
    removed fails for good and compensates. A completed step whose name is gone
    from the flow can't be undone, so the run ends `failed`.
23. The journal records the next step when a step completes, so changing the
    order of steps affects runs only from their next step on.

## Files and processes

24. `Open` takes an exclusive lock on `JOURNAL.lock`. A second `Open` of the
    same journal, from any process, fails with `ErrLocked`. The operating
    system releases the lock when the process ends, however it ends. (On
    Solaris, AIX and systems without `flock` or `LockFileEx`, there is no lock:
    don't open one journal twice there.)
25. `ReadJournal`, and the `runs`, `show` and `check` commands built on it,
    read without the lock and may see a torn final record from a write under
    way, which they leave out. On Windows they open the file so that
    compaction can still replace it.
26. `Compact` writes a new journal beside the old one, flushes it, and moves it
    over the old one. A crash during compaction leaves one or the other whole.
    Each run becomes a snapshot record plus one record for the state each of
    its completed steps left, so no record is bigger than the ones it replaces.
