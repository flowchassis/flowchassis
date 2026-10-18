# The journal file

One file holds every run. It is append-only: records are only ever added at
the end, except when `Compact` replaces the whole file.

## Layout

```
FLOWCJ01                         8-byte header: magic and format version
[len][crc][payload]              a record
[len][crc][payload]              ...
```

| Part | Size | Meaning |
|---|---|---|
| `len` | 4 bytes, little-endian | payload length, 1 to 16 MiB |
| `crc` | 4 bytes, little-endian | CRC-32C (Castagnoli) of the payload |
| `payload` | `len` bytes | one JSON object |

The version in the header is `JournalFormat`. A build refuses a journal of a
format it doesn't know.

## Reading it back

Records are read from the start. A record whose checksum fails, whose length
is zero or too big, or that is cut short, ends the read:

- It is a torn write from a crash, and is cut off, only if nothing whole
  follows it: fewer than 8 bytes are left, or everything from there on is
  zeros, or the frame has a plausible length that runs to or past the end of
  the file and no checksum-valid frame starts anywhere after it. The bytes cut
  off are appended to `JOURNAL.torn` and flushed before the journal is cut.
- Otherwise the file is damaged, and reading fails with `ErrCorrupt`. Nothing
  after the damage is used, and the file isn't changed.

A file shorter than the header that is the start of it, or a file of zeros,
is a new journal whose header a crash cut short: it opens as empty, with the
stray bytes kept in `JOURNAL.torn`.

`flowchassis check` exits 0 for a good journal, 1 for a damaged one, and 3
for one that ends in a torn write.

## Records

Every record has `t` (type), `run` (run ID) and `at` (UTC time). Byte fields
(`input`, values in `state`) are base64, as Go's JSON encoding writes them.

| `t` | Written when | Other fields |
|---|---|---|
| `start` | `Start` | `flow`, `input`, `next`: the first step |
| `begin` | an attempt of a step is about to run | `step`, `seq`, `attempt` |
| `done` | the step completed | `step`, `seq`, `state`: the whole state after it, `next`: the step after it, or `""` for the end |
| `fail` | an attempt failed | `step`, `attempt`, `err`, and `retry_at`; or `final: true`, which also starts compensation |
| `yield` | `Close` cut an attempt off; it doesn't count | `step`, `seq`, `attempt` |
| `cancel` | `Cancel` | |
| `comp` | compensation begins after a cancel | `err`: why |
| `ubegin` | an attempt of an undo is about to run | `step`, `seq` (of the completion being undone), `attempt` |
| `udone` | the undo completed | `step`, `seq` |
| `ufail` | an undo attempt failed | `step`, `seq`, `attempt`, `err`, and `retry_at`; or `final: true`, which also ends the run `failed` |
| `uyield` | `Close` cut an undo attempt off; it doesn't count | `step`, `seq`, `attempt` |
| `end` | the run finished | `status`: `completed`, `compensated` or `failed`, and `err` |
| `snapshot` | `Compact` | `snap`: the run, without the state of its completed steps |
| `sstate` | `Compact`, after a snapshot | `seq`, `state`: the state one completed step left |

A run's status, current step, attempts, state and history all follow from its
records in order. `Replay` does that, and the engine uses the same code both
when it opens a journal and as it writes, so what it holds in memory is
always what the file says.

## An example

From the crash demo, an order whose card charge failed twice before it went
through, and whose shipping was then refused. `flowchassis show` prints its
history from these records:

```
$ flowchassis show orders.journal order-0002
run      order-0002
flow     order
status   compensated
error    step ship: carrier refused the address
started  2026-10-18T09:50:07+03:00
ended    2026-10-18T09:50:07+03:00
done     reserve, charge, receipt
undone   charge, reserve
input    {"order":2}
state
  charge = order-0002/charge/2
  receipt = order-0002/receipt/3
  reserve = order-0002/reserve/1
history
  09:50:07.393  started order
  09:50:07.393  reserve: attempt 1 began
  09:50:07.416  reserve: done, next charge
  09:50:07.416  charge: attempt 1 began
  09:50:07.437  charge: attempt 1 failed, retry at 09:50:07.439: card network timed out
  09:50:07.595  charge: attempt 2 began
  09:50:07.617  charge: attempt 2 failed, retry at 09:50:07.621: card network timed out
  09:50:07.621  charge: attempt 3 began
  09:50:07.642  charge: done, next receipt
  09:50:07.643  receipt: attempt 1 began
  09:50:07.664  receipt: done, next ship
  09:50:07.665  ship: attempt 1 began
  09:50:07.686  ship: attempt 1 failed for good, compensating: carrier refused the address
  09:50:07.687  undo charge: attempt 1 began
  09:50:07.709  undo charge: done
  09:50:07.710  undo reserve: attempt 1 began
  09:50:07.732  undo reserve: done
  09:50:07.732  ended compensated (step ship: carrier refused the address)
```
