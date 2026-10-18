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

- If it is the last thing in the file (the frame runs to or past the end), or
  everything from there on is zeros, it is a torn write from a crash and is
  cut off.
- Otherwise the file is damaged, and reading fails with `ErrCorrupt`. Nothing
  after the damage is used.

## Records

Every record has `t` (type), `run` (run ID) and `at` (UTC time). Byte fields
(`input`, values in `state`) are base64, as Go's JSON encoding writes them.

| `t` | Written when | Other fields |
|---|---|---|
| `start` | `Start` | `flow`, `input`, `next`: the first step |
| `begin` | an attempt of a step is about to run | `step`, `seq`, `attempt` |
| `done` | the step completed | `step`, `seq`, `state`: the whole state after it, `next`: the step after it, or `""` for the end |
| `fail` | an attempt failed | `step`, `attempt`, `err`, and `retry_at` or `final: true` |
| `cancel` | `Cancel` | |
| `comp` | compensation begins | `err`: why |
| `ubegin` | an attempt of an undo is about to run | `step`, `seq` (of the completion being undone), `attempt` |
| `udone` | the undo completed | `step`, `seq` |
| `ufail` | an undo attempt failed | `step`, `seq`, `attempt`, `err`, and `retry_at` or `final: true` |
| `end` | the run finished | `status`: `completed`, `compensated` or `failed`, and `err` |
| `snapshot` | `Compact` | `snap`: the whole run in one record |

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
started  2026-10-18T08:45:21+03:00
ended    2026-10-18T08:45:22+03:00
done     reserve, charge, receipt
undone   charge, reserve
input    {"order":2}
state
  charge = order-0002/charge/2
  receipt = order-0002/receipt/3
  reserve = order-0002/reserve/1
history
  08:45:21.989  started order
  08:45:21.990  reserve: attempt 1 began
  08:45:22.012  reserve: done, next charge
  08:45:22.014  charge: attempt 1 began
  08:45:22.035  charge: attempt 1 failed, retry at 08:45:22.037: card network timed out
  08:45:22.194  charge: attempt 2 began
  08:45:22.215  charge: attempt 2 failed, retry at 08:45:22.219: card network timed out
  08:45:22.219  charge: attempt 3 began
  08:45:22.240  charge: done, next receipt
  08:45:22.241  receipt: attempt 1 began
  08:45:22.262  receipt: done, next ship
  08:45:22.263  ship: attempt 1 began
  08:45:22.284  ship: attempt 1 failed for good: carrier refused the address
  08:45:22.284  compensating: step ship: carrier refused the address
  08:45:22.285  undo charge: attempt 1 began
  08:45:22.306  undo charge: done
  08:45:22.306  undo reserve: attempt 1 began
  08:45:22.328  undo reserve: done
  08:45:22.328  ended compensated (step ship: carrier refused the address)
```
