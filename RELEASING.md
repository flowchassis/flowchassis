# Releasing FlowChassis

Releases go out on their own. Nobody clicks Publish. This file is the record of
how that works, for whoever (person or Claude session) works on this repo next.

## The short version

1. Raise `const Version = "X.Y.Z"` in `flowchassis.go`.
2. Write what changed into the commit message.
3. Push the commit to `main`.

When the `test` workflow passes on that commit (the full suite and the smoke
check on Linux, macOS and Windows), the `release` workflow builds the binaries,
smoke-tests them on each kind of machine, then tags the commit `vX.Y.Z` and
publishes the release "FlowChassis vX.Y.Z" with the archives and SHA256SUMS
attached. If any build or check fails, nothing is tagged and nothing goes
public. Fix it, push again, and the next green run picks it up.

Pushing without raising the version is fine: the release workflow sees the tag
already exists and stops quietly.

## What it does, step by step

[.github/workflows/release.yml](.github/workflows/release.yml) starts when a
`test` run finishes; the push alone doesn't start it.

- **plan.** Goes on only if the `test` run passed, came from a push to `main` in
  this repo, and `main` still points at that commit (if `main` moved on, the run
  for the newer commit decides). Reads `Version` from `flowchassis.go`. Stops
  quietly if `vX.Y.Z` is already a tag. Fails loudly if the version is lower
  than the newest tag. [scripts/release-plan.py](scripts/release-plan.py) has
  the rules, and [scripts/release_plan_test.py](scripts/release_plan_test.py)
  its cases.
- **build.** Builds `flowchassis` from that exact commit for Linux (x86-64 and
  ARM64), macOS (Apple silicon and Intel) and Windows, all pure Go with cgo off,
  checks each file is the architecture it should be and that the Linux binary
  is static, and packages each with LICENSE, NOTICE, THIRD_PARTY_LICENSES.md
  and README.md.
- **check.** Runs [scripts/smoke.py](scripts/smoke.py) on the Linux x86-64,
  Linux ARM64, macOS Apple silicon and Windows binaries, each on its own kind
  of runner. The Intel macOS build is checked for its architecture only.
- **release.** Runs only after every build and check passed. Writes SHA256SUMS
  and the notes, then creates the tag and the release in one step with
  `gh release create --target <commit>`, on the repo's own `GITHUB_TOKEN`
  (`contents: write` on this job only). A `concurrency` group per tag means two
  runs for the same version can't publish twice.

The smoke check, on each binary: it reports the expected version and journal
format; the crash demo runs 40 orders while 10 worker processes are killed and
started again, and every order checks out against its ledger; the journal it
leaves passes `check`, `runs` lists the compensated orders the demo counted,
`show` prints a run, `compact` keeps all 40 runs; and `check` refuses a damaged
copy.

## Files that go out

| System | File |
|---|---|
| Linux, x86-64 (static) | flowchassis_linux_amd64.tar.gz |
| Linux, ARM64 (static) | flowchassis_linux_arm64.tar.gz |
| macOS, Apple silicon | flowchassis_darwin_arm64.tar.gz |
| macOS, Intel | flowchassis_darwin_amd64.tar.gz |
| Windows, x86-64 | flowchassis_windows_amd64.zip |
| Checksums | SHA256SUMS |

Archive names carry no version on purpose: links like
`https://github.com/flowchassis/flowchassis/releases/latest/download/flowchassis_linux_amd64.tar.gz`
keep working from one release to the next, so the README and the site never
need editing for a release.

## After a release

A Claude session can't create tags or releases itself, which is why the
workflow does it on the repository's token. After pushing a raised version,
check:

1. The `test` run and then the `release` run finished green
   (`gh run list --repo flowchassis/flowchassis`).
2. The release exists with all six files
   (`gh release view vX.Y.Z --repo flowchassis/flowchassis`).
3. The `latest/download` links resolve, and a downloaded archive's SHA256
   matches SHA256SUMS.
4. A downloaded Linux binary runs: `flowchassis version` names the release.
5. `go list -m github.com/flowchassis/flowchassis@vX.Y.Z` finds the module.

## By hand

- A release published by hand gets the binaries attached by the same workflow.
- To rebuild the files of an existing tag, run the `release` workflow by hand
  with the tag (`gh workflow run release --repo flowchassis/flowchassis -f tag=vX.Y.Z`).
