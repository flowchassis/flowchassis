#!/bin/sh
# Copyright FlowChassis.com 2026
# SPDX-License-Identifier: Apache-2.0
#
# Runs every test, the race detector, the crash demo, the benchmarks and the
# smoke check on a release-style binary, measures sizes, and writes it all to
# test/results/ with a note of the machine it ran on. The numbers in the
# README and on flowchassis.com come from these files.
#
#   sh scripts/record-tests.sh
#
# Needs Go and Python 3.
set -eu
cd "$(dirname "$0")/.."
out=test/results
mkdir -p "$out"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Whether the checkout was clean before anything was recorded.
state="at $(git rev-parse --short HEAD 2>/dev/null || echo 'no commit')$(test -z "$(git status --porcelain 2>/dev/null)" || echo ' plus uncommitted changes')"

header() {
	echo "# $1"
	echo "# recorded $(date -u '+%Y-%m-%d %H:%M UTC') $state, FlowChassis $(sed -n 's/^const Version = "\(.*\)"$/\1/p' flowchassis.go), $(go version | cut -d' ' -f3-)"
	cpu=$(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ //' || true)
	test -n "$cpu" || cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo 'unknown CPU')
	echo "# $(uname -sr), $cpu, $(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo '?') CPUs"
	echo "# command: $2"
	echo
}

test -z "$(gofmt -l .)" || { gofmt -l .; echo "gofmt: files above need formatting" >&2; exit 1; }
go vet ./...

cmd="go test -count=1 -v ./..."
{ header "Every test, verbose" "$cmd"; $cmd 2>&1; } > "$out/tests.txt"

cmd="go test -race -count=1 ./..."
{ header "Every test with the race detector" "$cmd"; $cmd 2>&1; } > "$out/race.txt"

cmd="python3 -B -m unittest discover -s scripts -p *_test.py -v"
{ header "The release plan's cases" "$cmd"; $cmd 2>&1; } > "$out/release-plan.txt"

# The benchmarks write to a journal file in a temporary directory, so the
# numbers are the disk's: every record is flushed before its step goes on.
cmd="go test -run ^\$ -bench Runs -benchtime 5000x -count 3 ."
{ header "Throughput: runs of three empty steps, every checkpoint flushed to disk" "$cmd"; go test -run '^$' -bench Runs -benchtime 5000x -count 3 . 2>&1; } > "$out/bench.txt"

# A static binary, built as the release workflow builds it.
mkdir "$work/bin"
CGO_ENABLED=0 go build -trimpath -ldflags '-s -w' -o "$work/bin/flowchassis" ./cmd/flowchassis

# The crash demo at its defaults. Progress goes to stderr; the JSON report
# on stdout is the raw numbers, and carries its own environment details.
"$work/bin/flowchassis" demo > "$out/demo.json" 2>"$work/demo.log" || { cat "$work/demo.log" >&2; exit 1; }
# A bigger one: more orders, more kills, more in flight at once.
"$work/bin/flowchassis" demo -runs 1000 -kills 200 -workers 16 -seed 7 > "$out/demo-large.json" 2>"$work/demo-large.log" || { cat "$work/demo-large.log" >&2; exit 1; }

cmd="python3 scripts/smoke.py BIN"
{ header "The release smoke check on a static Linux binary built as the release workflow builds it" "$cmd"; python3 scripts/smoke.py "$work/bin" 2>&1; } > "$out/smoke.txt"

# Sizes: the binary, what the library adds to a program, the code, and the
# modules it depends on.
cat > "$work/hello.go" <<'EOF'
package main

import "fmt"

func main() { fmt.Println("hello") }
EOF
mkdir "$work/with"
cat > "$work/with/main.go" <<'EOF'
package main

import (
	"fmt"

	"github.com/flowchassis/flowchassis"
)

func main() {
	e, err := flowchassis.Open("j", flowchassis.Options{})
	fmt.Println(e, err)
}
EOF
CGO_ENABLED=0 go build -trimpath -ldflags '-s -w' -o "$work/hello" "$work/hello.go"
CGO_ENABLED=0 go build -trimpath -ldflags '-s -w' -o "$work/withlib" "$work/with/main.go"
size() { wc -c < "$1" | tr -d ' '; }
{
	header "Sizes" "see scripts/record-tests.sh"
	echo "flowchassis binary (static, stripped, $(go env GOOS)/$(go env GOARCH)): $(size "$work/bin/flowchassis") bytes"
	echo "a hello-world program: $(size "$work/hello") bytes"
	echo "the same program opening a FlowChassis journal: $(size "$work/withlib") bytes"
	echo "what the library adds: $(( $(size "$work/withlib") - $(size "$work/hello") )) bytes"
	echo "library source, without tests: $(cat $(ls *.go | grep -v _test.go) | wc -l | tr -d ' ') lines in $(ls *.go | grep -v _test.go | wc -l | tr -d ' ') files"
	echo "tests: $(cat *_test.go cmd/flowchassis/*_test.go | wc -l | tr -d ' ') lines; test functions: $(grep -h '^func Test' *_test.go cmd/flowchassis/*_test.go | grep -vc TestMain)"
	echo "modules required besides the standard library: $(go list -m all | tail -n +2 | wc -l | tr -d ' ')"
	go list -m all
} > "$out/sizes.txt"

grep -E '^(ok|FAIL|--- FAIL)' "$out/tests.txt" "$out/race.txt" || true
tail -n 1 "$out/release-plan.txt" "$out/smoke.txt"
cat "$work/demo.log" | tail -n 1
cat "$work/demo-large.log" | tail -n 1
grep -E 'steps/s' "$out/bench.txt"
grep -E '^(flowchassis binary|what the library|modules)' "$out/sizes.txt"
echo "results written to $out/"
