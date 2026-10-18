#!/usr/bin/env python3
# Copyright FlowChassis.com 2026
# SPDX-License-Identifier: Apache-2.0
#
# The smoke check for a set of release binaries: runs the flowchassis binary
# in BIN on the machine it was built for.
#
#   python3 scripts/smoke.py BIN
#
# It checks the version, runs the crash demo (worker processes killed and
# started again, then every order checked against its ledger), and checks,
# lists, shows and compacts the journal the demo leaves, and that a damaged
# copy is refused. The last line says what passed. Exit status 1 on failure.
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.realpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


def fail(msg):
    print(f"smoke check FAILED: {msg}", flush=True)
    sys.exit(1)


def version():
    with open(os.path.join(ROOT, "flowchassis.go"), encoding="utf-8") as f:
        m = re.search(r'^const Version = "(.*)"$', f.read(), re.M)
    if not m:
        fail("no Version in flowchassis.go")
    return m.group(1)


def main():
    if len(sys.argv) != 2:
        print(__doc__ or "usage: smoke.py BIN", file=sys.stderr)
        sys.exit(2)
    exe = os.path.join(sys.argv[1], "flowchassis.exe" if os.name == "nt" else "flowchassis")
    if not os.path.isfile(exe):
        fail(f"{exe} not found")
    exe = os.path.abspath(exe)
    want = version()

    def run(*args, code=0, timeout=600):
        r = subprocess.run([exe, *args], capture_output=True, text=True, timeout=timeout)
        if r.returncode != code:
            fail(f"flowchassis {' '.join(args)} exited {r.returncode}, want {code}\nstdout: {r.stdout}\nstderr: {r.stderr}")
        return r

    out = run("version").stdout
    if not out.startswith(f"flowchassis {want} (journal format 01"):
        fail(f"version says {out.strip()!r}, want flowchassis {want}")
    print(out.strip(), flush=True)

    work = tempfile.mkdtemp(prefix="flowchassis-smoke-")
    try:
        demo = os.path.join(work, "demo")
        r = run("demo", "-runs", "40", "-kills", "10", "-workers", "4", "-pause", "10ms", "-dir", demo)
        rep = json.loads(r.stdout)
        if not rep.get("ok") or rep["runs"] != 40 or rep["kills"] != 10 or rep["unfinished"] or rep["failed"]:
            fail(f"demo report: {r.stdout}")
        print(r.stderr.strip().splitlines()[-1], flush=True)

        journal = os.path.join(demo, "orders.journal")
        out = run("check", journal).stdout
        if "OK: every record has a good checksum" not in out or "40 runs" not in out:
            fail(f"check: {out}")

        listed = json.loads(run("runs", "-status", "compensated", "-json", journal).stdout) or []
        if len(listed) != rep["compensated"]:
            fail(f"runs lists {len(listed)} compensated, the demo counted {rep['compensated']}")

        out = run("show", journal, "order-0001").stdout
        if "status   completed" not in out and "status   compensated" not in out:
            fail(f"show: {out}")

        out = run("compact", journal).stdout
        if "40 runs kept" not in out:
            fail(f"compact: {out}")
        out = run("check", journal).stdout
        if "40 runs" not in out:
            fail(f"check after compact: {out}")

        bad = os.path.join(work, "bad.journal")
        data = bytearray(open(journal, "rb").read())
        data[20] ^= 0xFF
        open(bad, "wb").write(data)
        out = run("check", bad, code=1).stdout
        if "NOT OK" not in out:
            fail(f"check of a damaged journal: {out}")
    finally:
        shutil.rmtree(work, ignore_errors=True)

    print(f"smoke check passed: flowchassis {want}: version, crash demo ({rep['kills']} kills, {rep['runs']} orders, "
          f"{rep['completed']} completed, {rep['compensated']} compensated), check, runs, show, compact, damaged journal refused",
          flush=True)


if __name__ == "__main__":
    main()
