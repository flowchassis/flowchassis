#!/usr/bin/env python3
# Copyright FlowChassis.com 2026
# SPDX-License-Identifier: Apache-2.0
#
# The release workflow's plan: whether a run of .github/workflows/release.yml
# releases a new version, builds an existing tag's files again, or does
# nothing, with the tag and the commit, and why. The workflow's plan job runs
# it, and its outputs start the build jobs.
#
#   python3 scripts/release-plan.py
#
# The rules:
#   - After the test workflow passes for a push to main in this repository
#     (the plan job's `if` checks that), on the commit main is at now:
#     flowchassis.go's Version, X.Y.Z, releases vX.Y.Z, unless that tag exists.
#     A version below the newest vX.Y.Z tag fails the plan. If main has moved
#     on, the run for the newer commit decides.
#   - A release published by hand, or a run by hand with a tag, builds that
#     tag's files and attaches them.
#
# It reads the environment the workflow sets: EVENT (workflow_run, release
# or workflow_dispatch), RUN_WORKFLOW and RUN_SHA (the workflow that finished
# and its commit), RELEASE_TAG (a release published by hand), INPUT_TAG (a
# run by hand's input), GITHUB_OUTPUT; and Version in the checkout. It asks
# git for main's head and the tags only when it needs them.
#
# Its outputs: tag, ref, and new (true for a new release). It prints what it
# decided and why, as a notice under GitHub Actions, and exits with 0; or why
# the plan fails, as an error, and exits with 1.
#
# scripts/release_plan_test.py runs its cases.
import os
import re
import subprocess
import sys

ROOT = os.path.realpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
VERSION_FILE = "flowchassis.go"
NUM = r"(0|[1-9][0-9]*)"
FORM = re.compile(rf"^{NUM}\.{NUM}\.{NUM}$")
VERSION_LINE = re.compile(r'^const Version = "(.*)"$', re.M)


class Refusal(Exception):
    """The plan fails, for the reason given."""


class Plan:
    def __init__(self, action, why, tag="", ref=""):
        self.action = action  # release, attach or none
        self.why = why
        self.tag = tag
        self.ref = ref

    def outputs(self):
        if self.action == "none":
            return []
        out = [("tag", self.tag), ("ref", self.ref)]
        if self.action == "release":
            out.append(("new", "true"))
        return out


class Event:
    """What the plan decides from. main_head and tags are functions, so that
    nothing is asked of git that the plan doesn't need."""

    def __init__(self, event, run_workflow="", run_sha="", release_tag="", input_tag="", version=None,
                 main_head=None, tags=None):
        self.event = event
        self.run_workflow = run_workflow
        self.run_sha = run_sha
        self.release_tag = release_tag
        self.input_tag = input_tag.strip()
        self.version = version
        self.main_head = main_head
        self.tags = tags


def parts(version):
    m = FORM.match(version or "")
    return tuple(int(g) for g in m.groups()) if m else None


def short(sha):
    return sha[:7] if sha else "(none)"


def decide(ev):
    if ev.event == "workflow_run":
        return after_test(ev)
    if ev.event == "release":
        return by_tag(ev.release_tag, "a release published by hand")
    if ev.event == "workflow_dispatch":
        if not ev.input_tag:
            raise Refusal("a run by hand needs the tag to build again, such as v0.1.0")
        return by_tag(ev.input_tag, "a run by hand")
    return Plan("none", f"nothing to do after a {ev.event or 'missing'} event")


def by_tag(tag, how):
    if not tag or re.search(r"\s", tag) or not tag.startswith("v") or not parts(tag[1:]):
        raise Refusal(f"{tag!r} isn't a tag of the form vX.Y.Z")
    return Plan("attach", f"{how}: building the files for {tag}", tag=tag, ref=tag)


def after_test(ev):
    if ev.run_workflow != "test":
        return Plan("none", f"releases wait for the test workflow, and this run follows {ev.run_workflow!r}")
    head = ev.main_head()
    if head != ev.run_sha:
        return Plan("none", f"main has moved on from {short(ev.run_sha)} to {short(head)}, so the run for the newer commit decides")
    if not parts(ev.version):
        raise Refusal(f"{VERSION_FILE} has no Version of the form X.Y.Z")
    tags = ev.tags()
    tag = "v" + ev.version
    if tag in tags:
        return Plan("none", f"{tag} is out already")
    released = [p for p in (parts(t[1:]) for t in tags if t.startswith("v")) if p]
    if released and parts(ev.version) < max(released):
        top = ".".join(str(n) for n in max(released))
        raise Refusal(f"{tag} is older than the newest release, v{top}; raise the version above it")
    return Plan("release", f"releasing {tag} from {short(ev.run_sha)}, which the test workflow passed", tag=tag, ref=ev.run_sha)


def git(*args):
    try:
        r = subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True)
    except OSError as e:
        raise Refusal(f"git {' '.join(args)} failed: {e}")
    if r.returncode:
        raise Refusal(f"git {' '.join(args)} failed: {(r.stderr or r.stdout).strip()}")
    return r.stdout


def main_head():
    out = git("ls-remote", "origin", "refs/heads/main").split()
    if not out:
        raise Refusal("the repository has no branch main")
    return out[0]


def tags():
    names = []
    for line in git("ls-remote", "--tags", "--refs", "origin").splitlines():
        fields = line.split("\t")
        if len(fields) == 2 and fields[1].startswith("refs/tags/"):
            names.append(fields[1][len("refs/tags/"):])
    return names


def read_version():
    try:
        with open(os.path.join(ROOT, VERSION_FILE), encoding="utf-8") as f:
            found = VERSION_LINE.findall(f.read())
    except OSError:
        return None
    return found[0] if len(found) == 1 else None


def annotate(level, message):
    if os.environ.get("GITHUB_ACTIONS") == "true":
        message = message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print(f"::{level} title=Release plan::{message}", flush=True)


def main():
    env = os.environ
    ev = Event(env.get("EVENT", ""), run_workflow=env.get("RUN_WORKFLOW", ""), run_sha=env.get("RUN_SHA", ""),
               release_tag=env.get("RELEASE_TAG", ""), input_tag=env.get("INPUT_TAG", ""),
               version=read_version(), main_head=main_head, tags=tags)
    try:
        plan = decide(ev)
    except Refusal as e:
        print(f"the plan fails: {e}", file=sys.stderr)
        annotate("error", f"the plan fails: {e}")
        sys.exit(1)
    word = {"release": "release", "attach": "build", "none": "no release"}[plan.action]
    print(f"{word}: {plan.why}")
    annotate("notice", f"{word}: {plan.why}")
    out = env.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as f:
            for k, v in plan.outputs():
                f.write(f"{k}={v}\n")
    else:
        for k, v in plan.outputs():
            print(f"  {k}={v}")


if __name__ == "__main__":
    main()
