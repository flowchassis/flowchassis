# Copyright FlowChassis.com 2026
# SPDX-License-Identifier: Apache-2.0
"""Cases for scripts/release-plan.py.

    python3 -B -m unittest discover -s scripts -p '*_test.py'
"""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("release_plan", Path(__file__).with_name("release-plan.py"))
plan = importlib.util.module_from_spec(spec)
spec.loader.exec_module(plan)

HEAD = "a" * 40


def after_test(version="0.1.0", tags=(), head=HEAD, workflow="test"):
    asked = []

    def tag_list():
        asked.append("tags")
        return list(tags)

    ev = plan.Event("workflow_run", run_workflow=workflow, run_sha=HEAD, version=version,
                    main_head=lambda: head, tags=tag_list)
    return plan.decide(ev), asked


class ReleasePlan(unittest.TestCase):
    def test_new_version_releases_the_tested_commit(self):
        p, _ = after_test(tags=["v0.0.9"])
        self.assertEqual(p.action, "release")
        self.assertEqual(p.outputs(), [("tag", "v0.1.0"), ("ref", HEAD), ("new", "true")])

    def test_first_release_with_no_tags(self):
        p, _ = after_test(tags=[])
        self.assertEqual((p.action, p.tag), ("release", "v0.1.0"))

    def test_existing_tag_releases_nothing(self):
        p, _ = after_test(tags=["v0.1.0"])
        self.assertEqual(p.action, "none")
        self.assertEqual(p.outputs(), [])

    def test_older_version_fails(self):
        with self.assertRaises(plan.Refusal):
            after_test(version="0.1.0", tags=["v0.2.0"])

    def test_numeric_not_text_order(self):
        p, _ = after_test(version="0.10.0", tags=["v0.9.0"])
        self.assertEqual(p.action, "release")

    def test_moved_main_defers_without_reading_tags(self):
        p, asked = after_test(head="b" * 40)
        self.assertEqual(p.action, "none")
        self.assertEqual(asked, [])

    def test_other_workflow_releases_nothing(self):
        p, _ = after_test(workflow="long")
        self.assertEqual(p.action, "none")

    def test_bad_version_fails(self):
        for v in (None, "1.0", "01.0.0", "1.0.0-rc.1", "v1.0.0"):
            with self.subTest(v=v), self.assertRaises(plan.Refusal):
                after_test(version=v)

    def test_release_by_hand_attaches(self):
        p = plan.decide(plan.Event("release", release_tag="v0.1.0"))
        self.assertEqual(p.outputs(), [("tag", "v0.1.0"), ("ref", "v0.1.0")])

    def test_run_by_hand_needs_a_valid_tag(self):
        self.assertEqual(plan.decide(plan.Event("workflow_dispatch", input_tag=" v0.1.0 ")).action, "attach")
        for tag in ("", "0.1.0", "v0.1", "main"):
            with self.subTest(tag=tag), self.assertRaises(plan.Refusal):
                plan.decide(plan.Event("workflow_dispatch", input_tag=tag))

    def test_source_has_one_valid_version(self):
        self.assertIsNotNone(plan.parts(plan.read_version()))


if __name__ == "__main__":
    unittest.main()
