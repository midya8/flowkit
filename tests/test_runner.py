import time
import unittest

from flowkit.model import load_flow
from flowkit.runner import run_flow, run_step, Step


def flow(steps):
    return load_flow({"name": "t", "steps": steps})


class TestRunStep(unittest.TestCase):
    def test_success(self):
        result = run_step(Step(name="a", command="exit 0"), backoff_seconds=0)
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(result.attempts, 1)
        self.assertEqual(result.exit_code, 0)

    def test_failure_with_retries_counts_attempts(self):
        result = run_step(Step(name="a", command="exit 1", retries=2), backoff_seconds=0)
        self.assertEqual(result.status, "failed")
        self.assertEqual(result.attempts, 3)
        self.assertEqual(result.exit_code, 1)

    def test_timeout_is_a_failed_attempt(self):
        result = run_step(Step(name="a", command="exec sleep 5", timeout_seconds=0.2),
                          backoff_seconds=0)
        self.assertEqual(result.status, "failed")
        self.assertIsNone(result.exit_code)
        self.assertIn("timed out", result.last_output)

    def test_output_captured(self):
        result = run_step(Step(name="a", command="echo hello-flowkit"), backoff_seconds=0)
        self.assertIn("hello-flowkit", result.last_output)


class TestRunFlow(unittest.TestCase):
    def test_all_succeed(self):
        result = run_flow(flow([
            {"name": "a", "command": "exit 0"},
            {"name": "b", "command": "exit 0", "depends_on": ["a"]},
        ]), backoff_seconds=0)
        self.assertEqual(result.status, "succeeded")
        self.assertTrue(all(r.status == "succeeded" for r in result.results))

    def test_failure_skips_dependents_and_stops(self):
        result = run_flow(flow([
            {"name": "a", "command": "exit 1"},
            {"name": "b", "command": "exit 0", "depends_on": ["a"]},
            {"name": "c", "command": "exit 0"},
        ]), backoff_seconds=0)
        by_name = {r.name: r for r in result.results}
        self.assertEqual(by_name["a"].status, "failed")
        self.assertEqual(by_name["b"].status, "skipped")
        self.assertEqual(by_name["c"].status, "skipped")  # fail-fast stops new work
        self.assertEqual(result.status, "failed")

    def test_continue_on_error_keeps_independent_steps_running(self):
        result = run_flow(flow([
            {"name": "a", "command": "exit 1", "continue_on_error": True},
            {"name": "b", "command": "exit 0", "depends_on": ["a"]},
            {"name": "c", "command": "exit 0"},
        ]), backoff_seconds=0)
        by_name = {r.name: r for r in result.results}
        self.assertEqual(by_name["a"].status, "failed")
        self.assertEqual(by_name["b"].status, "skipped")  # dependents still skip
        self.assertEqual(by_name["c"].status, "succeeded")

    def test_skips_cascade_through_chains(self):
        result = run_flow(flow([
            {"name": "a", "command": "exit 1"},
            {"name": "b", "command": "exit 0", "depends_on": ["a"]},
            {"name": "c", "command": "exit 0", "depends_on": ["b"]},
        ]), backoff_seconds=0)
        self.assertEqual([r.status for r in result.results],
                         ["failed", "skipped", "skipped"])

    def test_results_in_declaration_order(self):
        result = run_flow(flow([
            {"name": "z", "command": "exit 0"},
            {"name": "a", "command": "exit 0"},
        ]), backoff_seconds=0)
        self.assertEqual([r.name for r in result.results], ["z", "a"])


class TestRunFlowParallel(unittest.TestCase):
    def test_independent_steps_run_concurrently(self):
        started = time.monotonic()
        result = run_flow(flow([
            {"name": "a", "command": "exec sleep 0.4"},
            {"name": "b", "command": "exec sleep 0.4"},
        ]), backoff_seconds=0, jobs=2)
        duration = time.monotonic() - started
        self.assertEqual(result.status, "succeeded")
        self.assertLess(duration, 0.75)  # sequential would take ~0.8s

    def test_results_stay_in_declaration_order(self):
        result = run_flow(flow([
            {"name": "slow", "command": "exec sleep 0.3"},
            {"name": "fast", "command": "exit 0"},
        ]), backoff_seconds=0, jobs=2)
        self.assertEqual([r.name for r in result.results], ["slow", "fast"])

    def test_dependency_waits_for_its_step(self):
        result = run_flow(flow([
            {"name": "a", "command": "exec sleep 0.3"},
            {"name": "b", "command": "exit 0", "depends_on": ["a"]},
            {"name": "c", "command": "exit 0"},
        ]), backoff_seconds=0, jobs=3)
        self.assertEqual([r.status for r in result.results],
                         ["succeeded", "succeeded", "succeeded"])

    def test_fail_fast_stops_new_work(self):
        result = run_flow(flow([
            {"name": "a", "command": "exit 1"},
            {"name": "b", "command": "exec sleep 0.3"},
            {"name": "c", "command": "exit 0"},
        ]), backoff_seconds=0, jobs=2)
        by_name = {r.name: r for r in result.results}
        self.assertEqual(by_name["a"].status, "failed")
        self.assertEqual(by_name["b"].status, "succeeded")  # already running: finishes
        self.assertEqual(by_name["c"].status, "skipped")    # never scheduled

    def test_continue_on_error_keeps_scheduling(self):
        result = run_flow(flow([
            {"name": "a", "command": "exit 1", "continue_on_error": True},
            {"name": "b", "command": "exec sleep 0.2"},
            {"name": "c", "command": "exit 0"},
        ]), backoff_seconds=0, jobs=2)
        by_name = {r.name: r for r in result.results}
        self.assertEqual(by_name["c"].status, "succeeded")

    def test_jobs_must_be_positive(self):
        with self.assertRaises(ValueError):
            run_flow(flow([{"name": "a", "command": "exit 0"}]), jobs=0)


if __name__ == "__main__":
    unittest.main()
