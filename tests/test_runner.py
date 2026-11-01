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


class TestStepEnvironment(unittest.TestCase):
    def test_step_env_reaches_the_command(self):
        f = flow([{"name": "a", "command": "test \"$FK_MARK\" = green",
                   "env": {"FK_MARK": "green"}}])
        result = run_flow(f, backoff_seconds=0, env={})
        self.assertEqual(result.results[0].status, "succeeded")

    def test_flow_env_applies_to_every_step(self):
        f = load_flow({"name": "t", "env": {"FK_MARK": "from-flow"},
                       "steps": [{"name": "a", "command": "test \"$FK_MARK\" = from-flow"}]})
        result = run_flow(f, backoff_seconds=0, env={})
        self.assertEqual(result.results[0].status, "succeeded")

    def test_step_env_overrides_flow_env(self):
        f = load_flow({"name": "t", "env": {"FK_MARK": "flow"},
                       "steps": [{"name": "a", "command": "test \"$FK_MARK\" = step",
                                  "env": {"FK_MARK": "step"}}]})
        result = run_flow(f, backoff_seconds=0, env={})
        self.assertEqual(result.results[0].status, "succeeded")

    def test_base_env_visible_when_not_overridden(self):
        f = flow([{"name": "a", "command": "test \"$FK_BASE\" = yes"}])
        result = run_flow(f, backoff_seconds=0, env={"FK_BASE": "yes"})
        self.assertEqual(result.results[0].status, "succeeded")

    def test_command_expansion_uses_merged_env(self):
        f = load_flow({"name": "t", "env": {"FK_WORD": "expanded-word"},
                       "steps": [{"name": "a", "command": "echo ${FK_WORD}"}]})
        result = run_flow(f, backoff_seconds=0, env={})
        self.assertEqual(result.results[0].status, "succeeded")
        self.assertIn("expanded-word", result.results[0].last_output)

    def test_default_used_when_unset(self):
        f = flow([{"name": "a", "command": "echo ${FK_UNSET:-the-default}"}])
        result = run_flow(f, backoff_seconds=0, env={})
        self.assertIn("the-default", result.results[0].last_output)


class TestMissingVars(unittest.TestCase):
    def test_unset_reference_reported(self):
        from flowkit.runner import missing_vars
        f = flow([{"name": "a", "command": "deploy $FK_TARGET"}])
        self.assertEqual(missing_vars(f, base={}), {"a": ["FK_TARGET"]})

    def test_defaults_and_env_satisfy_references(self):
        from flowkit.runner import missing_vars
        f = load_flow({"name": "t", "env": {"FK_A": "1"},
                       "steps": [{"name": "a", "command": "run $FK_A ${FK_B:-x}"}]})
        self.assertEqual(missing_vars(f, base={}), {})


class TestWhenConditions(unittest.TestCase):
    def _flow(self):
        return load_flow({"name": "t", "steps": [
            {"name": "a", "command": "exit 0"},
            {"name": "b", "command": "exit 0", "depends_on": ["a"],
             "when": {"env": "FK_GO", "equals": "yes"}},
            {"name": "c", "command": "exit 0", "depends_on": ["b"]},
        ]})

    def test_false_condition_skips_step_and_dependents(self):
        result = run_flow(self._flow(), backoff_seconds=0, env={})
        statuses = {r.name: r.status for r in result.results}
        self.assertEqual(statuses, {"a": "succeeded", "b": "skipped", "c": "skipped"})
        self.assertEqual(result.status, "succeeded")  # a conditional skip is not a failure

    def test_true_condition_runs(self):
        result = run_flow(self._flow(), backoff_seconds=0, env={"FK_GO": "yes"})
        self.assertTrue(all(r.status == "succeeded" for r in result.results))

    def test_condition_sees_flow_and_step_env(self):
        f = load_flow({"name": "t", "env": {"FK_GO": "yes"}, "steps": [
            {"name": "b", "command": "exit 0", "when": {"env": "FK_GO", "equals": "yes"}}]})
        result = run_flow(f, backoff_seconds=0, env={})
        self.assertEqual(result.results[0].status, "succeeded")


class TestStepSelection(unittest.TestCase):
    def _flow(self):
        return flow([
            {"name": "a", "command": "exit 0"},
            {"name": "b", "command": "exit 0", "depends_on": ["a"]},
            {"name": "c", "command": "exit 0"},
        ])

    def test_only_pulls_in_transitive_dependencies(self):
        result = run_flow(self._flow(), backoff_seconds=0, only={"b"})
        statuses = {r.name: r.status for r in result.results}
        self.assertEqual(statuses, {"a": "succeeded", "b": "succeeded", "c": "skipped"})
        self.assertEqual(result.status, "succeeded")

    def test_skip_cascades_to_dependents(self):
        result = run_flow(self._flow(), backoff_seconds=0, skip={"a"})
        statuses = {r.name: r.status for r in result.results}
        self.assertEqual(statuses, {"a": "skipped", "b": "skipped", "c": "succeeded"})
        self.assertEqual(result.status, "succeeded")

    def test_unknown_selection_names_rejected(self):
        from flowkit.runner import selected_steps
        with self.assertRaises(ValueError):
            selected_steps(self._flow(), only={"ghost"})
        with self.assertRaises(ValueError):
            selected_steps(self._flow(), skip={"ghost"})
