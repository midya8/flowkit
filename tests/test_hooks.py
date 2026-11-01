import unittest

from flowkit.model import FlowError, load_flow
from flowkit.runner import run_flow


def flow_with_hook(hook_extra=None, normal_fail=True):
    hook = {"name": "notify", "command": "echo notified",
            "trigger": "on_failure"}
    if hook_extra:
        hook.update(hook_extra)
    return load_flow({
        "name": "f",
        "steps": [
            {"name": "work", "command": "false" if normal_fail else "true"},
            hook,
        ],
    })


class TestTriggerValidation(unittest.TestCase):
    def test_bad_trigger_rejected(self):
        with self.assertRaises(FlowError):
            load_flow({"name": "f", "steps": [
                {"name": "a", "command": "true", "trigger": "sometimes"}]})

    def test_normal_cannot_depend_on_hook(self):
        with self.assertRaises(FlowError) as ctx:
            load_flow({"name": "f", "steps": [
                {"name": "h", "command": "true", "trigger": "on_failure"},
                {"name": "a", "command": "true", "depends_on": ["h"]}]})
        self.assertIn("cannot depend on each other", str(ctx.exception))

    def test_hook_cannot_depend_on_normal(self):
        with self.assertRaises(FlowError) as ctx:
            load_flow({"name": "f", "steps": [
                {"name": "a", "command": "true"},
                {"name": "h", "command": "true", "trigger": "on_failure",
                 "depends_on": ["a"]}]})
        self.assertIn("cannot depend on each other", str(ctx.exception))

    def test_hook_may_depend_on_hook(self):
        flow = load_flow({"name": "f", "steps": [
            {"name": "a", "command": "false"},
            {"name": "h1", "command": "true", "trigger": "on_failure"},
            {"name": "h2", "command": "true", "trigger": "on_failure",
             "depends_on": ["h1"]}]})
        self.assertEqual(flow.step("h2").trigger, "on_failure")


class TestHookExecution(unittest.TestCase):
    def test_hook_runs_only_when_normal_step_fails(self):
        result = run_flow(flow_with_hook(), backoff_seconds=0)
        by_name = {r.name: r for r in result.results}
        self.assertEqual(by_name["notify"].status, "succeeded")
        self.assertEqual(by_name["notify"].attempts, 1)
        self.assertEqual(result.status, "failed")  # work failed

    def test_hook_skipped_on_success(self):
        result = run_flow(flow_with_hook(normal_fail=False), backoff_seconds=0)
        by_name = {r.name: r for r in result.results}
        self.assertEqual(by_name["work"].status, "succeeded")
        self.assertEqual(by_name["notify"].status, "skipped")
        self.assertEqual(by_name["notify"].attempts, 0)
        self.assertEqual(result.status, "succeeded")

    def test_results_stay_in_declaration_order(self):
        result = run_flow(flow_with_hook(), backoff_seconds=0)
        self.assertEqual([r.name for r in result.results], ["work", "notify"])

    def test_hook_result_carries_trigger(self):
        result = run_flow(flow_with_hook(), backoff_seconds=0)
        self.assertEqual(result.results[-1].trigger, "on_failure")

    def test_failed_hook_does_not_stop_other_hooks(self):
        flow = load_flow({"name": "f", "steps": [
            {"name": "work", "command": "false"},
            {"name": "bad-hook", "command": "false", "trigger": "on_failure"},
            {"name": "good-hook", "command": "true", "trigger": "on_failure"},
        ]})
        result = run_flow(flow, backoff_seconds=0)
        by_name = {r.name: r for r in result.results}
        self.assertEqual(by_name["bad-hook"].status, "failed")
        self.assertEqual(by_name["good-hook"].status, "succeeded")

    def test_failed_hook_skips_its_hook_dependents(self):
        flow = load_flow({"name": "f", "steps": [
            {"name": "work", "command": "false"},
            {"name": "h1", "command": "false", "trigger": "on_failure"},
            {"name": "h2", "command": "true", "trigger": "on_failure",
             "depends_on": ["h1"]},
        ]})
        result = run_flow(flow, backoff_seconds=0)
        by_name = {r.name: r for r in result.results}
        self.assertEqual(by_name["h2"].status, "skipped")

    def test_hook_when_condition_respected(self):
        flow = flow_with_hook({"when": {"env": "FK_NOTIFY", "set": True}})
        result = run_flow(flow, backoff_seconds=0)
        self.assertEqual({r.name: r for r in result.results}["notify"].status,
                         "skipped")

    def test_hook_selection_via_only_and_skip(self):
        result = run_flow(flow_with_hook(), backoff_seconds=0, skip={"notify"})
        self.assertEqual({r.name: r for r in result.results}["notify"].status,
                         "skipped")
        result = run_flow(flow_with_hook(), backoff_seconds=0, only={"notify"})
        by_name = {r.name: r for r in result.results}
        self.assertEqual(by_name["work"].status, "skipped")  # not selected
        # work skipped (not failed) -> hook has no failure to fire on
        self.assertEqual(by_name["notify"].status, "skipped")


if __name__ == "__main__":
    unittest.main()


class TestEndPhaseTriggers(unittest.TestCase):
    def flow(self, work_ok, extra_triggers=("on_failure", "on_success", "finally")):
        steps = [{"name": "work",
                  "command": "true" if work_ok else "false"}]
        for trig in extra_triggers:
            steps.append({"name": trig.replace("_", "-"),
                          "command": "true", "trigger": trig})
        return load_flow({"name": "f", "steps": steps})

    def test_each_trigger_fires_on_its_outcome(self):
        result = run_flow(self.flow(work_ok=True), backoff_seconds=0)
        by_name = {r.name: r for r in result.results}
        self.assertEqual(by_name["on-failure"].status, "skipped")
        self.assertEqual(by_name["on-success"].status, "succeeded")
        self.assertEqual(by_name["finally"].status, "succeeded")

    def test_each_trigger_fires_on_failure(self):
        result = run_flow(self.flow(work_ok=False), backoff_seconds=0)
        by_name = {r.name: r for r in result.results}
        self.assertEqual(by_name["on-failure"].status, "succeeded")
        self.assertEqual(by_name["on-success"].status, "skipped")
        self.assertEqual(by_name["finally"].status, "succeeded")

    def test_results_carry_each_trigger(self):
        result = run_flow(self.flow(work_ok=True), backoff_seconds=0)
        self.assertEqual({r.name: r.trigger for r in result.results},
                         {"work": "always", "on-failure": "on_failure",
                          "on-success": "on_success", "finally": "finally"})

    def test_cross_hook_dependencies_allowed(self):
        flow = load_flow({"name": "f", "steps": [
            {"name": "work", "command": "false"},
            {"name": "cleanup", "command": "true", "trigger": "finally"},
            {"name": "notify", "command": "true", "trigger": "on_failure",
             "depends_on": ["cleanup"]},
        ]})
        result = run_flow(flow, backoff_seconds=0)
        by_name = {r.name: r for r in result.results}
        self.assertEqual(by_name["cleanup"].status, "succeeded")
        self.assertEqual(by_name["notify"].status, "succeeded")

    def test_gated_off_hook_cascades_to_dependents(self):
        flow = load_flow({"name": "f", "steps": [
            {"name": "work", "command": "true"},  # green run
            {"name": "page", "command": "true", "trigger": "on_failure"},
            {"name": "after-page", "command": "true", "trigger": "finally",
             "depends_on": ["page"]},
        ]})
        result = run_flow(flow, backoff_seconds=0)
        by_name = {r.name: r for r in result.results}
        self.assertEqual(by_name["page"].status, "skipped")
        self.assertEqual(by_name["after-page"].status, "skipped")

    def test_failed_finally_marks_flow_failed(self):
        flow = load_flow({"name": "f", "steps": [
            {"name": "work", "command": "true"},
            {"name": "cleanup", "command": "false", "trigger": "finally"},
        ]})
        result = run_flow(flow, backoff_seconds=0)
        self.assertEqual(result.status, "failed")

    def test_bad_trigger_value_still_rejected(self):
        with self.assertRaises(FlowError):
            load_flow({"name": "f", "steps": [
                {"name": "a", "command": "true", "trigger": "weekends"}]})
