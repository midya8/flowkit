import unittest

from flowkit.model import FlowError, load_flow, topo_order


def flow_data(steps, name="demo"):
    return {"name": name, "steps": steps}


class TestLoadFlow(unittest.TestCase):
    def test_minimal_flow(self):
        flow = load_flow(flow_data([{"name": "a", "command": "true"}]))
        self.assertEqual(flow.name, "demo")
        self.assertEqual(flow.steps[0].retries, 0)
        self.assertIsNone(flow.steps[0].timeout_seconds)

    def test_full_step_fields(self):
        flow = load_flow(flow_data([{
            "name": "a", "command": "echo hi", "depends_on": [],
            "retries": 2, "timeout_seconds": 5, "continue_on_error": True,
        }]))
        step = flow.steps[0]
        self.assertEqual((step.retries, step.timeout_seconds, step.continue_on_error),
                         (2, 5.0, True))

    def test_rejects_non_object(self):
        with self.assertRaises(FlowError):
            load_flow([1, 2])

    def test_missing_name(self):
        with self.assertRaises(FlowError):
            load_flow({"steps": [{"name": "a", "command": "true"}]})

    def test_empty_steps(self):
        with self.assertRaises(FlowError):
            load_flow(flow_data([]))

    def test_unknown_step_key(self):
        with self.assertRaisesRegex(FlowError, "unknown keys"):
            load_flow(flow_data([{"name": "a", "command": "true", "memroy": 1}]))

    def test_duplicate_step_names(self):
        with self.assertRaisesRegex(FlowError, "duplicate"):
            load_flow(flow_data([
                {"name": "a", "command": "true"},
                {"name": "a", "command": "true"},
            ]))

    def test_unknown_dependency(self):
        with self.assertRaisesRegex(FlowError, "unknown step"):
            load_flow(flow_data([{"name": "a", "command": "true", "depends_on": ["ghost"]}]))

    def test_self_dependency(self):
        with self.assertRaisesRegex(FlowError, "itself"):
            load_flow(flow_data([{"name": "a", "command": "true", "depends_on": ["a"]}]))

    def test_negative_retries(self):
        with self.assertRaisesRegex(FlowError, "retries"):
            load_flow(flow_data([{"name": "a", "command": "true", "retries": -1}]))

    def test_bad_timeout(self):
        with self.assertRaisesRegex(FlowError, "timeout"):
            load_flow(flow_data([{"name": "a", "command": "true", "timeout_seconds": 0}]))

    def test_cycle_detected(self):
        with self.assertRaisesRegex(FlowError, "cycle"):
            load_flow(flow_data([
                {"name": "a", "command": "true", "depends_on": ["b"]},
                {"name": "b", "command": "true", "depends_on": ["a"]},
            ]))


class TestTopoOrder(unittest.TestCase):
    def test_dependencies_run_first(self):
        flow = load_flow(flow_data([
            {"name": "deploy", "command": "true", "depends_on": ["test"]},
            {"name": "test", "command": "true", "depends_on": ["build"]},
            {"name": "build", "command": "true"},
        ]))
        self.assertEqual([s.name for s in topo_order(flow)], ["build", "test", "deploy"])

    def test_declaration_order_breaks_ties(self):
        flow = load_flow(flow_data([
            {"name": "b", "command": "true"},
            {"name": "a", "command": "true"},
        ]))
        self.assertEqual([s.name for s in topo_order(flow)], ["b", "a"])

    def test_diamond(self):
        flow = load_flow(flow_data([
            {"name": "d", "command": "true", "depends_on": ["b", "c"]},
            {"name": "b", "command": "true", "depends_on": ["a"]},
            {"name": "c", "command": "true", "depends_on": ["a"]},
            {"name": "a", "command": "true"},
        ]))
        names = [s.name for s in topo_order(flow)]
        self.assertEqual(names[0], "a")
        self.assertEqual(names[-1], "d")
        self.assertEqual(len(names), 4)


if __name__ == "__main__":
    unittest.main()


class TestEnvValidation(unittest.TestCase):
    def test_step_env_parsed(self):
        f = load_flow({"name": "t", "steps": [
            {"name": "a", "command": "x", "env": {"DATA_DIR": "/d", "TAG": "v1"}},
        ]})
        self.assertEqual(dict(f.step("a").env), {"DATA_DIR": "/d", "TAG": "v1"})

    def test_flow_env_parsed(self):
        f = load_flow({"name": "t", "env": {"GLOBAL": "g"},
                       "steps": [{"name": "a", "command": "x"}]})
        self.assertEqual(dict(f.env), {"GLOBAL": "g"})
        self.assertEqual(f.step("a").env, ())

    def test_env_must_be_an_object(self):
        with self.assertRaises(FlowError):
            load_flow({"name": "t", "steps": [
                {"name": "a", "command": "x", "env": ["A=1"]}]})

    def test_env_name_must_be_a_valid_variable_name(self):
        with self.assertRaises(FlowError):
            load_flow({"name": "t", "steps": [
                {"name": "a", "command": "x", "env": {"9BAD": "v"}}]})

    def test_env_value_must_be_a_string(self):
        with self.assertRaises(FlowError):
            load_flow({"name": "t", "steps": [
                {"name": "a", "command": "x", "env": {"N": 3}}]})

    def test_env_is_a_known_step_key(self):
        f = load_flow({"name": "t", "steps": [
            {"name": "a", "command": "x", "env": {}}]})
        self.assertEqual(f.step("a").env, ())


class TestCommandInterpolationSyntax(unittest.TestCase):
    def test_unclosed_brace_rejected_at_load(self):
        with self.assertRaises(FlowError):
            load_flow({"name": "t", "steps": [
                {"name": "a", "command": "run ${OPEN"}]})

    def test_valid_interpolation_loads(self):
        f = load_flow({"name": "t", "steps": [
            {"name": "a", "command": "run $A ${B} ${C:-d}"}]})
        self.assertTrue(f.step("a").command.startswith("run"))
