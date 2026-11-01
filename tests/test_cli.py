import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

from flowkit.__main__ import main

GOOD = {
    "name": "demo",
    "steps": [
        {"name": "build", "command": "exit 0"},
        {"name": "test", "command": "exit 0", "depends_on": ["build"]},
    ],
}

BAD = {"name": "demo", "steps": [{"name": "a", "command": "true", "depends_on": ["ghost"]}]}

FAILING = {"name": "demo", "steps": [{"name": "a", "command": "exit 1"}]}


def run(argv, payload):
    with TemporaryDirectory() as tmp:
        path = Path(tmp) / "flow.json"
        path.write_text(json.dumps(payload))
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = main(argv + [str(path)])
        return code, buf.getvalue()


class TestCli(unittest.TestCase):
    def test_success_exit_0(self):
        code, out = run(["--backoff", "0"], GOOD)
        self.assertEqual(code, 0)
        self.assertIn("Flow 'demo': succeeded", out)
        self.assertIn("2 succeeded, 0 failed, 0 skipped", out)

    def test_failing_flow_exit_1(self):
        code, out = run(["--backoff", "0"], FAILING)
        self.assertEqual(code, 1)
        self.assertIn("FAILED", out)

    def test_invalid_flow_exit_2(self):
        code, _ = run([], BAD)
        self.assertEqual(code, 2)

    def test_json_output(self):
        code, out = run(["--json", "--backoff", "0"], GOOD)
        self.assertEqual(code, 0)
        payload = json.loads(out)
        self.assertEqual(payload["status"], "succeeded")
        self.assertEqual([s["name"] for s in payload["steps"]], ["build", "test"])

    def test_dry_run_prints_plan_without_running(self):
        code, out = run(["--dry-run"], FAILING)
        self.assertEqual(code, 0)  # would be 1 if it actually ran
        self.assertIn("execution order", out)
        self.assertIn("a", out)

    def test_dry_run_rejects_invalid_flow(self):
        code, _ = run(["--dry-run"], BAD)
        self.assertEqual(code, 2)


class TestJobsFlag(unittest.TestCase):
    def test_jobs_runs_flow(self):
        code, out = run(["--jobs", "2", "--backoff", "0"], GOOD)
        assert code == 0
        assert "succeeded" in out

    def test_jobs_zero_rejected(self):
        code, _ = run(["--jobs", "0"], GOOD)
        assert code == 2


if __name__ == "__main__":
    unittest.main()


class TestOutputReporting(unittest.TestCase):
    def test_failed_step_shows_its_output(self):
        payload = {"name": "demo", "steps": [
            {"name": "a", "command": "echo boom-detail; exit 1"}]}
        code, out = run(["--backoff", "0"], payload)
        self.assertEqual(code, 1)
        self.assertIn("boom-detail", out)

    def test_successful_output_hidden_by_default(self):
        payload = {"name": "demo", "steps": [
            {"name": "a", "command": "echo quiet-detail"}]}
        code, out = run([], payload)
        self.assertEqual(code, 0)
        self.assertNotIn("quiet-detail", out)

    def test_show_output_reveals_successful_output(self):
        payload = {"name": "demo", "steps": [
            {"name": "a", "command": "echo loud-detail"}]}
        code, out = run(["--show-output"], payload)
        self.assertEqual(code, 0)
        self.assertIn("loud-detail", out)

    def test_json_payload_includes_output(self):
        code, out = run(["--json"], {"name": "demo", "steps": [
            {"name": "a", "command": "echo payload-detail"}]})
        self.assertEqual(code, 0)
        self.assertIn("payload-detail", json.loads(out)["steps"][0]["output"])


class TestStrictEnv(unittest.TestCase):
    def test_unset_variable_fails_before_running(self):
        payload = {"name": "demo", "steps": [
            {"name": "a", "command": "touch should-not-happen-$FK_NOPE"}]}
        code, _ = run(["--strict-env"], payload)
        self.assertEqual(code, 2)

    def test_default_or_env_satisfies_strict_mode(self):
        payload = {"name": "demo", "steps": [
            {"name": "a", "command": "echo ${FK_MAYBE:-fine}"}]}
        code, _ = run(["--strict-env"], payload)
        self.assertEqual(code, 0)


class TestReportFile(unittest.TestCase):
    def test_report_written_with_run_summary(self):
        with TemporaryDirectory() as tmp:
            flow_path = Path(tmp) / "flow.json"
            report_path = Path(tmp) / "report.json"
            flow_path.write_text(json.dumps(GOOD))
            code = main(["--report", str(report_path), str(flow_path)])
            self.assertEqual(code, 0)
            payload = json.loads(report_path.read_text())
            self.assertEqual(payload["flow"], "demo")
            self.assertEqual(payload["status"], "succeeded")

    def test_unwritable_report_path_exit_2(self):
        with TemporaryDirectory() as tmp:
            flow_path = Path(tmp) / "flow.json"
            flow_path.write_text(json.dumps(GOOD))
            code = main(["--report", str(Path(tmp) / "no-such-dir" / "r.json"),
                         str(flow_path)])
            self.assertEqual(code, 2)


class TestStepSelectionCli(unittest.TestCase):
    COND = {"name": "cond", "steps": [
        {"name": "build", "command": "exit 0"},
        {"name": "deploy", "command": "exit 0", "depends_on": ["build"],
         "when": {"env": "FK_DEPLOY", "equals": "yes"}},
    ]}

    def test_only_runs_selection_and_deps(self):
        code, out = run(["--only", "deploy"], self.COND)
        self.assertEqual(code, 0)
        self.assertIn("build", out)
        self.assertIn("skip", out)  # deploy skipped: condition false in test env

    def test_unknown_only_name_exit_2(self):
        code, _ = run(["--only", "ghost"], GOOD)
        self.assertEqual(code, 2)

    def test_dry_run_marks_conditions_and_selection(self):
        code, out = run(["--dry-run", "--only", "deploy"], self.COND)
        self.assertEqual(code, 0)
        self.assertIn("2 of 2 steps selected", out)
        self.assertIn("when $FK_DEPLOY", out)

    def test_skip_exits_0_and_skips(self):
        code, out = run(["--skip", "test"], GOOD)
        self.assertEqual(code, 0)
        self.assertIn("1 succeeded, 0 failed, 1 skipped", out)


class TestOutputDirCli(unittest.TestCase):
    def test_output_dir_writes_logs_and_payload_paths(self):
        import json as _json, os as _os, subprocess, sys
        from pathlib import Path as _Path
        from tempfile import TemporaryDirectory as _TD
        with _TD() as tmp:
            flow_path = _Path(tmp) / "f.json"
            flow_path.write_text(_json.dumps(GOOD))
            outdir = _Path(tmp) / "logs"
            out = subprocess.run(
                [sys.executable, "-m", "flowkit", "--backoff", "0",
                 "--output-dir", str(outdir), "--json", str(flow_path)],
                capture_output=True, text=True)
            self.assertEqual(out.returncode, 0)
            self.assertEqual(sorted(_os.listdir(outdir)), ["build.log", "test.log"])
            payload = _json.loads(out.stdout)
            self.assertTrue(payload["steps"][0]["output_file"].endswith("build.log"))
