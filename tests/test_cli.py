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


if __name__ == "__main__":
    unittest.main()
