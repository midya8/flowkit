# flowkit

A tiny workflow runner. You write a JSON file listing shell commands and what each depends on, and flowkit runs them in order with retries, timeouts and a clear pass/fail at the end. Standard library only.

I wanted something between a Makefile and Airflow for small nightly jobs, without installing anything.

```json
{
  "name": "nightly-etl",
  "env": {"DATA_DIR": "/data/nightly"},
  "steps": [
    {"name": "extract", "command": "python extract.py --out ${DATA_DIR}", "timeout_seconds": 300},
    {"name": "transform", "command": "python transform.py --in ${DATA_DIR}",
     "depends_on": ["extract"], "retries": 2},
    {"name": "load", "command": "python load.py ${DATA_DIR}", "depends_on": ["transform"]},
    {"name": "notify", "command": "./notify.sh 'nightly-etl FAILED'", "trigger": "on_failure"},
    {"name": "cleanup", "command": "rm -rf ${DATA_DIR}/scratch", "trigger": "finally"}
  ]
}
```

(The full version is in `examples/nightly-etl.json`. The scripts it calls aren't included, so dry-run it first and swap in your own.)

```bash
python -m flowkit examples/nightly-etl.json --dry-run   # check the order without running
python -m flowkit examples/nightly-etl.json
python -m flowkit examples/nightly-etl.json --jobs 4    # run independent steps in parallel
python -m flowkit flow.json --only extract,transform    # a subset plus its deps
python -m flowkit flow.json --report run.json --output-dir logs/
```

Exit code is 0 on success, 1 if a step failed, 2 if the flow file is broken (or `--strict-env` finds an unset variable).

How it behaves, briefly:

- The whole file is validated first. Cycles, missing dependencies, duplicate names and unknown keys are caught before anything runs.
- Steps run in dependency order, ties broken by the order you wrote them. Results always print in that order even with `--jobs`.
- `retries: 2` means up to three attempts with doubling backoff. A timeout counts as a failed attempt.
- By default the first failure stops new work and dependents are skipped. `continue_on_error` on a step lets the rest carry on.
- `on_failure`, `on_success` and `finally` steps run after the main pass, which is where cleanup and notifications go.
- `$VAR`, `${VAR}` and `${VAR:-default}` are expanded from the process env, then the flow env, then the step env.

Commands run in a real shell with your permissions and values aren't escaped, so only run flows you've read.

Tests: `python -m unittest discover -s tests`
