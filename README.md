# flowkit

Dependency-free workflow runner: JSON-defined DAGs of shell commands with
retries, timeouts and fail-fast or continue-on-error semantics. Standard
library only - if Python is installed, flowkit runs.

A flow names its steps and wires them by `depends_on`. Flow-level `env`
applies to every step; a step's own `env` overrides it for that step, and
commands interpolate both with `$VAR` / `${VAR}` / `${VAR:-default}`:

```json
{
  "name": "nightly-etl",
  "env": {"DATA_DIR": "/data/nightly"},
  "steps": [
    {"name": "extract", "command": "python extract.py --out ${DATA_DIR}",
     "timeout_seconds": 300},
    {"name": "transform", "command": "python transform.py --in ${DATA_DIR}",
     "depends_on": ["extract"], "retries": 2, "timeout_seconds": 600},
    {"name": "load", "command": "python load.py ${DATA_DIR}",
     "depends_on": ["transform"], "retries": 2, "timeout_seconds": 600},
    {"name": "notify", "command": "./notify.sh ${CHANNEL}", "depends_on": ["load"],
     "continue_on_error": true, "env": {"CHANNEL": "#data-ops"}}
  ]
}
```

```bash
python -m flowkit examples/nightly-etl.json            # run it
python -m flowkit examples/nightly-etl.json --json     # machine-readable summary
python -m flowkit examples/nightly-etl.json --dry-run  # validate, print the plan
python -m flowkit examples/nightly-etl.json --jobs 4   # independent steps in parallel
python -m flowkit examples/nightly-etl.json --strict-env   # fail on unset $VARs
python -m flowkit examples/nightly-etl.json --report run.json  # JSON run report
```

Exit codes make it a scriptable gate: 0 when the flow succeeds (or a dry
run validates), 1 when a step failed, 2 when the flow file itself is
unreadable or invalid - or when `--strict-env` finds a command
referencing an unset variable with no default.

## Semantics

- **Validation first.** Unknown keys, missing fields, duplicate step
  names, dangling dependencies and cycles are all rejected before
  anything runs, with a message naming the problem.
- **Deterministic order.** Steps run in topological order; among ready
  steps, the one declared first goes first. `--jobs N` runs up to N
  independent steps concurrently; results always report in declaration
  order, no matter which step finished first. Fail-fast means steps
  already running finish while nothing new is scheduled.
- **Retries with backoff.** `retries: 2` means up to three attempts;
  the backoff doubles each attempt from `--backoff` (default 0.5s).
- **Timeouts.** `timeout_seconds` bounds each attempt; a timeout is a
  failed attempt and still consumes retries.
- **Failures cascade.** A failed step skips its dependents (and theirs).
  The default is fail-fast: independent later work is skipped too. Mark
  a step `continue_on_error` to let the rest of the flow proceed while
  its own dependents still skip.
- **Environment, layered.** Every step runs with the process
  environment, plus the flow's `env`, plus its own `env` (most specific
  wins). Commands are expanded against that merged mapping before they
  reach the shell, so `${VAR:-default}` behaves the same everywhere.
  Malformed interpolation - an unclosed `${` - is rejected at load
  time, and `--strict-env` turns "typo'd variable expands to nothing"
  into a pre-run error naming the step and the variable.
- **Visible output.** Each step captures its combined stdout/stderr
  tail. A failed step always prints its tail under its report line;
  `--show-output` prints successful steps' tails too, and the `--json`
  summary and `--report` file carry an `output` field per step.

## Library

```python
from flowkit.model import load_flow
from flowkit.runner import run_flow

flow = load_flow({"name": "demo", "steps": [{"name": "a", "command": "make"}]})
result = run_flow(flow)
print(result.status)  # "succeeded" or "failed"
```

## Tests

```bash
python -m unittest discover -s tests -v
```

38 tests cover flow validation (unknown keys, duplicates, dangling deps,
cycles), topological ordering, retries, timeouts, skip/fail-fast
semantics and the CLI exit codes.

## Layout

```
flowkit/
  model.py     # flow definitions, validation, topological order
  runner.py    # sequential execution, retries, timeouts, skip semantics
  report.py    # text and JSON run summaries
  __main__.py  # CLI
tests/         # unittest suite, no external dependencies
examples/      # sample flow definitions
```
