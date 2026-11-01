# flowkit

Dependency-free workflow runner: JSON-defined DAGs of shell commands with
retries, timeouts and fail-fast or continue-on-error semantics. Standard
library only - if Python is installed, flowkit runs.

A flow names its steps and wires them by `depends_on`:

```json
{
  "name": "nightly-etl",
  "steps": [
    {"name": "extract", "command": "python extract.py", "timeout_seconds": 300},
    {"name": "transform", "command": "python transform.py", "depends_on": ["extract"],
     "retries": 2, "timeout_seconds": 600},
    {"name": "load", "command": "python load.py", "depends_on": ["transform"],
     "retries": 2, "timeout_seconds": 600},
    {"name": "notify", "command": "./notify.sh", "depends_on": ["load"],
     "continue_on_error": true}
  ]
}
```

```bash
python -m flowkit examples/nightly-etl.json            # run it
python -m flowkit examples/nightly-etl.json --json     # machine-readable summary
python -m flowkit examples/nightly-etl.json --dry-run  # validate, print the plan
python -m flowkit examples/nightly-etl.json --jobs 4  # independent steps in parallel
```

Exit codes make it a scriptable gate: 0 when the flow succeeds (or a dry
run validates), 1 when a step failed, 2 when the flow file itself is
unreadable or invalid.

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
