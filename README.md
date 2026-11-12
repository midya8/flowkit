# flowkit

Dependency-free workflow runner: JSON-defined DAGs of shell commands with
retries, timeouts and fail-fast or continue-on-error semantics. Standard
library only, with no runtime dependencies.

## Plan before running

`--dry-run` validates and lists the selected steps without executing commands.
It lists step names and dependencies, not expanded shell commands.
`--only` includes the named steps and their dependencies; `--skip` excludes a
step and causes its dependents to skip. End-phase hooks run after normal steps,
according to the normal phase's outcome.

Flow commands execute in a shell with the caller's permissions. Validation
checks the flow structure, not command safety. Interpolated environment values
are not shell-escaped. Review commands and values before execution.

## Define a flow

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
    {"name": "notify", "command": "./notify.sh ${CHANNEL} 'nightly-etl FAILED'",
     "trigger": "on_failure", "env": {"CHANNEL": "#data-ops"}},
    {"name": "announce", "command": "./notify.sh ${CHANNEL} 'nightly-etl OK'",
     "trigger": "on_success", "env": {"CHANNEL": "#data-ops"}},
    {"name": "cleanup", "command": "rm -rf ${DATA_DIR}/scratch",
     "trigger": "finally"}
  ]
}
```

The ETL file is a template: `extract.py`, `transform.py`, `load.py` and
`notify.sh` are not included. Its cleanup command deletes the configured scratch
directory. Review the JSON and use a dry-run to inspect the step order; supply
your own scripts and paths before running the full example.

```bash
python -m flowkit examples/nightly-etl.json --dry-run  # validate, list the steps
# After supplying the scripts and reviewing commands and paths:
python -m flowkit examples/nightly-etl.json            # run the adapted flow
python -m flowkit examples/nightly-etl.json --json     # machine-readable summary
python -m flowkit examples/nightly-etl.json --jobs 4   # independent steps in parallel
python -m flowkit examples/nightly-etl.json --strict-env   # fail on unset $VARs
python -m flowkit examples/nightly-etl.json --report run.json  # JSON run report
python -m flowkit examples/nightly-etl.json --only extract,transform  # subset + deps
python -m flowkit examples/nightly-etl.json --skip notify             # everything but
python -m flowkit examples/nightly-etl.json --output-dir logs/        # full per-step logs
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
  summary and `--report` file carry an `output` field per step. For the
  whole story, `--output-dir DIR` appends each attempt's FULL output to
  `DIR/<step>.log` (step names sanitized to safe file names) and the
  JSON report points at the file via `output_file` - memory keeps the
  tail, disk keeps everything.
- **End-phase hooks.** `"trigger": "on_failure"` holds a step out of the
  normal pass and runs it only when some normal step failed,
  `"on_success"` only when the normal pass fully succeeded, and
  `"finally"` either way - cleanup/notify work pinned to the run's
  outcome. Hooks run sequentially in dependency order after the normal
  pass; a failed, gated-off or condition-false hook skips its own hook
  dependents but never stops the other hooks (fail-fast would only hide
  cleanup), and hooks whose trigger does not match the outcome report
  as skipped. Hooks can depend only on other hooks, normal steps cannot
  depend on hooks, and both are rejected at load time. Dry-run lists
  hooks under their own "end of run" heading, and text/JSON results
  carry the trigger.
- **Conditional steps and selection.** A step can carry a `when`
  condition - `{"when": {"env": "DEPLOY", "equals": "yes"}}` or
  `{"when": {"env": "DRY", "set": false}}` - evaluated against the
  merged environment at run time; a false condition skips the step and
  its dependents without failing the flow. `--only a,b` runs the named
  steps plus their transitive dependencies; `--skip c` runs everything
  else (c's dependents skip too). `--dry-run` shows the selection and
  annotates conditions, so the plan is reviewable before anything runs.

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

The 123-test suite covers flow validation (unknown keys, duplicates, dangling deps,
cycles, trigger/dependency mismatches), topological ordering, retries,
timeouts, skip/fail-fast semantics, end-phase hook execution
(on_failure/on_success/finally gating, cascade and isolation between
hooks, conditions and selection), layered environments, output capture,
and the CLI exit codes.

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
