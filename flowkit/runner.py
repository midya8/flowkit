"""DAG execution with retries, timeouts, skip semantics and parallelism.

Steps run in topological order; with ``jobs > 1`` independent steps run
concurrently (threads - the work is subprocesses, so the GIL is not the
limit). Reporting stays deterministic: results are always in flow
declaration order, no matter which step finished first.
"""

from __future__ import annotations

import concurrent.futures
import os
import re
import subprocess
import time
from dataclasses import dataclass

from .envvar import expand
from .model import Flow, Step, topo_order, when_holds


@dataclass(frozen=True)
class StepResult:
    """The outcome of one step.

    status: "succeeded", "failed" (all attempts exhausted or timed out),
    or "skipped" (an ancestor failed, or the flow stopped earlier).
    attempts counts actual command executions: 1 + number of retries
    used, 0 when skipped. exit_code is the last attempt's code, or None
    for skips and timeouts. trigger mirrors the step's trigger:
    "always" for normal steps, "on_failure" for failure hooks.
    """

    name: str
    status: str
    attempts: int
    duration_seconds: float
    exit_code: int | None
    last_output: str = ""
    output_file: str | None = None
    trigger: str = "always"


@dataclass(frozen=True)
class RunResult:
    """The outcome of a whole flow run."""

    flow_name: str
    results: tuple[StepResult, ...]

    @property
    def status(self) -> str:
        """"succeeded" only when no step failed."""
        return "failed" if any(r.status == "failed" for r in self.results) else "succeeded"


def merged_env(flow: Flow, step: Step, base: dict | None = None) -> dict:
    """The environment a step runs with.

    Starts from ``base`` (the process environment by default), applies
    the flow-level ``env``, then the step's own ``env`` - most specific
    wins. The step's command is expanded against this mapping, and it
    is also passed to the subprocess itself.
    """
    env = dict(os.environ if base is None else base)
    env.update(flow.env)
    env.update(step.env)
    return env


def missing_vars(flow: Flow, base: dict | None = None) -> dict:
    """{step_name: [unset variable names]} across the flow, for --strict-env."""
    from .envvar import missing
    out = {}
    for step in flow.steps:
        names = missing(step.command, merged_env(flow, step, base))
        if names:
            out[step.name] = names
    return out


def output_path_for(output_dir: str, step_name: str) -> str:
    """The log file for a step inside an output directory.

    Step names come from JSON, so anything outside [A-Za-z0-9_.-] is
    flattened to '-' - the file layout must be predictable and safe to
    create.
    """
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", step_name).strip("-.") or "step"
    return os.path.join(output_dir, f"{safe}.log")


def _run_command(command: str, timeout: float | None,
                 env: dict | None = None, log_path: str | None = None,
                 attempt: int = 1) -> tuple[int | None, str]:
    """Run one attempt; return (exit_code, combined_output_tail).

    A timeout returns (None, ...) so the caller can treat it as a failed
    attempt and still retry. With ``log_path``, the attempt's FULL
    combined output is appended to that file (with an attempt header);
    the returned tail stays memory-bounded at 500 characters.
    """
    try:
        proc = subprocess.run(
            command, shell=True, capture_output=True, text=True,
            timeout=timeout, env=env,
        )
        exit_code: int | None = proc.returncode
        output = (proc.stdout + proc.stderr).strip()
    except subprocess.TimeoutExpired:
        exit_code = None
        output = f"timed out after {timeout:g}s"
    if log_path is not None:
        with open(log_path, "a") as fh:
            fh.write(f"--- attempt {attempt} (exit {exit_code}) ---\n")
            fh.write(output + "\n")
    return exit_code, output[-500:]


def run_step(step: Step, backoff_seconds: float = 0.5,
             env: dict | None = None, log_path: str | None = None) -> StepResult:
    """Run one step with its retry policy; never raises for step failure.

    ``env`` is the already-merged environment for the step (see
    merged_env); the command's $VAR references are expanded against it
    and it is handed to the subprocess.
    """
    command = expand(step.command, env) if env is not None else step.command
    attempts = 0
    exit_code: int | None = None
    output = ""
    started = time.monotonic()
    for attempt in range(1 + step.retries):
        attempts += 1
        exit_code, output = _run_command(command, step.timeout_seconds, env,
                                         log_path, attempt=attempt + 1)
        if exit_code == 0:
            break
        if attempt < step.retries and backoff_seconds > 0:
            time.sleep(backoff_seconds * (2 ** attempt))
    duration = time.monotonic() - started
    status = "succeeded" if exit_code == 0 else "failed"
    return StepResult(
        name=step.name,
        status=status,
        attempts=attempts,
        duration_seconds=round(duration, 3),
        exit_code=exit_code,
        last_output=output,
        output_file=log_path,
        trigger=step.trigger,
    )


def selected_steps(flow: Flow, only: set | None = None,
                   skip: set | None = None) -> set:
    """The steps a run should schedule.

    ``only`` selects those steps plus their transitive dependencies;
    ``skip`` removes steps (their dependents still skip through the
    normal cascade). Unknown names raise ValueError naming them.
    """
    known = {step.name for step in flow.steps}
    for label, names in (("--only", only), ("--skip", skip)):
        if names:
            unknown = names - known
            if unknown:
                raise ValueError(f"{label} names unknown steps: {sorted(unknown)}")
    if only is not None:
        selected: set = set()
        stack = list(only)
        while stack:
            name = stack.pop()
            if name in selected:
                continue
            selected.add(name)
            stack.extend(flow.step(name).depends_on)
    else:
        selected = set(known)
    return selected - (skip or set())


def run_flow(flow: Flow, backoff_seconds: float = 0.5, jobs: int = 1,
             env: dict | None = None, only: set | None = None,
             skip: set | None = None, output_dir: str | None = None) -> RunResult:
    """Run a flow, up to ``jobs`` independent steps at a time.

    A failed step marks its (transitive) dependents "skipped". Without
    ``continue_on_error`` the flow also stops scheduling new work:
    steps already running finish, everything later is skipped. Failed
    steps on a ``continue_on_error`` path let independent steps keep
    running. A step whose ``when`` condition is false, or that is
    outside the --only/--skip selection, is skipped the same way -
    dependents skip, the flow does not fail.

    Steps declared ``trigger: on_failure`` are held out of the normal
    pass. When any normal step failed, the hooks then run one at a time
    in dependency order - cleanup and notification work that should only
    happen on a failed run. A failed hook skips its own hook dependents
    but never stops the other hooks (the flow is already failing, so
    fail-fast would only hide cleanup). When the normal pass succeeds,
    hooks report as skipped. Raises ValueError for jobs < 1 or
    selection names that are not steps in the flow.
    """
    if jobs < 1:
        raise ValueError(f"jobs must be >= 1, got {jobs!r}")
    topo_order(flow)  # validate even for hand-built Flow objects
    if output_dir is not None:
        os.makedirs(output_dir, exist_ok=True)
    selected = selected_steps(flow, only=only, skip=skip)
    by_name = {step.name: step for step in flow.steps}
    normal_steps = [step for step in flow.steps if step.trigger == "always"]
    hook_steps = [step for step in flow.steps if step.trigger == "on_failure"]
    deps = {step.name: set(step.depends_on) for step in normal_steps}
    results: dict[str, StepResult] = {}
    succeeded: set[str] = set()
    failed: set[str] = set()
    stopped = False
    remaining = [step.name for step in normal_steps]

    with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as pool:
        pending: dict[concurrent.futures.Future, str] = {}
        while remaining or pending:
            progressed = False
            for name in list(remaining):
                if not deps[name] <= (succeeded | failed):
                    continue  # dependencies still running
                if len(pending) >= jobs:
                    break  # a slot frees up on the next completion
                remaining.remove(name)
                progressed = True
                step = by_name[name]
                step_env = None
                condition_ok = True
                if name in selected and step.when is not None:
                    step_env = merged_env(flow, step, env)
                    condition_ok = when_holds(step.when, step_env)
                if stopped or deps[name] & failed or name not in selected \
                        or not condition_ok:
                    results[name] = StepResult(
                        name=name, status="skipped", attempts=0,
                        duration_seconds=0.0, exit_code=None,
                    )
                    failed.add(name)  # skips cascade to dependents
                    continue
                log_path = (output_path_for(output_dir, name)
                            if output_dir is not None else None)
                pending[pool.submit(
                    run_step, step, backoff_seconds,
                    step_env if step_env is not None
                    else merged_env(flow, step, env),
                    log_path)] = name
            if not pending:
                if not progressed:
                    break  # unreachable on a validated DAG
                continue
            done, _ = concurrent.futures.wait(
                pending, return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                name = pending.pop(future)
                result = future.result()
                results[name] = result
                if result.status == "succeeded":
                    succeeded.add(name)
                else:
                    failed.add(name)
                    if not by_name[name].continue_on_error:
                        stopped = True
    _run_hooks(flow, hook_steps, selected, results, failed,
               backoff_seconds, env, output_dir)
    return RunResult(
        flow_name=flow.name,
        results=tuple(results[s.name] for s in flow.steps),
    )


def _skipped_result(step: Step) -> StepResult:
    return StepResult(
        name=step.name, status="skipped", attempts=0,
        duration_seconds=0.0, exit_code=None, trigger=step.trigger,
    )


def _run_hooks(flow: Flow, hook_steps: list, selected: set,
               results: dict, normal_failed: set,
               backoff_seconds: float, env: dict | None,
               output_dir: str | None) -> None:
    """Run the on-failure hooks after the normal pass, in place.

    Hooks run sequentially in dependency order when any normal step
    failed; otherwise every hook reports skipped. A failed or skipped
    hook cascades to its hook dependents but never stops other hooks.
    """
    if not hook_steps:
        return
    hook_names = {step.name for step in hook_steps}
    normal_failed_any = any(
        results[step.name].status == "failed" for step in flow.steps
        if step.trigger == "always"
    )
    if not normal_failed_any:
        for step in hook_steps:
            results[step.name] = _skipped_result(step)
        return
    hook_failed: set[str] = set()
    order = [step.name for step in topo_order(flow) if step.name in hook_names]
    for name in order:
        step = next(s for s in hook_steps if s.name == name)
        step_env = merged_env(flow, step, env)
        condition_ok = when_holds(step.when, step_env) if step.when is not None else True
        if (name not in selected or hook_failed & set(step.depends_on)
                or not condition_ok):
            results[name] = _skipped_result(step)
            hook_failed.add(name)  # skips cascade to hook dependents
            continue
        log_path = (output_path_for(output_dir, name)
                    if output_dir is not None else None)
        result = run_step(step, backoff_seconds, step_env, log_path)
        results[name] = result
        if result.status == "failed":
            hook_failed.add(name)
