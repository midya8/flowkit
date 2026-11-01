"""DAG execution with retries, timeouts, skip semantics and parallelism.

Steps run in topological order; with ``jobs > 1`` independent steps run
concurrently (threads - the work is subprocesses, so the GIL is not the
limit). Reporting stays deterministic: results are always in flow
declaration order, no matter which step finished first.
"""

from __future__ import annotations

import concurrent.futures
import subprocess
import time
from dataclasses import dataclass

from .model import Flow, Step, topo_order


@dataclass(frozen=True)
class StepResult:
    """The outcome of one step.

    status: "succeeded", "failed" (all attempts exhausted or timed out),
    or "skipped" (an ancestor failed, or the flow stopped earlier).
    attempts counts actual command executions: 1 + number of retries
    used, 0 when skipped. exit_code is the last attempt's code, or None
    for skips and timeouts.
    """

    name: str
    status: str
    attempts: int
    duration_seconds: float
    exit_code: int | None
    last_output: str = ""


@dataclass(frozen=True)
class RunResult:
    """The outcome of a whole flow run."""

    flow_name: str
    results: tuple[StepResult, ...]

    @property
    def status(self) -> str:
        """"succeeded" only when no step failed."""
        return "failed" if any(r.status == "failed" for r in self.results) else "succeeded"


def _run_command(command: str, timeout: float | None) -> tuple[int | None, str]:
    """Run one attempt; return (exit_code, combined_output_tail).

    A timeout returns (None, ...) so the caller can treat it as a failed
    attempt and still retry.
    """
    try:
        proc = subprocess.run(
            command, shell=True, capture_output=True, text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return None, f"timed out after {timeout:g}s"
    output = (proc.stdout + proc.stderr).strip()
    return proc.returncode, output[-500:]


def run_step(step: Step, backoff_seconds: float = 0.5) -> StepResult:
    """Run one step with its retry policy; never raises for step failure."""
    attempts = 0
    exit_code: int | None = None
    output = ""
    started = time.monotonic()
    for attempt in range(1 + step.retries):
        attempts += 1
        exit_code, output = _run_command(step.command, step.timeout_seconds)
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
    )


def run_flow(flow: Flow, backoff_seconds: float = 0.5, jobs: int = 1) -> RunResult:
    """Run a flow, up to ``jobs`` independent steps at a time.

    A failed step marks its (transitive) dependents "skipped". Without
    ``continue_on_error`` the flow also stops scheduling new work:
    steps already running finish, everything later is skipped. Failed
    steps on a ``continue_on_error`` path let independent steps keep
    running. Raises ValueError for jobs < 1.
    """
    if jobs < 1:
        raise ValueError(f"jobs must be >= 1, got {jobs!r}")
    topo_order(flow)  # validate even for hand-built Flow objects
    by_name = {step.name: step for step in flow.steps}
    deps = {step.name: set(step.depends_on) for step in flow.steps}
    results: dict[str, StepResult] = {}
    succeeded: set[str] = set()
    failed: set[str] = set()
    stopped = False
    remaining = [step.name for step in flow.steps]

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
                if stopped or deps[name] & failed:
                    results[name] = StepResult(
                        name=name, status="skipped", attempts=0,
                        duration_seconds=0.0, exit_code=None,
                    )
                    failed.add(name)  # skips cascade to dependents
                    continue
                pending[pool.submit(run_step, by_name[name], backoff_seconds)] = name
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
    return RunResult(
        flow_name=flow.name,
        results=tuple(results[s.name] for s in flow.steps),
    )
