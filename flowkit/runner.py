"""Sequential DAG execution with retries, timeouts and skip semantics.

v1 runs steps one at a time in topological order - boring and
predictable, which is what you want from automation you paged yourself
about. Each step is a shell command run with ``subprocess``; attempts
are bounded by ``retries`` and ``timeout_seconds``.
"""

from __future__ import annotations

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


def run_flow(flow: Flow, backoff_seconds: float = 0.5) -> RunResult:
    """Run a flow in topological order.

    A failed step marks its (transitive) dependents "skipped". Without
    ``continue_on_error`` the flow also stops scheduling new work: every
    later step is skipped. Failed steps on a ``continue_on_error`` path
    let independent steps keep running.
    """
    results: dict[str, StepResult] = {}
    failed: set[str] = set()
    stopped = False
    for step in topo_order(flow):
        if stopped or any(dep in failed for dep in step.depends_on):
            results[step.name] = StepResult(
                name=step.name, status="skipped", attempts=0,
                duration_seconds=0.0, exit_code=None,
            )
            failed.add(step.name)  # skips cascade to dependents
            continue
        result = run_step(step, backoff_seconds=backoff_seconds)
        results[step.name] = result
        if result.status == "failed":
            failed.add(step.name)
            if not step.continue_on_error:
                stopped = True
    return RunResult(
        flow_name=flow.name,
        results=tuple(results[s.name] for s in flow.steps),
    )
