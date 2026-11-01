"""Flow definitions: JSON in, validated DAG out.

A flow is a named set of shell-command steps wired into a DAG by
``depends_on``. The model layer validates everything before anything
runs: unknown keys, missing fields, duplicate names, dangling
dependencies and cycles all fail fast with a clear message.
"""

from __future__ import annotations

from dataclasses import dataclass


class FlowError(ValueError):
    """A flow definition is invalid. The message says exactly why."""


@dataclass(frozen=True)
class Step:
    """One unit of work in a flow.

    Attributes:
        name: Unique step name; other steps reference it in ``depends_on``.
        command: Shell command to run.
        depends_on: Steps that must succeed before this one starts.
        retries: Extra attempts after the first failure (0 = run once).
        timeout_seconds: Per-attempt timeout; None means no limit.
        continue_on_error: When True, a failure here does not stop the
            rest of the flow; dependents still skip.
    """

    name: str
    command: str
    depends_on: tuple[str, ...] = ()
    retries: int = 0
    timeout_seconds: float | None = None
    continue_on_error: bool = False


@dataclass(frozen=True)
class Flow:
    """A validated workflow: a name plus its steps."""

    name: str
    steps: tuple[Step, ...]

    def step(self, name: str) -> Step:
        for step in self.steps:
            if step.name == name:
                return step
        raise KeyError(name)


_STEP_KEYS = {"name", "command", "depends_on", "retries", "timeout_seconds",
              "continue_on_error"}


def load_flow(data: dict) -> Flow:
    """Validate a parsed JSON object into a Flow.

    Raises FlowError with a specific message for every structural
    problem: wrong types, unknown keys, missing fields, duplicate step
    names, dependencies on unknown steps, negative retries, non-positive
    timeouts.
    """
    if not isinstance(data, dict):
        raise FlowError("flow must be a JSON object")
    name = data.get("name")
    if not isinstance(name, str) or not name.strip():
        raise FlowError("flow needs a non-empty string 'name'")
    raw_steps = data.get("steps")
    if not isinstance(raw_steps, list) or not raw_steps:
        raise FlowError("flow needs a non-empty 'steps' list")

    steps: list[Step] = []
    seen: set[str] = set()
    for i, raw in enumerate(raw_steps):
        where = f"step #{i + 1}"
        if not isinstance(raw, dict):
            raise FlowError(f"{where} must be an object")
        unknown = set(raw) - _STEP_KEYS
        if unknown:
            raise FlowError(f"{where} has unknown keys: {sorted(unknown)}")
        step_name = raw.get("name")
        if not isinstance(step_name, str) or not step_name.strip():
            raise FlowError(f"{where} needs a non-empty string 'name'")
        where = f"step {step_name!r}"
        if step_name in seen:
            raise FlowError(f"duplicate step name {step_name!r}")
        seen.add(step_name)
        command = raw.get("command")
        if not isinstance(command, str) or not command.strip():
            raise FlowError(f"{where} needs a non-empty string 'command'")
        depends_on = raw.get("depends_on", [])
        if not isinstance(depends_on, list) or not all(
                isinstance(d, str) for d in depends_on):
            raise FlowError(f"{where}: 'depends_on' must be a list of step names")
        retries = raw.get("retries", 0)
        if not isinstance(retries, int) or isinstance(retries, bool) or retries < 0:
            raise FlowError(f"{where}: 'retries' must be a non-negative integer")
        timeout = raw.get("timeout_seconds")
        if timeout is not None and (
                not isinstance(timeout, (int, float)) or isinstance(timeout, bool)
                or timeout <= 0):
            raise FlowError(f"{where}: 'timeout_seconds' must be a positive number")
        steps.append(Step(
            name=step_name,
            command=command,
            depends_on=tuple(depends_on),
            retries=retries,
            timeout_seconds=float(timeout) if timeout is not None else None,
            continue_on_error=bool(raw.get("continue_on_error", False)),
        ))

    known = {step.name for step in steps}
    for step in steps:
        for dep in step.depends_on:
            if dep not in known:
                raise FlowError(f"step {step.name!r} depends on unknown step {dep!r}")
            if dep == step.name:
                raise FlowError(f"step {step.name!r} depends on itself")

    flow = Flow(name=name, steps=tuple(steps))
    topo_order(flow)  # raises on cycles
    return flow


def topo_order(flow: Flow) -> list[Step]:
    """Steps in an order where every dependency runs before its dependents.

    Kahn's algorithm, deterministic: among ready steps, the one declared
    first in the flow goes first. Raises FlowError naming the cycle
    members when the graph cannot be fully ordered.
    """
    indegree = {step.name: len(step.depends_on) for step in flow.steps}
    by_name = {step.name: step for step in flow.steps}
    order: list[Step] = []
    ready = [step.name for step in flow.steps if indegree[step.name] == 0]
    while ready:
        name = ready.pop(0)
        order.append(by_name[name])
        for step in flow.steps:
            if name in step.depends_on:
                indegree[step.name] -= 1
                if indegree[step.name] == 0:
                    ready.append(step.name)
    if len(order) != len(flow.steps):
        cyclic = sorted(n for n, d in indegree.items() if d > 0)
        raise FlowError(f"dependency cycle among steps: {cyclic}")
    return order
