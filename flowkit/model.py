"""Flow definitions: JSON in, validated DAG out.

A flow is a named set of shell-command steps wired into a DAG by
``depends_on``. The model layer validates everything before anything
runs: unknown keys, missing fields, duplicate names, dangling
dependencies and cycles all fail fast with a clear message.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .envvar import check_syntax


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
        env: Extra environment variables for this step, as (name, value)
            pairs. They override the flow-level ``env`` and the process
            environment for this step's command only.
        when: Optional run condition: {"env": "VAR", "equals": "value"}
            or {"env": "VAR", "set": true/false}. A step whose condition
            is false at run time is skipped (dependents skip too), and
            the skip does not fail the flow.
    """

    name: str
    command: str
    depends_on: tuple[str, ...] = ()
    retries: int = 0
    timeout_seconds: float | None = None
    continue_on_error: bool = False
    env: tuple[tuple[str, str], ...] = ()
    when: dict | None = None


@dataclass(frozen=True)
class Flow:
    """A validated workflow: a name plus its steps.

    env holds flow-level environment variables as (name, value) pairs,
    applied to every step below the process environment and below each
    step's own ``env``.
    """

    name: str
    steps: tuple[Step, ...]
    env: tuple[tuple[str, str], ...] = ()

    def step(self, name: str) -> Step:
        for step in self.steps:
            if step.name == name:
                return step
        raise KeyError(name)


_STEP_KEYS = {"name", "command", "depends_on", "retries", "timeout_seconds",
              "continue_on_error", "env", "when"}


def _validate_when(raw: object, where: str) -> dict | None:
    """Validate a ``when`` condition: {"env": name, "equals": str} or {"env": name, "set": bool}."""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise FlowError(f"{where}: 'when' must be an object")
    unknown = set(raw) - {"env", "equals", "set"}
    if unknown:
        raise FlowError(f"{where}: 'when' has unknown keys: {sorted(unknown)}")
    name = raw.get("env")
    if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
        raise FlowError(f"{where}: 'when' needs an 'env' variable name")
    has_equals = "equals" in raw
    has_set = "set" in raw
    if has_equals == has_set:
        raise FlowError(f"{where}: 'when' needs exactly one of 'equals' or 'set'")
    if has_equals and not isinstance(raw["equals"], str):
        raise FlowError(f"{where}: 'when' 'equals' must be a string")
    if has_set and not isinstance(raw["set"], bool):
        raise FlowError(f"{where}: 'when' 'set' must be true or false")
    return dict(raw)


def when_holds(when: dict | None, env: dict) -> bool:
    """Evaluate a step's run condition against the merged environment."""
    if when is None:
        return True
    value = env.get(when["env"])
    if "equals" in when:
        return value == when["equals"]
    return (value is not None and value != "") == when["set"]

_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _validate_env(raw: object, where: str) -> tuple[tuple[str, str], ...]:
    """Validate an ``env`` object into sorted-stable (name, value) pairs."""
    if raw is None:
        return ()
    if not isinstance(raw, dict):
        raise FlowError(f"{where}: 'env' must be an object mapping names to values")
    pairs = []
    for key, value in raw.items():
        if not isinstance(key, str) or not _NAME_RE.fullmatch(key):
            raise FlowError(f"{where}: bad environment variable name {key!r}")
        if not isinstance(value, str):
            raise FlowError(f"{where}: environment value for {key!r} must be a string")
        pairs.append((key, value))
    return tuple(pairs)


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
        try:
            check_syntax(command)
        except ValueError as exc:
            raise FlowError(f"{where}: {exc}") from exc
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
            env=_validate_env(raw.get("env"), where),
            when=_validate_when(raw.get("when"), where),
        ))

    known = {step.name for step in steps}
    for step in steps:
        for dep in step.depends_on:
            if dep not in known:
                raise FlowError(f"step {step.name!r} depends on unknown step {dep!r}")
            if dep == step.name:
                raise FlowError(f"step {step.name!r} depends on itself")

    flow_env = _validate_env(data.get("env"), "flow")
    flow = Flow(name=name, steps=tuple(steps), env=flow_env)
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
