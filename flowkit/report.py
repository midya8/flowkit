"""Render run results as text or a JSON-ready dict."""

from __future__ import annotations

from .runner import RunResult

_MARK = {"succeeded": "ok", "failed": "FAILED", "skipped": "skip"}


def _output_lines(output: str) -> list[str]:
    """An output tail as indented lines, for under a step's report line."""
    return [f"      | {line}" for line in output.splitlines()]


def render_text(result: RunResult, show_output: bool = False) -> str:
    """One line per step plus a summary line.

    A failed step always shows its captured output tail (that is the
    first thing a person debugging a run wants). With ``show_output``,
    successful steps show theirs too.
    """
    lines = [f"Flow {result.flow_name!r}: {result.status}", ""]
    for r in result.results:
        detail = ""
        if r.status == "succeeded":
            detail = f"{r.duration_seconds:.1f}s"
        elif r.status == "failed":
            reason = "timeout" if r.exit_code is None else f"exit {r.exit_code}"
            detail = f"{reason} after {r.attempts} attempt{'s' if r.attempts != 1 else ''}"
        hook = " [on failure]" if r.trigger == "on_failure" else ""
        lines.append(f"  {r.name:24s} {_MARK[r.status]:6s} {detail}{hook}".rstrip())
        if r.last_output and (r.status == "failed" or (show_output and r.status == "succeeded")):
            lines.extend(_output_lines(r.last_output))
    counts = {s: sum(1 for r in result.results if r.status == s) for s in _MARK}
    lines.extend([
        "",
        f"{counts['succeeded']} succeeded, {counts['failed']} failed, "
        f"{counts['skipped']} skipped",
    ])
    return "\n".join(lines) + "\n"


def result_payload(result: RunResult) -> dict:
    """Machine-readable run summary for --json."""
    return {
        "flow": result.flow_name,
        "status": result.status,
        "steps": [
            {"name": r.name, "status": r.status, "attempts": r.attempts,
             "duration_seconds": r.duration_seconds, "exit_code": r.exit_code,
             "output": r.last_output, "output_file": r.output_file,
             "trigger": r.trigger}
            for r in result.results
        ],
    }
