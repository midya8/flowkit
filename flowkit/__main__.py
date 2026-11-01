"""Command-line entry point: ``python -m flowkit``.

    python -m flowkit FLOW.json              run a flow
    python -m flowkit FLOW.json --json       machine-readable run summary
    python -m flowkit FLOW.json --dry-run    validate and print the plan only

Exit codes: 0 flow succeeded (or dry-run validated), 1 a step failed,
2 the flow file is unreadable or invalid.
"""

from __future__ import annotations

import argparse
import json
import sys

from .model import FlowError, load_flow, topo_order
from .report import render_text, result_payload
from .runner import run_flow


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="flowkit")
    parser.add_argument("file", help="flow definition JSON ('-' for stdin)")
    parser.add_argument("--json", action="store_true",
                        help="emit the run summary as JSON")
    parser.add_argument("--dry-run", action="store_true",
                        help="validate and print the execution plan without running")
    parser.add_argument("--backoff", type=float, default=0.5, metavar="SECONDS",
                        help="base backoff between retries (doubles each attempt)")
    parser.add_argument("--jobs", type=int, default=1, metavar="N",
                        help="run up to N independent steps concurrently")
    args = parser.parse_args(argv)

    try:
        raw = sys.stdin if args.file == "-" else open(args.file)
        with raw:
            data = json.load(raw)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"error: cannot read flow file: {exc}", file=sys.stderr)
        return 2
    try:
        flow = load_flow(data)
    except FlowError as exc:
        print(f"error: invalid flow: {exc}", file=sys.stderr)
        return 2

    if args.jobs < 1:
        print("error: --jobs must be >= 1", file=sys.stderr)
        return 2
    if args.dry_run:
        print(f"Flow {flow.name!r}: {len(flow.steps)} steps, execution order:")
        for step in topo_order(flow):
            deps = f" (after {', '.join(step.depends_on)})" if step.depends_on else ""
            print(f"  {step.name}{deps}")
        return 0

    result = run_flow(flow, backoff_seconds=args.backoff, jobs=args.jobs)
    if args.json:
        print(json.dumps(result_payload(result), indent=2))
    else:
        print(render_text(result), end="")
    return 0 if result.status == "succeeded" else 1


if __name__ == "__main__":
    sys.exit(main())
