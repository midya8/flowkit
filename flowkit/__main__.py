"""Command-line entry point: ``python -m flowkit``.

    python -m flowkit FLOW.json              run a flow
    python -m flowkit FLOW.json --json       machine-readable run summary
    python -m flowkit FLOW.json --dry-run    validate and print the plan only
    python -m flowkit FLOW.json --strict-env fail if a command references an unset variable
    python -m flowkit FLOW.json --report R.json   also write the run summary to a file

Exit codes: 0 flow succeeded (or dry-run validated), 1 a step failed,
2 the flow file is unreadable or invalid, or --strict-env found an
unset variable with no default.
"""

from __future__ import annotations

import argparse
import json
import sys

from .model import FlowError, load_flow, topo_order
from .report import render_text, result_payload
from .runner import missing_vars, run_flow


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
    parser.add_argument("--show-output", action="store_true",
                        help="show each successful step's output tail too "
                             "(failed steps always show theirs)")
    parser.add_argument("--strict-env", action="store_true",
                        help="fail before running if a command references an "
                             "unset variable with no default")
    parser.add_argument("--report", metavar="PATH",
                        help="also write the JSON run summary to PATH")
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
    if args.strict_env:
        unset = missing_vars(flow)
        if unset:
            detail = "; ".join(
                f"{step}: {', '.join(names)}" for step, names in unset.items())
            print(f"error: unset environment variables: {detail}", file=sys.stderr)
            return 2
    if args.dry_run:
        print(f"Flow {flow.name!r}: {len(flow.steps)} steps, execution order:")
        for step in topo_order(flow):
            deps = f" (after {', '.join(step.depends_on)})" if step.depends_on else ""
            print(f"  {step.name}{deps}")
        return 0

    result = run_flow(flow, backoff_seconds=args.backoff, jobs=args.jobs)
    if args.report:
        try:
            with open(args.report, "w") as fh:
                json.dump(result_payload(result), fh, indent=2)
                fh.write("\n")
        except OSError as exc:
            print(f"error: cannot write report file: {exc}", file=sys.stderr)
            return 2
    if args.json:
        print(json.dumps(result_payload(result), indent=2))
    else:
        print(render_text(result, show_output=args.show_output), end="")
    return 0 if result.status == "succeeded" else 1


if __name__ == "__main__":
    sys.exit(main())
