"""Evaluate a chaos test's pass/fail checks.

    verify.py <test> --manifest M.json [--consumer C.json] [--var name=value ...] "<expr> :: <description>" ...

Each expression is Python evaluated with:
  m: simulator manifest   k: m["kafka"]   c: consumer summary   plus any --var values (as numbers).
Prints one PASS/FAIL line per check; exits 1 if any check fails.
"""

import argparse
import json
import sys


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("test")
    p.add_argument("--manifest", action="append", default=[], help="name=path (first one is also `m`)")
    p.add_argument("--consumer", action="append", default=[], help="name=path (first one is also `c`)")
    p.add_argument("--var", action="append", default=[])
    p.add_argument("checks", nargs="+")
    args = p.parse_args()

    scope: dict = {}
    for kind, items in (("m", args.manifest), ("c", args.consumer)):
        for i, item in enumerate(items):
            name, _, path = item.rpartition("=") if "=" in item else ("", "", item)
            with open(path) as f:
                data = json.load(f)
            if name:
                scope[name] = data
            if i == 0:
                scope[kind] = data
    if "m" in scope:
        scope["k"] = scope["m"].get("kafka", {})
    for var in args.var:
        name, _, value = var.partition("=")
        scope[name] = float(value) if value.replace(".", "", 1).lstrip("-").isdigit() else value

    failed = 0
    print(f"== {args.test} ==")
    for check in args.checks:
        expr, _, description = check.partition("::")
        try:
            ok = bool(eval(expr, {}, dict(scope)))
            error = ""
        except Exception as exc:  # a broken check is a failed check
            ok, error = False, f" ({type(exc).__name__}: {exc})"
        failed += not ok
        print(f"  {'PASS' if ok else 'FAIL'}  {description.strip() or expr.strip()}{error}")
    print(f"== {args.test}: {'PASS' if not failed else f'FAIL ({failed} checks)'} ==")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
