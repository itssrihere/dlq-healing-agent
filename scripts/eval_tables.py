#!/usr/bin/env python3
"""Keeps the evaluation tables in README.md identical to the committed evaluation reports, so nobody retypes numbers.

The source of truth is docs/eval/*.md, which LabelledEvalIT writes. This script reads those reports and produces the two
comparison tables in the README (v1, and v2 with the v1 value in brackets). Each table sits between a pair of markers:

    <!-- eval-table:v1 begin ... -->
    ...generated, do not edit by hand...
    <!-- eval-table:v1 end -->

    python3 scripts/eval_tables.py --check    # exit 1 and show a diff if the README is out of step (the default; CI runs it)
    python3 scripts/eval_tables.py --write    # rewrite the tables in the README from the reports
    python3 scripts/eval_tables.py --print    # show the generated tables

Only the tables are generated. Numbers quoted in the surrounding prose, for example "9 of 13 non-correct results", are
written by hand from the per-case detail and stay hand-written. Standard library only.
"""
import argparse
import difflib
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
REPORTS = ROOT / "docs" / "eval"
README = ROOT / "README.md"

# (column heading, report file). The files are the published runs; add a row here when a new run is published.
V1 = [("`gpt-4o-mini` (alias)", "gpt-4o-mini-v1-2026-11-05.md"),
      ("`gpt-4.1` (resolved `gpt-4.1-2025-04-14`)", "gpt-4.1-v1-2026-11-05.md")]
V2 = [("`gpt-4o-mini`", "gpt-4o-mini-v2-2026-11-05.md"),
      ("`gpt-4.1`", "gpt-4.1-v2-2026-11-05.md")]

VERDICT_ROW = re.compile(r"^\| (CORRECT|ACCEPTABLE|SAFE_MISS|WRONG_ROUTE|UNSAFE_REPLAY|ERROR) \| (\d+) \| (\S+) \|$", re.M)


def parse(path):
    """Reads one report written by EvalReport.markdown and returns the numbers the README tables use."""
    text = (REPORTS / path).read_text(encoding="utf-8")
    verdicts = {name: (int(n), share) for name, n, share in VERDICT_ROW.findall(text)}
    if len(verdicts) != 6:
        raise SystemExit(f"{path}: expected 6 verdict rows, found {sorted(verdicts)}")

    def one(pattern, what):
        m = re.search(pattern, text)
        if not m:
            raise SystemExit(f"{path}: cannot find {what}")
        return m.groups()

    calls = int(one(r"= (\d+) calls", "the number of calls")[0])
    exact, held, wrong = one(r"exactly the right payload: ([\d.]+%)\. Held or quarantined instead \(safe, but no automation\): "
                             r"([\d.]+%)\. Wrong payload replayed: ([\d.]+%)", "the mechanical-case automation line")
    strategy = one(r"Right strategy on ([\d.]+%) of", "the strategy line")[0]
    p50, p95, tools = one(r"p50 (\d+) ms, p95 (\d+) ms\. Tool calls per case: ([\d.]+)", "the latency line")
    return {"calls": calls, "verdicts": verdicts, "exact": exact, "held": held, "wrong": wrong, "strategy": strategy,
            "p50": int(p50), "p95": int(p95), "tools": tools}


def count(r, verdict, of_valid=False):
    n, share = r["verdicts"][verdict]
    if of_valid and r["verdicts"]["ERROR"][0] > 0:
        return f"{n} of {r['calls'] - r['verdicts']['ERROR'][0]} ({share})"
    return f"{n} ({share})" if n else "0"


def table(columns, previous=None):
    """previous: the reports of the earlier version, in the same column order, shown in brackets on some rows."""
    reports = [parse(f) for _, f in columns]
    older = [parse(f) for _, f in previous] if previous else None

    def bracket(key, i):
        return f"{reports[i][key]} ({older[i][key]})" if older else reports[i][key]

    rows = [
        ("Model or API errors (excluded" + (")" if older else " from the rates)"), [str(r["verdicts"]["ERROR"][0]) for r in reports]),
        ("CORRECT", [count(r, "CORRECT", of_valid=True) for r in reports]),
        ("ACCEPTABLE", [count(r, "ACCEPTABLE") for r in reports]),
        ("SAFE_MISS", [count(r, "SAFE_MISS") for r in reports]),
        ("WRONG_ROUTE", [count(r, "WRONG_ROUTE") for r in reports]),
        ("**UNSAFE_REPLAY**", [f"**{count(r, 'UNSAFE_REPLAY')}**" for r in reports]),
        ("Mechanical cases replayed with exactly the right payload" + (" (v1 in brackets)" if older else ""),
         [bracket("exact", i) for i in range(len(reports))]),
        ("Mechanical cases held or quarantined instead" + (" (v1)" if older else " (safe)"),
         [bracket("held", i) for i in range(len(reports))]),
    ]
    if not older:
        rows.append(("Mechanical cases replayed with a wrong payload", [r["wrong"] for r in reports]))
    rows.append(("Planner picked an allowed strategy" + (" (v1)" if older else ""),
                 [bracket("strategy", i) for i in range(len(reports))]))
    if not older:
        rows.append(("Tool calls per case", [r["tools"] for r in reports]))
        rows.append(("Triage latency p50 / p95", [f"{r['p50'] / 1000:.1f} s / {r['p95'] / 1000:.1f} s" for r in reports]))

    lines = ["| | " + " | ".join(h for h, _ in columns) + " |", "|---|" + "---|" * len(columns)]
    lines += [f"| {label} | " + " | ".join(cells) + " |" for label, cells in rows]
    return "\n".join(lines)


def generated():
    return {"v1": table(V1), "v2": table(V2, previous=V1)}


def block(name):
    begin = re.compile(rf"<!-- eval-table:{name} begin[^>]*-->\n")
    end = f"<!-- eval-table:{name} end -->"
    return begin, end


def render(readme, tables):
    out = readme
    for name, body in tables.items():
        begin, end = block(name)
        m = begin.search(out)
        stop = out.find(end)
        if not m or stop < 0:
            raise SystemExit(f"README.md has no '<!-- eval-table:{name} begin ... -->' / '{end}' markers")
        out = out[:m.end()] + body + "\n" + out[stop:]
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="fail if the README tables differ from the reports (default)")
    mode.add_argument("--write", action="store_true", help="rewrite the README tables from the reports")
    mode.add_argument("--print", action="store_true", help="print the generated tables")
    args = ap.parse_args()

    tables = generated()
    if args.print:
        for name, body in tables.items():
            print(f"### {name}\n{body}\n")
        return 0
    current = README.read_text(encoding="utf-8")
    wanted = render(current, tables)
    if args.write:
        README.write_text(wanted, encoding="utf-8")
        print("README.md evaluation tables rewritten" if wanted != current else "README.md already up to date")
        return 0
    if wanted == current:
        print("README.md evaluation tables match docs/eval")
        return 0
    sys.stdout.writelines(difflib.unified_diff(current.splitlines(True), wanted.splitlines(True),
                                               "README.md (committed)", "README.md (from docs/eval)"))
    print("\nThe README evaluation tables are out of step with docs/eval. Run: python3 scripts/eval_tables.py --write",
          file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
