#!/usr/bin/env python3
"""
run_queries.py - Execute every query in queries.sql and print the results.

Usage:
    python run_queries.py                 # uses movies.db next to this script
    python run_queries.py --db other.db --max-rows 20
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


def split_statements(sql_text: str) -> list[tuple[str, str]]:
    """Split a SQL script into (title, statement) pairs.

    The title is the closest preceding comment line that starts with 'Q' or
    'Bonus', so the output is labelled with the question it answers.
    sqlite3.complete_statement() is used to detect statement boundaries safely.
    """
    statements, buffer, title = [], [], ""
    for line in sql_text.splitlines():
        stripped = line.strip()
        if not buffer and stripped.startswith("--"):
            comment = stripped.lstrip("-").strip()
            if comment.startswith(("Q", "Bonus")):
                title = comment
            continue
        if not stripped and not buffer:
            continue
        buffer.append(line)
        if sqlite3.complete_statement("\n".join(buffer)):
            statements.append((title, "\n".join(buffer).strip()))
            buffer = []
    return statements


def print_table(columns: list[str], rows: list[tuple], max_rows: int) -> None:
    shown = [tuple("" if v is None else str(v) for v in row) for row in rows[:max_rows]]
    widths = [max(len(c), *(len(r[i]) for r in shown)) if shown else len(c)
              for i, c in enumerate(columns)]
    print("  " + " | ".join(c.ljust(w) for c, w in zip(columns, widths)))
    print("  " + "-+-".join("-" * w for w in widths))
    for row in shown:
        print("  " + " | ".join(v.ljust(w) for v, w in zip(row, widths)))
    if len(rows) > max_rows:
        print(f"  ... {len(rows) - max_rows} more rows")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run queries.sql against the movie database")
    parser.add_argument("--db", type=Path, default=BASE_DIR / "movies.db")
    parser.add_argument("--sql", type=Path, default=BASE_DIR / "queries.sql")
    parser.add_argument("--max-rows", type=int, default=15)
    args = parser.parse_args()

    if not args.db.exists():
        print(f"Database {args.db} not found - run `python etl.py` first.", file=sys.stderr)
        return 1

    with sqlite3.connect(args.db) as conn:
        for title, statement in split_statements(args.sql.read_text(encoding="utf-8")):
            cursor = conn.execute(statement)
            columns = [d[0] for d in cursor.description]
            print(f"\n=== {title} ===")
            print_table(columns, cursor.fetchall(), args.max_rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
