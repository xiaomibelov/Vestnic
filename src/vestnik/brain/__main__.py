from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timezone

from vestnik.brain.pipeline import generate_report


def _parse_dt(s: str) -> datetime:
    # accept ISO like 2026-02-09T01:02:03+00:00 or Z
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)


def build_parser():
    ap = argparse.ArgumentParser(prog="vestnik.brain")
    sub = ap.add_subparsers(dest="cmd", required=True)

    o = sub.add_parser("oneshot")
    o.add_argument("--pack-key", required=True)
    o.add_argument("--hours", type=int, default=24)
    o.add_argument("--limit", type=int, default=120)
    o.add_argument("--save", action="store_true")
    o.add_argument("--user-tg-id", type=int, default=None)
    o.add_argument("--period-start", default=None, help="ISO datetime (UTC recommended)")
    o.add_argument("--period-end", default=None, help="ISO datetime (UTC recommended)")

    return ap


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

    args = build_parser().parse_args()

    if args.cmd == "oneshot":
        ps = _parse_dt(args.period_start) if args.period_start else None
        pe = _parse_dt(args.period_end) if args.period_end else None
        res = asyncio.run(
            generate_report(
                pack_key=args.pack_key,
                hours=args.hours,
                limit=args.limit,
                user_tg_id=args.user_tg_id,
                save=bool(args.save),
                period_start=ps,
                period_end=pe,
            )
        )
        print(res.report_text)
        return


if __name__ == "__main__":
    main()
