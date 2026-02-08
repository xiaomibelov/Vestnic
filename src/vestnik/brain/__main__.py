from __future__ import annotations

import argparse
import asyncio

from vestnik.brain.pipeline import generate_report


def build_parser():
    ap = argparse.ArgumentParser(prog="vestnik.brain")
    sub = ap.add_subparsers(dest="cmd", required=True)

    o = sub.add_parser("oneshot")
    o.add_argument("--pack-key", required=True)
    o.add_argument("--hours", type=int, default=24)
    o.add_argument("--limit", type=int, default=120)
    o.add_argument("--save", action="store_true")
    o.add_argument("--user-tg-id", type=int, default=None)

    return ap


def main():
    args = build_parser().parse_args()

    if args.cmd == "oneshot":
        res = asyncio.run(
            generate_report(
                pack_key=args.pack_key,
                hours=args.hours,
                limit=args.limit,
                user_tg_id=args.user_tg_id,
                save=bool(args.save),
            )
        )
        print(res.report_text)
        return


if __name__ == "__main__":
    main()
