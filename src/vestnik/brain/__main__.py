import argparse
import asyncio
import logging
import sys

from vestnik.brain.pipeline import generate_report


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m vestnik.brain")
    sub = p.add_subparsers(dest="cmd", required=True)

    oneshot = sub.add_parser("oneshot", help="Generate a report once (stage1+stage2).")
    oneshot.add_argument("--pack-key", required=True)
    oneshot.add_argument("--hours", type=int, default=24)
    oneshot.add_argument("--limit", type=int, default=120)
    oneshot.add_argument("--save", action="store_true")
    oneshot.add_argument(
        "--period-end",
        default=None,
        help="ISO8601 end timestamp for the window (UTC if naive). Example: 2026-02-09T18:00:00+00:00",
    )
    oneshot.add_argument(
        "--snap",
        default="minute",
        choices=["none", "minute", "5m", "10m", "hour"],
        help="Snap window end time for idempotency/cache. Default: minute.",
    )
    oneshot.add_argument(
        "--user-tg-id",
        type=int,
        default=0,
        help="Telegram user id for report ownership (optional; may be 0)",
    )

    return p


def main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

    p = _build_parser()
    args = p.parse_args(argv)

    if args.cmd == "oneshot":
        res = asyncio.run(
            generate_report(
                pack_key=str(args.pack_key),
                hours=int(args.hours),
                limit=int(args.limit),
                user_tg_id=(None if int(args.user_tg_id) == 0 else int(args.user_tg_id)),
                save=bool(args.save),
                period_end=args.period_end,
                snap=str(args.snap) if args.snap else "minute",
            )
        )
        sys.stdout.write(res.report_text)
        if not res.report_text.endswith("\n"):
            sys.stdout.write("\n")
        return 0

    raise RuntimeError(f"unknown cmd: {args.cmd}")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
