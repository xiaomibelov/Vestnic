from __future__ import annotations

import argparse
import asyncio
import os
import sys


def _int_env(name: str, default: int) -> int:
    v = (os.getenv(name, "") or "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m vestnik.brain")
    sp = p.add_subparsers(dest="cmd")

    oneshot = sp.add_parser("oneshot", help="Generate one report for a pack")
    oneshot.add_argument("--pack-key", required=True)
    oneshot.add_argument("--hours", type=int, default=24)
    oneshot.add_argument("--limit", type=int, default=120)
    oneshot.add_argument("--save", action="store_true")
    oneshot.add_argument(
        "--user-tg-id",
        type=int,
        default=_int_env("VESTNIK_USER_TG_ID", 0),
        help="Telegram user id for report ownership (optional; may be 0)",
    )

    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.cmd != "oneshot":
        parser.print_help()
        return 2

    from vestnik.brain.pipeline import generate_report

    res = asyncio.run(
        generate_report(
            user_tg_id=int(args.user_tg_id) if args.user_tg_id is not None else 0,
            pack_key=str(args.pack_key),
            hours=int(args.hours),
            limit=int(args.limit),
            save=bool(args.save),
        )
    )

    if res is None:
        raise SystemExit("generate_report returned None (see logs above)")

    print(res.report_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
