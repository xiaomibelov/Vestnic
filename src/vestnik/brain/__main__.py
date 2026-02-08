import argparse
import asyncio

from vestnik.brain.pipeline import generate_report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["oneshot"])
    ap.add_argument("--pack-key", required=True)
    ap.add_argument("--hours", type=int, default=24)
    ap.add_argument("--limit", type=int, default=120)
    args = ap.parse_args()

    if args.cmd == "oneshot":
        res = asyncio.run(generate_report(pack_key=args.pack_key, hours=args.hours, limit=args.limit))
        print(res.report_text)
        return


if __name__ == "__main__":
    main()
