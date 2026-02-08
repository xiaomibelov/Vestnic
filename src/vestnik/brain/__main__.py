from __future__ import annotations

import argparse
import asyncio

from sqlalchemy import text

from vestnik.db import session_scope
from vestnik.schema import ensure_schema
from vestnik.settings import OPENAI_API_KEY

from vestnik.brain.pipeline import generate_report, save_report, ReportResult


async def _pick_user_id(user_tg_id: int | None) -> int:
    async with session_scope() as session:
        await ensure_schema(session)
        if user_tg_id is not None:
            row = (
                await session.execute(
                    text("select id from users where tg_id=:tg limit 1"),
                    {"tg": int(user_tg_id)},
                )
            ).first()
            if not row:
                raise RuntimeError(f"user not found by tg_id={user_tg_id}")
            return int(row[0])

        row = (await session.execute(text("select id from users order by id limit 1"))).first()
        if not row:
            raise RuntimeError("no users in DB")
        return int(row[0])


async def cmd_oneshot(args) -> None:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is empty (set it in .env or pass -e OPENAI_API_KEY=... to docker compose run)")

    res: ReportResult = await generate_report(pack_key=args.pack_key, hours=args.hours, limit=args.limit)
    if args.save:
        uid = await _pick_user_id(args.user_tg_id)
        await save_report(user_id=uid, result=res)
    print(res.report_text)


async def cmd_save_dummy(args) -> None:
    # Saves a deterministic placeholder report without calling LLM (acceptance: DB write path)
    uid = await _pick_user_id(args.user_tg_id)

    res = ReportResult(
        pack_id=0,
        pack_key=args.pack_key,
        pack_title="DUMMY",
        period_start=args.period_start,
        period_end=args.period_end,
        report_text=f"📅 ЧИСТАЯ СВОДКА: DUMMY\nПериод: {args.period_start} — {args.period_end}\nИсточников: 0\n\n(placeholder)\n",
        sources=[],
    )
    await save_report(user_id=uid, result=res)
    print("saved dummy report into reports")


def build_parser():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    o = sub.add_parser("oneshot")
    o.add_argument("--pack-key", required=True)
    o.add_argument("--hours", type=int, default=24)
    o.add_argument("--limit", type=int, default=120)
    o.add_argument("--save", action="store_true")
    o.add_argument("--user-tg-id", type=int, default=None)

    d = sub.add_parser("save-dummy")
    d.add_argument("--pack-key", required=True)
    d.add_argument("--user-tg-id", type=int, default=None)
    d.add_argument("--period-start", required=True)
    d.add_argument("--period-end", required=True)

    return ap


def main():
    args = build_parser().parse_args()
    if args.cmd == "oneshot":
        asyncio.run(cmd_oneshot(args))
        return
    if args.cmd == "save-dummy":
        asyncio.run(cmd_save_dummy(args))
        return


if __name__ == "__main__":
    main()
