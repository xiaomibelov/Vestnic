from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from vestnik.db import session_scope
from vestnik.settings import AI_ENABLED, AI_STAGE1_MODEL, AI_STAGE2_MODEL
from vestnik.brain.stage1 import run_stage1, Stage1Item
from vestnik.brain.stage2 import run_stage2


@dataclass
class ReportResult:
    report_text: str
    sources: list[Stage1Item]


async def generate_report(pack_key: str, hours: int = 24, limit: int = 120) -> ReportResult:
    if not AI_ENABLED:
        raise RuntimeError("AI_ENABLED=0")

    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=int(hours))

    async with session_scope() as session:
        pack = (await session.execute(text("select id, title from packs where key=:k limit 1"), {"k": pack_key})).first()
        if not pack:
            raise RuntimeError(f"pack not found: {pack_key}")
        pack_id = int(pack[0])
        pack_title = str(pack[1])

        refs = (
            await session.execute(
                text(
                    """
                    select replace(c.username,'@','') as ref
                    from pack_channels pc
                    join channels c on c.id = pc.channel_id
                    where pc.pack_id = :pid and coalesce(c.is_active,true)=true
                    """
                ),
                {"pid": pack_id},
            )
        ).scalars().all()

        if not refs:
            raise RuntimeError(f"pack has no channels: {pack_key}")

        posts = (
            await session.execute(
                text(
                    """
                    select p.channel_ref, p.message_id, p.url, p.text
                    from posts_cache p
                    where p.is_deleted=false
                      and p.expires_at > :now
                      and p.parsed_at between :start and :end
                      and p.channel_ref = any(:refs)
                    order by p.parsed_at desc
                    limit :lim
                    """
                ),
                {"now": end, "start": start, "end": end, "refs": list(refs), "lim": int(limit)},
            )
        ).all()

    stage1_in = []
    for r in posts:
        stage1_in.append(
            {
                "channel_name": str(r[0]),
                "url": str(r[2] or ""),
                "text": str(r[3] or ""),
            }
        )

    sources = await run_stage1(model=AI_STAGE1_MODEL, posts=stage1_in)

    if len(sources) < 3:
        txt = (
            f"📅 ЧИСТАЯ СВОДКА: {pack_title}\n"
            f"За период {start.strftime('%Y-%m-%d %H:%M')}—{end.strftime('%Y-%m-%d %H:%M')} значимых событий не обнаружено.\n"
        )
        return ReportResult(report_text=txt[:4096], sources=sources)

    report = await run_stage2(model=AI_STAGE2_MODEL, pack_name=pack_title, start=start, end=end, items=sources)
    return ReportResult(report_text=report, sources=sources)
