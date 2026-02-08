from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from vestnik.db import session_scope
from vestnik.settings import AI_ENABLED, AI_STAGE1_MODEL, AI_STAGE2_MODEL
from vestnik.brain.stage1 import run_stage1, Stage1Item
from vestnik.brain.stage2 import run_stage2


@dataclass
class ReportResult:
    pack_id: int
    pack_key: str
    pack_title: str
    period_start: datetime
    period_end: datetime
    report_text: str
    sources: list[Stage1Item]


async def _load_pack(session, pack_key: str) -> tuple[int, str]:
    row = (
        await session.execute(
            text("select id, title from packs where key=:k and is_active=true limit 1"),
            {"k": pack_key},
        )
    ).first()
    if not row:
        raise RuntimeError(f"pack not found or inactive: {pack_key}")
    return int(row[0]), str(row[1])


async def _load_pack_channels_refs(session, pack_id: int) -> list[str]:
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
    return [str(x) for x in refs if x]


async def _load_posts(session, refs: list[str], start: datetime, end: datetime, limit: int) -> list[dict[str, str]]:
    rows = (
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

    out: list[dict[str, str]] = []
    for ch_ref, msg_id, url, txt in rows:
        out.append(
            {
                "channel_name": f"@{ch_ref}",
                "url": str(url or ""),
                "text": str(txt or ""),
            }
        )
    return out


async def generate_report(pack_key: str, hours: int = 24, limit: int = 120) -> ReportResult:
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=int(hours))

    async with session_scope() as session:
        pack_id, pack_title = await _load_pack(session, pack_key)
        refs = await _load_pack_channels_refs(session, pack_id)
        if not refs:
            raise RuntimeError(f"pack has no channels: {pack_key}")
        posts = await _load_posts(session, refs, start, end, int(limit))

    if not posts:
        txt = (
            f"📅 ЧИСТАЯ СВОДКА: {pack_title}\n"
            f"За период {start.strftime('%Y-%m-%d %H:%M')}—{end.strftime('%Y-%m-%d %H:%M')} значимых событий не обнаружено.\n"
        )
        return ReportResult(
            pack_id=pack_id,
            pack_key=pack_key,
            pack_title=pack_title,
            period_start=start,
            period_end=end,
            report_text=txt[:4096],
            sources=[],
        )

    if not AI_ENABLED:
        raise RuntimeError("AI_ENABLED=0")

    sources = await run_stage1(model=AI_STAGE1_MODEL, posts=posts)

    if len(sources) < 3:
        txt = (
            f"📅 ЧИСТАЯ СВОДКА: {pack_title}\n"
            f"За период {start.strftime('%Y-%m-%d %H:%M')}—{end.strftime('%Y-%m-%d %H:%M')} значимых событий не обнаружено.\n"
        )
        return ReportResult(
            pack_id=pack_id,
            pack_key=pack_key,
            pack_title=pack_title,
            period_start=start,
            period_end=end,
            report_text=txt[:4096],
            sources=sources,
        )

    report = await run_stage2(model=AI_STAGE2_MODEL, pack_name=pack_title, start=start, end=end, items=sources)
    return ReportResult(
        pack_id=pack_id,
        pack_key=pack_key,
        pack_title=pack_title,
        period_start=start,
        period_end=end,
        report_text=report,
        sources=sources,
    )


async def save_report(
    *,
    user_id: int,
    result: ReportResult,
) -> None:
    # reports schema expected:
    # (user_id, pack_id, pack_key, period_start, period_end, sources_json, report_text, created_at)
    sources_json = json.dumps(
        [{"summary": s.summary, "url": s.url, "channel_name": s.channel_name} for s in result.sources],
        ensure_ascii=False,
    )

    async with session_scope() as session:
        await session.execute(
            text(
                """
                insert into reports (user_id, pack_id, pack_key, period_start, period_end, sources_json, report_text)
                values (:uid, :pid, :pkey, :ps, :pe, :sj, :rt)
                """
            ),
            {
                "uid": int(user_id),
                "pid": int(result.pack_id),
                "pkey": str(result.pack_key),
                "ps": result.period_start,
                "pe": result.period_end,
                "sj": sources_json,
                "rt": str(result.report_text),
            },
        )
        await session.commit()
