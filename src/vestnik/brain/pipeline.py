import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
import sqlalchemy as sa

from sqlalchemy import text

from vestnik.db import session_scope
from vestnik.schema import ensure_schema
from vestnik.settings import (
    AI_CACHE_ENABLED,
    AI_ENABLED,
    AI_STAGE1_MODEL,
    AI_STAGE2_MODEL,
)
from vestnik.brain.stage1 import Stage1Item, run_stage1
from vestnik.brain.stage2 import run_stage2

log = logging.getLogger("vestnik.brain")


@dataclass(frozen=True)
class ReportResult:
    pack_id: int
    pack_key: str
    pack_title: str
    period_start: datetime
    period_end: datetime
    report_text: str
    items: list[Stage1Item]
    input_hash: str
    stage2_model: str


def _brain_parse_period_end(period_end: str | None, fallback_end: datetime) -> datetime:
    if not period_end:
        return fallback_end.astimezone(timezone.utc)

    s = period_end.strip()
    if not s:
        return fallback_end.astimezone(timezone.utc)

    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _brain_snap_end(dt: datetime, mode: str | None) -> datetime:
    m = (mode or "minute").strip().lower()
    if m in ("none", "no", "off", "0"):
        return dt

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)

    if m in ("minute", "min", "1m"):
        return dt.replace(second=0, microsecond=0)

    if m in ("hour", "h", "1h"):
        return dt.replace(minute=0, second=0, microsecond=0)

    if m in ("5m", "5min"):
        snapped_minute = (dt.minute // 5) * 5
        return dt.replace(minute=snapped_minute, second=0, microsecond=0)

    if m in ("10m", "10min"):
        snapped_minute = (dt.minute // 10) * 10
        return dt.replace(minute=snapped_minute, second=0, microsecond=0)

    return dt.replace(second=0, microsecond=0)


async def _load_pack(session, pack_key: str) -> tuple[int, str]:
    row = (
        await session.execute(
            text(
                """
                select id, title
                from packs
                where key = :k
                limit 1
                """
            ),
            {"k": pack_key},
        )
    ).first()
    if not row:
        raise RuntimeError(f"pack not found: {pack_key}")
    return int(row[0]), str(row[1])


def _prompt_key(pack_key: str) -> str:
    return f"brain:{pack_key}:prompt"


async def _load_prompt(session, pack_key: str) -> str:
    k = _prompt_key(pack_key)
    row = (
        await session.execute(
            text(
                """
                select text
                from prompts
                where key = :k
                order by id desc
                limit 1
                """
            ),
            {"k": k},
        )
    ).first()
    if row and row[0]:
        return str(row[0])

    return (
        "Ты — редактор новостной сводки. Сделай краткую, ясную и полезную сводку по фактам ниже. "
        "Не выдумывай. Если фактов нет — скажи, что событий не обнаружено."
    )


async def _pick_user_id(session, user_tg_id: int | None) -> int:
    tg_id = None if (user_tg_id is None or int(user_tg_id) == 0) else int(user_tg_id)

    if tg_id is not None:
        row = (
            await session.execute(
                text("select id from users where tg_id = :tg limit 1"),
                {"tg": tg_id},
            )
        ).first()
        if not row:
            raise RuntimeError(f"user not found by tg_id={tg_id}")
        return int(row[0])

    row = (await session.execute(text("select id from users order by id asc limit 1"))).first()
    if not row:
        raise RuntimeError("no users found (cannot choose default user)")
    return int(row[0])


async def _load_pack_refs(session, pack_id: int) -> list[str]:
    rows = (
        await session.execute(
            text(
                """
                select replace(c.username, '@', '') as ref
                from pack_channels pc
                join channels c on c.id = pc.channel_id
                where pc.pack_id = :pid
                order by pc.id asc
                """
            ),
            {"pid": int(pack_id)},
        )
    ).all()
    return [str(r[0]) for r in rows if r and r[0]]


async def _load_posts(session, refs: list[str], start: datetime, end: datetime, limit: int) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    rows = (
        await session.execute(
            text(
                """
                select
                  p.channel_ref,
                  p.message_id,
                  p.parsed_at as posted_at,
                  p.text,
                  p.url,
                  p.channel_ref as channel_name
                from posts_cache p
                where p.is_deleted = false
                  and p.expires_at > :now
                  and p.channel_ref = any(:refs)
                  and p.parsed_at >= :start
                  and p.parsed_at < :end
                order by p.parsed_at desc
                limit :lim
                """
            ),
            {
                "now": now,
                "refs": refs,
                "start": start,
                "end": end,
                "lim": int(limit),
            },
        )
    ).mappings().all()
    return [dict(r) for r in rows]


async def _load_facts(session, keys):
    # keys: list[(channel_ref, message_id)]
    if not keys:
        return {}

    # DB schema: post_facts.message_id is text -> force str
    norm = []
    for ch_ref, msg_id in keys:
        norm.append((ch_ref, str(msg_id)))

    stmt = sa.text("""
        select
          pf.channel_ref,
          pf.message_id,
          pf.text_sha256,
          pf.summary,
          pf.model
        from post_facts pf
        where (pf.channel_ref, pf.message_id) in :keys
    """).bindparams(sa.bindparam("keys", expanding=True))

    res = await session.execute(stmt, {"keys": norm})
    rows = res.fetchall()

    out = {}
    for r in rows:
        # r is Row: (channel_ref, message_id, text_sha256, summary, model)
        out[(r[0], r[1])] = {
            "text_sha256": r[2],
            "summary": r[3],
            "model": r[4],
        }
    return out

async def _upsert_facts(session, items: list[Stage1Item]) -> None:
    if not items:
        return
    await session.execute(
        text(
            """
            insert into post_facts (channel_ref, message_id, text_sha256, summary, model, updated_at)
            values (:ch, :mid, :sha, :sum, :model, :ts)
            on conflict (channel_ref, message_id)
            do update set
              text_sha256 = excluded.text_sha256,
              summary = excluded.summary,
              model = excluded.model,
              updated_at = excluded.updated_at
            """
        ),
        [
            {
                "ch": it.channel_ref,
                "mid": str(it.message_id),
                "sha": it.text_sha256,
                "sum": it.summary,
                "model": it.model,
                "ts": datetime.now(timezone.utc),
            }
            for it in items
        ],
    )


async def _load_cached_report(session, *, user_id: int, pack_key: str, start: datetime, end: datetime, input_hash: str) -> str | None:
    row = (
        await session.execute(
            text(
                """
                select report_text
                from reports
                where user_id = :uid
                  and pack_key = :pk
                  and period_start = :ps
                  and period_end = :pe
                  and input_hash = :ih
                order by id desc
                limit 1
                """
            ),
            {"uid": int(user_id), "pk": pack_key, "ps": start, "pe": end, "ih": input_hash},
        )
    ).first()
    if row and row[0]:
        return str(row[0])
    return None


async def _save_report(session, *, user_id: int, res: ReportResult) -> None:
    await session.execute(
        text(
            """
            insert into reports (user_id, pack_id, pack_key, period_start, period_end, report_text, input_hash, stage2_model, created_at)
            values (:uid, :pid, :pk, :ps, :pe, :txt, :ih, :m, :ts)
            """
        ),
        {
            "uid": int(user_id),
            "pid": int(res.pack_id),
            "pk": res.pack_key,
            "ps": res.period_start,
            "pe": res.period_end,
            "txt": res.report_text,
            "ih": res.input_hash,
            "m": res.stage2_model,
            "ts": datetime.now(timezone.utc),
        },
    )
    await session.commit()


async def generate_report(
    *,
    pack_key: str,
    hours: int = 24,
    limit: int = 120,
    user_tg_id: int | None = None,
    save: bool = False,
    period_start: datetime | None = None,
    period_end: str | None = None,
    snap: str | None = "minute",
) -> ReportResult:
    if not AI_ENABLED:
        raise RuntimeError("AI is disabled (AI_ENABLED=false)")

    now_utc = datetime.now(timezone.utc)
    end = _brain_parse_period_end(period_end, now_utc)
    end = _brain_snap_end(end, snap)

    if period_start is not None:
        ps = period_start
        if ps.tzinfo is None:
            ps = ps.replace(tzinfo=timezone.utc)
        start = ps.astimezone(timezone.utc)
    else:
        start = end - timedelta(hours=int(hours))

    if (snap or "minute") != "none":
        start = start.replace(second=0, microsecond=0)
        end = end.replace(second=0, microsecond=0)

    log.info(
        "report window: pack=%s start=%s end=%s snap=%s hours=%s",
        pack_key,
        start.isoformat(),
        end.isoformat(),
        (snap or "minute"),
        hours,
    )

    async with session_scope() as session:
        await ensure_schema(session)

        pack_id, pack_title = await _load_pack(session, pack_key)
        prompt_text = await _load_prompt(session, pack_key)

        refs = await _load_pack_refs(session, pack_id)

        uid = None
        if save or AI_CACHE_ENABLED:
            try:
                uid = await _pick_user_id(session, user_tg_id)
            except Exception:
                if save:
                    raise
                uid = None

        def _prehash(items: list[Stage1Item]) -> str:
            payload = {
                "pack_key": pack_key,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "prompt": prompt_text,
                "model": AI_STAGE2_MODEL,
                "items": [
                    {
                        "channel_ref": it.channel_ref,
                        "message_id": it.message_id,
                        "text_sha256": it.text_sha256,
                        "summary": it.summary,
                        "model": it.model,
                    }
                    for it in items
                ],
            }
            raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
            return hashlib.sha256(raw).hexdigest()

        posts = await _load_posts(session, refs, start, end, int(limit))

        if not posts:
            prehash = _prehash([])
            if AI_CACHE_ENABLED and uid is not None:
                cached_text = await _load_cached_report(
                    session,
                    user_id=uid,
                    pack_key=pack_key,
                    start=start,
                    end=end,
                    input_hash=prehash,
                )
                if cached_text:
                    log.info("stage2 cache hit: input_hash=%s", prehash)
                    return ReportResult(pack_id, pack_key, pack_title, start, end, cached_text, [], prehash, AI_STAGE2_MODEL)

            txt = (
                "📅 ЧИСТАЯ СВОДКА: " + str(pack_title) + "\n"
                + "За период " + start.strftime("%Y-%m-%d %H:%M") + "—" + end.strftime("%Y-%m-%d %H:%M")
                + " значимых событий не обнаружено.\n"
            )[:4096]

            res = ReportResult(pack_id, pack_key, pack_title, start, end, txt, [], prehash, AI_STAGE2_MODEL)
            if save and uid is not None:
                await _save_report(session, user_id=uid, res=res)
            return res

        keys = [(str(p["channel_ref"]), int(p["message_id"])) for p in posts]
        stage1_items = await _load_facts(session, keys)

        to_process: list[dict[str, Any]] = []
        cached = 0

        for p in posts:
            k = (str(p["channel_ref"]), int(p["message_id"]))
            if k in stage1_items and stage1_items[k].summary:
                cached += 1
                continue
            to_process.append(p)

        log.info("stage1 cache: cached=%s need_process=%s total_posts=%s", cached, len(to_process), len(posts))

        if to_process:
            if not AI_CACHE_ENABLED:
                stage1_items = {}

            new_items = await run_stage1(posts=to_process, model=AI_STAGE1_MODEL)
            await _upsert_facts(session, new_items)
            await session.commit()
            for it in new_items:
                stage1_items[(it.channel_ref, it.message_id)] = it

        ordered: list[Stage1Item] = []
        for p in posts:
            k = (str(p["channel_ref"]), int(p["message_id"]))
            if k in stage1_items:
                ordered.append(stage1_items[k])

        prehash = _prehash(ordered)

        if AI_CACHE_ENABLED and uid is not None:
            cached_text = await _load_cached_report(
                session,
                user_id=uid,
                pack_key=pack_key,
                start=start,
                end=end,
                input_hash=prehash,
            )
            if cached_text:
                log.info("stage2 cache hit: input_hash=%s", prehash)
                return ReportResult(pack_id, pack_key, pack_title, start, end, cached_text, ordered, prehash, AI_STAGE2_MODEL)

        if len(ordered) < 1:
            txt = (
                "📅 ЧИСТАЯ СВОДКА: " + str(pack_title) + "\n"
                + "За период " + start.strftime("%Y-%m-%d %H:%M") + "—" + end.strftime("%Y-%m-%d %H:%M")
                + " значимых событий не обнаружено.\n"
            )[:4096]
            res = ReportResult(pack_id, pack_key, pack_title, start, end, txt, ordered, prehash, AI_STAGE2_MODEL)
            if save and uid is not None:
                await _save_report(session, user_id=uid, res=res)
            return res

        report_text = await run_stage2(
            items=ordered,
            prompt=prompt_text,
            model=AI_STAGE2_MODEL,
            pack_title=pack_title,
            period_start=start,
            period_end=end,
        )
        if not isinstance(report_text, str):
            report_text = str(report_text or "")

        report_text = report_text.strip()
        if len(report_text) > 4096:
            report_text = report_text[:4096]

        res = ReportResult(pack_id, pack_key, pack_title, start, end, report_text, ordered, prehash, AI_STAGE2_MODEL)
        if save and uid is not None:
            await _save_report(session, user_id=uid, res=res)
        return res
