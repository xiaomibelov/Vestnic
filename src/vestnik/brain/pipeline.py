from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from vestnik.db import session_scope
from vestnik.schema import ensure_schema
from vestnik.settings import AI_ENABLED, AI_CACHE_ENABLED, AI_STAGE1_MODEL, AI_STAGE2_MODEL

from vestnik.brain.stage1 import Stage1Item, run_stage1
from vestnik.brain.stage2 import run_stage2

log = logging.getLogger("vestnik.brain")


@dataclass
class ReportResult:
    pack_id: int
    pack_key: str
    pack_title: str
    period_start: datetime
    period_end: datetime
    report_text: str
    sources: list[Stage1Item]
    input_hash: str | None
    stage2_model: str | None


def _sha256_text(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(second=0, microsecond=0)


async def _load_prompt(session, pack_key: str) -> str:
    row = (await session.execute(text("select text from prompts where key=:k limit 1"), {"k": pack_key})).first()
    if row and str(row[0]).strip():
        return str(row[0])
    row = (await session.execute(text("select text from prompts where key='default' limit 1"))).first()
    if row and str(row[0]).strip():
        return str(row[0])
    return (
        "Формируй стерильную сводку. Запрещено добавлять факты, которых нет в источниках. "
        "Если данных мало — сокращай вывод без домыслов."
    )


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


async def _load_pack_refs(session, pack_id: int) -> list[str]:
    refs = (
        await session.execute(
            text(
                """
                select replace(c.username,'@','') as ref
                from pack_channels pc
                join channels c on c.id=pc.channel_id
                where pc.pack_id=:pid and coalesce(c.is_active,true)=true
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
                select channel_ref, message_id, url, text
                from posts_cache
                where is_deleted=false
                  and expires_at > :now
                  and parsed_at between :start and :end
                  and channel_ref = any(:refs)
                order by parsed_at desc
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
                "channel_ref": str(ch_ref),
                "message_id": str(msg_id),
                "url": str(url or ""),
                "channel_name": f"@{ch_ref}",
                "text": str(txt or ""),
            }
        )
    return out


async def _load_facts(session, refs: list[str], message_ids: list[str]) -> dict[tuple[str, str], dict]:
    if not refs or not message_ids:
        return {}
    rows = (
        await session.execute(
            text(
                """
                select channel_ref, message_id, text_sha256, summary, url, channel_name, model
                from post_facts
                where channel_ref = any(:refs) and message_id = any(:mids)
                """
            ),
            {"refs": list(refs), "mids": list(message_ids)},
        )
    ).all()

    m: dict[tuple[str, str], dict] = {}
    for ch, mid, tsha, summ, url, cname, model in rows:
        m[(str(ch), str(mid))] = {
            "text_sha256": str(tsha),
            "summary": str(summ or ""),
            "url": str(url or ""),
            "channel_name": str(cname or ""),
            "model": str(model or ""),
        }
    return m


async def _upsert_facts(session, items: list[Stage1Item]) -> None:
    if not items:
        return
    for it in items:
        await session.execute(
            text(
                """
                insert into post_facts(channel_ref, message_id, text_sha256, summary, url, channel_name, model, updated_at)
                values (:ch, :mid, :sha, :sum, :url, :cname, :model, now())
                on conflict(channel_ref, message_id)
                do update set
                  text_sha256=excluded.text_sha256,
                  summary=excluded.summary,
                  url=excluded.url,
                  channel_name=excluded.channel_name,
                  model=excluded.model,
                  updated_at=now()
                """
            ),
            {
                "ch": it.channel_ref,
                "mid": it.message_id,
                "sha": it.text_sha256,
                "sum": it.summary,
                "url": it.url,
                "cname": it.channel_name,
                "model": it.model,
            },
        )


async def _reports_columns(session) -> set[str]:
    cols = (
        await session.execute(
            text(
                """
                select column_name
                from information_schema.columns
                where table_schema='public' and table_name='reports'
                """
            )
        )
    ).scalars().all()
    return {str(c) for c in cols}


async def _pick_user_id(session, user_tg_id: int | None) -> int:
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


async def _load_cached_report(session, *, user_id: int, pack_key: str, start: datetime, end: datetime, input_hash: str) -> str | None:
    cols = await _reports_columns(session)
    if "input_hash" not in cols:
        return None
    text_col = "report_text" if "report_text" in cols else ("text" if "text" in cols else None)
    if not text_col:
        return None

    q = text(f"""
        select {text_col}
        from reports
        where user_id=:uid and pack_key=:pk and period_start=:ps and period_end=:pe and input_hash=:ih
        order by id desc
        limit 1
    """)
    row = (
        await session.execute(
            q,
            {"uid": user_id, "pk": pack_key, "ps": start, "pe": end, "ih": input_hash},
        )
    ).first()
    return str(row[0]) if row and row[0] else None


async def _save_report(session, *, user_id: int, res: ReportResult) -> None:
    cols = await _reports_columns(session)

    sources_json = json.dumps(
        [{"summary": i.summary, "url": i.url, "channel_name": i.channel_name} for i in res.sources],
        ensure_ascii=False,
    )

    values: dict[str, object] = {
        "user_id": int(user_id),
        "pack_id": int(res.pack_id),
        "pack_key": str(res.pack_key),
        "period_start": res.period_start,
        "period_end": res.period_end,
        "sources_json": sources_json,
        "report_text": res.report_text,
        "input_hash": res.input_hash,
        "stage2_model": res.stage2_model,
        "stage1_count": len(res.sources),
    }

    insert_cols = []
    params = {}
    for k, v in values.items():
        if k in cols:
            insert_cols.append(k)
            params[k] = v

    if "report_text" not in cols and "text" in cols:
        params["text"] = res.report_text
        insert_cols.append("text")

    if "sources_json" not in cols and "sources" in cols:
        params["sources"] = sources_json
        insert_cols.append("sources")

    if not insert_cols:
        raise RuntimeError("reports: no compatible columns to insert")

    cols_sql = ", ".join(insert_cols)
    ph_sql = ", ".join([f":{c}" for c in insert_cols])

    await session.execute(text(f"insert into reports ({cols_sql}) values ({ph_sql})"), params)
    await session.commit()


async def generate_report(
    *,
    pack_key: str,
    hours: int = 24,
    limit: int = 120,
    user_tg_id: int | None = None,
    save: bool = False,
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> ReportResult:
    if not AI_ENABLED:
        raise RuntimeError("AI_ENABLED=0")

    if period_end is not None:
        end = _utc(period_end)
    else:
        end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    if period_start is not None:
        start = _utc(period_start)
    else:
        start = (end - timedelta(hours=int(hours))).replace(microsecond=0)

    async with session_scope() as session:
        await ensure_schema(session)

        pack_id, pack_title = await _load_pack(session, pack_key)
        prompt_text = await _load_prompt(session, pack_key)

        refs = await _load_pack_refs(session, pack_id)
        if not refs:
            raise RuntimeError(f"pack has no channels: {pack_key}")

        posts = await _load_posts(session, refs, start, end, int(limit))
        if not posts:
            txt = (
                "📅 ЧИСТАЯ СВОДКА: " + str(pack_title) + "\n"
                + "За период " + start.strftime("%Y-%m-%d %H:%M") + "—" + end.strftime("%Y-%m-%d %H:%M") + " значимых событий не обнаружено.\n"
            )[:4096]
            payload = {
                "pack_key": pack_key,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "prompt": prompt_text,
                "model": AI_STAGE2_MODEL,
                "items": [],
            }
            prehash = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
            res = ReportResult(pack_id, pack_key, pack_title, start, end, txt, [], prehash, AI_STAGE2_MODEL)
            if save:
                uid = await _pick_user_id(session, user_tg_id)
                try:
                    cached_text = await _load_cached_report(
                        session,
                        user_id=uid,
                        pack_key=pack_key,
                        start=start,
                        end=end,
                        input_hash=prehash,
                    )
                except Exception:
                    log.exception('stage2 cache check failed (unmasked)')
                    raise
                if cached_text:
                    log.info("stage2 cache hit: input_hash=%s", prehash)
                    return ReportResult(pack_id, pack_key, pack_title, start, end, cached_text, [], prehash, AI_STAGE2_MODEL)
                await _save_report(session, user_id=uid, res=res)
            return res

        for p in posts:
            p["text_sha256"] = _sha256_text(p.get("text", ""))

        mids = list({p["message_id"] for p in posts if p.get("message_id")})
        facts_map = {}
        cached = 0
        to_process: list[dict[str, str]] = []

        if AI_CACHE_ENABLED:
            facts_map = await _load_facts(session, refs, mids)

        stage1_items: dict[tuple[str, str], Stage1Item] = {}

        for p in posts:
            key = (p["channel_ref"], p["message_id"])
            tsha = p["text_sha256"]

            if AI_CACHE_ENABLED and key in facts_map and facts_map[key].get("text_sha256") == tsha and facts_map[key].get("summary"):
                f = facts_map[key]
                cached += 1
                stage1_items[key] = Stage1Item(
                    channel_ref=p["channel_ref"],
                    message_id=p["message_id"],
                    text_sha256=tsha,
                    summary=str(f.get("summary", "")).strip(),
                    url=str(f.get("url", p.get("url", ""))).strip(),
                    channel_name=str(f.get("channel_name", p.get("channel_name", ""))).strip() or p.get("channel_name", ""),
                    model=str(f.get("model", "")),
                )
            else:
                to_process.append(p)

        log.info("stage1 cache: cached=%s need_process=%s total_posts=%s", cached, len(to_process), len(posts))

        if to_process:
            new_items = await run_stage1(model=AI_STAGE1_MODEL, posts=to_process)
            await _upsert_facts(session, new_items)
            await session.commit()
            for it in new_items:
                stage1_items[(it.channel_ref, it.message_id)] = it

        ordered: list[Stage1Item] = []
        for p in posts:
            k = (p["channel_ref"], p["message_id"])
            if k in stage1_items:
                ordered.append(stage1_items[k])

        if len(ordered) < 1:

            txt = (

                "📅 ЧИСТАЯ СВОДКА: " + str(pack_title) + "\n"

                + "За период " + start.strftime("%Y-%m-%d %H:%M") + "—" + end.strftime("%Y-%m-%d %H:%M") + " значимых событий не обнаружено.\n"

            )[:4096]

            payload = {

                "pack_key": pack_key,

                "start": start.isoformat(),

                "end": end.isoformat(),

                "prompt": prompt_text,

                "model": AI_STAGE2_MODEL,

                "items": [

                    {

                        "channel_ref": i.channel_ref,

                        "message_id": i.message_id,

                        "text_sha256": i.text_sha256,

                        "summary": i.summary,

                        "url": i.url,

                        "channel_name": i.channel_name,

                    }

                    for i in ordered

                ],

            }

            prehash = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

            res = ReportResult(pack_id, pack_key, pack_title, start, end, txt, ordered, prehash, AI_STAGE2_MODEL)

            if save:

                uid = await _pick_user_id(session, user_tg_id)

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

                await _save_report(session, user_id=uid, res=res)

            if res is None:
                raise RuntimeError("generate_report produced None (BUG)")
            return res
