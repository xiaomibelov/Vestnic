from __future__ import annotations
import os

import hashlib
from dataclasses import dataclass
from typing import Any

from vestnik.settings import (
    OPENAI_API_KEY,
    OPENAI_BASE_URL,
    AI_MAX_RETRIES,
    AI_RETRY_SLEEP_SEC,
)
from vestnik.brain.openai_http import OpenAIConfig, chat_json


@dataclass(frozen=True)
class Stage1Item:
    channel_ref: str
    message_id: str
    text_sha256: str
    summary: str
    url: str
    channel_name: str
    model: str


def _sha256_text(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


async def run_stage1(*, model: str, posts: list[dict[str, str]]) -> list[Stage1Item]:
    if not (os.getenv("DEEPSEEK_API_KEY","") or os.getenv("OPENAI_API_KEY","")):
        raise RuntimeError("AI API key is empty (set DEEPSEEK_API_KEY or OPENAI_API_KEY)")
    cfg = OpenAIConfig(
        api_key=(os.getenv("DEEPSEEK_API_KEY","") or os.getenv("OPENAI_API_KEY","")),
        base_url=(os.getenv("DEEPSEEK_BASE_URL","") or OPENAI_BASE_URL),
        max_retries=int(AI_MAX_RETRIES),
        retry_sleep_sec=int(AI_RETRY_SLEEP_SEC),
    )

    # posts entries must have: channel_ref, message_id, url, channel_name, text, text_sha256
    compact: list[dict[str, Any]] = []
    for p in posts:
        compact.append(
            {
                "channel_ref": p.get("channel_ref", ""),
                "message_id": p.get("message_id", ""),
                "url": p.get("url", ""),
                "channel_name": p.get("channel_name", ""),
                "text_sha256": p.get("text_sha256", _sha256_text(p.get("text", ""))),
                "text": (p.get("text", "") or "")[:5000],
            }
        )

    system = (
        "Ты — Stage 1 системы «Чистый вестник».\n"
        "Задача: удалить дубли/рекламу/репосты, выделить факт, убрать эмоции и оценочные суждения.\n"
        "Каждый пост сжать до 1–2 предложений, без домыслов.\n"
        "Если EN — передать суть на русском.\n"
        "Верни ТОЛЬКО JSON-массив объектов:\n"
        "{channel_ref, message_id, text_sha256, summary, url, channel_name}.\n"
        "Никаких других ключей. Никакого текста вне JSON."
    )

    user = "Посты:\n" + str(compact)

    data = await chat_json(
        cfg,
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.2,
        max_tokens=1400,
    )

    if not isinstance(data, list):
        raise RuntimeError("Stage1: expected JSON array")

    out: list[Stage1Item] = []
    seen = set()
    for it in data:
        if not isinstance(it, dict):
            continue
        ch = str(it.get("channel_ref", "")).strip()
        mid = str(it.get("message_id", "")).strip()
        tsha = str(it.get("text_sha256", "")).strip()
        summary = str(it.get("summary", "")).strip()
        url = str(it.get("url", "")).strip()
        cname = str(it.get("channel_name", "")).strip()
        if not ch or not mid or not tsha or not summary:
            continue
        key = (ch, mid)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            Stage1Item(
                channel_ref=ch,
                message_id=mid,
                text_sha256=tsha,
                summary=summary,
                url=url,
                channel_name=cname or f"@{ch}",
                model=model,
            )
        )
    return out
