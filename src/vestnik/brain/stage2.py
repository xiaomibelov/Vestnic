from __future__ import annotations
import os

import hashlib
import json
from datetime import datetime
from typing import Optional

from vestnik.settings import (
    OPENAI_API_KEY,
    OPENAI_BASE_URL,
    AI_MAX_RETRIES,
    AI_RETRY_SLEEP_SEC,
)
from vestnik.brain.openai_http import OpenAIConfig, chat_text
from vestnik.brain.stage1 import Stage1Item


def _clip_4096(s: str) -> str:
    s = (s or "").strip()
    if len(s) <= 4096:
        return s
    return s[:4090].rstrip() + "…"


def _input_hash(pack_key: str, start: datetime, end: datetime, prompt: str, model: str, items: list[Stage1Item]) -> str:
    payload = {
        "pack_key": pack_key,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "prompt": prompt,
        "model": model,
        "items": [
            {
                "channel_ref": i.channel_ref,
                "message_id": i.message_id,
                "text_sha256": i.text_sha256,
                "summary": i.summary,
                "url": i.url,
                "channel_name": i.channel_name,
            }
            for i in items
        ],
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def run_stage2(
    *,
    model: str,
    pack_key: str,
    pack_name: str,
    start: datetime,
    end: datetime,
    prompt_text: str,
    items: list[Stage1Item],
) -> tuple[str, str]:
    if not (os.getenv("DEEPSEEK_API_KEY","") or os.getenv("OPENAI_API_KEY","")):
        raise RuntimeError("AI API key is empty (set DEEPSEEK_API_KEY or OPENAI_API_KEY)")
cfg = OpenAIConfig(
        api_key=OPENAI_API_KEY,
        base_url=OPENAI_BASE_URL,
        max_retries=int(AI_MAX_RETRIES),
        retry_sleep_sec=int(AI_RETRY_SLEEP_SEC),
    )

    # Keep input compact: stage2 consumes only processed facts, not raw posts.
    facts = [
        {
            "title": i.summary.split(".")[0][:140],
            "summary": i.summary,
            "url": i.url,
            "channel": i.channel_name,
        }
        for i in items
    ]

    system = (
        "Ты — Stage 2 системы «Чистый вестник».\n"
        "Стиль: стерильный, нейтральный, без оценок.\n"
        "Запрещено додумывать факты.\n"
        "Выход: один текст до 4096 символов.\n"
        "Используй ссылки из входных данных."
    )

    user = (
        f"PACK_NAME: {pack_name}\n"
        f"PACK_KEY: {pack_key}\n"
        f"PERIOD: {start.strftime('%Y-%m-%d %H:%M')} — {end.strftime('%Y-%m-%d %H:%M')}\n\n"
        f"PROMPT_RULES:\n{prompt_text.strip()}\n\n"
        f"STAGE1_FACTS_JSON:\n{json.dumps(facts, ensure_ascii=False)}\n\n"
        "Сформируй отчёт строго в формате:\n"
        "📅 ЧИСТАЯ СВОДКА: {PACK_NAME}\n"
        "Период: {START} — {END}\n"
        "Источников: {COUNT}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🔥 ФАКТЫ И СОБЫТИЯ\n"
        "• ...\n"
        "🔗 ...\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📈 ТРЕНДЫ\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡️ СИГНАЛЫ\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📊 СИНТЕЗ\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🏷 #ЧистыйВестник #"
        + pack_key
        + "\n"
        "Не добавляй ничего вне этого шаблона."
    )

    txt = await chat_text(
        cfg,
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.2,
        max_tokens=1400,
    )

    ih = _input_hash(pack_key, start, end, prompt_text, model, items)
    return _clip_4096(txt), ih
