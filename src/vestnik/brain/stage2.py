from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime
from typing import Any

from vestnik.brain.openai_http import OpenAIConfig, chat_text
from vestnik.brain.stage1 import Stage1Item


def _clip_4096(s: str) -> str:
    s = (s or "").strip()
    if len(s) <= 4096:
        return s
    return s[:4090].rstrip() + "…"


def _int_env(name: str, default: int) -> int:
    v = (os.getenv(name, "") or "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


def _ai_key() -> str:
    return (os.getenv("DEEPSEEK_API_KEY", "") or os.getenv("OPENAI_API_KEY", "")).strip()


def _ai_base_url() -> str:
    return (
        os.getenv("DEEPSEEK_BASE_URL", "").strip()
        or os.getenv("OPENAI_BASE_URL", "").strip()
        or "https://api.deepseek.com"
    )


def _mk_cfg() -> OpenAIConfig:
    api_key = _ai_key()
    if not api_key:
        raise RuntimeError("AI API key is empty (set DEEPSEEK_API_KEY or OPENAI_API_KEY)")
    base_url = _ai_base_url()

    max_retries = _int_env("AI_MAX_RETRIES", 2)
    retry_sleep_sec = _int_env("AI_RETRY_SLEEP_SEC", 2)

    # OpenAIConfig may differ by version; keep it tolerant.
    try:
        return OpenAIConfig(
            api_key=api_key,
            base_url=base_url,
            max_retries=max_retries,
            retry_sleep_sec=retry_sleep_sec,
        )
    except TypeError:
        return OpenAIConfig(api_key=api_key, base_url=base_url)


def _sanitize_line(s: str) -> str:
    s = (s or "").replace("\r", " ").replace("\n", " ").strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _input_hash(pack_key: str, start: datetime, end: datetime, prompt: str, model: str, items: list[Stage1Item]) -> str:
    payload: dict[str, Any] = {
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
    cfg = _mk_cfg()

    facts = []
    for i in items:
        summ = _sanitize_line(i.summary)
        title = (summ.split(".")[0] if summ else "").strip()
        if len(title) > 140:
            title = title[:140].rstrip() + "…"
        facts.append(
            {
                "title": title,
                "summary": summ,
                "url": (i.url or "").strip(),
                "channel": (i.channel_name or "").strip(),
            }
        )

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
        temperature=float(os.getenv("AI_STAGE2_TEMPERATURE", "0.2") or "0.2"),
        max_tokens=_int_env("AI_STAGE2_MAX_TOKENS", 1400),
    )

    ih = _input_hash(pack_key, start, end, prompt_text, model, items)
    return _clip_4096(txt), ih
