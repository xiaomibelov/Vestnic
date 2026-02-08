from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from vestnik.brain.openai_http import chat_completions

_JSON_CLEAN_RE = re.compile(r"^[\s`]*|[\s`]*$", re.S)


@dataclass
class Stage1Item:
    summary: str
    url: str
    channel_name: str


_SYSTEM = (
    "Ты — модуль фильтрации новостей. Твоя задача — убрать информационный шум и эмоции.\n"
    "Правила:\n"
    "1) Не выдумывай факты. Используй только текст входных постов.\n"
    "2) Удали рекламу, розыгрыши, призывы подписаться, промо, UTM-спам.\n"
    "3) Удали повторы и репосты (оставь один источник).\n"
    "4) Из каждого поста выдели факт и сожми до 1–2 предложений, нейтрально.\n"
    "5) Если пост на EN — переведи смысл на RU.\n"
    "Выход: СТРОГО валидный JSON-массив объектов вида "
    "[{\"summary\":\"...\",\"url\":\"...\",\"channel_name\":\"...\"}]."
)


def _strip_code_fences(s: str) -> str:
    s = s.strip()
    s = _JSON_CLEAN_RE.sub("", s)
    if s.startswith("```"):
        s = s.split("\n", 1)[-1]
    if s.endswith("```"):
        s = s.rsplit("\n", 1)[0]
    return s.strip()


def _parse_json_array(s: str) -> list[dict[str, Any]]:
    raw = _strip_code_fences(s)
    start = raw.find("[")
    end = raw.rfind("]")
    if start != -1 and end != -1 and end > start:
        raw = raw[start : end + 1]
    return json.loads(raw)


async def run_stage1(model: str, posts: list[dict[str, str]]) -> list[Stage1Item]:
    if not posts:
        return []

    user = "Входные посты (JSON):\n" + json.dumps(posts, ensure_ascii=False)

    out = await chat_completions(model=model, system=_SYSTEM, user=user, temperature=0.1)

    arr = _parse_json_array(out)
    items: list[Stage1Item] = []
    for o in arr:
        summary = str(o.get("summary") or "").strip()
        url = str(o.get("url") or "").strip()
        ch = str(o.get("channel_name") or "").strip()
        if not summary or not url:
            continue
        items.append(Stage1Item(summary=summary, url=url, channel_name=ch or ""))
    return items
