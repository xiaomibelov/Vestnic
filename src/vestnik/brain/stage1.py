from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from typing import Any

from vestnik.brain.openai_http import OpenAIConfig, chat_text


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


def _int_env(name: str, default: int) -> int:
    v = os.getenv(name, "")
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


def _strip_code_fences(s: str) -> str:
    s = s.strip()
    if s.startswith("```"):
        # drop first fence line
        s = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", s)
        # drop last fence
        s = re.sub(r"\s*```$", "", s)
    return s.strip()


def _extract_json_array(text: str) -> list[Any] | None:
    t = _strip_code_fences(text)

    # find first '[' ... last ']'
    i = t.find("[")
    j = t.rfind("]")
    if i == -1 or j == -1 or j <= i:
        return None

    cand = t[i : j + 1].strip()
    try:
        obj = json.loads(cand)
        if isinstance(obj, list):
            return obj
        return None
    except Exception:
        return None


def _sanitize_summary(s: str) -> str:
    s = (s or "").replace("\r", " ").replace("\n", " ").strip()
    s = s.replace("\\", " ")
    s = s.replace('"', "'")
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) > 220:
        s = s[:217].rstrip() + "…"
    return s


def _build_sources(posts: list[dict[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    out: dict[tuple[str, str], dict[str, str]] = {}
    for p in posts:
        ch = str(p.get("channel_ref", "")).strip()
        mid = str(p.get("message_id", "")).strip()
        if not ch or not mid:
            continue
        out[(ch, mid)] = {
            "text_sha256": str(p.get("text_sha256", "")).strip() or _sha256_text(str(p.get("text", "") or "")),
            "url": str(p.get("url", "")).strip(),
            "channel_name": str(p.get("channel_name", "")).strip(),
        }
    return out


def _chunk(lst: list[dict[str, str]], n: int) -> list[list[dict[str, str]]]:
    if n <= 0:
        return [lst]
    return [lst[i : i + n] for i in range(0, len(lst), n)]


async def _call_stage1_llm(*, cfg: OpenAIConfig, model: str, compact: list[dict[str, str]]) -> list[Any]:
    system = (
        "Ты — Stage 1 системы «Чистый вестник».\n"
        "Задача: удалить дубли/рекламу/репосты, выделить факт, убрать эмоции и оценочные суждения.\n"
        "Каждый пост сжать до 1–2 предложений, без домыслов. Если EN — передать суть на русском.\n"
        "\n"
        "Ограничения:\n"
        "• summary: 1–2 предложения, максимум 220 символов.\n"
        "• В summary нельзя использовать кавычки \" и обратные слэши \\ и переносы строк.\n"
        "• Верни ТОЛЬКО валидный JSON-массив, без markdown и без комментариев.\n"
        "• Если не уверен — верни [].\n"
        "\n"
        "Формат результата — JSON-массив объектов строго с ключами:\n"
        "channel_ref, message_id, text_sha256, summary, url, channel_name\n"
    )

    user = "POSTS_JSON:\n" + json.dumps(compact, ensure_ascii=False)

    txt = await chat_text(
        cfg,
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.0,
        max_tokens=_int_env("AI_STAGE1_MAX_TOKENS", 1600),
    )

    arr = _extract_json_array(txt)
    if arr is not None:
        return arr

    # repair pass: ask model to fix its own output into valid JSON array
    repair_user = (
        "Почини и верни ТОЛЬКО валидный JSON-массив (без markdown). "
        "Внутри массива — объекты с ключами channel_ref, message_id, text_sha256, summary, url, channel_name.\n\n"
        "RAW_OUTPUT:\n"
        + txt
    )

    txt2 = await chat_text(
        cfg,
        model=model,
        messages=[
            {"role": "system", "content": "Ты валидатор JSON. Всегда возвращай только валидный JSON."},
            {"role": "user", "content": repair_user},
        ],
        temperature=0.0,
        max_tokens=_int_env("AI_STAGE1_REPAIR_MAX_TOKENS", 1800),
    )

    arr2 = _extract_json_array(txt2)
    return arr2 if arr2 is not None else []


async def run_stage1(*, model: str, posts: list[dict[str, str]]) -> list[Stage1Item]:
    cfg = _mk_cfg()

    batch_n = _int_env("AI_STAGE1_BATCH", 10)
    max_text = _int_env("AI_STAGE1_TEXT_MAX", 1200)

    # compact input, keep deterministic sha/url/name
    compact_all: list[dict[str, str]] = []
    for p in posts:
        compact_all.append(
            {
                "channel_ref": str(p.get("channel_ref", "") or "")[:200],
                "message_id": str(p.get("message_id", "") or "")[:200],
                "url": str(p.get("url", "") or "")[:1000],
                "channel_name": str(p.get("channel_name", "") or "")[:200],
                "text_sha256": str(p.get("text_sha256", "") or "")[:128] or _sha256_text(str(p.get("text", "") or "")),
                "text": (str(p.get("text", "") or "")[:max_text]),
            }
        )

    sources = _build_sources(compact_all)

    out: list[Stage1Item] = []
    seen: set[tuple[str, str]] = set()

    for chunk in _chunk(compact_all, batch_n):
        data = await _call_stage1_llm(cfg=cfg, model=model, compact=chunk)
        if not isinstance(data, list):
            continue

        for it in data:
            if not isinstance(it, dict):
                continue

            ch = str(it.get("channel_ref", "")).strip()
            mid = str(it.get("message_id", "")).strip()
            if not ch or not mid:
                continue

            key = (ch, mid)
            if key in seen:
                continue

            src = sources.get(key, {})
            tsha = str(it.get("text_sha256", "")).strip() or src.get("text_sha256", "")
            url = str(it.get("url", "")).strip() or src.get("url", "")
            cname = str(it.get("channel_name", "")).strip() or src.get("channel_name", "") or f"@{ch}"

            summary = _sanitize_summary(str(it.get("summary", "")).strip())
            if not tsha or not summary:
                continue

            seen.add(key)
            out.append(
                Stage1Item(
                    channel_ref=ch,
                    message_id=mid,
                    text_sha256=tsha,
                    summary=summary,
                    url=url,
                    channel_name=cname,
                    model=model,
                )
            )

    return out
