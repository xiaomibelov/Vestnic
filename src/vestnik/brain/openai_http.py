from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, Optional

import httpx


@dataclass(frozen=True)
class OpenAIConfig:
    api_key: str
    base_url: str = "https://api.openai.com/v1"
    max_retries: int = 3
    retry_sleep_sec: int = 30
    timeout_sec: int = 60


def _extract_text(resp: dict) -> str:
    # chat.completions -> choices[0].message.content
    try:
        return (resp["choices"][0]["message"]["content"] or "").strip()
    except Exception:
        return ""


def _parse_json_best_effort(s: str) -> Any:
    s = s.strip()
    try:
        return json.loads(s)
    except Exception:
        # attempt to cut array/object
        for left, right in [("[", "]"), ("{", "}")]:
            li = s.find(left)
            ri = s.rfind(right)
            if li != -1 and ri != -1 and ri > li:
                chunk = s[li : ri + 1]
                return json.loads(chunk)
        raise


async def chat_completion(
    cfg: OpenAIConfig,
    *,
    model: str,
    messages: list[dict[str, str]],
    temperature: float = 0.2,
    max_tokens: int = 1200,
) -> dict:
    url = cfg.base_url.rstrip("/") + "/chat/completions"
    headers = {
        "Authorization": f"Bearer {cfg.api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": messages,
        "temperature": float(temperature),
        "max_tokens": int(max_tokens),
    }

    last_err: Optional[Exception] = None
    async with httpx.AsyncClient(timeout=cfg.timeout_sec) as client:
        for attempt in range(1, cfg.max_retries + 1):
            try:
                r = await client.post(url, headers=headers, content=json.dumps(payload))
                if r.status_code >= 400:
                    raise RuntimeError(f"OpenAI HTTP {r.status_code}: {r.text[:2000]}")
                return r.json()
            except Exception as e:
                last_err = e
                if attempt < cfg.max_retries:
                    await asyncio.sleep(cfg.retry_sleep_sec)
                else:
                    raise
    raise last_err or RuntimeError("OpenAI request failed")


async def chat_text(
    cfg: OpenAIConfig,
    *,
    model: str,
    messages: list[dict[str, str]],
    temperature: float = 0.2,
    max_tokens: int = 1200,
) -> str:
    resp = await chat_completion(cfg, model=model, messages=messages, temperature=temperature, max_tokens=max_tokens)
    return _extract_text(resp)


async def chat_json(
    cfg: OpenAIConfig,
    *,
    model: str,
    messages: list[dict[str, str]],
    temperature: float = 0.2,
    max_tokens: int = 1200,
) -> Any:
    txt = await chat_text(cfg, model=model, messages=messages, temperature=temperature, max_tokens=max_tokens)
    return _parse_json_best_effort(txt)
