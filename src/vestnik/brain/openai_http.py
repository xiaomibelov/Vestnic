from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx

from vestnik.settings import AI_HTTP_TIMEOUT_SEC, OPENAI_API_KEY, OPENAI_BASE_URL


class LLMError(RuntimeError):
    pass


async def chat_completions(model: str, system: str, user: str, temperature: float = 0.2) -> str:
    if not OPENAI_API_KEY:
        raise LLMError("OPENAI_API_KEY is not set")

    url = OPENAI_BASE_URL.rstrip("/") + "/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}

    payload: dict[str, Any] = {
        "model": model,
        "temperature": float(temperature),
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }

    last_err: Exception | None = None
    for attempt in range(1, 4):
        try:
            async with httpx.AsyncClient(timeout=AI_HTTP_TIMEOUT_SEC) as client:
                r = await client.post(url, headers=headers, json=payload)
                r.raise_for_status()
                data = r.json()
                return str(data["choices"][0]["message"]["content"])
        except Exception as e:
            last_err = e
            if attempt < 3:
                await asyncio.sleep(30)
                continue
            break

    raise LLMError(f"LLM request failed after 3 attempts: {last_err}")
