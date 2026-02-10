from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx


def _env_first(*names: str, default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v and v.strip():
            return v.strip()
    return default


def get_api_key() -> str:
    return _env_first(
        "DEEPSEEK_API_KEY",
        "AI_API_KEY",
        "OPENAI_API_KEY",
        default="",
    )


def get_base_url() -> str:
    # DeepSeek docs: https://api.deepseek.com
    # Some deployments may use /v1, keep compatible
    return _env_first(
        "DEEPSEEK_BASE_URL",
        "AI_BASE_URL",
        "OPENAI_BASE_URL",
        default="https://api.deepseek.com",
    ).rstrip("/")


def _chat_completions_url(base_url: str) -> str:
    # DeepSeek endpoint: POST /chat/completions
    # If base_url ends with /v1 -> /v1/chat/completions (ok for OpenAI-compatible gateways)
    return base_url.rstrip("/") + "/chat/completions"


@dataclass
class ChatCompletionResult:
    content: str
    raw: Dict[str, Any]
    usage: Dict[str, Any]


@dataclass
class OpenAIConfig:
    # Backward-compat config used by stage1/stage2
    api_key: str = ""
    base_url: str = ""
    model: str = ""
    max_retries: int = 3
    retry_sleep_sec: int = 2


def _resolve_cfg(cfg: Optional[OpenAIConfig]) -> Tuple[str, str, str]:
    api_key = (cfg.api_key if cfg else "").strip() or get_api_key().strip()
    base_url = (cfg.base_url if cfg else "").strip() or get_base_url().strip()
    model = (cfg.model if cfg else "").strip()
    if not api_key:
        raise RuntimeError("AI API key is empty. Set DEEPSEEK_API_KEY (or AI_API_KEY/OPENAI_API_KEY) in .env")
    if not base_url:
        base_url = "https://api.deepseek.com"
    return api_key, base_url.rstrip("/"), model


async def create_chat_completion(
    *,
    model: str,
    messages: List[Dict[str, Any]],
    temperature: float = 0.2,
    max_tokens: Optional[int] = None,
    response_format: Optional[Dict[str, Any]] = None,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    timeout_sec: float = 60.0,
    retries: int = 3,
) -> Dict[str, Any]:
    api_key = (api_key or get_api_key()).strip()
    if not api_key:
        raise RuntimeError("AI API key is empty. Set DEEPSEEK_API_KEY (or AI_API_KEY / OPENAI_API_KEY) in .env")

    base_url = (base_url or get_base_url()).rstrip("/")
    url = _chat_completions_url(base_url)

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    payload: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": float(temperature),
    }
    if max_tokens is not None:
        payload["max_tokens"] = int(max_tokens)
    if response_format is not None:
        payload["response_format"] = response_format

    last_err: Optional[Exception] = None
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_sec)) as client:
        for attempt in range(1, retries + 1):
            try:
                r = await client.post(url, headers=headers, json=payload)
                if r.status_code >= 400:
                    raise RuntimeError(f"AI HTTP {r.status_code}: {r.text[:800]}")
                return r.json()
            except Exception as e:
                last_err = e
                if attempt >= retries:
                    break
                await asyncio.sleep(1.2 * attempt)

    raise RuntimeError(f"AI request failed after {retries} tries: {last_err!r}")


def _extract_json_candidate(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return s

    # strip markdown fences
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", s)
        s = re.sub(r"\s*```$", "", s).strip()

    # try to find first JSON object/array
    first_obj = s.find("{")
    first_arr = s.find("[")
    if first_obj == -1 and first_arr == -1:
        return s

    start = first_obj if first_obj != -1 else first_arr
    if first_arr != -1 and first_arr < start:
        start = first_arr

    cand = s[start:].strip()
    return cand


def _loads_json_relaxed(s: str) -> Any:
    cand = _extract_json_candidate(s)
    try:
        return json.loads(cand)
    except Exception:
        # last resort: try trim to last closing bracket/brace
        last_brace = cand.rfind("}")
        last_brack = cand.rfind("]")
        end = max(last_brace, last_brack)
        if end != -1:
            cand2 = cand[: end + 1]
            return json.loads(cand2)
        raise


async def chat_completion(
    *,
    model: str,
    messages: List[Dict[str, Any]],
    temperature: float = 0.2,
    max_tokens: Optional[int] = None,
    response_format: Optional[Dict[str, Any]] = None,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    timeout_sec: float = 60.0,
    retries: int = 3,
) -> ChatCompletionResult:
    data = await create_chat_completion(
        model=model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
        response_format=response_format,
        api_key=api_key,
        base_url=base_url,
        timeout_sec=timeout_sec,
        retries=retries,
    )

    content = ""
    try:
        content = (data.get("choices") or [{}])[0].get("message", {}).get("content") or ""
    except Exception:
        content = ""

    usage = data.get("usage") or {}
    return ChatCompletionResult(content=content, raw=data, usage=usage)


async def chat_completion_text(
    *,
    model: str,
    messages: List[Dict[str, Any]],
    temperature: float = 0.2,
    max_tokens: Optional[int] = None,
    response_format: Optional[Dict[str, Any]] = None,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    timeout_sec: float = 60.0,
    retries: int = 3,
) -> str:
    res = await chat_completion(
        model=model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
        response_format=response_format,
        api_key=api_key,
        base_url=base_url,
        timeout_sec=timeout_sec,
        retries=retries,
    )
    return res.content


# -------------------------
# Backward-compat API (used by current stage1/stage2)
# -------------------------
async def chat_json(
    cfg: OpenAIConfig,
    *,
    messages: List[Dict[str, Any]],
    model: Optional[str] = None,
    temperature: float = 0.2,
    max_tokens: Optional[int] = None,
    timeout_sec: float = 60.0,
    retries: int = 3,
) -> Any:
    api_key, base_url, cfg_model = _resolve_cfg(cfg)
    use_model = (model or cfg_model or _env_first("AI_STAGE1_MODEL", "AI_STAGE2_MODEL", default="deepseek-chat")).strip()

    txt = await chat_completion_text(
        model=use_model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
        response_format=None,
        api_key=api_key,
        base_url=base_url,
        timeout_sec=timeout_sec,
        retries=retries,
    )
    return _loads_json_relaxed(txt)


async def chat_text(
    cfg: OpenAIConfig,
    *,
    messages: List[Dict[str, Any]],
    model: Optional[str] = None,
    temperature: float = 0.2,
    max_tokens: Optional[int] = None,
    timeout_sec: float = 60.0,
    retries: int = 3,
) -> str:
    api_key, base_url, cfg_model = _resolve_cfg(cfg)
    use_model = (model or cfg_model or _env_first("AI_STAGE2_MODEL", "AI_STAGE1_MODEL", default="deepseek-chat")).strip()

    return await chat_completion_text(
        model=use_model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
        response_format=None,
        api_key=api_key,
        base_url=base_url,
        timeout_sec=timeout_sec,
        retries=retries,
    )


# Alias names (на случай старых импортов)
openai_chat_completion = chat_completion
openai_chat_completion_text = chat_completion_text
