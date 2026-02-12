from __future__ import annotations

import re
from pathlib import Path

def patch_schema_py() -> None:
    p = Path("src/vestnik/schema.py")
    s = p.read_text(encoding="utf-8")

    if "async def maybe_ensure_schema(" in s:
        print("skip: maybe_ensure_schema already exists:", p)
        return

    if "from vestnik.settings import env_bool" not in s:
        # вставим после AsyncSession импорта
        m = re.search(r"from sqlalchemy\.ext\.asyncio import AsyncSession\s*\n", s)
        if not m:
            raise SystemExit("cannot locate AsyncSession import in schema.py")
        ins_at = m.end()
        s = s[:ins_at] + "from vestnik.settings import env_bool\n\n" + s[ins_at:]

    # вставим helper сразу после импортов (после блока import...)
    m2 = re.search(r"\n\nasync def _get_table_columns", s)
    if not m2:
        raise SystemExit("cannot locate insertion point in schema.py")
    helper = (
        "\n\nasync def maybe_ensure_schema(session: AsyncSession) -> None:\n"
        "    # По умолчанию авто-DDL в рантайме выключен, чтобы не ловить lock waits.\n"
        "    if not env_bool(\"VESTNIK_SCHEMA_AUTO\", False):\n"
        "        return\n"
        "    await ensure_schema(session)\n"
        "    # DDL транзакционный; страхуемся явным commit.\n"
        "    try:\n"
        "        await session.commit()\n"
        "    except Exception:\n"
        "        pass\n"
    )
    s = s[:m2.start()] + helper + s[m2.start():]
    p.write_text(s, encoding="utf-8")
    print("patched:", p)

def patch_callsites() -> None:
    targets = [
        Path("src/vestnik/worker/__main__.py"),
        Path("src/vestnik/bot/__main__.py"),
        Path("src/vestnik/harvester/__main__.py"),
        Path("src/vestnik/brain/pipeline.py"),
    ]
    for p in targets:
        if not p.exists():
            print("skip: missing", p)
            continue
        s = p.read_text(encoding="utf-8")

        s2 = s

        # import
        s2 = re.sub(
            r"\bfrom\s+vestnik\.schema\s+import\s+ensure_schema\b",
            "from vestnik.schema import maybe_ensure_schema",
            s2,
        )

        # await call
        s2 = re.sub(
            r"\bawait\s+ensure_schema\(\s*session\s*\)",
            "await maybe_ensure_schema(session)",
            s2,
        )

        if s2 != s:
            p.write_text(s2, encoding="utf-8")
            print("patched:", p)
        else:
            print("no changes:", p)

patch_schema_py()
patch_callsites()
