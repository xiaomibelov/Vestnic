from __future__ import annotations

from pathlib import Path

p = Path("src/vestnik/db.py")
s = p.read_text(encoding="utf-8")

old = "    finally:\n        await session.close()\n"
if old not in s:
    raise SystemExit("pattern not found in src/vestnik/db.py (session_scope finally block)")

new = (
    "    finally:\n"
    "        try:\n"
    "            if session.in_transaction():\n"
    "                await session.rollback()\n"
    "        except Exception:\n"
    "            pass\n"
    "        await session.close()\n"
)

s2 = s.replace(old, new, 1)
if s2 == s:
    raise SystemExit("no change applied (unexpected)")

p.write_text(s2, encoding="utf-8")
print("patched:", p)
