from __future__ import annotations
from pathlib import Path
import re

path = Path("src/vestnik/worker/__main__.py")
s = path.read_text(encoding="utf-8")

orig = s

def ensure_commit_before_return(block_regex: str, return_regex: str) -> None:
    global s
    m = re.search(block_regex, s, flags=re.S | re.M)
    if not m:
        return
    block = m.group(0)
    if re.search(r"await\s+session\.(commit|rollback)\s*\(", block):
        return
    rm = re.search(return_regex, block, flags=re.M)
    if not rm:
        return
    insert_at = rm.start()
    block2 = block[:insert_at] + "    await session.commit()\n" + block[insert_at:]
    s = s.replace(block, block2, 1)

# 1) packs select: "... from packs where id = any(:pids) order by id" then return out
ensure_commit_before_return(
    block_regex=r"^async\s+def\s+.*?\n(?:.*\n)*?\s+sel\s*\+=\s*\" from packs where id = any\(:pids\) order by id\"\n(?:.*\n)*?\s+out:\s+list\[dict\[str,\s+Any\]\]\s*=\s*\[\]\n(?:.*\n)*?\n\s+return\s+out\s*$",
    return_regex=r"^\s+return\s+out\s*$",
)

# 2) information_schema.columns helper: select column_name ... from information_schema.columns ... return out
ensure_commit_before_return(
    block_regex=r"^async\s+def\s+_table_cols\(.*?\):\n(?:.*\n)*?\s+res\s*=\s*await\s+session\.execute\(\s*text\(q\)\s*\)\n(?:.*\n)*?\s+out\s*=\s*\[.*?\]\n\s+return\s+out\s*$",
    return_regex=r"^\s+return\s+out\s*$",
)

# 3) information_schema.tables helper: return out
ensure_commit_before_return(
    block_regex=r"^async\s+def\s+_list_tables\(.*?\):\n(?:.*\n)*?\s+res\s*=\s*await\s+session\.execute\(\s*text\(q\)\s*\)\n(?:.*\n)*?\s+out\s*=\s*\[.*?\]\n\s+return\s+out\s*$",
    return_regex=r"^\s+return\s+out\s*$",
)

if s == orig:
    print("patch: no changes (patterns not matched or already has commit/rollback)")
else:
    path.write_text(s, encoding="utf-8")
    print("patch: updated", path)
