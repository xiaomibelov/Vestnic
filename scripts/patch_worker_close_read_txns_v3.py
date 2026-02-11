from __future__ import annotations

from pathlib import Path
import re
import sys


TARGET = Path("src/vestnik/worker/__main__.py")

TARGET_FUNCS = [
    "_list_tables",
    "_table_cols",
    "_get_user_settings",
    "_fetch_users",
    "_selected_pack_ids",
    "_channels_for_pack_ids",
    "_packs_for_ids",
    "_fetch_unsent_posts",
    "_find_report_id",
]

def _find_async_def(lines: list[str], name: str) -> int | None:
    prefix = f"async def {name}"
    for i, l in enumerate(lines):
        if l.startswith(prefix):
            return i
    return None

def _block_end(lines: list[str], start_i: int) -> int:
    for j in range(start_i + 1, len(lines)):
        if lines[j].startswith("async def ") or lines[j].startswith("def "):
            return j
    return len(lines)

def patch_text(text: str) -> tuple[str, bool]:
    src_lines_nl = text.splitlines(keepends=True)
    plain = [l.rstrip("\n") for l in src_lines_nl]
    out = list(src_lines_nl)
    changed = False

    for fname in TARGET_FUNCS:
        si = _find_async_def(plain, fname)
        if si is None:
            continue
        ei = _block_end(plain, si)

        first_exec = None
        for j in range(si, ei):
            if "await session.execute" in plain[j]:
                first_exec = j
                break
        if first_exec is None:
            continue

        j = si
        while j < ei:
            if j <= first_exec:
                j += 1
                continue

            m = re.match(r"^(\s*)return\b(.*)$", plain[j])
            if not m:
                j += 1
                continue

            indent = m.group(1)

            # if previous non-empty line already commits/rollbacks — skip
            k = j - 1
            while k >= si and plain[k].strip() == "":
                k -= 1
            if k >= si and plain[k].strip() in ("await session.commit()", "await session.rollback()"):
                j += 1
                continue

            line = plain[j].strip()
            expr = line[len("return "):].strip()

            # If return expression consumes `res.*` directly — materialize first, then commit, then return
            if "res." in plain[j]:
                tmp = f"__ret_{fname.strip('_')}"
                new = [
                    f"{indent}{tmp} = {expr}\n",
                    f"{indent}await session.commit()\n",
                    f"{indent}return {tmp}\n",
                ]
                out[j:j+1] = new
                plain[j:j+1] = [x.rstrip("\n") for x in new]
                delta = len(new) - 1
                ei += delta
                changed = True
                j += len(new)
                continue

            # Otherwise: insert commit before return (results already materialized into local vars/out lists)
            out.insert(j, f"{indent}await session.commit()\n")
            plain.insert(j, f"{indent}await session.commit()")
            ei += 1
            changed = True
            j += 2

    return ("".join(out), changed)

def main() -> int:
    if not TARGET.exists():
        print(f"ERROR: file not found: {TARGET}", file=sys.stderr)
        return 2

    original = TARGET.read_text(encoding="utf-8")
    patched, changed = patch_text(original)
    if not changed:
        print("noop: nothing to patch (already patched?)")
        return 0

    TARGET.write_text(patched, encoding="utf-8")
    print(f"patch: updated {TARGET}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
