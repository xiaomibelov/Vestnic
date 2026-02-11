from __future__ import annotations

from pathlib import Path
import subprocess
import re
import sys


def repo_root() -> Path:
    return Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip())


def slice_block(s: str, name: str) -> tuple[int, int, str] | None:
    needle = f"async def {name}("
    i = s.find(needle)
    if i < 0:
        return None
    j_async = s.find("\nasync def ", i + 1)
    j_def = s.find("\ndef ", i + 1)
    cand = [x for x in (j_async, j_def) if x != -1]
    j = min(cand) if cand else len(s)
    return i, j, s[i:j]


def replace_block(s: str, name: str, new_seg: str) -> str:
    blk = slice_block(s, name)
    if not blk:
        raise RuntimeError(f"cannot find block: {name}")
    i, j, old_seg = blk
    return s[:i] + new_seg + s[j:]


def ensure_commit_before_returns(seg: str) -> str:
    lines = seg.splitlines(True)
    out: list[str] = []
    for idx, line in enumerate(lines):
        m = re.match(r"^(\s*)return\b", line)
        if not m:
            out.append(line)
            continue
        indent = m.group(1)
        # find previous non-empty line in out
        k = len(out) - 1
        while k >= 0 and out[k].strip() == "":
            k -= 1
        prev = out[k] if k >= 0 else ""
        if re.match(rf"^{re.escape(indent)}await session\.(commit|rollback)\(\)\s*$", prev.strip()):
            out.append(line)
            continue
        out.append(f"{indent}await session.commit()\n")
        out.append(line)
    return "".join(out)


def main() -> int:
    root = repo_root()
    path = root / "src/vestnik/worker/__main__.py"
    s = path.read_text(encoding="utf-8")

    changed = False

    # 1) Functions that currently do: return {.. for .. in res.all()} / return [.. for .. in res.all()]
    # Need to materialize rows BEFORE commit.
    for fn, kind in [
        ("_list_tables", "set"),
        ("_table_cols", "set"),
        ("_selected_pack_ids", "list_int"),
    ]:
        blk = slice_block(s, fn)
        if not blk:
            continue
        i, j, seg = blk
        if "await session.commit()" in seg and "rows = res.all()" in seg:
            continue

        if kind == "set":
            pat = re.compile(r"^(\s*)return\s+\{r\[0\]\s+for\s+r\s+in\s+res\.all\(\)\}\s*$", re.M)
            m = pat.search(seg)
            if not m:
                continue
            ind = m.group(1)
            repl = (
                f"{ind}rows = res.all()\n"
                f"{ind}out = {{r[0] for r in rows}}\n"
                f"{ind}await session.commit()\n"
                f"{ind}return out"
            )
            new_seg = pat.sub(repl, seg, count=1)
        else:
            pat = re.compile(r"^(\s*)return\s+\[int\(r\[0\]\)\s+for\s+r\s+in\s+res\.all\(\)\]\s*$", re.M)
            m = pat.search(seg)
            if not m:
                continue
            ind = m.group(1)
            repl = (
                f"{ind}rows = res.all()\n"
                f"{ind}out = [int(r[0]) for r in rows]\n"
                f"{ind}await session.commit()\n"
                f"{ind}return out"
            )
            new_seg = pat.sub(repl, seg, count=1)

        if new_seg != seg:
            s = s[:i] + new_seg + s[j:]
            changed = True

    # 2) Functions that build 'out' (or similar) and then return it -> insert commit right before returns
    for fn in [
        "_fetch_users",
        "_channels_for_pack_ids",
        "_packs_for_ids",
        "_fetch_unsent_posts",
    ]:
        blk = slice_block(s, fn)
        if not blk:
            continue
        i, j, seg = blk
        if re.search(r"await session\.commit\(\)\s*\n\s*return\b", seg):
            continue
        # only patch the final 'return out' line (safe; out already materialized)
        pat = re.compile(r"^(\s*)return\s+out\s*$", re.M)
        if not pat.search(seg):
            continue
        new_seg = pat.sub(r"\1await session.commit()\n\1return out", seg, count=1)
        if new_seg != seg:
            s = s[:i] + new_seg + s[j:]
            changed = True

    # 3) Functions with multiple returns: ensure commit before EACH return (safe: return expr does not depend on live cursor)
    for fn in ["_get_user_settings", "_find_report_id"]:
        blk = slice_block(s, fn)
        if not blk:
            continue
        i, j, seg = blk
        new_seg = ensure_commit_before_returns(seg)
        if new_seg != seg:
            s = s[:i] + new_seg + s[j:]
            changed = True

    if not changed:
        print("patch: no changes (already applied or patterns not found)")
        return 0

    path.write_text(s, encoding="utf-8")
    print("patch: updated", path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
