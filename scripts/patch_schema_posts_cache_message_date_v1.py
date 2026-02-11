from pathlib import Path
import re
import sys

p = Path("src/vestnik/schema.py")
s = p.read_text("utf-8", errors="replace")

# --- 1) ensure_schema: добавить ensure_column для message_date/message_text/created_at в posts_cache секции ---
sec_pat = re.compile(r"(\n\s*# posts_cache\n)(.*?)(\n\s*# deliveries\n)", re.S)
m = sec_pat.search(s)
if not m:
    print("ERROR: posts_cache section not found", file=sys.stderr)
    sys.exit(2)

head, body, tail = m.group(1), m.group(2), m.group(3)

need_lines = [
    '    await _ensure_column(session, posts_cache_cols, "posts_cache", "message_date", "alter table posts_cache add column message_date timestamptz;")',
    '    await _ensure_column(session, posts_cache_cols, "posts_cache", "message_text", "alter table posts_cache add column message_text text;")',
    '    await _ensure_column(session, posts_cache_cols, "posts_cache", "created_at", "alter table posts_cache add column created_at timestamptz;")',
]

if not all(line in body for line in need_lines):
    anchor = 'await _ensure_column(session, posts_cache_cols, "posts_cache", "message_id_int", "alter table posts_cache add column message_id_int bigint;")'
    if anchor not in body:
        print("ERROR: anchor ensure_column message_id_int not found in posts_cache section", file=sys.stderr)
        sys.exit(2)

    insert = anchor + "\n" + "\n".join(need_lines)
    body = body.replace(anchor, insert, 1)

new_section = head + body + tail
s2 = s[:m.start()] + new_section + s[m.end():]

# --- 2) check_schema: добавить message_date в required_cols["posts_cache"] ---
# Ищем entry "posts_cache": [...]
entry_pat = re.compile(r'("posts_cache"\s*:\s*\[)([^\]]*)(\])', re.S)
m2 = entry_pat.search(s2)
if not m2:
    print('ERROR: required_cols["posts_cache"] entry not found', file=sys.stderr)
    sys.exit(2)

inside = m2.group(2)
if "message_date" not in inside:
    items = [x.strip() for x in inside.split(",") if x.strip()]
    items.append('"message_date"')
    new_inside = ", ".join(items)
    s2 = s2[:m2.start(2)] + new_inside + s2[m2.end(2):]

p.write_text(s2, "utf-8")
print("OK: patched src/vestnik/schema.py")
