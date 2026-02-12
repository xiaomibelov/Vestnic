cd "$(git rev-parse --show-toplevel)"

TS="$(date +%F_%H%M%S)"
TAG="cp/fix_schema_init_deliveries_columns_v1_start_${TS}"
git tag -a "$TAG" -m "checkpoint: fix schema init deliveries columns v1 start (${TS})" >/dev/null 2>&1 || true
echo "checkpoint tag: $TAG"

cat > scripts/patch_schema_deliveries_columns_v1.py <<'PY'
from pathlib import Path
import re
import sys

p = Path("src/vestnik/schema.py")
s = p.read_text("utf-8", errors="replace")

# --- Patch ensure_schema deliveries section ---
sec_pat = re.compile(r"(\n\s*# deliveries\n)(.*?)(\n\s*# user_settings\n)", re.S)
m = sec_pat.search(s)
if not m:
    print("ERROR: deliveries section not found", file=sys.stderr)
    sys.exit(2)

head, body, tail = m.group(1), m.group(2), m.group(3)

# We expect create table deliveries exists in body.
if "create table if not exists deliveries" not in body:
    print("ERROR: deliveries create table not found", file=sys.stderr)
    sys.exit(2)

# Insert ensure columns before index creation.
# Find first index line for deliveries.
idx_anchor = 'await session.execute(text("create index if not exists ix_deliveries_user_id on deliveries(user_id);"))'
if idx_anchor not in body:
    print("ERROR: deliveries index anchor not found", file=sys.stderr)
    sys.exit(2)

ensure_block = """
    deliveries_cols = await _get_table_columns(session, "deliveries")
    await _ensure_column(session, deliveries_cols, "deliveries", "user_id", "alter table deliveries add column user_id integer;")
    await _ensure_column(session, deliveries_cols, "deliveries", "pack_id", "alter table deliveries add column pack_id integer;")
    await _ensure_column(session, deliveries_cols, "deliveries", "channel_id", "alter table deliveries add column channel_id integer;")
    await _ensure_column(session, deliveries_cols, "deliveries", "post_id", "alter table deliveries add column post_id varchar;")
    await _ensure_column(session, deliveries_cols, "deliveries", "status", "alter table deliveries add column status varchar(32);")
    await _ensure_column(session, deliveries_cols, "deliveries", "error", "alter table deliveries add column error text;")
    await _ensure_column(session, deliveries_cols, "deliveries", "created_at", "alter table deliveries add column created_at timestamptz;")
""".rstrip()

if "deliveries_cols = await _get_table_columns(session, \"deliveries\")" not in body:
    body = body.replace(idx_anchor, ensure_block + "\n\n" + "    " + idx_anchor.lstrip(), 1)

new_section = head + body + tail
s2 = s[:m.start()] + new_section + s[m.end():]

# --- Patch check_schema required_cols: add deliveries columns ---
# Find required_cols dict. We'll add/merge deliveries entry.
if '"deliveries"' not in s2:
    # Add a new entry near posts_cache for readability: after posts_cache entry.
    ins_pat = re.compile(r'("posts_cache"\s*:\s*\[[^\]]*\]\s*,\s*\n)')
    m3 = ins_pat.search(s2)
    if not m3:
        print("ERROR: cannot find insertion point near posts_cache in required_cols", file=sys.stderr)
        sys.exit(2)
    insert = m3.group(1) + '        "deliveries": ["user_id", "pack_id", "channel_id", "post_id", "status", "error", "created_at"],\n'
    s2 = s2[:m3.start(1)] + insert + s2[m3.end(1):]
else:
    # Update existing deliveries entry (if exists) to include pack_id at least.
    entry_pat = re.compile(r'("deliveries"\s*:\s*\[)([^\]]*)(\])', re.S)
    m4 = entry_pat.search(s2)
    if m4:
        inside = m4.group(2)
        needed = ["user_id", "pack_id", "channel_id", "post_id", "status", "error", "created_at"]
        items = [x.strip().strip('"') for x in inside.split(",") if x.strip()]
        for col in needed:
            if col not in items:
                items.append(col)
        new_inside = ", ".join([f'"{x}"' for x in items])
        s2 = s2[:m4.start(2)] + new_inside + s2[m4.end(2):]

p.write_text(s2, "utf-8")
print("OK: patched deliveries in src/vestnik/schema.py")
PY

echo
echo "== run patcher =="
python scripts/patch_schema_deliveries_columns_v1.py || true

echo
echo "== py_compile =="
python -m py_compile src/vestnik/schema.py || true

echo
echo "== docker rebuild worker (no cache) =="
docker compose build --no-cache worker

echo
echo "== schema init (container) =="
docker compose run --rm -e VESTNIK_SCHEMA_AUTO=1 worker python -m vestnik.schema init || true

echo
echo "== schema check (container) =="
docker compose run --rm worker python -m vestnik.schema check || true

echo
echo "== git diff --stat =="
git diff --stat || true

echo
echo "== commit + push =="
git add -A
git status -sb || true
git commit -m "fix(db): ensure deliveries columns before indexes; include in schema check" || true
git push || true
