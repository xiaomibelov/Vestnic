cd "$(git rev-parse --show-toplevel)"

TS="$(date +%F_%H%M%S)"
TAG="cp/fix_schema_init_subscriptions_ends_at_v1_start_${TS}"
git tag -a "$TAG" -m "checkpoint: fix schema init subscriptions ends_at v1 start (${TS})" >/dev/null 2>&1 || true
echo "checkpoint tag: $TAG"

cat > scripts/patch_schema_subscriptions_ends_at_v1.py <<'PY'
from pathlib import Path
import re
import sys

p = Path("src/vestnik/schema.py")
s = p.read_text("utf-8", errors="replace")

# --- 1) ensure_schema: subscriptions section ---
sec_pat = re.compile(r"(\n\s*# subscriptions\n)(.*?)(\n\s*# packs\n)", re.S)
m = sec_pat.search(s)
if not m:
    print("ERROR: subscriptions section not found", file=sys.stderr)
    sys.exit(2)

head, body, tail = m.group(1), m.group(2), m.group(3)

if "create table if not exists subscriptions" not in body:
    print("ERROR: subscriptions create table not found", file=sys.stderr)
    sys.exit(2)

idx_anchor = 'await session.execute(text("create index if not exists ix_subscriptions_ends_at on subscriptions(ends_at);"))'
if idx_anchor not in body:
    print("ERROR: subscriptions index anchor not found", file=sys.stderr)
    sys.exit(2)

ensure_block = """
    subscriptions_cols = await _get_table_columns(session, "subscriptions")
    await _ensure_column(session, subscriptions_cols, "subscriptions", "user_id", "alter table subscriptions add column user_id integer;")
    await _ensure_column(session, subscriptions_cols, "subscriptions", "starts_at", "alter table subscriptions add column starts_at timestamptz;")
    await _ensure_column(session, subscriptions_cols, "subscriptions", "ends_at", "alter table subscriptions add column ends_at timestamptz;")
    await _ensure_column(session, subscriptions_cols, "subscriptions", "status", "alter table subscriptions add column status varchar(32);")
    await _ensure_column(session, subscriptions_cols, "subscriptions", "created_at", "alter table subscriptions add column created_at timestamptz;")
""".rstrip()

if "subscriptions_cols = await _get_table_columns(session, \"subscriptions\")" not in body:
    body = body.replace(idx_anchor, ensure_block + "\n\n" + "    " + idx_anchor.lstrip(), 1)

new_section = head + body + tail
s2 = s[:m.start()] + new_section + s[m.end():]

# --- 2) check_schema: required_cols add/update subscriptions ---
entry_pat = re.compile(r'("subscriptions"\s*:\s*\[)([^\]]*)(\])', re.S)
m2 = entry_pat.search(s2)

needed = ["user_id", "starts_at", "ends_at", "status", "created_at"]

if not m2:
    # Insert near "deliveries" entry if exists, else near posts_cache.
    ins_pat = re.compile(r'("deliveries"\s*:\s*\[[^\]]*\]\s*,\s*\n)')
    m3 = ins_pat.search(s2)
    if not m3:
        ins_pat = re.compile(r'("posts_cache"\s*:\s*\[[^\]]*\]\s*,\s*\n)')
        m3 = ins_pat.search(s2)
    if not m3:
        print("ERROR: cannot find insertion point for subscriptions in required_cols", file=sys.stderr)
        sys.exit(2)
    insert = m3.group(1) + '        "subscriptions": ["' + '", "'.join(needed) + '"],\n'
    s2 = s2[:m3.start(1)] + insert + s2[m3.end(1):]
else:
    inside = m2.group(2)
    items = [x.strip().strip('"') for x in inside.split(",") if x.strip()]
    for col in needed:
        if col not in items:
            items.append(col)
    new_inside = ", ".join([f'"{x}"' for x in items])
    s2 = s2[:m2.start(2)] + new_inside + s2[m2.end(2):]

p.write_text(s2, "utf-8")
print("OK: patched subscriptions in src/vestnik/schema.py")
PY

echo
echo "== run patcher =="
python scripts/patch_schema_subscriptions_ends_at_v1.py || true

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
git commit -m "fix(db): ensure subscriptions columns before ends_at index; include in schema check" || true
git push || true
