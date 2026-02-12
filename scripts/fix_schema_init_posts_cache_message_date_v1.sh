cd "$(git rev-parse --show-toplevel)"

TS="$(date +%F_%H%M%S)"
TAG="cp/fix_schema_init_posts_cache_message_date_v1_start_${TS}"
git tag -a "$TAG" -m "checkpoint: fix schema init posts_cache message_date v1 start (${TS})" >/dev/null 2>&1 || true
echo "checkpoint tag: $TAG"

python - <<'PY'
import re
from pathlib import Path

p = Path("src/vestnik/schema.py")
s = p.read_text("utf-8", errors="replace")

# 1) ensure_schema: posts_cache block — добавить ensure_column для message_date/message_text/created_at
needle = r"""# posts_cache
    await session\.execute\(
        text\(
            """
            create table if not exists posts_cache \("""
m = re.search(needle, s)
if not m:
    raise SystemExit("ERROR: cannot find posts_cache create table block marker")

# найдём место после posts_cache_cols и текущих ensure_column (channel_id/message_id_int)
block_pat = r"""# posts_cache.*?posts_cache_cols = await _get_table_columns\(session, "posts_cache"\)\n(?P<body>.*?)(?=\n\n    # deliveries)"""
m2 = re.search(block_pat, s, flags=re.S)
if not m2:
    raise SystemExit("ERROR: cannot locate posts_cache section up to deliveries")

section = m2.group(0)

# Если уже есть ensure_column message_date — не дублируем
if ' "message_date" ' not in section:
    # вставим сразу после ensure for message_id_int
    insert_after = 'await _ensure_column(session, posts_cache_cols, "posts_cache", "message_id_int", "alter table posts_cache add column message_id_int bigint;")\n'
    if insert_after not in section:
        raise SystemExit("ERROR: expected ensure_column for message_id_int not found in posts_cache section")

    add_lines = (
        '    await _ensure_column(session, posts_cache_cols, "posts_cache", "message_date", "alter table posts_cache add column message_date timestamptz;")\n'
        '    await _ensure_column(session, posts_cache_cols, "posts_cache", "message_text", "alter table posts_cache add column message_text text;")\n'
        '    await _ensure_column(session, posts_cache_cols, "posts_cache", "created_at", "alter table posts_cache add column created_at timestamptz;")\n'
    )
    section = section.replace(insert_after, insert_after + add_lines)

# 2) Индекс на message_date — должен создаваться после ensure_column message_date (он уже ниже)
# Просто оставляем create index как есть, теперь колонка гарантированно будет.

s = re.sub(block_pat, section, s, flags=re.S)

# 3) check_schema: required_cols["posts_cache"] — добавить message_date (чтобы check ловил несоответствие)
# Найдём словарь required_cols и поправим строку posts_cache.
req_pat = r'("posts_cache"\s*:\s*\[)([^\]]*)(\])'
m3 = re.search(req_pat, s)
if not m3:
    raise SystemExit('ERROR: cannot find required_cols["posts_cache"] entry')

inside = m3.group(2)
# уже есть message_date?
if "message_date" not in inside:
    # аккуратно добавим в конец списка
    # нормализуем пробелы и кавычки не трогаем, просто добавим элемент
    new_inside = inside.strip()
    if new_inside and not new_inside.endswith(","):
        new_inside += ","
    new_inside += ' "message_date"'
    s = s[:m3.start(2)] + new_inside + s[m3.end(2):]

p.write_text(s, "utf-8")
print("OK: patched src/vestnik/schema.py")
PY

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
echo "== worker oneshot (noschema runtime) =="
docker compose run --rm -e VESTNIK_SCHEMA_AUTO=0 worker python -m vestnik.worker oneshot || true

echo
echo "== git diff --stat =="
git diff --stat || true

echo
echo "== commit + push =="
git add -A
git status -sb || true
git commit -m "fix(db): ensure posts_cache message_date before index; include in schema check" || true
git push || true
