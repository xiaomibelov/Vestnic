cd "$(git rev-parse --show-toplevel)"

TS="$(date +%F_%H%M%S)"
TAG="cp/pr4_smoke_schema_init_cli_start_${TS}"
git tag -a "$TAG" -m "checkpoint: PR4 smoke schema init cli start (${TS})" >/dev/null 2>&1 || true
echo "checkpoint tag: $TAG"

echo
echo "== git status / branch =="
git status -sb || true

echo
echo "== py_compile (host) =="
python -m py_compile src/vestnik/schema.py || true
python -m py_compile src/vestnik/db.py || true

echo
echo "== build worker =="
docker compose build --no-cache worker

echo
echo "== schema --help (container) =="
docker compose run --rm worker python -m vestnik.schema --help || true

echo
echo "== schema check (container) =="
docker compose run --rm worker python -m vestnik.schema check || true

echo
echo "== schema init (container, DDL step) =="
docker compose run --rm -e VESTNIK_SCHEMA_AUTO=1 worker python -m vestnik.schema init || true

echo
echo "== schema check again (container) =="
docker compose run --rm worker python -m vestnik.schema check || true

echo
echo "== worker oneshot (noschema runtime) =="
docker compose run --rm -e VESTNIK_SCHEMA_AUTO=0 worker python -m vestnik.worker oneshot || true

echo
echo "== git: last commit & files =="
git show -s --format='%ci %h %d %s' HEAD || true
git show --name-only --format='' HEAD | sed -n '1,220p' || true

echo
echo "== git diff --stat vs main =="
git fetch origin main >/dev/null 2>&1 || true
git diff --stat origin/main...HEAD || true
