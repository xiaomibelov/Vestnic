cd "$(git rev-parse --show-toplevel)"

TS="$(date +%F_%H%M%S)"
TAG="cp/deploy_smoke_schema_then_worker_start_${TS}"
git tag -a "$TAG" -m "checkpoint: deploy smoke schema then worker start (${TS})" >/dev/null 2>&1 || true
echo "checkpoint tag: $TAG"

echo
echo "== build worker =="
docker compose build worker

echo
echo "== schema check (should be ok) =="
docker compose run --rm worker python -m vestnik.schema check || true

echo
echo "== schema init (idempotent; safe to run) =="
docker compose run --rm worker python -m vestnik.schema init || true

echo
echo "== schema check again (must be ok) =="
docker compose run --rm worker python -m vestnik.schema check || true

echo
echo "== worker oneshot (noschema runtime) =="
docker compose run --rm -e VESTNIK_SCHEMA_AUTO=0 worker python -m vestnik.worker oneshot || true
