cd "$(git rev-parse --show-toplevel)"

TS="$(date +%F_%H%M%S)"
TAG="cp/pack_schema_init_inputs_start_${TS}"
git tag -a "$TAG" -m "checkpoint: pack schema init inputs start (${TS})" >/dev/null 2>&1 || true
echo "checkpoint tag: $TAG"

echo
echo "== sanity: locate session_scope =="
grep -RIn --exclude-dir=node_modules --exclude-dir=.venv --exclude-dir=.git \
  -E "def session_scope|async def session_scope|session_scope\(" src/vestnik | head -n 60 || true

echo
echo "== pack files =="
rm -f schema_init_inputs.zip
zip -r schema_init_inputs.zip src/vestnik/schema.py src/vestnik/db.py >/dev/null 2>&1 || true

ls -la schema_init_inputs.zip || true
