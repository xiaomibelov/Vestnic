cd "$(git rev-parse --show-toplevel)"

set -u

TS="$(date +%F_%H%M%S)"
TAG="cp/pr4_compose_schema_tools_start_${TS}"
git tag -a "$TAG" -m "checkpoint: add compose schema tools start (${TS})" >/dev/null 2>&1 || true
echo "checkpoint tag: $TAG"

echo
echo "== git (before) =="
git status -sb || true
git show -s --format='%ci %h %d %s' HEAD || true

echo
echo "== write docker-compose.yml =="
cat > docker-compose.yml <<'YAML'
services:
  db:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: vestnik
      POSTGRES_USER: vestnik
      POSTGRES_PASSWORD: vestnik
    ports:
      - "5434:5432"
    volumes:
      - vestnik_pg:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  # --- schema tooling (explicit, no runtime DDL) ---
  schema_check:
    build: .
    env_file:
      - .env
    depends_on:
      - db
      - redis
    profiles: ["tools"]
    command: ["python", "-m", "vestnik.schema", "check"]

  schema_init:
    build: .
    env_file:
      - .env
    depends_on:
      - db
      - redis
    profiles: ["tools"]
    command: ["python", "-m", "vestnik.schema", "init"]

  # --- app services (runtime must NOT do schema DDL) ---
  bot:
    build: .
    env_file:
      - .env
    environment:
      VESTNIK_SCHEMA_AUTO: "0"
    depends_on:
      - db
      - redis
    command: ["python", "-m", "vestnik.bot"]

  worker:
    build: .
    env_file:
      - .env
    environment:
      VESTNIK_SCHEMA_AUTO: "0"
    depends_on:
      - db
      - redis
    command: ["python", "-m", "vestnik.worker"]

  harvester:
    build: .
    env_file:
      - .env
    environment:
      VESTNIK_SCHEMA_AUTO: "0"
    depends_on:
      - db
      - redis
    command: ["python", "-m", "vestnik.harvester"]

  web:
    build: .
    env_file:
      - .env
    environment:
      VESTNIK_SCHEMA_AUTO: "0"
    depends_on:
      - db
      - redis
    ports:
      - "8001:8000"
    command: ["python", "-m", "vestnik.web"]

volumes:
  vestnik_pg:
YAML

echo
echo "== patch docs/deploy_checklist.md (append block if missing) =="
mkdir -p docs
if [ ! -f docs/deploy_checklist.md ]; then
  cat > docs/deploy_checklist.md <<'MD'
# Deploy checklist
MD
fi

python - <<'PY'
from pathlib import Path

p = Path("docs/deploy_checklist.md")
s = p.read_text("utf-8", errors="replace")

block = """\n\n## DB schema (explicit)\n\n1) Ensure DB/Redis are up:\n   docker compose up -d db redis\n\n2) Validate schema:\n   docker compose --profile tools run --rm schema_check\n\n3) Apply schema (idempotent):\n   docker compose --profile tools run --rm schema_init\n\n4) Validate again (must be ok):\n   docker compose --profile tools run --rm schema_check\n\nNote: runtime services must run with VESTNIK_SCHEMA_AUTO=0 (no automatic DDL).\n"""

if "## DB schema (explicit)" not in s:
    s = s.rstrip() + block
    p.write_text(s, "utf-8")
    print("OK: deploy checklist updated")
else:
    print("OK: deploy checklist already has schema section")
PY

echo
echo "== smoke: compose config (syntax) =="
docker compose config >/dev/null 2>&1 && echo "OK: docker compose config" || echo "WARN: docker compose config failed"

echo
echo "== smoke: schema tools (requires db/redis) =="
docker compose up -d db redis >/dev/null 2>&1 || true
docker compose --profile tools run --rm schema_check || true

echo
echo "== git diff --stat =="
git diff --stat || true

echo
echo "== commit + push =="
git add -A
git status -sb || true
git commit -m "chore(deploy): add explicit schema init/check compose commands; disable runtime auto DDL" || true
git push || true

echo
echo "== done =="
echo "rollback: git checkout \"$TAG\""
