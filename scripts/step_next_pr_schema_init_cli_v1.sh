cd "$(git rev-parse --show-toplevel)"

TS="$(date +%F_%H%M%S)"
TAG="cp/step_next_pr_schema_init_cli_v1_start_${TS}"
git tag -a "$TAG" -m "checkpoint: step next pr schema init cli v1 start (${TS})" >/dev/null 2>&1 || true
echo "checkpoint tag: $TAG"

echo
echo "== git (before) =="
git status -sb || true

BR="feat/schema-init-cli-v1"
git switch -c "$BR" 2>/dev/null || git switch "$BR"

echo
echo "== apply changes =="
bash scripts/apply_schema_init_cli_and_deploy_checklist_v1.sh

echo
echo "== git diff --stat =="
git diff --stat || true

echo
echo "== commit =="
git add -A
git status -sb || true
git commit -m "feat(db): add init/check schema CLI + deploy checklist" || true

echo
echo "== push =="
git push -u origin "$BR" || true

echo
echo "== PR create link (GitHub) =="
ORIGIN="$(git remote get-url origin | tr -d '\n')"
REPO=""
if echo "$ORIGIN" | grep -qE '^git@github\.com:'; then
  REPO="$(echo "$ORIGIN" | sed -E 's#^git@github\.com:##; s#\.git$##')"
elif echo "$ORIGIN" | grep -qE '^https://github\.com/'; then
  REPO="$(echo "$ORIGIN" | sed -E 's#^https://github\.com/##; s#\.git$##')"
fi

if [ -n "$REPO" ]; then
  echo "https://github.com/${REPO}/pull/new/${BR}"
else
  echo "origin not GitHub or not detected: $ORIGIN"
  echo "Open PR manually for branch: $BR"
fi
