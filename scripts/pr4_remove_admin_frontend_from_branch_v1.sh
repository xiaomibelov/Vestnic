cd "$(git rev-parse --show-toplevel)"

TS="$(date +%F_%H%M%S)"
TAG="cp/pr4_remove_admin_frontend_start_${TS}"
git tag -a "$TAG" -m "checkpoint: remove admin_frontend from PR4 branch start (${TS})" >/dev/null 2>&1 || true
echo "checkpoint tag: $TAG"

echo
echo "== git (before) =="
git status -sb || true
git show -s --format='%ci %h %d %s' HEAD || true

echo
echo "== remove admin_frontend from git index (keep local files) =="
git rm -r --cached admin_frontend >/dev/null 2>&1 || true

echo
echo "== ensure local admin_frontend is ignored (project-level) =="
if [ -d admin_frontend ]; then
  if [ ! -f .gitignore ]; then
    : > .gitignore
  fi
  if ! grep -qE '^(admin_frontend/|/admin_frontend/)$' .gitignore; then
    printf "\n/admin_frontend/\n" >> .gitignore
  fi
fi

echo
echo "== git diff --stat =="
git diff --stat || true

echo
echo "== commit + push =="
git add -A
git status -sb || true
git commit -m "chore: drop accidental admin_frontend from schema init PR" || true
git push || true

echo
echo "== done =="
echo "rollback: git checkout \"$TAG\""
