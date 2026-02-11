cd "$(git rev-parse --show-toplevel)"

TS="$(date +%F_%H%M%S)"
TAG="cp/ignore_local_debug_scripts_start_${TS}"
git tag -a "$TAG" -m "checkpoint: ignore local debug scripts start (${TS})" >/dev/null 2>&1 || true
echo "checkpoint tag: $TAG"

touch .gitignore

# добавим блок (один раз)
grep -q "VESTNIK LOCAL DEBUG SCRIPTS" .gitignore || cat >> .gitignore <<'EOF'

# VESTNIK LOCAL DEBUG SCRIPTS (generated during incident work)
scripts/cli_introspect_worker.sh
scripts/diag_container_code_identity.sh
scripts/diag_lock_compare_schema_on_off.sh
scripts/find_lock_diag_artifacts.sh
scripts/rebuild_worker.sh
scripts/rebuild_worker_and_verify_code.sh
scripts/repro_lock_ddl_vs_readlock.sh
scripts/rollback_worker_main_to_head.sh
scripts/run_diag_db_lock_details_v2_direct.sh
scripts/run_worker_oneshot_noschema_and_verify.sh
scripts/diag_locks_ab.sh
scripts/apply_add_diag_locks_cli_v1.sh
scripts/db_terminate_suspicious_idle_pid_2196.sh
EOF

echo
echo "== git status =="
git status -sb || true
