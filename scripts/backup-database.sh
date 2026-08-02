#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKUP_DIR="${1:-${ROOT_DIR}/backups/database}"

umask 077
mkdir -p "$BACKUP_DIR"
BACKUP_DIR="$(realpath "$BACKUP_DIR")"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
FINAL_NAME="dispatch_${TIMESTAMP}.dump"
FINAL_PATH="${BACKUP_DIR}/${FINAL_NAME}"
TEMP_PATH="${BACKUP_DIR}/.${FINAL_NAME}.$$.tmp"
chmod 700 "$BACKUP_DIR"
exec 9>"${BACKUP_DIR}/.backup.lock"
if ! flock -n 9; then
    printf 'Another database backup is already running\n' >&2
    exit 75
fi

cleanup() {
    rm -f "$TEMP_PATH"
}
trap cleanup EXIT HUP INT TERM

for command in docker flock pg_restore realpath sha256sum; do
    command -v "$command" >/dev/null
done
test ! -e "$FINAL_PATH"

cd "$ROOT_DIR"
docker compose config --quiet
docker compose exec -T postgres pg_dump \
    --username=dispatch \
    --dbname=dispatch \
    --format=custom \
    --compress=6 \
    --no-owner \
    --no-privileges \
    > "$TEMP_PATH"
test -s "$TEMP_PATH"
pg_restore --list "$TEMP_PATH" >/dev/null
chmod 600 "$TEMP_PATH"
mv "$TEMP_PATH" "$FINAL_PATH"
(cd "$BACKUP_DIR" && sha256sum "$FINAL_NAME" > "${FINAL_NAME}.sha256")
chmod 600 "${FINAL_PATH}.sha256"

printf 'Database backup created: %s\n' "$FINAL_PATH"
printf 'Run scripts/verify-database-backup.sh %q before off-host upload.\n' \
    "$FINAL_PATH"
