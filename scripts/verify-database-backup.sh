#!/usr/bin/env bash
set -Eeuo pipefail

if [[ $# -ne 1 ]]; then
    printf 'Usage: %s /path/to/dispatch_YYYYMMDDTHHMMSSZ.dump\n' "$0" >&2
    exit 2
fi

DUMP_PATH="$(realpath "$1")"
DUMP_DIR="$(dirname "$DUMP_PATH")"
DUMP_FILE="$(basename "$DUMP_PATH")"
IMAGE="postgres:17-alpine@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193"
CONTAINER="dispatch-restore-drill-$$"
POSTGRES_PASSWORD="dispatch-restore-drill-$$-$(date +%s)"

cleanup() {
    docker rm --force "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM

for command in docker realpath sha256sum; do
    command -v "$command" >/dev/null
done
test -s "$DUMP_PATH"
if [[ -f "${DUMP_PATH}.sha256" ]]; then
    (cd "$DUMP_DIR" && sha256sum -c "${DUMP_FILE}.sha256")
fi
docker image inspect "$IMAGE" >/dev/null 2>&1 || docker pull "$IMAGE" >/dev/null

docker run --detach \
    --name "$CONTAINER" \
    --network none \
    --env POSTGRES_DB=dispatch_restore \
    --env POSTGRES_USER=postgres \
    --env POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
    --volume "${DUMP_DIR}:/backup:ro" \
    "$IMAGE" >/dev/null

attempt=0
until docker exec "$CONTAINER" pg_isready \
    --username=postgres --dbname=dispatch_restore >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [[ "$attempt" -ge 60 ]]; then
        docker logs "$CONTAINER" >&2
        printf 'Restore-drill PostgreSQL did not become ready\n' >&2
        exit 1
    fi
    sleep 1
done

docker exec "$CONTAINER" pg_restore \
    --username=postgres \
    --dbname=dispatch_restore \
    --no-owner \
    --no-privileges \
    --exit-on-error \
    "/backup/${DUMP_FILE}"

verification="$(docker exec "$CONTAINER" psql \
    --username=postgres \
    --dbname=dispatch_restore \
    --tuples-only \
    --no-align \
    --command="SELECT
        to_regclass('public.work_orders') IS NOT NULL
        AND to_regclass('public.worker_heartbeats') IS NOT NULL
        AND EXISTS (
            SELECT 1 FROM schema_migrations
            WHERE version = '0020_operational_health.sql'
        );")"
[[ "$verification" = "t" ]]

printf 'Database restore drill passed: %s\n' "$DUMP_PATH"
