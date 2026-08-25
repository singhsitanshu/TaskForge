#!/bin/sh

set -eu

direction=${1:-up}
case "$direction" in
    up|down) ;;
    *)
        echo "usage: $0 [up|down]" >&2
        exit 2
        ;;
esac

script_directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_directory=$(dirname -- "$script_directory")
migrations_directory="$repository_directory/migrations"
database_name=${POSTGRES_DB:-taskforge}
database_user=${POSTGRES_USER:-taskforge}

cd "$repository_directory"

docker compose up -d postgres

readiness_attempt=0
until docker compose exec -T postgres pg_isready \
    --username "$database_user" \
    --dbname "$database_name" >/dev/null 2>&1; do
    readiness_attempt=$((readiness_attempt + 1))
    if [ "$readiness_attempt" -ge 60 ]; then
        echo "PostgreSQL did not become ready within 60 seconds" >&2
        exit 1
    fi
    sleep 1
done

if [ "$direction" = "up" ]; then
    migration_files=$(find "$migrations_directory" -name '*.up.sql' -type f | LC_ALL=C sort)
else
    migration_files=$(find "$migrations_directory" -name '*.down.sql' -type f | LC_ALL=C sort -r)
fi

emit_migration_program() {
    printf '%s\n' '\set ON_ERROR_STOP on'
    printf '%s\n' 'SELECT pg_advisory_lock(607235154548546);'
    printf '%s\n' 'CREATE TABLE IF NOT EXISTS schema_migrations ('
    printf '%s\n' '    version varchar(255) PRIMARY KEY,'
    printf '%s\n' '    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),'
    printf '%s\n' "    CONSTRAINT schema_migrations_version_not_blank CHECK (btrim(version) <> '')"
    printf '%s\n' ');'
    cat <<'SQL'
DO $bootstrap$
DECLARE
    existing_core_objects integer;
BEGIN
    SELECT count(*)
    INTO existing_core_objects
    FROM (
        VALUES
            (to_regclass(current_schema() || '.tasks')),
            (to_regclass(current_schema() || '.workers')),
            (to_regclass(current_schema() || '.task_attempts'))
    ) AS core_objects(object_name)
    WHERE object_name IS NOT NULL;

    IF existing_core_objects > 0 AND existing_core_objects < 3 THEN
        RAISE EXCEPTION
            'cannot baseline a partial pre-versioned TaskForge schema (% of 3 core tables exist)',
            existing_core_objects;
    END IF;

    IF existing_core_objects = 3
       AND NOT EXISTS (
           SELECT 1 FROM schema_migrations
           WHERE version = '000001_tasks_workers_attempts'
       ) THEN
        IF to_regtype(current_schema() || '.task_status') IS NULL
           OR to_regprocedure(current_schema() || '.set_updated_at()') IS NULL THEN
            RAISE EXCEPTION
                'cannot baseline pre-versioned TaskForge schema: foundational objects are missing';
        END IF;

        INSERT INTO schema_migrations (version)
        VALUES ('000001_tasks_workers_attempts');
    END IF;

    IF EXISTS (
           SELECT 1 FROM schema_migrations
           WHERE version = '000001_tasks_workers_attempts'
       )
       AND NOT EXISTS (
           SELECT 1 FROM schema_migrations
           WHERE version = '000002_first_worker_claim'
       )
       AND EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema = current_schema()
             AND table_name = 'tasks'
             AND column_name = 'claimed_by_worker_id'
       ) THEN
        INSERT INTO schema_migrations (version)
        VALUES ('000002_first_worker_claim');
    END IF;
END
$bootstrap$;
SQL

    for migration_file in $migration_files; do
        migration_name=$(basename -- "$migration_file" ".$direction.sql")
        printf "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = '%s') AS migration_applied \\gset\n" "$migration_name"
        if [ "$direction" = "up" ]; then
            printf '%s\n' '\if :migration_applied'
            printf '\\echo migration %s already applied\n' "$migration_name"
            printf '%s\n' '\else'
            printf '\\echo applying migration %s\n' "$migration_name"
            sed -n '1,$p' "$migration_file"
            printf '%s\n' '\endif'
        else
            printf '%s\n' '\if :migration_applied'
            printf '\\echo rolling back migration %s\n' "$migration_name"
            sed -n '1,$p' "$migration_file"
            printf '%s\n' '\else'
            printf '\\echo migration %s is not applied\n' "$migration_name"
            printf '%s\n' '\endif'
        fi
    done

    printf '%s\n' 'SELECT pg_advisory_unlock(607235154548546);'
}

emit_migration_program | docker compose exec -T postgres psql \
    --set ON_ERROR_STOP=1 \
    --username "$database_user" \
    --dbname "$database_name"
