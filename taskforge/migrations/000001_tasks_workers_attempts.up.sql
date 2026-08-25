BEGIN;

CREATE TYPE task_status AS ENUM (
    'QUEUED',
    'LEASED',
    'RUNNING',
    'RETRYING',
    'SUCCEEDED',
    'FAILED',
    'CANCELLED'
);

CREATE FUNCTION set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE TABLE workers (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name varchar(255) NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    concurrency_limit integer NOT NULL DEFAULT 1,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    last_seen_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT workers_name_not_blank CHECK (btrim(name) <> ''),
    CONSTRAINT workers_name_unique UNIQUE (name),
    CONSTRAINT workers_concurrency_limit_positive CHECK (concurrency_limit > 0),
    CONSTRAINT workers_metadata_is_object CHECK (jsonb_typeof(metadata) = 'object'),
    CONSTRAINT workers_timestamps_ordered CHECK (
        updated_at >= created_at
        AND (last_seen_at IS NULL OR last_seen_at >= created_at)
    )
);

CREATE TABLE tasks (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    queue varchar(128) NOT NULL DEFAULT 'default',
    task_type varchar(255) NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    status task_status NOT NULL DEFAULT 'QUEUED',
    priority smallint NOT NULL DEFAULT 0,
    max_attempts smallint NOT NULL DEFAULT 3,
    attempt_count smallint NOT NULL DEFAULT 0,
    scheduled_at timestamptz NOT NULL DEFAULT now(),
    leased_by_worker_id uuid REFERENCES workers (id) ON DELETE RESTRICT,
    lease_token uuid,
    lease_expires_at timestamptz,
    completed_at timestamptz,
    result jsonb,
    last_error text,
    idempotency_key varchar(255),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT tasks_queue_not_blank CHECK (btrim(queue) <> ''),
    CONSTRAINT tasks_type_not_blank CHECK (btrim(task_type) <> ''),
    CONSTRAINT tasks_lease_token_unique UNIQUE (lease_token),
    CONSTRAINT tasks_payload_is_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT tasks_result_is_object CHECK (
        result IS NULL OR jsonb_typeof(result) = 'object'
    ),
    CONSTRAINT tasks_max_attempts_range CHECK (max_attempts BETWEEN 1 AND 100),
    CONSTRAINT tasks_attempt_count_range CHECK (
        attempt_count BETWEEN 0 AND max_attempts
    ),
    CONSTRAINT tasks_retry_attempts_remaining CHECK (
        status <> 'RETRYING' OR attempt_count < max_attempts
    ),
    CONSTRAINT tasks_lease_shape CHECK (
        (
            status IN ('LEASED', 'RUNNING')
            AND leased_by_worker_id IS NOT NULL
            AND lease_token IS NOT NULL
            AND lease_expires_at IS NOT NULL
        )
        OR
        (
            status NOT IN ('LEASED', 'RUNNING')
            AND leased_by_worker_id IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
        )
    ),
    CONSTRAINT tasks_completion_shape CHECK (
        (
            status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
            AND completed_at IS NOT NULL
        )
        OR
        (
            status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
            AND completed_at IS NULL
        )
    ),
    CONSTRAINT tasks_timestamps_ordered CHECK (
        updated_at >= created_at
        AND (lease_expires_at IS NULL OR lease_expires_at > updated_at)
        AND (completed_at IS NULL OR completed_at >= created_at)
    )
);

CREATE TABLE task_attempts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id uuid NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
    worker_id uuid NOT NULL REFERENCES workers (id) ON DELETE RESTRICT,
    attempt_number smallint NOT NULL,
    status task_status NOT NULL DEFAULT 'LEASED',
    lease_token uuid NOT NULL,
    leased_at timestamptz NOT NULL DEFAULT now(),
    lease_expires_at timestamptz NOT NULL,
    started_at timestamptz,
    finished_at timestamptz,
    output jsonb,
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT task_attempts_task_number_unique UNIQUE (task_id, attempt_number),
    CONSTRAINT task_attempts_lease_token_unique UNIQUE (lease_token),
    CONSTRAINT task_attempts_number_positive CHECK (attempt_number > 0),
    CONSTRAINT task_attempts_status_allowed CHECK (
        status IN ('LEASED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')
    ),
    CONSTRAINT task_attempts_output_is_object CHECK (
        output IS NULL OR jsonb_typeof(output) = 'object'
    ),
    CONSTRAINT task_attempts_running_started CHECK (
        status <> 'RUNNING' OR started_at IS NOT NULL
    ),
    CONSTRAINT task_attempts_completion_shape CHECK (
        (
            status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
            AND finished_at IS NOT NULL
        )
        OR
        (
            status IN ('LEASED', 'RUNNING')
            AND finished_at IS NULL
        )
    ),
    CONSTRAINT task_attempts_timestamps_ordered CHECK (
        lease_expires_at > leased_at
        AND created_at <= updated_at
        AND (started_at IS NULL OR started_at >= leased_at)
        AND (finished_at IS NULL OR finished_at >= leased_at)
    )
);

CREATE UNIQUE INDEX tasks_queue_idempotency_key_idx
    ON tasks (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX tasks_dispatch_idx
    ON tasks (queue, priority DESC, scheduled_at, created_at)
    WHERE status IN ('QUEUED', 'RETRYING');

CREATE INDEX tasks_expired_lease_idx
    ON tasks (lease_expires_at)
    WHERE status IN ('LEASED', 'RUNNING');

CREATE INDEX tasks_status_updated_idx
    ON tasks (status, updated_at DESC);

CREATE INDEX workers_available_idx
    ON workers (last_seen_at DESC)
    WHERE enabled;

CREATE INDEX task_attempts_worker_active_idx
    ON task_attempts (worker_id, lease_expires_at)
    WHERE status IN ('LEASED', 'RUNNING');

CREATE INDEX task_attempts_finished_idx
    ON task_attempts (status, finished_at DESC)
    WHERE status IN ('SUCCEEDED', 'FAILED', 'CANCELLED');

CREATE TRIGGER workers_set_updated_at
BEFORE UPDATE ON workers
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER tasks_set_updated_at
BEFORE UPDATE ON tasks
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER task_attempts_set_updated_at
BEFORE UPDATE ON task_attempts
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
