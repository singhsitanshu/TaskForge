BEGIN;

ALTER TABLE task_attempts
    ADD COLUMN queue_entered_at timestamptz,
    ADD COLUMN scheduled_at_snapshot timestamptz,
    ADD COLUMN retry_scheduled_at timestamptz,
    ADD COLUMN recovered_lease_expires_at timestamptz,
    ADD COLUMN recovered_at timestamptz,
    ADD COLUMN recovery_action varchar(16),
    ADD CONSTRAINT task_attempts_queue_timing_ordered CHECK (
        queue_entered_at IS NULL
        OR started_at IS NULL
        OR started_at >= queue_entered_at
    ),
    ADD CONSTRAINT task_attempts_recovery_evidence_shape CHECK (
        (
            recovered_at IS NULL
            AND recovered_lease_expires_at IS NULL
            AND recovery_action IS NULL
        )
        OR
        (
            status = 'ABANDONED'
            AND recovered_at IS NOT NULL
            AND recovered_lease_expires_at IS NOT NULL
            AND recovered_at >= recovered_lease_expires_at
            AND recovery_action IN ('requeued', 'failed')
        )
    );

COMMENT ON COLUMN task_attempts.queue_entered_at IS
    'Immutable queue-entry timestamp copied from tasks.queued_at by the atomic claim transaction. NULL identifies legacy attempts whose historical value is unknowable.';
COMMENT ON COLUMN task_attempts.scheduled_at_snapshot IS
    'Immutable tasks.scheduled_at value observed by the claim that created this attempt.';
COMMENT ON COLUMN task_attempts.retry_scheduled_at IS
    'Due timestamp assigned when this attempt durably scheduled its successor retry.';
COMMENT ON COLUMN task_attempts.recovered_at IS
    'Timestamp at which this abandoned attempt was durably recovered.';

INSERT INTO schema_migrations (version)
VALUES ('000010_attempt_timing_evidence');

COMMIT;
