BEGIN;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = GREATEST(clock_timestamp(), OLD.updated_at, NEW.created_at);
    RETURN NEW;
END;
$$;

INSERT INTO schema_migrations (version)
VALUES ('000009_monotonic_timestamps');

COMMIT;
