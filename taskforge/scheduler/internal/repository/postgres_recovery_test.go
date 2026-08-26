package repository_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"taskforge/scheduler/internal/domain"
	"taskforge/scheduler/internal/repository"
)

const (
	recoveryScanners   = 20
	recoveryIterations = 50
)

type testDatabase struct {
	admin  *pgxpool.Pool
	pool   *pgxpool.Pool
	store  *repository.Postgres
	schema string
}

type recoveryResult struct {
	batch domain.RecoveryBatch
	err   error
}

type runningTask struct {
	id             string
	workerID       string
	attemptNumber  int16
	leaseExpiresAt time.Time
}

func TestSingleExpiredTaskRecovery(t *testing.T) {
	database := newTestDatabase(t)
	task := insertRunningTask(t, database, 1, 3, -time.Second, "")
	var queuedBefore time.Time
	if err := database.pool.QueryRow(
		context.Background(),
		"SELECT queued_at FROM tasks WHERE id = $1::uuid",
		task.id,
	).Scan(&queuedBefore); err != nil {
		t.Fatalf("read pre-recovery queued_at: %v", err)
	}
	time.Sleep(time.Millisecond)

	batch, err := database.store.RecoverExpired(context.Background(), 100)
	if err != nil {
		t.Fatalf("recover expired task: %v", err)
	}
	if len(batch.Recovered) != 1 || len(batch.Violations) != 0 {
		t.Fatalf("unexpected recovery batch: %+v", batch)
	}
	if batch.Recovered[0].Action != domain.RecoveryRequeued {
		t.Fatalf("unexpected action: %+v", batch.Recovered[0])
	}
	if batch.Recovered[0].RecoveryLag < time.Second || batch.Recovered[0].RecoveryLag > 5*time.Second {
		t.Fatalf("unexpected database recovery lag: %s", batch.Recovered[0].RecoveryLag)
	}

	assertTaskState(t, database, task.id, "QUEUED", 1, nil, nil, "lease_expired")
	assertAttemptState(t, database, task.id, 1, task.workerID, "ABANDONED", "lease_expired")
	var queuedAfter time.Time
	if err := database.pool.QueryRow(
		context.Background(),
		"SELECT queued_at FROM tasks WHERE id = $1::uuid",
		task.id,
	).Scan(&queuedAfter); err != nil {
		t.Fatalf("read recovered queued_at: %v", err)
	}
	if !queuedAfter.After(queuedBefore) {
		t.Fatalf("queued_at was not reset: before=%s after=%s", queuedBefore, queuedAfter)
	}

	second, err := database.store.RecoverExpired(context.Background(), 100)
	if err != nil || len(second.Recovered) != 0 || len(second.Violations) != 0 {
		t.Fatalf("task recovered more than once: batch=%+v error=%v", second, err)
	}
}

func TestRecoveryUsesExtendedQueryProtocol(t *testing.T) {
	database := newTestDatabase(t)
	task := insertRunningTask(t, database, 1, 1, -time.Second, "extended-protocol")
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	configuration, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse extended protocol pool: %v", err)
	}
	configuration.ConnConfig.RuntimeParams["search_path"] = database.schema
	extendedPool, err := pgxpool.NewWithConfig(context.Background(), configuration)
	if err != nil {
		t.Fatalf("create extended protocol pool: %v", err)
	}
	defer extendedPool.Close()

	batch, err := repository.NewPostgres(extendedPool).RecoverExpired(context.Background(), 1)
	if err != nil {
		t.Fatalf("extended-protocol recovery: %v", err)
	}
	if len(batch.Recovered) != 1 || batch.Recovered[0].TaskID != task.id ||
		batch.Recovered[0].Action != domain.RecoveryFailed {
		t.Fatalf("unexpected extended-protocol batch: %+v", batch)
	}
}

func TestSingleTaskRecoveryContention(t *testing.T) {
	database := newTestDatabase(t)
	ctx := context.Background()
	totalWinners := 0
	for iteration := 1; iteration <= recoveryIterations; iteration++ {
		task := insertRunningTask(t, database, 1, 3, -time.Second, "")
		results := recoverSimultaneously(ctx, database.store, recoveryScanners, 1)
		winners := 0
		for _, result := range results {
			if result.err != nil {
				t.Fatalf("iteration %d recovery: %v", iteration, result.err)
			}
			winners += len(result.batch.Recovered)
			if len(result.batch.Violations) != 0 {
				t.Fatalf("iteration %d violations: %+v", iteration, result.batch.Violations)
			}
		}
		if winners != 1 {
			t.Fatalf("iteration %d winners=%d", iteration, winners)
		}
		totalWinners += winners
		assertTaskState(t, database, task.id, "QUEUED", 1, nil, nil, "lease_expired")
		assertAttemptState(t, database, task.id, 1, task.workerID, "ABANDONED", "lease_expired")
		if _, err := database.pool.Exec(ctx, "DELETE FROM tasks WHERE id = $1::uuid", task.id); err != nil {
			t.Fatalf("iteration %d cleanup: %v", iteration, err)
		}
	}
	t.Logf(
		"RECOVERY_CONTENTION scanners=%d iterations=%d winners=%d duplicate_recoveries=0",
		recoveryScanners,
		recoveryIterations,
		totalWinners,
	)
}

func TestBatchRecoveryWithManyScanners(t *testing.T) {
	database := newTestDatabase(t)
	workerID := registerWorker(t, database, "batch")
	insertRunningTasks(t, database, workerID, 500, -time.Second, "batch-expired")

	results := recoverSimultaneously(context.Background(), database.store, 10, 100)
	totalRecovered := 0
	scannersWithWork := 0
	for _, result := range results {
		if result.err != nil {
			t.Fatalf("batch recovery: %v", result.err)
		}
		if len(result.batch.Violations) != 0 {
			t.Fatalf("batch violations: %+v", result.batch.Violations)
		}
		totalRecovered += len(result.batch.Recovered)
		if len(result.batch.Recovered) > 0 {
			scannersWithWork++
		}
	}
	if totalRecovered != 500 {
		t.Fatalf("recovered=%d, expected 500", totalRecovered)
	}

	var queued, abandoned, duplicateAttempts, badCounts, wrongOwners int
	err := database.pool.QueryRow(context.Background(), `
		SELECT
			count(*) FILTER (WHERE task.status = 'QUEUED'),
			(SELECT count(*) FROM task_attempts WHERE status = 'ABANDONED'),
			(SELECT count(*) FROM (
				SELECT task_id, attempt_number
				FROM task_attempts
				GROUP BY task_id, attempt_number
				HAVING count(*) > 1
			) AS duplicates),
			count(*) FILTER (WHERE task.attempt_count <> 1),
			count(*) FILTER (
				WHERE task.claimed_by_worker_id IS NOT NULL OR task.lease_expires_at IS NOT NULL
			)
		FROM tasks AS task
	`).Scan(&queued, &abandoned, &duplicateAttempts, &badCounts, &wrongOwners)
	if err != nil {
		t.Fatalf("read batch metrics: %v", err)
	}
	if queued != 500 || abandoned != 500 || duplicateAttempts != 0 || badCounts != 0 || wrongOwners != 0 {
		t.Fatalf(
			"queued=%d abandoned=%d duplicates=%d bad_counts=%d wrong_owners=%d",
			queued,
			abandoned,
			duplicateAttempts,
			badCounts,
			wrongOwners,
		)
	}
	t.Logf(
		"RECOVERY_BATCH expired=500 scanners=10 scanners_with_work=%d recovered=500 duplicates=0 errors=0",
		scannersWithWork,
	)
}

func TestNonExpiredTasksAreUntouched(t *testing.T) {
	database := newTestDatabase(t)
	workerID := registerWorker(t, database, "mixed")
	insertRunningTasks(t, database, workerID, 100, -time.Second, "expired")
	insertRunningTasks(t, database, workerID, 100, time.Minute, "valid")

	batch, err := database.store.RecoverExpired(context.Background(), 500)
	if err != nil {
		t.Fatalf("recover mixed tasks: %v", err)
	}
	if len(batch.Recovered) != 100 || len(batch.Violations) != 0 {
		t.Fatalf("unexpected mixed batch: recovered=%d violations=%d", len(batch.Recovered), len(batch.Violations))
	}

	var validRunning, validOwner, validLease, validAttempts int
	err = database.pool.QueryRow(context.Background(), `
		SELECT
			count(*) FILTER (WHERE status = 'RUNNING'),
			count(*) FILTER (WHERE claimed_by_worker_id = $1::uuid),
			count(*) FILTER (WHERE lease_expires_at > clock_timestamp()),
			(SELECT count(*) FROM task_attempts AS attempt
			 JOIN tasks AS task ON task.id = attempt.task_id
			 WHERE task.task_type = 'valid' AND attempt.status = 'RUNNING')
		FROM tasks
		WHERE task_type = 'valid'
	`, workerID).Scan(&validRunning, &validOwner, &validLease, &validAttempts)
	if err != nil {
		t.Fatalf("read valid tasks: %v", err)
	}
	if validRunning != 100 || validOwner != 100 || validLease != 100 || validAttempts != 100 {
		t.Fatalf(
			"valid tasks changed running=%d owner=%d lease=%d attempts=%d",
			validRunning,
			validOwner,
			validLease,
			validAttempts,
		)
	}
}

func TestBoundaryAndWorkerLivenessIndependence(t *testing.T) {
	database := newTestDatabase(t)
	activeExpired := insertRunningTask(t, database, 1, 3, 0, "active-expired")
	deadValid := insertRunningTask(t, database, 1, 3, time.Minute, "dead-valid")
	_, err := database.pool.Exec(context.Background(), `
		UPDATE workers
		SET
			created_at = CASE
				WHEN id = $2::uuid THEN clock_timestamp() - interval '2 days'
				ELSE created_at
			END,
			last_seen_at = CASE
				WHEN id = $1::uuid THEN clock_timestamp()
				ELSE clock_timestamp() - interval '1 day'
			END
		WHERE id IN ($1::uuid, $2::uuid)
	`, activeExpired.workerID, deadValid.workerID)
	if err != nil {
		t.Fatalf("set worker liveness fixtures: %v", err)
	}

	batch, err := database.store.RecoverExpired(context.Background(), 100)
	if err != nil {
		t.Fatalf("recover by lease boundary: %v", err)
	}
	if len(batch.Recovered) != 1 || batch.Recovered[0].TaskID != activeExpired.id {
		t.Fatalf("lease authority mismatch: %+v", batch)
	}
	assertTaskState(t, database, activeExpired.id, "QUEUED", 1, nil, nil, "lease_expired")
	assertTaskState(t, database, deadValid.id, "RUNNING", 1, &deadValid.workerID, &deadValid.leaseExpiresAt, "")

	if _, err := database.pool.Exec(
		context.Background(),
		"UPDATE tasks SET lease_expires_at = clock_timestamp() WHERE id = $1::uuid",
		deadValid.id,
	); err != nil {
		t.Fatalf("expire dead worker lease: %v", err)
	}
	second, err := database.store.RecoverExpired(context.Background(), 100)
	if err != nil || len(second.Recovered) != 1 || second.Recovered[0].TaskID != deadValid.id {
		t.Fatalf("dead worker task not recovered after expiration: batch=%+v error=%v", second, err)
	}
}

func TestMaxAttemptExhaustion(t *testing.T) {
	database := newTestDatabase(t)
	for attemptNumber := int16(1); attemptNumber <= 3; attemptNumber++ {
		t.Run(fmt.Sprintf("attempt_%d", attemptNumber), func(t *testing.T) {
			task := insertRunningTask(t, database, attemptNumber, 3, -time.Second, "")
			batch, err := database.store.RecoverExpired(context.Background(), 1)
			if err != nil || len(batch.Recovered) != 1 {
				t.Fatalf("recover attempt %d: batch=%+v error=%v", attemptNumber, batch, err)
			}
			expectedStatus := "QUEUED"
			expectedError := "lease_expired"
			expectedAction := domain.RecoveryRequeued
			if attemptNumber == 3 {
				expectedStatus = "FAILED"
				expectedError = domain.MaxAttemptsExpiredError
				expectedAction = domain.RecoveryFailed
			}
			if batch.Recovered[0].Action != expectedAction {
				t.Fatalf("attempt %d action=%s", attemptNumber, batch.Recovered[0].Action)
			}
			assertTaskState(t, database, task.id, expectedStatus, attemptNumber, nil, nil, expectedError)
			assertAttemptState(t, database, task.id, attemptNumber, task.workerID, "ABANDONED", "lease_expired")

			var claimable bool
			if err := database.pool.QueryRow(context.Background(), `
				SELECT EXISTS (
					SELECT 1 FROM tasks
					WHERE id = $1::uuid
					  AND status = 'QUEUED'
					  AND attempt_count < max_attempts
				)
			`, task.id).Scan(&claimable); err != nil {
				t.Fatalf("check claim eligibility: %v", err)
			}
			if claimable != (attemptNumber < 3) {
				t.Fatalf("attempt %d claimable=%t", attemptNumber, claimable)
			}
		})
	}
}

func TestRecoveryTransactionRollsBack(t *testing.T) {
	database := newTestDatabase(t)
	task := insertRunningTask(t, database, 1, 3, -time.Second, "")
	_, err := database.pool.Exec(context.Background(), `
		CREATE FUNCTION reject_recovery_task_transition()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			IF OLD.status = 'RUNNING' AND NEW.status <> 'RUNNING' THEN
				RAISE EXCEPTION 'TF-008 injected task transition failure';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER reject_recovery_task_transition
		BEFORE UPDATE ON tasks
		FOR EACH ROW EXECUTE FUNCTION reject_recovery_task_transition();
	`)
	if err != nil {
		t.Fatalf("install recovery failure trigger: %v", err)
	}

	_, recoveryErr := database.store.RecoverExpired(context.Background(), 1)
	if recoveryErr == nil || !strings.Contains(recoveryErr.Error(), "injected task transition failure") {
		t.Fatalf("expected injected failure, got %v", recoveryErr)
	}
	assertTaskState(t, database, task.id, "RUNNING", 1, &task.workerID, &task.leaseExpiresAt, "")
	assertAttemptState(t, database, task.id, 1, task.workerID, "RUNNING", "")

	if _, err := database.pool.Exec(context.Background(), `
		DROP TRIGGER reject_recovery_task_transition ON tasks;
		DROP FUNCTION reject_recovery_task_transition();
	`); err != nil {
		t.Fatalf("remove recovery failure trigger: %v", err)
	}
	batch, err := database.store.RecoverExpired(context.Background(), 1)
	if err != nil || len(batch.Recovered) != 1 {
		t.Fatalf("recover after rollback: batch=%+v error=%v", batch, err)
	}
}

func TestInvariantCorruptionIsSurfacedAndUntouched(t *testing.T) {
	database := newTestDatabase(t)
	missing := insertRunningTask(t, database, 1, 3, -time.Second, "missing")
	if _, err := database.pool.Exec(
		context.Background(),
		"DELETE FROM task_attempts WHERE task_id = $1::uuid",
		missing.id,
	); err != nil {
		t.Fatalf("remove active attempt: %v", err)
	}
	mismatch := insertRunningTask(t, database, 1, 3, -time.Second, "mismatch")
	wrongWorker := registerWorker(t, database, "wrong-attempt-owner")
	if _, err := database.pool.Exec(
		context.Background(),
		"UPDATE task_attempts SET worker_id = $2::uuid WHERE task_id = $1::uuid",
		mismatch.id,
		wrongWorker,
	); err != nil {
		t.Fatalf("mismatch active attempt owner: %v", err)
	}

	batch, err := database.store.RecoverExpired(context.Background(), 10)
	if err != nil {
		t.Fatalf("scan corrupt tasks: %v", err)
	}
	if len(batch.Recovered) != 0 || len(batch.Violations) != 2 {
		t.Fatalf("corrupt tasks were not surfaced: %+v", batch)
	}
	reasons := map[string]bool{}
	for _, violation := range batch.Violations {
		reasons[violation.Reason] = true
	}
	if !reasons["active_attempt_missing"] || !reasons["active_attempt_worker_mismatch"] {
		t.Fatalf("unexpected violation reasons: %+v", batch.Violations)
	}
	assertTaskState(t, database, missing.id, "RUNNING", 1, &missing.workerID, &missing.leaseExpiresAt, "")
	assertTaskState(t, database, mismatch.id, "RUNNING", 1, &mismatch.workerID, &mismatch.leaseExpiresAt, "")
	assertAttemptState(t, database, mismatch.id, 1, wrongWorker, "RUNNING", "")
}

func TestRecoveryRejectsOldWorkerMutations(t *testing.T) {
	database := newTestDatabase(t)
	task := insertRunningTask(t, database, 1, 3, -time.Second, "")
	batch, err := database.store.RecoverExpired(context.Background(), 1)
	if err != nil || len(batch.Recovered) != 1 {
		t.Fatalf("recover task: batch=%+v error=%v", batch, err)
	}

	queries := []string{
		`UPDATE tasks SET lease_expires_at = clock_timestamp() + interval '1 minute' WHERE id = $1::uuid AND status = 'RUNNING' AND claimed_by_worker_id = $2::uuid AND attempt_count = 1 AND lease_expires_at > clock_timestamp()`,
		`UPDATE tasks SET status = 'SUCCEEDED', completed_at = clock_timestamp(), claimed_by_worker_id = NULL, lease_expires_at = NULL WHERE id = $1::uuid AND status = 'RUNNING' AND claimed_by_worker_id = $2::uuid AND attempt_count = 1 AND lease_expires_at > clock_timestamp()`,
		`UPDATE tasks SET status = 'FAILED', completed_at = clock_timestamp(), claimed_by_worker_id = NULL, lease_expires_at = NULL WHERE id = $1::uuid AND status = 'RUNNING' AND claimed_by_worker_id = $2::uuid AND attempt_count = 1 AND lease_expires_at > clock_timestamp()`,
	}
	for index, query := range queries {
		commandTag, err := database.pool.Exec(context.Background(), query, task.id, task.workerID)
		if err != nil {
			t.Fatalf("old worker mutation %d: %v", index, err)
		}
		if commandTag.RowsAffected() != 0 {
			t.Fatalf("old worker mutation %d affected %d rows", index, commandTag.RowsAffected())
		}
	}
}

func TestSchedulerReplicaSafety(t *testing.T) {
	database := newTestDatabase(t)
	workerID := registerWorker(t, database, "replicas")
	insertRunningTasks(t, database, workerID, 300, -time.Second, "replica-expired")
	results := recoverSimultaneously(context.Background(), database.store, 3, 200)
	recovered := 0
	for _, result := range results {
		if result.err != nil {
			t.Fatalf("scheduler replica recovery: %v", result.err)
		}
		recovered += len(result.batch.Recovered)
	}
	if recovered != 300 {
		t.Fatalf("scheduler replicas recovered=%d", recovered)
	}
	var abandoned int
	if err := database.pool.QueryRow(
		context.Background(),
		"SELECT count(*) FROM task_attempts WHERE status = 'ABANDONED'",
	).Scan(&abandoned); err != nil {
		t.Fatalf("count replica recoveries: %v", err)
	}
	if abandoned != 300 {
		t.Fatalf("scheduler replicas produced %d abandoned attempts", abandoned)
	}
	t.Log("SCHEDULER_REPLICAS replicas=3 expired=300 recovered=300 duplicate_recoveries=0")
}

func TestExpiredScanUsesRunningLeaseIndex(t *testing.T) {
	database := newTestDatabase(t)
	workerID := registerWorker(t, database, "query-plan")
	_, err := database.pool.Exec(context.Background(), `
		INSERT INTO tasks (
			task_type, status, claimed_by_worker_id, attempt_count, lease_expires_at
		)
		SELECT
			'test.echo', 'RUNNING', $1::uuid, 1,
			clock_timestamp() - interval '1 second'
		FROM generate_series(1, 20000)
	`, workerID)
	if err != nil {
		t.Fatalf("insert query-plan tasks: %v", err)
	}
	if _, err := database.pool.Exec(context.Background(), "ANALYZE tasks"); err != nil {
		t.Fatalf("analyze tasks: %v", err)
	}
	rows, err := database.pool.Query(context.Background(), `
		EXPLAIN (ANALYZE, BUFFERS)
		SELECT task.id
		FROM tasks AS task
		WHERE task.status = 'RUNNING'
		  AND task.lease_expires_at <= clock_timestamp()
		ORDER BY task.lease_expires_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 100
	`)
	if err != nil {
		t.Fatalf("explain recovery scan: %v", err)
	}
	defer rows.Close()
	lines := make([]string, 0)
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan query plan: %v", err)
		}
		lines = append(lines, line)
	}
	plan := strings.Join(lines, "\n")
	if !strings.Contains(plan, "tasks_running_lease_idx") {
		t.Fatalf("recovery scan did not use lease index:\n%s", plan)
	}
	t.Logf("RECOVERY_QUERY_PLAN\n%s", plan)
}

func newTestDatabase(t *testing.T) *testDatabase {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL recovery tests")
	}
	ctx := context.Background()
	adminConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse admin database URL: %v", err)
	}
	admin, err := pgxpool.NewWithConfig(ctx, adminConfig)
	if err != nil {
		t.Fatalf("create admin pool: %v", err)
	}
	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		admin.Close()
		t.Fatalf("generate schema name: %v", err)
	}
	schema := "tf008_" + hex.EncodeToString(randomBytes)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}

	database := &testDatabase{admin: admin, schema: schema}
	t.Cleanup(func() {
		if database.pool != nil {
			database.pool.Close()
		}
		_, _ = database.admin.Exec(
			context.Background(),
			"DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE",
		)
		database.admin.Close()
	})

	testConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	testConfig.MaxConns = 32
	testConfig.ConnConfig.RuntimeParams["search_path"] = schema
	testConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	database.pool, err = pgxpool.NewWithConfig(ctx, testConfig)
	if err != nil {
		t.Fatalf("create test pool: %v", err)
	}
	applyMigrations(t, database.pool)
	warmConnections(t, database.pool, recoveryScanners)
	database.store = repository.NewPostgres(database.pool)
	return database
}

func applyMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve recovery test source path")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "../../.."))
	files, err := filepath.Glob(filepath.Join(repositoryRoot, "migrations", "*.up.sql"))
	if err != nil {
		t.Fatalf("find migrations: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("no migrations found under %s", repositoryRoot)
	}
	sort.Strings(files)
	for _, file := range files {
		contents, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read migration %s: %v", file, err)
		}
		if _, err := pool.Exec(context.Background(), string(contents)); err != nil {
			t.Fatalf("apply migration %s: %v", file, err)
		}
	}
}

func warmConnections(t *testing.T, pool *pgxpool.Pool, count int) {
	t.Helper()
	connections := make([]*pgxpool.Conn, 0, count)
	for index := 0; index < count; index++ {
		connection, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatalf("warm test connection %d: %v", index, err)
		}
		connections = append(connections, connection)
	}
	for _, connection := range connections {
		connection.Release()
	}
}

func registerWorker(t *testing.T, database *testDatabase, label string) string {
	t.Helper()
	var workerID string
	err := database.pool.QueryRow(context.Background(), `
		INSERT INTO workers (instance_id, name, last_seen_at)
		VALUES ($1, $2, clock_timestamp())
		RETURNING id::text
	`, database.schema+"-"+label+"-"+randomSuffix(t), label).Scan(&workerID)
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}
	return workerID
}

func insertRunningTask(
	t *testing.T,
	database *testDatabase,
	attemptNumber int16,
	maxAttempts int16,
	leaseOffset time.Duration,
	label string,
) runningTask {
	t.Helper()
	workerID := registerWorker(t, database, "owner")
	if label == "" {
		label = "test.echo"
	}
	var task runningTask
	err := database.pool.QueryRow(context.Background(), `
		INSERT INTO tasks (
			task_type,
			status,
			claimed_by_worker_id,
			attempt_count,
			max_attempts,
			lease_expires_at
		)
		VALUES (
			$1, 'RUNNING', $2::uuid, $3, $4,
			clock_timestamp() + $5 * interval '1 microsecond'
		)
		RETURNING id::text, claimed_by_worker_id::text, attempt_count, lease_expires_at
	`, label, workerID, attemptNumber, maxAttempts, leaseOffset.Microseconds()).Scan(
		&task.id,
		&task.workerID,
		&task.attemptNumber,
		&task.leaseExpiresAt,
	)
	if err != nil {
		t.Fatalf("insert running task: %v", err)
	}
	for number := int16(1); number < attemptNumber; number++ {
		if _, err := database.pool.Exec(context.Background(), `
			INSERT INTO task_attempts (
				task_id, worker_id, attempt_number, status, started_at, finished_at, error
			)
			VALUES (
				$1::uuid, $2::uuid, $3, 'ABANDONED',
				clock_timestamp(), clock_timestamp(), 'lease_expired'
			)
		`, task.id, workerID, number); err != nil {
			t.Fatalf("insert previous attempt %d: %v", number, err)
		}
	}
	if _, err := database.pool.Exec(context.Background(), `
		INSERT INTO task_attempts (
			task_id, worker_id, attempt_number, status, started_at
		)
		VALUES ($1::uuid, $2::uuid, $3, 'RUNNING', clock_timestamp())
	`, task.id, workerID, attemptNumber); err != nil {
		t.Fatalf("insert active attempt: %v", err)
	}
	return task
}

func insertRunningTasks(
	t *testing.T,
	database *testDatabase,
	workerID string,
	count int,
	leaseOffset time.Duration,
	taskType string,
) {
	t.Helper()
	_, err := database.pool.Exec(context.Background(), `
		INSERT INTO tasks (
			task_type, status, claimed_by_worker_id, attempt_count,
			max_attempts, lease_expires_at
		)
		SELECT
			$1, 'RUNNING', $2::uuid, 1, 3,
			clock_timestamp() + $3 * interval '1 microsecond'
		FROM generate_series(1, $4)
	`, taskType, workerID, leaseOffset.Microseconds(), count)
	if err != nil {
		t.Fatalf("insert %d running tasks: %v", count, err)
	}
	_, err = database.pool.Exec(context.Background(), `
		INSERT INTO task_attempts (
			task_id, worker_id, attempt_number, status, started_at
		)
		SELECT id, claimed_by_worker_id, 1, 'RUNNING', clock_timestamp()
		FROM tasks
		WHERE task_type = $1
	`, taskType)
	if err != nil {
		t.Fatalf("insert %d running attempts: %v", count, err)
	}
}

func recoverSimultaneously(
	ctx context.Context,
	store *repository.Postgres,
	scanners int,
	batchSize int,
) []recoveryResult {
	ready := make(chan struct{}, scanners)
	start := make(chan struct{})
	results := make(chan recoveryResult, scanners)
	for range scanners {
		go func() {
			ready <- struct{}{}
			<-start
			batch, err := store.RecoverExpired(ctx, batchSize)
			results <- recoveryResult{batch: batch, err: err}
		}()
	}
	for range scanners {
		<-ready
	}
	close(start)
	recovered := make([]recoveryResult, 0, scanners)
	for range scanners {
		recovered = append(recovered, <-results)
	}
	return recovered
}

func assertTaskState(
	t *testing.T,
	database *testDatabase,
	taskID string,
	status string,
	attemptNumber int16,
	workerID *string,
	leaseExpiresAt *time.Time,
	lastError string,
) {
	t.Helper()
	var actualStatus string
	var actualAttemptNumber int16
	var actualWorkerID *string
	var actualLeaseExpiresAt *time.Time
	var actualLastError *string
	var completedAt *time.Time
	err := database.pool.QueryRow(context.Background(), `
		SELECT
			status::text,
			attempt_count,
			claimed_by_worker_id::text,
			lease_expires_at,
			last_error,
			completed_at
		FROM tasks
		WHERE id = $1::uuid
	`, taskID).Scan(
		&actualStatus,
		&actualAttemptNumber,
		&actualWorkerID,
		&actualLeaseExpiresAt,
		&actualLastError,
		&completedAt,
	)
	if err != nil {
		t.Fatalf("read task %s: %v", taskID, err)
	}
	if actualStatus != status || actualAttemptNumber != attemptNumber || !equalStrings(actualWorkerID, workerID) {
		t.Fatalf(
			"task %s status=%s attempt=%d worker=%v, expected status=%s attempt=%d worker=%v",
			taskID,
			actualStatus,
			actualAttemptNumber,
			actualWorkerID,
			status,
			attemptNumber,
			workerID,
		)
	}
	if leaseExpiresAt == nil && actualLeaseExpiresAt != nil {
		t.Fatalf("task %s retained lease %v", taskID, actualLeaseExpiresAt)
	}
	if leaseExpiresAt != nil && (actualLeaseExpiresAt == nil || !actualLeaseExpiresAt.Equal(*leaseExpiresAt)) {
		t.Fatalf("task %s lease=%v, expected %v", taskID, actualLeaseExpiresAt, leaseExpiresAt)
	}
	if lastError == "" {
		if actualLastError != nil {
			t.Fatalf("task %s last_error=%v", taskID, actualLastError)
		}
	} else if actualLastError == nil || *actualLastError != lastError {
		t.Fatalf("task %s last_error=%v, expected %s", taskID, actualLastError, lastError)
	}
	if status == "FAILED" && completedAt == nil {
		t.Fatalf("failed task %s has no completion timestamp", taskID)
	}
}

func assertAttemptState(
	t *testing.T,
	database *testDatabase,
	taskID string,
	attemptNumber int16,
	workerID string,
	status string,
	errorText string,
) {
	t.Helper()
	var actualWorkerID, actualStatus string
	var actualError *string
	var finishedAt *time.Time
	err := database.pool.QueryRow(context.Background(), `
		SELECT worker_id::text, status::text, error, finished_at
		FROM task_attempts
		WHERE task_id = $1::uuid AND attempt_number = $2
	`, taskID, attemptNumber).Scan(&actualWorkerID, &actualStatus, &actualError, &finishedAt)
	if err != nil {
		t.Fatalf("read attempt %s/%d: %v", taskID, attemptNumber, err)
	}
	if actualWorkerID != workerID || actualStatus != status {
		t.Fatalf(
			"attempt %s/%d worker=%s status=%s, expected worker=%s status=%s",
			taskID,
			attemptNumber,
			actualWorkerID,
			actualStatus,
			workerID,
			status,
		)
	}
	if errorText == "" {
		if actualError != nil {
			t.Fatalf("attempt %s/%d error=%v", taskID, attemptNumber, actualError)
		}
	} else if actualError == nil || *actualError != errorText {
		t.Fatalf("attempt %s/%d error=%v, expected %s", taskID, attemptNumber, actualError, errorText)
	}
	if status == "ABANDONED" && finishedAt == nil {
		t.Fatalf("abandoned attempt %s/%d has no finished_at", taskID, attemptNumber)
	}
}

func equalStrings(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func randomSuffix(t *testing.T) string {
	t.Helper()
	bytes := make([]byte, 6)
	if _, err := rand.Read(bytes); err != nil {
		t.Fatalf("generate random suffix: %v", err)
	}
	return hex.EncodeToString(bytes)
}
