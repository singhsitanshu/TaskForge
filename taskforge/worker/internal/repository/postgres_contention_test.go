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

	"taskforge/worker/internal/domain"
	"taskforge/worker/internal/handler"
	"taskforge/worker/internal/repository"
)

const (
	contentionClaimers   = 20
	singleTaskIterations = 50
	testPoolSize         = 32
)

type testDatabase struct {
	admin  *pgxpool.Pool
	pool   *pgxpool.Pool
	store  *repository.Postgres
	schema string
}

type testWorker struct {
	ID         string
	InstanceID string
}

type claimResult struct {
	worker testWorker
	task   *domain.ClaimedTask
	err    error
}

type workloadMetrics struct {
	tasks         int
	completed     int
	attempts      int
	distinctTasks int
	duplicates    int
	badCounts     int
	distribution  map[string]int
	elapsed       time.Duration
}

func TestSingleTaskContention(t *testing.T) {
	database := newTestDatabase(t)
	workers := registerWorkers(t, database, contentionClaimers)
	ctx := context.Background()

	for iteration := 1; iteration <= singleTaskIterations; iteration++ {
		taskID := insertTask(t, database, 0, time.Time{}, "")
		results := claimSimultaneously(ctx, database.store, workers)

		winners := make([]claimResult, 0, 1)
		losers := 0
		for _, result := range results {
			if result.err != nil {
				t.Fatalf("iteration %d claim by %s: %v", iteration, result.worker.InstanceID, result.err)
			}
			if result.task == nil {
				losers++
				continue
			}
			winners = append(winners, result)
		}

		if len(winners) != 1 || losers != contentionClaimers-1 {
			t.Fatalf("iteration %d: winners=%d losers=%d", iteration, len(winners), losers)
		}
		winner := winners[0]
		if winner.task.ID != taskID {
			t.Fatalf("iteration %d: claimed task %s, expected %s", iteration, winner.task.ID, taskID)
		}

		var status string
		var claimedBy string
		var attemptCount int
		if err := database.pool.QueryRow(
			ctx,
			`SELECT status::text, claimed_by_worker_id::text, attempt_count FROM tasks WHERE id = $1::uuid`,
			taskID,
		).Scan(&status, &claimedBy, &attemptCount); err != nil {
			t.Fatalf("iteration %d read task: %v", iteration, err)
		}
		if status == "QUEUED" || claimedBy != winner.worker.ID || attemptCount != 1 {
			t.Fatalf(
				"iteration %d invalid task state status=%s claimed_by=%s attempt_count=%d winner=%s",
				iteration,
				status,
				claimedBy,
				attemptCount,
				winner.worker.ID,
			)
		}

		var attemptRows int
		var attemptNumber int
		var attemptWorker string
		if err := database.pool.QueryRow(
			ctx,
			`SELECT count(*), min(attempt_number), min(worker_id::text) FROM task_attempts WHERE task_id = $1::uuid`,
			taskID,
		).Scan(&attemptRows, &attemptNumber, &attemptWorker); err != nil {
			t.Fatalf("iteration %d read attempt: %v", iteration, err)
		}
		if attemptRows != 1 || attemptNumber != 1 || attemptWorker != winner.worker.ID {
			t.Fatalf(
				"iteration %d attempts=%d number=%d worker=%s winner=%s",
				iteration,
				attemptRows,
				attemptNumber,
				attemptWorker,
				winner.worker.ID,
			)
		}

		if _, err := database.pool.Exec(ctx, `DELETE FROM tasks WHERE id = $1::uuid`, taskID); err != nil {
			t.Fatalf("iteration %d cleanup task: %v", iteration, err)
		}
	}

	t.Logf(
		"SINGLE_TASK iterations=%d claimers=%d winners_per_iteration=1 losers_per_iteration=%d failures=0",
		singleTaskIterations,
		contentionClaimers,
		contentionClaimers-1,
	)
}

func TestMultiWorkerContentionAndLoadScenarios(t *testing.T) {
	database := newTestDatabase(t)
	workers := registerWorkers(t, database, contentionClaimers)
	scenarios := []struct {
		name    string
		tasks   int
		workers int
	}{
		{name: "A", tasks: 100, workers: 2},
		{name: "B", tasks: 500, workers: 5},
		{name: "contention-500x20", tasks: 500, workers: 20},
		{name: "C", tasks: 1000, workers: 10},
		{name: "D", tasks: 1000, workers: 20},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			resetTasks(t, database)
			insertTasks(t, database, scenario.tasks, 0)
			metrics := runWorkload(t, database, workers[:scenario.workers])

			if metrics.tasks != scenario.tasks ||
				metrics.completed != scenario.tasks ||
				metrics.attempts != scenario.tasks ||
				metrics.distinctTasks != scenario.tasks ||
				metrics.duplicates != 0 ||
				metrics.badCounts != 0 {
				t.Fatalf("scenario %s invalid metrics: %+v", scenario.name, metrics)
			}
			if len(metrics.distribution) < 2 {
				t.Fatalf("scenario %s used only %d worker: %v", scenario.name, len(metrics.distribution), metrics.distribution)
			}

			t.Logf(
				"LOAD scenario=%s tasks=%d workers=%d completed=%d attempts=%d distinct_tasks=%d duplicates=%d elapsed=%s distribution=%v",
				scenario.name,
				metrics.tasks,
				scenario.workers,
				metrics.completed,
				metrics.attempts,
				metrics.distinctTasks,
				metrics.duplicates,
				metrics.elapsed,
				metrics.distribution,
			)
		})
	}
}

func TestPriorityUnderConcurrentClaims(t *testing.T) {
	database := newTestDatabase(t)
	workers := registerWorkers(t, database, 5)
	ctx := context.Background()

	for _, priority := range []int{100, 50, 10} {
		insertTasks(t, database, 20, priority)
	}
	for _, expectedPriority := range []int{100, 50, 10} {
		for batch := 0; batch < 4; batch++ {
			results := claimSimultaneously(ctx, database.store, workers)
			assertClaimedPriority(t, database, results, expectedPriority)
		}
	}

	resetTasks(t, database)
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	olderIDs := make(map[string]struct{})
	for index := 1; index <= 10; index++ {
		id := fmt.Sprintf("00000000-0000-0000-0000-%012d", index)
		insertTask(t, database, 25, baseTime.Add(time.Duration(index)*time.Second), id)
		if index <= 5 {
			olderIDs[id] = struct{}{}
		}
	}
	assertClaimedIDs(t, claimSimultaneously(ctx, database.store, workers), olderIDs)

	resetTasks(t, database)
	lowerIDs := make(map[string]struct{})
	for index := 1; index <= 10; index++ {
		id := fmt.Sprintf("10000000-0000-0000-0000-%012d", index)
		insertTask(t, database, 25, baseTime, id)
		if index <= 5 {
			lowerIDs[id] = struct{}{}
		}
	}
	assertClaimedIDs(t, claimSimultaneously(ctx, database.store, workers), lowerIDs)
	t.Log("PRIORITY batches=12 batch_size=5 priorities=100,50,10 created_at_tiebreak=pass id_tiebreak=pass")
}

func TestClaimRollbackWhenAttemptInsertFails(t *testing.T) {
	database := newTestDatabase(t)
	worker := registerWorkers(t, database, 1)[0]
	ctx := context.Background()
	taskID := insertTask(t, database, 0, time.Time{}, "")

	_, err := database.pool.Exec(ctx, `
		CREATE FUNCTION reject_tf005_attempt()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			RAISE EXCEPTION 'TF-005 injected attempt failure';
		END;
		$$;
		CREATE TRIGGER reject_tf005_attempt
		BEFORE INSERT ON task_attempts
		FOR EACH ROW EXECUTE FUNCTION reject_tf005_attempt();
	`)
	if err != nil {
		t.Fatalf("install failure trigger: %v", err)
	}

	claimed, claimErr := database.store.ClaimNext(ctx, worker.ID)
	if claimErr == nil || claimed != nil || !strings.Contains(claimErr.Error(), "injected attempt failure") {
		t.Fatalf("expected injected claim failure, task=%v error=%v", claimed, claimErr)
	}

	var status string
	var attemptCount int
	var claimedBy *string
	var leaseExpiresAt *time.Time
	if err := database.pool.QueryRow(
		ctx,
		`SELECT status::text, attempt_count, claimed_by_worker_id::text, lease_expires_at FROM tasks WHERE id = $1::uuid`,
		taskID,
	).Scan(&status, &attemptCount, &claimedBy, &leaseExpiresAt); err != nil {
		t.Fatalf("read rolled back task: %v", err)
	}
	var attempts int
	if err := database.pool.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE task_id = $1::uuid`, taskID).Scan(&attempts); err != nil {
		t.Fatalf("count rolled back attempts: %v", err)
	}
	if status != "QUEUED" || attemptCount != 0 || claimedBy != nil || leaseExpiresAt != nil || attempts != 0 {
		t.Fatalf("rollback failed status=%s attempts=%d claimed_by=%v lease=%v attempt_rows=%d", status, attemptCount, claimedBy, leaseExpiresAt, attempts)
	}

	if _, err := database.pool.Exec(ctx, `DROP TRIGGER reject_tf005_attempt ON task_attempts; DROP FUNCTION reject_tf005_attempt()`); err != nil {
		t.Fatalf("remove failure trigger: %v", err)
	}
	reclaimed, err := database.store.ClaimNext(ctx, worker.ID)
	if err != nil || reclaimed == nil || reclaimed.ID != taskID || reclaimed.AttemptNumber != 1 {
		t.Fatalf("claim after rollback task=%v error=%v", reclaimed, err)
	}
	t.Log("ROLLBACK injected_attempt_insert_failure=pass task_remained_queued=true subsequent_claim=pass")
}

func TestClaimTransactionEndsBeforeHandlerExecution(t *testing.T) {
	database := newTestDatabase(t)
	workers := registerWorkers(t, database, 2)
	insertTasks(t, database, 2, 0)
	ctx := context.Background()

	first, err := database.store.ClaimNext(ctx, workers[0].ID)
	if err != nil || first == nil {
		t.Fatalf("first claim task=%v error=%v", first, err)
	}
	// The first task is deliberately left RUNNING. A second successful claim
	// proves the first ClaimNext committed and returned its row lock before any
	// handler or completion work occurred.
	second, err := database.store.ClaimNext(ctx, workers[1].ID)
	if err != nil || second == nil {
		t.Fatalf("second claim while first handler is pending task=%v error=%v", second, err)
	}
	if first.ID == second.ID {
		t.Fatalf("workers claimed the same task %s", first.ID)
	}
	if acquired := database.pool.Stat().AcquiredConns(); acquired != 0 {
		t.Fatalf("claim connections were not returned to the pool: acquired=%d", acquired)
	}
	t.Log("LOCK_SCOPE second_claim_succeeded_while_first_handler_pending=true acquired_connections=0")
}

func TestClaimQueryPlanUsesPriorityIndex(t *testing.T) {
	database := newTestDatabase(t)
	insertTasks(t, database, 20000, 0)
	ctx := context.Background()
	if _, err := database.pool.Exec(ctx, `ANALYZE tasks`); err != nil {
		t.Fatalf("analyze tasks: %v", err)
	}

	rows, err := database.pool.Query(ctx, `
		EXPLAIN (ANALYZE, BUFFERS)
		SELECT
			candidate.id::text,
			candidate.task_type,
			candidate.payload,
			(candidate.attempt_count + 1)::smallint
		FROM tasks AS candidate
		WHERE candidate.status = 'QUEUED'
		  AND candidate.scheduled_at <= clock_timestamp()
		  AND candidate.attempt_count < candidate.max_attempts
		ORDER BY
			candidate.priority DESC,
			candidate.created_at ASC,
			candidate.id ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`)
	if err != nil {
		t.Fatalf("explain claim query: %v", err)
	}
	defer rows.Close()
	planLines := make([]string, 0)
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan plan: %v", err)
		}
		planLines = append(planLines, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read plan: %v", err)
	}
	plan := strings.Join(planLines, "\n")
	if !strings.Contains(plan, "tasks_claim_priority_idx") {
		t.Fatalf("priority index not used by realistic claim plan:\n%s", plan)
	}
	t.Logf("QUERY_PLAN\n%s", plan)
}

func newTestDatabase(t *testing.T) *testDatabase {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL contention tests")
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
	schema := "tf005_" + hex.EncodeToString(randomBytes)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}

	database := &testDatabase{admin: admin, schema: schema}
	t.Cleanup(func() {
		if database.pool != nil {
			database.pool.Close()
		}
		_, _ = database.admin.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		database.admin.Close()
	})

	testConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	testConfig.MaxConns = testPoolSize
	testConfig.ConnConfig.RuntimeParams["search_path"] = schema
	testConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	database.pool, err = pgxpool.NewWithConfig(ctx, testConfig)
	if err != nil {
		t.Fatalf("create test pool: %v", err)
	}
	applyMigrations(t, database.pool)
	warmConnections(t, database.pool, contentionClaimers)
	database.store = repository.NewPostgres(database.pool)
	return database
}

func applyMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve contention test source path")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "../../.."))
	files, err := filepath.Glob(filepath.Join(repositoryRoot, "migrations", "*.up.sql"))
	if err != nil {
		t.Fatalf("find migrations: %v", err)
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

func registerWorkers(t *testing.T, database *testDatabase, count int) []testWorker {
	t.Helper()
	workers := make([]testWorker, 0, count)
	for index := 0; index < count; index++ {
		instanceID := fmt.Sprintf("%s-worker-%02d", database.schema, index)
		workerID, err := database.store.RegisterWorker(context.Background(), instanceID, "TF-005 contender")
		if err != nil {
			t.Fatalf("register worker %s: %v", instanceID, err)
		}
		workers = append(workers, testWorker{ID: workerID, InstanceID: instanceID})
	}
	return workers
}

func claimSimultaneously(ctx context.Context, store *repository.Postgres, workers []testWorker) []claimResult {
	ready := make(chan struct{}, len(workers))
	start := make(chan struct{})
	results := make(chan claimResult, len(workers))
	for _, worker := range workers {
		go func(worker testWorker) {
			ready <- struct{}{}
			<-start
			task, err := store.ClaimNext(ctx, worker.ID)
			results <- claimResult{worker: worker, task: task, err: err}
		}(worker)
	}
	for range workers {
		<-ready
	}
	close(start)
	claimed := make([]claimResult, 0, len(workers))
	for range workers {
		claimed = append(claimed, <-results)
	}
	return claimed
}

func runWorkload(t *testing.T, database *testDatabase, workers []testWorker) workloadMetrics {
	t.Helper()
	ctx := context.Background()
	registry := handler.NewRegistry()
	ready := make(chan struct{}, len(workers))
	start := make(chan struct{})
	firstClaimsFinished := make(chan struct{}, len(workers))
	releaseExecution := make(chan struct{})
	results := make(chan struct {
		worker testWorker
		count  int
		err    error
	}, len(workers))

	startedAt := time.Now()
	for _, worker := range workers {
		go func(worker testWorker) {
			ready <- struct{}{}
			<-start
			task, err := database.store.ClaimNext(ctx, worker.ID)
			firstClaimsFinished <- struct{}{}
			<-releaseExecution
			claimedCount := 0
			for err == nil && task != nil {
				output, executionErr := registry.Execute(ctx, task.Type, task.Payload)
				if executionErr != nil {
					err = fmt.Errorf("execute task %s: %w", task.ID, executionErr)
					break
				}
				if err = database.store.Complete(ctx, worker.ID, task, output, nil); err != nil {
					err = fmt.Errorf("complete task %s: %w", task.ID, err)
					break
				}
				claimedCount++
				task, err = database.store.ClaimNext(ctx, worker.ID)
			}
			results <- struct {
				worker testWorker
				count  int
				err    error
			}{worker: worker, count: claimedCount, err: err}
		}(worker)
	}
	for range workers {
		<-ready
	}
	close(start)
	for range workers {
		<-firstClaimsFinished
	}
	close(releaseExecution)
	for range workers {
		result := <-results
		if result.err != nil {
			t.Fatalf("worker %s workload: %v", result.worker.InstanceID, result.err)
		}
	}
	if acquired := database.pool.Stat().AcquiredConns(); acquired != 0 {
		t.Fatalf("workload left %d database connections acquired", acquired)
	}

	metrics := workloadMetrics{distribution: make(map[string]int), elapsed: time.Since(startedAt)}
	if err := database.pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE status = 'SUCCEEDED') FROM tasks`).Scan(&metrics.tasks, &metrics.completed); err != nil {
		t.Fatalf("read task metrics: %v", err)
	}
	if err := database.pool.QueryRow(ctx, `SELECT count(*), count(DISTINCT task_id) FROM task_attempts WHERE attempt_number = 1`).Scan(&metrics.attempts, &metrics.distinctTasks); err != nil {
		t.Fatalf("read attempt metrics: %v", err)
	}
	if err := database.pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT task_id, attempt_number FROM task_attempts GROUP BY task_id, attempt_number HAVING count(*) > 1) AS duplicates`).Scan(&metrics.duplicates); err != nil {
		t.Fatalf("read duplicate metrics: %v", err)
	}
	if err := database.pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE attempt_count <> 1`).Scan(&metrics.badCounts); err != nil {
		t.Fatalf("read attempt-count metrics: %v", err)
	}
	rows, err := database.pool.Query(ctx, `SELECT w.instance_id, count(*) FROM task_attempts AS ta JOIN workers AS w ON w.id = ta.worker_id GROUP BY w.instance_id ORDER BY w.instance_id`)
	if err != nil {
		t.Fatalf("read worker distribution: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var instanceID string
		var count int
		if err := rows.Scan(&instanceID, &count); err != nil {
			t.Fatalf("scan worker distribution: %v", err)
		}
		metrics.distribution[instanceID] = count
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read worker distribution rows: %v", err)
	}
	return metrics
}

func insertTasks(t *testing.T, database *testDatabase, count int, priority int) {
	t.Helper()
	_, err := database.pool.Exec(
		context.Background(),
		`INSERT INTO tasks (task_type, payload, priority) SELECT 'test.echo', jsonb_build_object('sequence', value), $1 FROM generate_series(1, $2) AS value`,
		priority,
		count,
	)
	if err != nil {
		t.Fatalf("insert %d tasks: %v", count, err)
	}
}

func insertTask(t *testing.T, database *testDatabase, priority int, createdAt time.Time, taskID string) string {
	t.Helper()
	query := `INSERT INTO tasks (task_type, payload, priority) VALUES ('test.echo', '{}', $1) RETURNING id::text`
	arguments := []any{priority}
	if !createdAt.IsZero() && taskID != "" {
		query = `INSERT INTO tasks (id, task_type, payload, priority, created_at, scheduled_at) VALUES ($1::uuid, 'test.echo', '{}', $2, $3, $3) RETURNING id::text`
		arguments = []any{taskID, priority, createdAt}
	}
	var insertedID string
	if err := database.pool.QueryRow(context.Background(), query, arguments...).Scan(&insertedID); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	return insertedID
}

func resetTasks(t *testing.T, database *testDatabase) {
	t.Helper()
	if _, err := database.pool.Exec(context.Background(), `TRUNCATE task_attempts, tasks`); err != nil {
		t.Fatalf("reset tasks: %v", err)
	}
}

func assertClaimedPriority(t *testing.T, database *testDatabase, results []claimResult, expected int) {
	t.Helper()
	for _, result := range results {
		if result.err != nil || result.task == nil {
			t.Fatalf("expected priority %d claim, task=%v error=%v", expected, result.task, result.err)
		}
		var priority int
		if err := database.pool.QueryRow(context.Background(), `SELECT priority FROM tasks WHERE id = $1::uuid`, result.task.ID).Scan(&priority); err != nil {
			t.Fatalf("read claimed priority: %v", err)
		}
		if priority != expected {
			t.Fatalf("claimed priority %d while priority %d remained eligible", priority, expected)
		}
	}
}

func assertClaimedIDs(t *testing.T, results []claimResult, expected map[string]struct{}) {
	t.Helper()
	actual := make(map[string]struct{}, len(results))
	for _, result := range results {
		if result.err != nil || result.task == nil {
			t.Fatalf("expected deterministic claim, task=%v error=%v", result.task, result.err)
		}
		actual[result.task.ID] = struct{}{}
	}
	if len(actual) != len(expected) {
		t.Fatalf("claimed IDs %v, expected %v", actual, expected)
	}
	for taskID := range expected {
		if _, ok := actual[taskID]; !ok {
			t.Fatalf("claimed IDs %v, expected %v", actual, expected)
		}
	}
}
