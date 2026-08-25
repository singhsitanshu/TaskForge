//go:build tf005test

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"taskforge/worker/internal/repository"
)

type harnessEvent struct {
	Event          string `json:"event"`
	WorkerID       string `json:"worker_id,omitempty"`
	WorkerInstance string `json:"worker_instance_id,omitempty"`
	Claimed        bool   `json:"claimed,omitempty"`
	TaskID         string `json:"task_id,omitempty"`
	AttemptID      string `json:"attempt_id,omitempty"`
	AttemptNumber  int16  `json:"attempt_number,omitempty"`
	Error          string `json:"error,omitempty"`
}

func main() {
	if err := run(); err != nil {
		_ = json.NewEncoder(os.Stdout).Encode(harnessEvent{Event: "error", Error: err.Error()})
		os.Exit(1)
	}
}

func run() error {
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	instanceID := strings.TrimSpace(os.Getenv("WORKER_ID"))
	if instanceID == "" {
		return errors.New("WORKER_ID is required")
	}
	name := strings.TrimSpace(os.Getenv("WORKER_NAME"))
	if name == "" {
		name = instanceID
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("create database pool: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}

	store := repository.NewPostgres(pool)
	workerID, err := store.RegisterWorker(ctx, instanceID, name)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	if err := encoder.Encode(harnessEvent{
		Event:          "ready",
		WorkerID:       workerID,
		WorkerInstance: instanceID,
	}); err != nil {
		return fmt.Errorf("write ready event: %w", err)
	}
	if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
		return fmt.Errorf("wait for release: %w", err)
	}

	task, err := store.ClaimNext(ctx, workerID)
	if err != nil {
		return err
	}
	event := harnessEvent{
		Event:          "claim_result",
		WorkerID:       workerID,
		WorkerInstance: instanceID,
		Claimed:        task != nil,
	}
	if task != nil {
		event.TaskID = task.ID
		event.AttemptID = task.AttemptID
		event.AttemptNumber = task.AttemptNumber
	}
	if err := encoder.Encode(event); err != nil {
		return fmt.Errorf("write claim result: %w", err)
	}
	return nil
}
