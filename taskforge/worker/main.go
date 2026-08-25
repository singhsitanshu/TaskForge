package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"taskforge/worker/internal/handler"
	"taskforge/worker/internal/repository"
	workerservice "taskforge/worker/internal/service"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}

	workerName, err := resolveWorkerName()
	if err != nil {
		return err
	}
	pollInterval, err := durationEnv("POLL_INTERVAL", time.Second)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("create database pool: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}

	store := repository.NewPostgres(pool)
	workerID, err := store.RegisterWorker(ctx, workerName)
	if err != nil {
		return err
	}
	log.Printf("registered worker id=%s name=%s", workerID, workerName)

	worker := workerservice.New(
		store,
		handler.NewRegistry(),
		workerID,
		pollInterval,
		log.Default(),
	)
	server := &http.Server{
		Addr:              envOrDefault("HTTP_ADDR", ":8080"),
		Handler:           newMux(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		log.Printf("worker health server listening on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErrors <- err
		}
	}()

	workerStopped := make(chan struct{})
	go func() {
		defer close(workerStopped)
		worker.Run(ctx)
	}()

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-serverErrors:
		stop()
	case <-workerStopped:
		if ctx.Err() == nil {
			runErr = errors.New("worker loop stopped unexpectedly")
		}
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil && runErr == nil {
		runErr = fmt.Errorf("shutdown health server: %w", err)
	}
	return runErr
}

func newMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"service": "worker",
			"status":  "ok",
		})
	})
	return mux
}

func resolveWorkerName() (string, error) {
	if configured := strings.TrimSpace(os.Getenv("WORKER_NAME")); configured != "" {
		return configured, nil
	}
	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("resolve worker name: %w", err)
	}
	if strings.TrimSpace(hostname) == "" {
		return "", errors.New("WORKER_NAME or a non-empty hostname is required")
	}
	return hostname, nil
}

func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration", key)
	}
	return duration, nil
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
