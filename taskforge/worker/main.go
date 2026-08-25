package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	workerconfig "taskforge/worker/internal/config"
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

	identity, err := resolveWorkerIdentity()
	if err != nil {
		return err
	}
	pollInterval, err := durationEnv("POLL_INTERVAL", time.Second)
	if err != nil {
		return err
	}
	heartbeatConfig, err := resolveHeartbeatConfig()
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
	workerID, err := store.RegisterWorker(ctx, identity.InstanceID, identity.Name)
	if err != nil {
		return err
	}
	log.Printf(
		"event=worker_registered worker_instance_id=%s worker_id=%s name=%q heartbeat_interval=%s",
		identity.InstanceID,
		workerID,
		identity.Name,
		heartbeatConfig.Interval,
	)

	worker := workerservice.New(
		store,
		handler.NewRegistry(),
		workerID,
		identity.InstanceID,
		pollInterval,
		log.Default(),
	)
	heartbeater := workerservice.NewHeartbeater(
		store,
		workerID,
		identity.InstanceID,
		heartbeatConfig.Interval,
		heartbeatConfig.Timeout,
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
	var lifecycle sync.WaitGroup
	lifecycle.Add(2)
	go func() {
		defer lifecycle.Done()
		defer close(workerStopped)
		worker.Run(ctx)
	}()
	go func() {
		defer lifecycle.Done()
		heartbeater.Run(ctx)
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
	stop()

	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil && runErr == nil {
		runErr = fmt.Errorf("shutdown health server: %w", err)
	}
	lifecycle.Wait()
	log.Printf(
		"event=worker_shutdown worker_instance_id=%s worker_id=%s",
		identity.InstanceID,
		workerID,
	)
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

type workerIdentity struct {
	InstanceID string
	Name       string
}

func resolveWorkerIdentity() (workerIdentity, error) {
	instanceID := strings.TrimSpace(os.Getenv("WORKER_ID"))
	name := strings.TrimSpace(os.Getenv("WORKER_NAME"))

	hostname := ""
	if instanceID == "" || name == "" {
		resolvedHostname, err := os.Hostname()
		if err != nil {
			return workerIdentity{}, fmt.Errorf("resolve worker hostname: %w", err)
		}
		hostname = strings.TrimSpace(resolvedHostname)
		if hostname == "" {
			return workerIdentity{}, errors.New(
				"WORKER_ID and WORKER_NAME, or a non-empty hostname, are required",
			)
		}
	}

	if instanceID == "" {
		randomBytes := make([]byte, 16)
		if _, err := rand.Read(randomBytes); err != nil {
			return workerIdentity{}, fmt.Errorf("generate worker instance id: %w", err)
		}
		suffix := "-" + hex.EncodeToString(randomBytes)
		maxHostnameLength := 255 - len(suffix)
		if len(hostname) > maxHostnameLength {
			hostname = hostname[:maxHostnameLength]
		}
		instanceID = hostname + suffix
	}
	if name == "" {
		name = hostname
	}
	if len(instanceID) > 255 {
		return workerIdentity{}, errors.New("WORKER_ID must not exceed 255 characters")
	}
	if len(name) > 255 {
		return workerIdentity{}, errors.New("WORKER_NAME must not exceed 255 characters")
	}

	return workerIdentity{InstanceID: instanceID, Name: name}, nil
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

func resolveHeartbeatConfig() (workerconfig.Heartbeat, error) {
	interval, err := durationEnv(
		"WORKER_HEARTBEAT_INTERVAL",
		workerconfig.DefaultHeartbeatInterval,
	)
	if err != nil {
		return workerconfig.Heartbeat{}, err
	}
	staleAfter, err := durationEnv("WORKER_STALE_AFTER", workerconfig.DefaultStaleAfter)
	if err != nil {
		return workerconfig.Heartbeat{}, err
	}
	deadAfter, err := durationEnv("WORKER_DEAD_AFTER", workerconfig.DefaultDeadAfter)
	if err != nil {
		return workerconfig.Heartbeat{}, err
	}
	timeout, err := durationEnv(
		"WORKER_HEARTBEAT_TIMEOUT",
		workerconfig.DefaultHeartbeatTimeout,
	)
	if err != nil {
		return workerconfig.Heartbeat{}, err
	}

	configuration := workerconfig.Heartbeat{
		Interval:   interval,
		StaleAfter: staleAfter,
		DeadAfter:  deadAfter,
		Timeout:    timeout,
	}
	if err := configuration.Validate(); err != nil {
		return workerconfig.Heartbeat{}, err
	}
	return configuration, nil
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
