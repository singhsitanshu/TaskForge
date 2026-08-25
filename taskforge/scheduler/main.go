package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"taskforge/scheduler/internal/config"
	"taskforge/scheduler/internal/repository"
	"taskforge/scheduler/internal/service"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("scheduler stopped", "event", "scheduler_failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	recoveryConfig, err := config.RecoveryFromEnv()
	if err != nil {
		return err
	}
	promotionConfig, err := config.RetryPromotionFromEnv()
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
	connectContext, cancelConnect := context.WithTimeout(ctx, recoveryConfig.DBTimeout)
	err = pool.Ping(connectContext)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}

	store := repository.NewPostgres(pool)
	recovery := service.NewRecovery(
		store,
		recoveryConfig,
		logger,
	)
	promotion := service.NewRetryPromotion(store, promotionConfig, logger)
	server := &http.Server{
		Addr:              envOrDefault("HTTP_ADDR", ":8080"),
		Handler:           newMux(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info(
			"scheduler health server listening",
			"event", "scheduler_started",
			"address", server.Addr,
			"recovery_interval", recoveryConfig.Interval,
			"recovery_batch_size", recoveryConfig.BatchSize,
			"recovery_db_timeout", recoveryConfig.DBTimeout,
			"retry_promotion_interval", promotionConfig.Interval,
			"retry_promotion_batch_size", promotionConfig.BatchSize,
			"retry_promotion_db_timeout", promotionConfig.DBTimeout,
		)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- err
		}
	}()

	var lifecycle sync.WaitGroup
	lifecycle.Add(2)
	go func() {
		defer lifecycle.Done()
		recovery.Run(ctx)
	}()
	go func() {
		defer lifecycle.Done()
		promotion.Run(ctx)
	}()

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-serverErrors:
		stop()
	}
	stop()

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownContext); err != nil && runErr == nil {
		runErr = fmt.Errorf("shutdown health server: %w", err)
	}
	lifecycle.Wait()
	logger.Info("scheduler shutdown complete", "event", "scheduler_shutdown")
	return runErr
}

func newMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"service": "scheduler",
			"status":  "ok",
		})
	})
	return mux
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
