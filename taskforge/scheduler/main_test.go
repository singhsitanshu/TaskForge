package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthcheck(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()
	newMux().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}
}

func TestReadinessReflectsDatabaseCheck(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want int
	}{{"ready", nil, http.StatusOK}, {"unavailable", errors.New("down"), http.StatusServiceUnavailable}} {
		t.Run(test.name, func(t *testing.T) {
			handler := newOperationalMux(http.NotFoundHandler(), func(context.Context) error { return test.err })
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if response.Code != test.want {
				t.Fatalf("status=%d want=%d", response.Code, test.want)
			}
		})
	}
}
