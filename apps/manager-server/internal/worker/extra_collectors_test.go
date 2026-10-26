package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
)

func TestStartExtraCollectorsPollsEachExtraUpstreamWithThePrimaryKey(t *testing.T) {
	var polled, badKey atomic.Int32
	extra := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v0/management/usage-queue" {
			polled.Add(1)
			if r.Header.Get("Authorization") != "Bearer management-key" {
				badKey.Add(1)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[]`))
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(extra.Close)

	cfg := workerTestConfig(t)
	cfg.CPAUpstreamURL = "http://primary.local:8317"
	cfg.ExtraUpstreamURLs = []string{"http://primary.local:8317/", extra.URL}
	cfg.ManagementKey = "management-key"
	cfg.CollectorMode = "http"
	cfg.PollInterval = time.Hour
	db, err := store.Open(cfg.DBPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	managers := StartExtraCollectors(ctx, cfg, db, NewUsageEventFanout())
	if len(managers) != 1 {
		t.Fatalf("started %d extra collectors, want 1 (the primary upstream must be skipped)", len(managers))
	}
	deadline := time.Now().Add(5 * time.Second)
	for polled.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if polled.Load() == 0 {
		t.Fatalf("extra upstream was never polled; status %#v", managers[0].Status())
	}
	if badKey.Load() != 0 {
		t.Fatalf("extra upstream polled without the primary management key")
	}
	if got := managers[0].Status().Upstream; got != extra.URL {
		t.Fatalf("extra collector upstream = %q, want %q", got, extra.URL)
	}
}

func TestStartExtraCollectorsWithoutExtrasStartsNothing(t *testing.T) {
	cfg := workerTestConfig(t)
	cfg.ManagementKey = "management-key"
	db, err := store.Open(cfg.DBPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if managers := StartExtraCollectors(context.Background(), cfg, db, NewUsageEventFanout()); managers != nil {
		t.Fatalf("started %d collectors with no extra upstreams", len(managers))
	}
}
