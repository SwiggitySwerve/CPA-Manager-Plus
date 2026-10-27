package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/collector"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil"
)

func TestStatusIncludesExtraCollectorsWithoutChangingLegacyCollector(t *testing.T) {
	cfg := config.Config{DBPath: filepath.Join(t.TempDir(), "usage.sqlite"), Queue: "primary"}
	database, err := store.Open(cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	testutil.EnsureAdminCredential(t, database)
	server := New(cfg, database, collector.NewManager(cfg, database))
	extra := collector.NewManager(config.Config{Queue: "claude"}, database)
	server.AppContext().CollectorService.SetExtraManagers([]*collector.Manager{extra})
	request := httptest.NewRequest(http.MethodGet, "/status", nil)
	request.Header.Set("Authorization", "Bearer "+testutil.AdminKey)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	var payload struct {
		Collector  collector.Status   `json:"collector"`
		Collectors []collector.Status `json:"collectors"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Collector.Queue != "primary" || len(payload.Collectors) != 2 || payload.Collectors[1].Queue != "claude" {
		t.Fatalf("collector status missing or legacy shape changed: %#v", payload)
	}
}
