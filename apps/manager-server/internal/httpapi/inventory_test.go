package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/collector"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil"
)

func TestInventoryEndpointReportsTheCredentialVersionToThePanelOnly(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v0/management/auth-files" || r.Header.Get("Authorization") != "Bearer management-key" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"files":[{"name":"codex-a.json","provider":"codex"},{"name":"secret-person@example.com.json","provider":"claude"}]}`))
	}))
	defer upstream.Close()
	cfg := testutil.NewConfig(t)
	cfg.CPAUpstreamURL = upstream.URL
	cfg.ManagementKey = "management-key"
	db := testutil.NewStore(t, cfg)
	handler := New(cfg, db, collector.NewManager(cfg, db)).Handler()

	denied := testutil.Request(t, handler, http.MethodGet, "/usage-service/inventory", "", "")
	if denied.Code != http.StatusUnauthorized && denied.Code != http.StatusForbidden {
		t.Fatalf("unauthenticated status = %d, want 401 or 403", denied.Code)
	}

	rr := testutil.Request(t, handler, http.MethodGet, "/usage-service/inventory", "", testutil.AdminKey)
	testutil.RequireStatus(t, rr, http.StatusOK)
	var body struct {
		Version    string `json:"version"`
		SyncedAtMS int64  `json:"syncedAtMs"`
		Count      int    `json:"count"`
	}
	testutil.DecodeJSON(t, rr, &body)
	if len(body.Version) != 16 || body.Count != 2 || body.SyncedAtMS <= 0 {
		t.Fatalf("inventory = %+v, want a 16-character version over 2 credentials", body)
	}
	if strings.Contains(rr.Body.String(), "secret-person") || strings.Contains(rr.Body.String(), "codex-a") {
		t.Fatalf("inventory leaks credential names: %s", rr.Body.String())
	}

	if post := testutil.Request(t, handler, http.MethodPost, "/usage-service/inventory", "{}", testutil.AdminKey); post.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", post.Code)
	}
	var again struct{ Version string }
	testutil.DecodeJSON(t, testutil.Request(t, handler, http.MethodGet, "/usage-service/inventory", "", testutil.AdminKey), &again)
	if again.Version != body.Version {
		t.Fatalf("version changed without a change: %s vs %s", again.Version, body.Version)
	}
}
