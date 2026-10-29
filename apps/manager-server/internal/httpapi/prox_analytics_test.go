package httpapi

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func TestProxAnalyticsPanelAuthorizationAndStrictBody(t *testing.T) {
	cpa := testutil.NewCPAMock(t)
	handler, db := newCompatHandler(t, testutil.NewConfig(t), &store.Setup{CPAUpstreamURL: cpa.URL(), ManagementKey: "fixture-only", Queue: "usage", PopSide: "right"})
	if _, err := db.InsertEvents(context.Background(), []usage.Event{compatEvent("prox-api", 10)}); err != nil {
		t.Fatal(err)
	}
	path := "/v0/management/monitoring/prox-analytics"
	body := `{"from_ms":1778000000000,"to_ms":1778000060000,"time_zone":"UTC","granularity":"hour","outcome":"all","limit":1}`
	testutil.RequireStatus(t, testutil.Request(t, handler, http.MethodPost, path, body, ""), http.StatusUnauthorized)
	testutil.RequireStatus(t, testutil.Request(t, handler, http.MethodPost, path, body, "fixture-only"), http.StatusUnauthorized)
	testutil.RequireStatus(t, testutil.Request(t, handler, http.MethodGet, path, "", testutil.AdminKey), http.StatusMethodNotAllowed)
	for _, bad := range []string{body + ` {}`, strings.Replace(body, `"limit":1`, `"filters":{"secret":"x"}`, 1), strings.Replace(body, `"limit":1`, `"limit":201`, 1), strings.Replace(body, `"time_zone":"UTC"`, `"time_zone":"fake"`, 1), `null`} {
		testutil.RequireStatus(t, testutil.Request(t, handler, http.MethodPost, path, bad, testutil.AdminKey), http.StatusBadRequest)
	}
	rr := testutil.Request(t, handler, http.MethodPost, path, body, testutil.AdminKey)
	testutil.RequireStatus(t, rr, http.StatusOK)
	if !strings.Contains(rr.Body.String(), `"schema_version":1`) || strings.Contains(rr.Body.String(), `"raw_json"`) || rr.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("invalid response: %s", rr.Body.String())
	}
}
