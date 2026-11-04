package inventory

import (
	"net/http"
	"strings"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/app"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/http/middleware"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/http/response"
)

// freshness bounds how stale the version a panel poll receives may be.
const freshness = 2 * time.Second

type Handler struct {
	App *app.Context
}

// Handle returns the credential inventory version. The panel polls it and
// revalidates its account and credential screens when the version moves. The
// body carries no credential names.
func (h *Handler) Handle(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if strings.TrimRight(r.URL.Path, "/") != "/usage-service/inventory" || r.Method != http.MethodGet {
		response.MethodNotAllowed(w)
		return
	}
	if !middleware.AuthorizePanel(w, r, h.App.AdminAuthService) {
		return
	}
	response.JSON(w, http.StatusOK, h.App.InventoryService.Fresh(r.Context(), freshness))
}
