package monitoring

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/http/response"
	monitoringsvc "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/monitoring"
)

func (h *Handler) handleProxAnalytics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		response.MethodNotAllowed(w)
		return
	}
	var q monitoringsvc.ProxAnalyticsQuery
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&q); err != nil {
		response.Error(w, http.StatusBadRequest, errors.New("invalid analytics query"))
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		response.Error(w, http.StatusBadRequest, errors.New("expected one analytics query"))
		return
	}
	if err := monitoringsvc.ValidateProxAnalyticsQuery(q); err != nil {
		response.Error(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	result, err := h.App.MonitoringService.ProxAnalytics(ctx, q)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, errors.New("analytics query failed"))
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	response.JSON(w, http.StatusOK, result)
}
