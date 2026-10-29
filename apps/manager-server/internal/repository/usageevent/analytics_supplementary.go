package usageevent

import (
	"context"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

// HasSupplementaryEvents uses the narrow partial index. An absent index fails
// explicitly rather than falling back to repeated multi-gigabyte envelope scans.
func (r *repository) HasSupplementaryEvents(ctx context.Context, fromMS, toMS int64) (bool, error) {
	var found bool
	err := r.db.QueryRowContext(ctx, `select exists(select 1 from usage_events indexed by `+usage.AnalyticsSupplementaryIndex+` where `+usage.SQLSupplementaryAnalyticsRecord("raw_json")+` and timestamp_ms >= ? and timestamp_ms < ?)`, fromMS, toMS).Scan(&found)
	return found, err
}
