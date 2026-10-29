package sqlite

import (
	"database/sql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

// EnsureAnalyticsSupplementaryIndex is a narrow schema upgrade: one partial
// index, no row rewrite or table. SQLite builds it atomically once and maintains
// it on insert, archive restore, and deletion. Existing IDs and rollups survive.
func EnsureAnalyticsSupplementaryIndex(db *sql.DB) error {
	_, err := db.Exec(`create index if not exists ` + usage.AnalyticsSupplementaryIndex + ` on usage_events(timestamp_ms, id) where ` + usage.SQLSupplementaryAnalyticsRecord("raw_json"))
	return err
}
