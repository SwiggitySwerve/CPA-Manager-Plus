package usageevent

import (
	"context"
	"path/filepath"
	"testing"

	sqliterepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/sqlite"
)

func TestResponseMetadataBackfillCandidateNoOpDoesNotBlockReadiness(t *testing.T) {
	db, err := sqliterepo.Open(filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	repo := New(db)

	// Insert a row that matches SQL candidate predicate (raw_json contains 'X-Ratelimit-Limit-Requests'
	// while $.rate_limit.requests.limit is null in metadata_json), but already has response_metadata_json = '{}'
	// and raw_json doesn't actually contain valid response header structures.
	rawJSON := `{"prompt":"User asked about X-Ratelimit-Limit-Requests"}`
	_, err = db.ExecContext(ctx, `insert into usage_events (
		event_hash, timestamp_ms, timestamp, model, raw_json, response_metadata_json, created_at_ms
	) values ('hash-1', 1700000000000, '2023-11-14T22:13:20Z', 'gpt-4o', ?, '{}', 1700000000000)`, rawJSON)
	if err != nil {
		t.Fatalf("insert usage_event: %v", err)
	}

	// Verify the SQL candidate query actually matches this row
	rows, err := db.QueryContext(ctx, responseMetadataBackfillSelect, int64(0), 10)
	if err != nil {
		t.Fatalf("query candidates: %v", err)
	}
	defer rows.Close()
	candidateCount := 0
	for rows.Next() {
		candidateCount++
	}
	if candidateCount != 1 {
		t.Fatalf("expected 1 SQL candidate, got %d", candidateCount)
	}

	// Verify BackfillResponseMetadata does not update the no-op row
	updated, err := repo.BackfillResponseMetadata(ctx, 100)
	if err != nil {
		t.Fatalf("BackfillResponseMetadata: %v", err)
	}
	if updated != 0 {
		t.Fatalf("expected 0 updates for no-op candidate, got %d", updated)
	}

	// Verify ResponseMetadataBackfillPending returns false
	pending, err := repo.ResponseMetadataBackfillPending(ctx)
	if err != nil {
		t.Fatalf("ResponseMetadataBackfillPending: %v", err)
	}
	if pending {
		t.Fatalf("expected pending = false for no-op candidate, got true")
	}
}

// The startup backfill must not rescan the whole table on every start. Once a scan finds
// nothing left, later starts check only rows added since; rows that need the backfill,
// including rows an older build writes later, are still found, and a change to the
// selection (a new header parser) rescans everything.
func TestResponseMetadataBackfillChecksOnlyNewRowsAfterCompleting(t *testing.T) {
	db, err := sqliterepo.Open(filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	repo := New(db)
	rawJSON := `{"response_headers":{"X-Codex-Plan-Type":["plus"],"X-OAI-Request-ID":["req-cursor"]}}`
	insertLegacy := func(hash string) int64 {
		t.Helper()
		result, err := db.ExecContext(ctx, `insert into usage_events (
			event_hash, timestamp_ms, timestamp, model, raw_json, created_at_ms
		) values (?, 1700000000000, '2023-11-14T22:13:20Z', 'gpt-4o', ?, 1700000000000)`, hash, rawJSON)
		if err != nil {
			t.Fatalf("insert %s: %v", hash, err)
		}
		id, _ := result.LastInsertId()
		return id
	}
	backfill := func() int {
		t.Helper()
		total := 0
		for {
			updated, err := repo.BackfillResponseMetadata(ctx, 1)
			if err != nil {
				t.Fatalf("backfill: %v", err)
			}
			if updated == 0 {
				return total
			}
			total += updated
		}
	}
	metadataMissing := func(id int64) bool {
		t.Helper()
		var missing bool
		if err := db.QueryRowContext(ctx, `select response_metadata_json is null from usage_events where id = ?`, id).Scan(&missing); err != nil {
			t.Fatal(err)
		}
		return missing
	}

	first, second := insertLegacy("legacy-1"), insertLegacy("legacy-2")
	if updated := backfill(); updated != 2 {
		t.Fatalf("first start updated %d rows, want 2", updated)
	}
	startID, err := repo.(*repository).responseMetadataBackfillStartID(ctx)
	if err != nil || startID != second {
		t.Fatalf("cursor after the first start = %d, %v; want %d", startID, err, second)
	}

	// A row the cursor has passed is not read again: the second start does no full scan.
	if _, err := db.ExecContext(ctx, `update usage_events set response_metadata_json = null where id = ?`, first); err != nil {
		t.Fatal(err)
	}
	// A row written afterwards, here as an older build would write it, is still found.
	newer := insertLegacy("legacy-after-cursor")
	if updated := backfill(); updated != 1 || !metadataMissing(first) || metadataMissing(newer) {
		t.Fatalf("second start updated %d rows (passed row missing %t, newer row missing %t); want only the newer row",
			updated, metadataMissing(first), metadataMissing(newer))
	}
	if pending, err := repo.ResponseMetadataBackfillPending(ctx); err != nil || pending {
		t.Fatalf("pending after the second start = %t, %v", pending, err)
	}

	// A cursor recorded for another selection does not count.
	if _, err := db.ExecContext(ctx, `update settings set value = json_set(value, '$.select_sha256', 'older-selection') where key = ?`, responseMetadataBackfillCursorKey); err != nil {
		t.Fatal(err)
	}
	if pending, err := repo.ResponseMetadataBackfillPending(ctx); err != nil || !pending {
		t.Fatalf("pending with a stale cursor = %t, %v; want true", pending, err)
	}
	if updated := backfill(); updated != 1 || metadataMissing(first) {
		t.Fatalf("start with a stale cursor updated %d rows (passed row missing %t); want the full rescan to find it", updated, metadataMissing(first))
	}
}
