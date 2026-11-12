package usageprojection

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

func TestSearchIndexLikePatternKeepsExactFallbackBoundary(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		wantPattern string
		wantOK      bool
	}{
		{name: "ordinary substring", query: " Trace-ABC ", wantPattern: "%trace-abc%", wantOK: true},
		{name: "three unicode characters", query: "中文测", wantPattern: "%中文测%", wantOK: true},
		{name: "one character", query: "a", wantOK: false},
		{name: "two characters", query: "ab", wantOK: false},
		{name: "two unicode characters", query: "中文", wantOK: false},
		{name: "percent wildcard", query: "trace%", wantOK: false},
		{name: "underscore wildcard", query: "trace_", wantOK: false},
		{name: "projection separator", query: "a\x1fb", wantOK: false},
		{name: "control character", query: "a\nb", wantOK: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pattern, ok := SearchIndexLikePattern(test.query)
			if ok != test.wantOK || pattern != test.wantPattern {
				t.Fatalf("SearchIndexLikePattern(%q) = %q, %v; want %q, %v", test.query, pattern, ok, test.wantPattern, test.wantOK)
			}
		})
	}
}

func TestVerifyRetainedEdgeTx(t *testing.T) {
	ctx := context.Background()
	setupDB := func(t *testing.T) *sql.DB {
		t.Helper()
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatalf("open sqlite: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		schema := `
			create table usage_monitoring_rollup_state (
				rollup_name text primary key,
				schema_version int,
				structure_revision text,
				status text,
				coverage_event_id int
			);
			create table usage_archive_event_refs (
				raw_event_id int,
				raw_deleted_at_ms int,
				timestamp_ms int
			);
			create table usage_monitoring_event_projection_v1 (
				event_id int
			);
		`
		if _, err := db.Exec(schema); err != nil {
			t.Fatalf("create schema: %v", err)
		}
		return db
	}

	currentRevision := usageidentity.MonitoringProjectionStructureRevision()

	t.Run("missing projection state", func(t *testing.T) {
		db := setupDB(t)
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		err = VerifyRetainedEdgeTx(ctx, tx, 1000, 2000)
		if !errors.Is(err, ErrRetainedCoverageIncomplete) {
			t.Fatalf("expected ErrRetainedCoverageIncomplete, got %v", err)
		}
	})

	t.Run("incompatible schema version", func(t *testing.T) {
		db := setupDB(t)
		if _, err := db.Exec(`insert into usage_monitoring_rollup_state values ('projection_v1', 0, ?, 'ready', 10)`, currentRevision); err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		err = VerifyRetainedEdgeTx(ctx, tx, 1000, 2000)
		if !errors.Is(err, ErrRetainedCoverageIncomplete) {
			t.Fatalf("expected ErrRetainedCoverageIncomplete, got %v", err)
		}
	})

	t.Run("incompatible structure revision", func(t *testing.T) {
		db := setupDB(t)
		if _, err := db.Exec(`insert into usage_monitoring_rollup_state values ('projection_v1', 1, 'obsolete', 'ready', 10)`); err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		err = VerifyRetainedEdgeTx(ctx, tx, 1000, 2000)
		if !errors.Is(err, ErrRetainedCoverageIncomplete) {
			t.Fatalf("expected ErrRetainedCoverageIncomplete, got %v", err)
		}
	})

	t.Run("clearing status", func(t *testing.T) {
		db := setupDB(t)
		if _, err := db.Exec(`insert into usage_monitoring_rollup_state values ('projection_v1', 1, ?, 'clearing', 10)`, currentRevision); err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		err = VerifyRetainedEdgeTx(ctx, tx, 1000, 2000)
		if !errors.Is(err, ErrRetainedCoverageIncomplete) {
			t.Fatalf("expected ErrRetainedCoverageIncomplete, got %v", err)
		}
	})

	t.Run("missing deleted edge event in projection", func(t *testing.T) {
		db := setupDB(t)
		if _, err := db.Exec(`insert into usage_monitoring_rollup_state values ('projection_v1', 1, ?, 'ready', 10)`, currentRevision); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`insert into usage_archive_event_refs values (5, 1500, 1500)`); err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		err = VerifyRetainedEdgeTx(ctx, tx, 1000, 2000)
		if !errors.Is(err, ErrRetainedCoverageIncomplete) {
			t.Fatalf("expected ErrRetainedCoverageIncomplete, got %v", err)
		}
	})

	t.Run("edge event beyond coverage ID", func(t *testing.T) {
		db := setupDB(t)
		if _, err := db.Exec(`insert into usage_monitoring_rollup_state values ('projection_v1', 1, ?, 'ready', 5)`, currentRevision); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`insert into usage_archive_event_refs values (10, 1500, 1500)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`insert into usage_monitoring_event_projection_v1 values (10)`); err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		err = VerifyRetainedEdgeTx(ctx, tx, 1000, 2000)
		if !errors.Is(err, ErrRetainedCoverageIncomplete) {
			t.Fatalf("expected ErrRetainedCoverageIncomplete, got %v", err)
		}
	})

	t.Run("complete retained edge", func(t *testing.T) {
		db := setupDB(t)
		if _, err := db.Exec(`insert into usage_monitoring_rollup_state values ('projection_v1', 1, ?, 'ready', 10)`, currentRevision); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`insert into usage_archive_event_refs values (5, 1500, 1500)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`insert into usage_monitoring_event_projection_v1 values (5)`); err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		err = VerifyRetainedEdgeTx(ctx, tx, 1000, 2000)
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	})

	t.Run("real SQL query error does not return ErrRetainedCoverageIncomplete", func(t *testing.T) {
		db := setupDB(t)
		if _, err := db.Exec(`drop table usage_monitoring_rollup_state`); err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		err = VerifyRetainedEdgeTx(ctx, tx, 1000, 2000)
		if err == nil {
			t.Fatal("expected error on dropped table, got nil")
		}
		if errors.Is(err, ErrRetainedCoverageIncomplete) {
			t.Fatalf("SQL query error must not be classified as ErrRetainedCoverageIncomplete: %v", err)
		}
	})
}

// The trigram index lists matches of all time. A one-hour search for a word in most events
// must not verify every match ever stored, so the lookup is bounded by the range's event IDs;
// those bounds come from timestamps, so events stored out of time order still match.
func TestSearchMatchSQLBoundsIndexLookupByRange(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`
		create table usage_monitoring_event_projection_v1 (event_id integer primary key, timestamp_ms integer not null, search_text text not null);
		create index idx_usage_monitoring_event_projection_timestamp on usage_monitoring_event_projection_v1(timestamp_ms desc, event_id desc);
		create virtual table usage_monitoring_event_search_v1 using fts5(search_text, content = 'usage_monitoring_event_projection_v1',
			content_rowid = 'event_id', columnsize = 0, detail = 'none', tokenize = 'trigram');
		create trigger projection_insert after insert on usage_monitoring_event_projection_v1 begin
			insert into usage_monitoring_event_search_v1(rowid, search_text) values (new.event_id, new.search_text);
		end;`); err != nil {
		t.Fatal(err)
	}
	texts := []string{"claude-opus-5-5\x1fpost", "gpt-6.1-sol\x1fpost", "claude-sonnet\x1fo'brien \"q\"", "é-opus\x1fnull", "x\x1fopus"}
	for id := int64(1); id <= 400; id++ {
		timestamp := id * 1_000
		if id%97 == 0 {
			timestamp = 5_000 + id // stored late, timestamped early
		}
		if _, err := db.Exec(`insert into usage_monitoring_event_projection_v1 values (?, ?, ?)`, id, timestamp, texts[id%int64(len(texts))]); err != nil {
			t.Fatal(err)
		}
	}
	ids := func(where string, args ...any) []int64 {
		t.Helper()
		rows, err := db.Query(`select event_id from usage_monitoring_event_projection_v1 p where `+where+` order by event_id`, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		result := []int64{}
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			result = append(result, id)
		}
		return result
	}
	for _, query := range []string{"opus", "OPUS", "op", "sonnet\x1fo", "o'brien", `"q"`, "é-op", "nothing"} {
		for _, span := range [][2]int64{{0, 1_000_000}, {4_000, 12_000}, {100_000, 101_000}, {390_500, 391_000}} {
			match, args := SearchMatchSQL("p.event_id", "p.search_text", query, span[0], span[1])
			where := "p.timestamp_ms >= ? and p.timestamp_ms < ? and " + match
			got := ids(where, append([]any{span[0], span[1]}, args...)...)
			want := ids("p.timestamp_ms >= ? and p.timestamp_ms < ? and p.search_text like ?", span[0], span[1], "%"+strings.ToLower(query)+"%")
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("query %q span %v: got %v, want %v", query, span, got, want)
			}
		}
	}
	match, args := SearchMatchSQL("p.event_id", "p.search_text", "opus", 100_000, 101_000)
	rows, err := db.Query(`explain query plan select count(*) from usage_monitoring_event_projection_v1 p where p.timestamp_ms >= ? and p.timestamp_ms < ? and `+match, append([]any{100_000, 101_000}, args...)...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	plan := []string{}
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	// FTS5 marks a LIKE constraint L and lower and upper rowid bounds > and <.
	if !strings.Contains(strings.Join(plan, "\n"), "VIRTUAL TABLE INDEX 0:L0><") {
		t.Fatalf("search index lookup is not bounded by event ID; plan=%v", plan)
	}
}
