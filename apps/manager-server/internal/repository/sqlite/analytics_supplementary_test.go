package sqlite

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func TestSupplementaryIndexBuildIsAtomicAndTracksRawLifecycle(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`drop index ` + usage.AnalyticsSupplementaryIndex); err != nil {
		t.Fatal(err)
	}
	for i, raw := range []string{`{}`, `invalid-json`, `{"record_kind":"execution"}`, `{"record_kind":"supplementary"}`, `{"record_kind":" supplementary "}`} {
		if _, err := db.Exec(`insert into usage_events(event_hash,timestamp_ms,timestamp,model,created_at_ms,raw_json) values(?,100,'time','test',100,?)`, i, raw); err != nil {
			t.Fatal(err)
		}
	}
	// A schema upgrade must never update or delete raw rows.
	for _, verb := range []string{"update", "delete"} {
		if _, err := db.Exec(`create trigger immutable_` + verb + ` before ` + verb + ` on usage_events begin select raise(abort,'raw changed'); end`); err != nil {
			t.Fatal(err)
		}
	}
	if err := EnsureAnalyticsSupplementaryIndex(db); err != nil {
		t.Fatal(err)
	}
	if err := EnsureAnalyticsSupplementaryIndex(db); err != nil {
		t.Fatal(err)
	}
	predicate := usage.SQLSupplementaryAnalyticsRecord("raw_json")
	query := `select count(*) from usage_events indexed by ` + usage.AnalyticsSupplementaryIndex + ` where ` + predicate + ` and timestamp_ms>=0 and timestamp_ms<200`
	var count int
	if err := db.QueryRow(query).Scan(&count); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	rows, err := db.Query(`explain query plan ` + query)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for rows.Next() {
		var a, b, c int
		var detail string
		if err := rows.Scan(&a, &b, &c, &detail); err != nil {
			t.Fatal(err)
		}
		found = found || strings.Contains(detail, usage.AnalyticsSupplementaryIndex)
	}
	rows.Close()
	if !found {
		t.Fatal("query did not use supplementary index")
	}
	if _, err := db.Exec(`insert into usage_events(event_hash,timestamp_ms,timestamp,model,created_at_ms,raw_json) values('new',101,'time','test',100,'{"record_kind":"supplementary"}')`); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(query).Scan(&count); err != nil || count != 2 {
		t.Fatalf("insert count=%d err=%v", count, err)
	}
	if _, err := db.Exec(`drop trigger immutable_delete`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`delete from usage_events where event_hash='new'`); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(query).Scan(&count); err != nil || count != 1 {
		t.Fatalf("delete count=%d err=%v", count, err)
	}
	if _, err := db.Exec(`insert into usage_events(event_hash,timestamp_ms,timestamp,model,created_at_ms,raw_json) values('new',101,'time','test',100,'{"record_kind":"supplementary"}')`); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(query).Scan(&count); err != nil || count != 2 {
		t.Fatalf("restore count=%d err=%v", count, err)
	}
}
