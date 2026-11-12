package usageevent

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	sqliterepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/sqlite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func TestGroupedLatencyMatchesOwnerAcrossTimeZones(t *testing.T) {
	for _, tc := range []struct{ zone, start string }{{"America/New_York", "2026-11-01T00:00:00Z"}, {"America/New_York", "2026-03-08T00:00:00Z"}, {"Australia/Lord_Howe", "2026-04-04T12:00:00Z"}, {"Asia/Kathmandu", "2026-10-29T00:00:00Z"}} {
		t.Run(tc.zone+tc.start, func(t *testing.T) {
			location, err := time.LoadLocation(tc.zone)
			if err != nil {
				t.Fatal(err)
			}
			base, err := time.Parse(time.RFC3339, tc.start)
			if err != nil {
				t.Fatal(err)
			}
			db, err := sqliterepo.Open(filepath.Join(t.TempDir(), "usage.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			repo := New(db)
			events := make([]usage.Event, 0, 96)
			for i := 0; i < 96; i++ {
				timestamp := base.Add(time.Duration(i) * 15 * time.Minute)
				latency := int64(i + 1)
				ttft := latency/2 + 1
				events = append(events, usage.Event{EventHash: canonicalTestHash(fmt.Sprint(i)), TimestampMS: timestamp.UnixMilli(), Timestamp: timestamp.Format(time.RFC3339Nano), Model: "test", LatencyMS: &latency, TTFTMS: &ttft, CreatedAtMS: timestamp.UnixMilli()})
			}
			if _, err := repo.InsertBatch(context.Background(), events); err != nil {
				t.Fatal(err)
			}
			filter := AnalyticsFilter{FromMS: base.UnixMilli() + 1, ToMS: base.Add(24 * time.Hour).UnixMilli(), IncludeFailed: true}
			for _, granularity := range []string{"hour", "day"} {
				expected, err := repo.LatencyPercentilesWithFilter(context.Background(), filter, granularity, location)
				if err != nil {
					t.Fatal(err)
				}
				rows, err := repo.GroupedLatencyWithFilter(context.Background(), filter, "timeline", granularity, location)
				if err != nil {
					t.Fatal(err)
				}
				actual := make([]LatencyPercentiles, 0, len(rows))
				for _, row := range rows {
					actual = append(actual, LatencyPercentiles{BucketMS: row.BucketMS, P95LatencyMS: row.P95LatencyMS, P95TTFTMS: row.P95TTFTMS})
				}
				if !reflect.DeepEqual(actual, expected) {
					t.Fatalf("%s mismatch actual=%#v expected=%#v", granularity, actual, expected)
				}
				// The single-scan breakdown buckets and ranks the same samples identically.
				breakdown, err := repo.LatencyBreakdownWithFilter(context.Background(), filter, granularity, location)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(breakdown.Timeline, rows) {
					t.Fatalf("%s breakdown timeline=%#v grouped=%#v", granularity, breakdown.Timeline, rows)
				}
			}
		})
	}
}

func TestLatencyBreakdownMatchesSQLRanks(t *testing.T) {
	db, err := sqliterepo.Open(filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Production scans run with the derived indexes present, so build them here too.
	if err := sqliterepo.RunDerivedStartupMaintenance(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	var indexes int
	if err := db.QueryRow(`select count(*) from sqlite_master where name = 'idx_usage_events_ts_latency_groups_cover'`).Scan(&indexes); err != nil || indexes != 1 {
		t.Fatalf("covering index missing: count=%d err=%v", indexes, err)
	}
	repo := New(db)
	base := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	models := []string{"alpha", "beta", "gamma"}
	// Reasoning suffixes collapse in AnalyticsModel; an unknown alias in parentheses stays.
	suffixes := []string{"", "(high)", "(8192)", "(custom)"}
	providers := []string{"claude", "codex", ""}
	events := make([]usage.Event, 0, 310)
	for i := 0; i < 310; i++ {
		timestamp := base.Add(time.Duration(i) * time.Minute)
		model := models[i%3] + suffixes[(i/3)%4]
		event := usage.Event{EventHash: canonicalTestHash(fmt.Sprint("pair", i)), TimestampMS: timestamp.UnixMilli(), Timestamp: timestamp.Format(time.RFC3339Nano), Model: model, Provider: providers[(i/3)%3], CreatedAtMS: timestamp.UnixMilli()}
		// The provider snapshot wins over the event provider, and requested models alias.
		if i%5 == 0 {
			event.AuthProviderSnapshot = "snapshot"
		}
		if i%4 == 0 {
			event.RequestedModel = "requested-" + model
		}
		// Only suffixed alpha rows carry this provider, so a pair that skips the
		// suffix collapse cannot list it under "alpha".
		if i >= 300 {
			event.Model, event.RequestedModel, event.Provider, event.AuthProviderSnapshot = "alpha(high)", "", "suffix-only", ""
		}
		if i%11 != 0 {
			latency := int64((i * 37) % 500)
			event.LatencyMS = &latency
		}
		if i%7 != 0 {
			ttft := int64((i * 13) % 200)
			event.TTFTMS = &ttft
		}
		events = append(events, event)
	}
	if _, err := repo.InsertBatch(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	filter := AnalyticsFilter{FromMS: base.UnixMilli(), ToMS: base.Add(6 * time.Hour).UnixMilli(), IncludeFailed: true}
	breakdown, err := repo.LatencyBreakdownWithFilter(context.Background(), filter, "day", time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	summary, err := repo.LatencySummaryWithFilter(context.Background(), filter)
	if err != nil {
		t.Fatal(err)
	}
	if breakdown.Overall.P95LatencyMS != summary.P95LatencyMS || breakdown.Overall.P95TTFTMS != summary.P95TTFTMS {
		t.Fatalf("overall %#v differs from summary %#v", breakdown.Overall, summary)
	}
	if got := breakdown.ModelProviders["alpha"]; !reflect.DeepEqual(got, []string{"", "claude", "codex", "snapshot", "suffix-only"}) {
		t.Fatalf("alpha providers = %#v", got)
	}
	for _, model := range []string{"alpha(high)", "alpha(8192)", "requested-alpha(high)"} {
		if _, ok := breakdown.ModelProviders[model]; ok {
			t.Fatalf("model %q kept its reasoning suffix", model)
		}
	}
	if _, ok := breakdown.ModelProviders["beta(custom)"]; !ok {
		t.Fatalf("unknown alias suffix was collapsed: %#v", breakdown.ModelProviders)
	}
	for dimension, actual := range map[string][]GroupedLatency{"model": breakdown.Models, "provider": breakdown.Providers} {
		expected, err := repo.GroupedLatencyWithFilter(context.Background(), filter, dimension, "day", nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(expected) < 2 || !reflect.DeepEqual(actual, expected) {
			t.Fatalf("%s mismatch actual=%#v expected=%#v", dimension, actual, expected)
		}
	}
}
