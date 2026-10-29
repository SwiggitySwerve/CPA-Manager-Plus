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
			}
		})
	}
}
