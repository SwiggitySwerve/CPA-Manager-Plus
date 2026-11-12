package usageevent

import (
	"context"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	sqliterepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/sqlite"
)

// The raw percentile scan runs for every Prox analytics view. It must stay
// index-only: touching the wide usage_events rows made a cold 30-day view take
// 20-30 seconds on a multi-gigabyte database.
func TestLatencyBreakdownScanIsIndexOnly(t *testing.T) {
	db, err := sqliterepo.Open(filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Derived indexes are created while the table is still empty; large databases
	// build them with the offline cleanup-derived command.
	if err := sqliterepo.RunDerivedStartupMaintenance(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	base := AnalyticsFilter{FromMS: 1_000, ToMS: 2_000, IncludeFailed: true}
	for name, filter := range map[string]AnalyticsFilter{
		"unfiltered":             base,
		"success only":           {FromMS: 1_000, ToMS: 2_000},
		"failed only":            {FromMS: 1_000, ToMS: 2_000, IncludeFailed: true, FailedOnly: true},
		"provider":               {FromMS: 1_000, ToMS: 2_000, IncludeFailed: true, Providers: []string{"claude"}},
		"model":                  {FromMS: 1_000, ToMS: 2_000, IncludeFailed: true, Models: []string{"claude-opus-5-5"}},
		"supplementary excluded": {FromMS: 1_000, ToMS: 2_000, IncludeFailed: true, ExcludeSupplementary: true},
	} {
		t.Run(name, func(t *testing.T) {
			query, args := latencyBreakdownQuery(filter)
			rows, err := db.Query("explain query plan "+query, args...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			plan := []string{}
			covered := false
			for rows.Next() {
				var id, parent, unused int
				var detail string
				if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
					t.Fatal(err)
				}
				plan = append(plan, detail)
				covered = covered || strings.Contains(detail, "COVERING INDEX idx_usage_events_ts_latency_groups_cover")
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !covered {
				t.Fatalf("latency scan is not served by the covering index; plan=%v", plan)
			}
		})
	}
}

// nearestRank must return exactly what sorting every sample would, including for
// duplicate-heavy, tiny and already-ordered inputs.
func TestNearestRankMatchesFullSort(t *testing.T) {
	random := rand.New(rand.NewSource(1))
	shapes := map[string]func(n int) []float64{
		"random": func(n int) []float64 {
			return fill(n, func(int) float64 { return float64(random.Intn(1_000_000) + 1) })
		},
		"duplicates": func(n int) []float64 { return fill(n, func(int) float64 { return float64(random.Intn(4) + 1) }) },
		"constant":   func(n int) []float64 { return fill(n, func(int) float64 { return 7 }) },
		"ascending":  func(n int) []float64 { return fill(n, func(i int) float64 { return float64(i + 1) }) },
		"descending": func(n int) []float64 { return fill(n, func(i int) float64 { return float64(n - i) }) },
	}
	for name, shape := range shapes {
		for n := 1; n <= 300; n++ {
			values := shape(n)
			sorted := append([]float64(nil), values...)
			sort.Float64s(sorted)
			want := sorted[(n*95+99)/100-1]
			if got := nearestRank(values); got != want {
				t.Fatalf("%s n=%d got %v want %v", name, n, got, want)
			}
		}
	}
	values := shapes["random"](200_000)
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if got, want := nearestRank(values), sorted[(len(sorted)*95+99)/100-1]; got != want {
		t.Fatalf("large got %v want %v", got, want)
	}
}

func fill(n int, value func(int) float64) []float64 {
	values := make([]float64, n)
	for i := range values {
		values[i] = value(i)
	}
	return values
}
