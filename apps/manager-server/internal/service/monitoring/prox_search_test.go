package monitoring

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	sqliterepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/sqlite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

// A search-filtered Prox view reads its matches from the monitoring projection and its
// trigram index. With the projection's coverage at zero every view searches the raw rows
// column by column instead, which is the reference: the response must be the same, byte for
// byte, for every search term, filter, time zone and range length. Each combination runs on
// one range, rotating; PROX_SEARCH_EQUIVALENCE=full runs every one on all of them.
func TestProxSearchMatchesColumnSearch(t *testing.T) {
	db, sqlDB, endMS := proxSearchFixture(t)
	ctx := context.Background()
	service := New(db, true)
	hour := time.Hour.Milliseconds()
	type view struct{ zone, granularity string }
	views := []view{{"UTC", "hour"}, {"America/New_York", "day"}, {"Asia/Kolkata", "hour"}, {"Australia/Lord_Howe", "day"}, {"Europe/Berlin", "hour"}}
	type span struct{ fromMS, toMS int64 }
	spans := []span{}
	for _, hours := range []int64{1, 6, 24, 7 * 24, 30 * 24, 31 * 24} {
		aligned := endMS - endMS%hour
		spans = append(spans, span{endMS - hours*hour, endMS}, span{aligned - hours*hour, aligned})
	}
	searches := []string{
		"needle", "NEEDLE", "Needle-Team", "ne", "n", "claude", "opus", "/v1/chat", "post",
		"o'brien", `"quoted"`, "100%", "a_b", "%", "_", "ünï", "ÜNÏ", "acct-needle", "proj-needle ",
		"team ", "nothing-matches", "req-1", "gpt", "e-t",
	}
	queries := []ProxAnalyticsQuery{}
	index := 0
	for _, search := range searches {
		for _, provider := range []string{"", "claude", "CODEX"} {
			for _, model := range []string{"", "shared-model"} {
				for _, outcome := range []string{"all", "success", "failed"} {
					for spanIndex, span := range spans {
						if os.Getenv("PROX_SEARCH_EQUIVALENCE") != "full" && spanIndex != index%len(spans) {
							continue
						}
						view := views[(index+spanIndex)%len(views)]
						q := ProxAnalyticsQuery{FromMS: span.fromMS, ToMS: span.toMS, TimeZone: view.zone, Granularity: view.granularity,
							Provider: provider, Model: model, Outcome: outcome, Search: search, Limit: 3}
						if err := ValidateProxAnalyticsQuery(q); err != nil {
							t.Fatalf("query %+v: %v", q, err)
						}
						queries = append(queries, q)
					}
					index++
				}
			}
		}
	}
	indexed := make([][]byte, len(queries))
	for i, q := range queries {
		indexed[i] = proxResponseJSON(t, ctx, service, q, false)
	}
	var coverage int64
	if err := sqlDB.QueryRowContext(ctx, `select coverage_event_id from usage_monitoring_rollup_state where rollup_name = 'projection_v1'`).Scan(&coverage); err != nil || coverage == 0 {
		t.Fatalf("projection coverage = %d, %v", coverage, err)
	}
	if _, err := sqlDB.ExecContext(ctx, `update usage_monitoring_rollup_state set coverage_event_id = 0 where rollup_name = 'projection_v1'`); err != nil {
		t.Fatal(err)
	}
	matched := map[string]int{}
	for i, q := range queries {
		reference := proxResponseJSON(t, ctx, service, q, false)
		if !bytes.Equal(indexed[i], reference) {
			t.Fatalf("search response differs from the column search for %+v\nindexed:   %s\nreference: %s", q, indexed[i], reference)
		}
		var response ProxAnalyticsResponse
		if err := json.Unmarshal(reference, &response); err != nil {
			t.Fatal(err)
		}
		if response.Summary.Calls > 0 || len(response.Timeline) > 0 {
			matched[q.Search]++
		}
	}
	for _, search := range searches {
		if want := search == "nothing-matches"; (matched[search] == 0) != want {
			t.Fatalf("search %q matched events in %d views", search, matched[search])
		}
	}
}

// proxSearchFixture holds 33 days of events whose searchable columns carry mixed case, quotes,
// LIKE wildcards, non-ASCII text, a Codex account marker in the project column (which the
// projection's search document leaves out and the raw search reads) and a project ID with
// spaces. The last events are stored after the projection caught up, past its coverage.
func proxSearchFixture(t *testing.T) (*store.Store, *sql.DB, int64) {
	t.Helper()
	sqlDB, err := sqliterepo.Open(filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	db := store.New(sqlDB)
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if err := db.SaveModelPrices(ctx, map[string]store.ModelPrice{
		"shared-model": {Prompt: 1, Completion: 2, Cache: 0.1, ContextTiers: []store.ModelPriceContextTier{{ThresholdTokens: 20_000, Prompt: 2, PromptConfigured: true}}},
		"claude-opus":  {Prompt: 5, Completion: 25},
	}); err != nil {
		t.Fatal(err)
	}
	endMS := time.Date(2026, time.November, 3, 14, 17, 23, 456_000_000, time.UTC).UnixMilli()
	startMS := endMS - 33*24*time.Hour.Milliseconds()
	random := rand.New(rand.NewSource(20261112))
	agents := []string{"claude-cli/2.1 (O'Brien)", `agent "quoted"`, "cli 100% done", "a_b-agent", "Ünïcode-agent ünï", "plain-agent", ""}
	labels := []string{"Needle-Team", "needle", "other", "NEEDLE", ""}
	events := make([]usage.Event, 0, 700)
	for index := 0; index < cap(events); index++ {
		at := startMS + random.Int63n(endMS-startMS)
		if index%2 == 0 {
			at = endMS - 1 - random.Int63n(3*24*time.Hour.Milliseconds())
		}
		model := []string{"shared-model", "claude-opus", "gpt-6"}[random.Intn(3)]
		latency := int64(random.Intn(60_000) + 1)
		event := monitoringEvent(fmt.Sprintf("search-%d", index), at, model, fmt.Sprintf("auth-%d", random.Intn(4)), "source", random.Intn(10) == 0, int64(random.Intn(60_000)), int64(random.Intn(4_000)), 0, 0, 0, &latency)
		event.TotalTokens = event.InputTokens + event.OutputTokens
		event.RequestID = fmt.Sprintf("req-%d", index)
		event.UserAgent = agents[random.Intn(len(agents))]
		event.AuthLabelSnapshot = labels[random.Intn(len(labels))]
		event.Provider, event.AuthProviderSnapshot = "claude", "claude"
		if model == "gpt-6" || random.Intn(5) == 0 {
			event.Provider, event.AuthProviderSnapshot = "codex", "codex"
		}
		switch random.Intn(8) {
		case 0:
			// A historical Codex account marker is raw project text but not projected search text.
			event.AuthProjectIDSnapshot = usageidentity.CodexAccountIDSnapshot("acct-needle")
		case 1:
			event.AuthProjectIDSnapshot = " proj-needle  "
		}
		if random.Intn(3) == 0 {
			ttft := latency / 3
			event.TTFTMS = &ttft
		}
		if event.Failed {
			event.FailStatusCode = 529
			event.FailSummary = "Overloaded team"
		}
		if random.Intn(3) != 0 {
			event.RawJSON = `{"record_kind":"execution","usage_presence":"reported","usage_fields":{"input_tokens":true,"output_tokens":true},"ttft_method":"first-byte"}`
		}
		events = append(events, event)
	}
	covered := len(events) - 40
	if _, err := db.InsertEvents(ctx, events[:covered]); err != nil {
		t.Fatal(err)
	}
	catchUpMonitoringArchiveDeleteReadiness(t, ctx, db)
	if _, err := db.InsertEvents(ctx, events[covered:]); err != nil {
		t.Fatal(err)
	}
	return db, sqlDB, endMS
}
