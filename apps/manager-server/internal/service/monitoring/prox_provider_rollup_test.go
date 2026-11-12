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
)

// A provider-filtered Prox view is served by the hourly rollups. Its response must be the
// one the raw path gives, byte for byte, for every filter combination, time zone and range
// length, whether or not the range ends on an hour. Each filter combination runs on a
// rotating third of the ranges; PROX_ROLLUP_EQUIVALENCE=full runs every one on all of them.
func TestProxProviderRollupMatchesRawPath(t *testing.T) {
	db, sqlDB, endMS := proxProviderRollupFixture(t)
	ctx := context.Background()
	service := New(db, true)
	hour := time.Hour.Milliseconds()
	catalog := []RegisteredAnalyticsModel{{InstanceID: "instance", InstanceLabel: "Instance", Provider: "claude", ID: "claude-only"}}

	type view struct{ zone, granularity string }
	views := []view{
		{"UTC", "hour"}, {"UTC", "day"},
		{"America/New_York", "hour"}, {"America/New_York", "day"},
		{"Asia/Kolkata", "hour"}, {"Asia/Kolkata", "day"},
		{"Australia/Lord_Howe", "hour"}, {"Europe/Berlin", "day"},
	}
	// Every range length, ending inside an hour and on one. The unaligned ranges read both
	// partial-hour edges from events; the fixture's last events are past the rollup's coverage.
	type span struct{ fromMS, toMS int64 }
	spans := []span{}
	for _, hours := range []int64{1, 6, 24, 7 * 24, 30 * 24, 31 * 24} {
		aligned := endMS - endMS%hour
		spans = append(spans, span{endMS - hours*hour, endMS}, span{aligned - hours*hour, aligned})
	}
	queries, compared := 0, map[string]int{}
	for _, provider := range []string{"", "claude", "CODEX", "gemini", "nobody"} {
		for _, model := range []string{"", "shared-model"} {
			for _, outcome := range []string{"all", "success", "failed"} {
				for _, search := range []string{"", "needle"} {
					for index, span := range spans {
						if os.Getenv("PROX_ROLLUP_EQUIVALENCE") != "full" && (index/2+queries)%3 != 0 {
							continue
						}
						view := views[(queries+index)%len(views)]
						q := ProxAnalyticsQuery{FromMS: span.fromMS, ToMS: span.toMS, TimeZone: view.zone, Granularity: view.granularity,
							Provider: provider, Model: model, Outcome: outcome, Search: search, Limit: 3, Catalog: catalog}
						if err := ValidateProxAnalyticsQuery(q); err != nil {
							t.Fatalf("query %+v: %v", q, err)
						}
						if eligible := proxRollupEligible(q); eligible != (search == "") {
							t.Fatalf("rollup eligible = %v for %+v", eligible, q)
						}
						raw, rollup := proxResponseJSON(t, ctx, service, q, true), proxResponseJSON(t, ctx, service, q, false)
						if !bytes.Equal(raw, rollup) {
							t.Fatalf("rollup response differs from raw for %+v\nraw:    %s\nrollup: %s", q, raw, rollup)
						}
						var response ProxAnalyticsResponse
						if err := json.Unmarshal(rollup, &response); err != nil {
							t.Fatal(err)
						}
						if response.Summary.Calls > 0 {
							compared[provider]++
						}
					}
					queries++
				}
			}
		}
	}
	for _, provider := range []string{"", "claude", "CODEX", "gemini"} {
		if compared[provider] < len(spans) {
			t.Fatalf("provider %q matched events in only %d compared views", provider, compared[provider])
		}
	}
	if compared["nobody"] != 0 {
		t.Fatalf("unknown provider matched events in %d views", compared["nobody"])
	}

	// The stored provider rows are what the view reads, not a raw scan that agrees with them:
	// a change to one provider's stored rows, and to the core row they refine, shows in that
	// provider's view and in no other.
	q := ProxAnalyticsQuery{FromMS: endMS - endMS%hour - 24*hour, ToMS: endMS - endMS%hour, TimeZone: "UTC", Granularity: "hour", Provider: "claude", Outcome: "all"}
	before := map[string]int64{}
	for _, provider := range []string{"claude", "codex"} {
		q.Provider = provider
		response, err := service.ProxAnalytics(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		before[provider] = response.Summary.Calls
	}
	var bucketMS int64
	var model, billingModel, serviceTier string
	if err := sqlDB.QueryRowContext(ctx, `select bucket_ms, model, billing_model, service_tier
		from usage_pricing_hourly_provider_rollups_v1
		where bucket_ms >= ? and bucket_ms < ? and failed = 0 and provider = 'claude' and auth_provider_snapshot = 'claude'
		order by bucket_ms limit 1`, q.FromMS, q.ToMS).Scan(&bucketMS, &model, &billingModel, &serviceTier); err != nil {
		t.Fatalf("find a stored claude row: %v", err)
	}
	for _, statement := range []string{
		`update usage_hourly_aggregate_v1 set calls = calls + 1000
			where bucket_ms = ? and model = ? and billing_model = ? and service_tier = ? and failed = 0`,
		`update usage_pricing_hourly_provider_rollups_v1 set calls = calls + 1000 where rowid = (
			select rowid from usage_pricing_hourly_provider_rollups_v1
			where bucket_ms = ? and model = ? and billing_model = ? and service_tier = ? and failed = 0
				and provider = 'claude' and auth_provider_snapshot = 'claude' limit 1)`,
	} {
		result, err := sqlDB.ExecContext(ctx, statement, bucketMS, model, billingModel, serviceTier)
		if err != nil {
			t.Fatal(err)
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			t.Fatalf("marked %d rows with %s", changed, statement)
		}
	}
	for provider, want := range map[string]int64{"claude": before["claude"] + 1000, "codex": before["codex"]} {
		q.Provider = provider
		response, err := service.ProxAnalytics(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		if response.Summary.Calls != want {
			t.Fatalf("%s calls = %d, want %d from the stored rows", provider, response.Summary.Calls, want)
		}
	}
}

// A supplementary record in range sends every view to the raw path, which excludes it;
// a provider filter must not put the view back on rollups that count it.
func TestProxProviderFilterExcludesSupplementaryRecords(t *testing.T) {
	db, _, endMS := proxProviderRollupFixture(t)
	ctx := context.Background()
	hour := time.Hour.Milliseconds()
	q := ProxAnalyticsQuery{FromMS: endMS - endMS%hour - 48*hour, ToMS: endMS - endMS%hour, TimeZone: "UTC", Granularity: "hour", Provider: "claude", Outcome: "all", Limit: 3}
	service := New(db, true)
	before, err := service.ProxAnalytics(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	latency := int64(700)
	extra := monitoringEvent("provider-supplementary", q.ToMS-90*60*1000, "claude-only", "auth", "source", false, 100, 10, 0, 0, 110, &latency)
	extra.Provider, extra.AuthProviderSnapshot = "claude", "claude"
	extra.RawJSON = `{"record_kind":"supplementary","parent_execution_id":"execution"}`
	if _, err := db.InsertEvents(ctx, []usage.Event{extra}); err != nil {
		t.Fatal(err)
	}
	catchUpMonitoringArchiveDeleteReadiness(t, ctx, db)
	after, err := service.ProxAnalytics(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if after.Summary.Calls != before.Summary.Calls || after.Requests.TotalCount != before.Requests.TotalCount {
		t.Fatalf("supplementary record counted: calls %d -> %d, requests %d -> %d", before.Summary.Calls, after.Summary.Calls, before.Requests.TotalCount, after.Requests.TotalCount)
	}
	if raw, rollup := proxResponseJSON(t, ctx, service, q, true), proxResponseJSON(t, ctx, service, q, false); !bytes.Equal(raw, rollup) {
		t.Fatalf("responses differ with a supplementary record in range\nraw:    %s\nrollup: %s", raw, rollup)
	}
}

func proxResponseJSON(t *testing.T, ctx context.Context, service *Service, q ProxAnalyticsQuery, forceRaw bool) []byte {
	t.Helper()
	var response ProxAnalyticsResponse
	err := service.store.WithModelPriceSnapshot(func() error {
		var err error
		response, err = service.proxAnalytics(ctx, q, forceRaw)
		return err
	})
	if err != nil {
		t.Fatalf("prox analytics (raw=%v) %+v: %v", forceRaw, q, err)
	}
	response.GeneratedAtMS = 0
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// proxProviderRollupFixture holds 33 days of events ending inside an hour, across both
// daylight-saving changes of autumn 2026. Providers repeat in every combination the filter
// distinguishes: either column alone, both, different case, and a provider whose auth
// snapshot names another. The last events are inserted after the rollups caught up.
func proxProviderRollupFixture(t *testing.T) (*store.Store, *sql.DB, int64) {
	t.Helper()
	sqlDB, err := sqliterepo.Open(filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	db := store.New(sqlDB)
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if err := db.SaveModelPrices(ctx, map[string]store.ModelPrice{
		"shared-model": {
			Prompt: 1, Completion: 2, Cache: 0.1,
			ContextTiers: []store.ModelPriceContextTier{{ThresholdTokens: 100_000, Prompt: 3, PromptConfigured: true}},
		},
		"claude-only": {
			Prompt: 2, Completion: 4,
			ServiceTiers: []store.ModelPriceServiceTier{{Mode: "fast", ServiceTier: "priority", Prompt: 7, PromptConfigured: true}},
		},
		"resolved-model": {Prompt: 5, Completion: 6},
	}); err != nil {
		t.Fatal(err)
	}
	endMS := time.Date(2026, time.November, 3, 14, 17, 23, 456_000_000, time.UTC).UnixMilli()
	startMS := endMS - 33*24*time.Hour.Milliseconds()
	pairs := [][2]string{
		{"claude", "claude"}, {"claude", "claude"}, {"claude", "claude"}, {"codex", "codex"}, {"codex", "codex"},
		{"claude", ""}, {"", "CODEX"}, {"gemini", "claude"}, {"Gemini", ""}, {"", ""},
	}
	random := rand.New(rand.NewSource(20261112))
	events := make([]usage.Event, 0, 900)
	for index := 0; index < cap(events); index++ {
		// Half the events fall in the last three days so the short ranges are populated.
		at := startMS + random.Int63n(endMS-startMS)
		if index%2 == 0 {
			at = endMS - 1 - random.Int63n(3*24*time.Hour.Milliseconds())
		}
		pair := pairs[random.Intn(len(pairs))]
		model := "shared-model"
		switch {
		case random.Intn(3) == 0:
		case pair[1] == "claude" || pair[0] == "claude":
			model = "claude-only"
		case pair[0] == "codex":
			model = "codex-only"
		default:
			model = "other-model"
		}
		input := int64(random.Intn(60_000))
		switch random.Intn(10) {
		case 0:
			input = 0
		case 1:
			input = 120_000 + int64(random.Intn(250_000))
		}
		output := int64(random.Intn(4_000))
		cached := int64(random.Intn(2)) * (input / 4)
		var latency *int64
		if value := int64(random.Intn(60_000)); value%20 != 0 {
			latency = &value
		} else if value%40 == 0 {
			zero := int64(0)
			latency = &zero
		}
		event := monitoringEvent(fmt.Sprintf("provider-rollup-%d", index), at, model, fmt.Sprintf("auth-%d", random.Intn(4)), "source", random.Intn(12) == 0, input, output, int64(random.Intn(300)), cached, 0, latency)
		if input+output > 0 || random.Intn(2) == 0 {
			event.TotalTokens = input + output
		}
		event.Provider, event.AuthProviderSnapshot = pair[0], pair[1]
		event.CacheReadTokens = int64(random.Intn(2)) * int64(random.Intn(500))
		event.CacheCreationTokens = int64(random.Intn(2)) * int64(random.Intn(500))
		if latency != nil && *latency > 0 && random.Intn(4) != 0 {
			ttft := *latency / int64(2+random.Intn(5))
			event.TTFTMS = &ttft
		}
		if event.Failed {
			event.FailStatusCode = []int{429, 500, 0}[random.Intn(3)]
		}
		event.ServiceTier = []string{"", "", "priority", "flex"}[random.Intn(4)]
		if random.Intn(6) == 0 {
			event.ResolvedModel = "resolved-model"
		}
		if model == "shared-model" && random.Intn(4) == 0 {
			event.Model = "shared-model(high)"
		}
		if random.Intn(8) == 0 {
			event.AuthLabelSnapshot = "needle-team"
		}
		if random.Intn(3) != 0 {
			event.RawJSON = `{"record_kind":"execution","usage_presence":"reported","usage_fields":{"input_tokens":true,"output_tokens":true},"ttft_method":"first-byte","instance_id":"instance"}`
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
