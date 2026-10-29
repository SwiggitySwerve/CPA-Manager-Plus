package monitoring

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func TestProxAnalyticsWholeScopePagingRatesAndSafeFields(t *testing.T) {
	db := newMonitoringTestStore(t)
	ctx := context.Background()
	base := int64(1778000000000)
	duration := int64(1000)
	if err := db.SaveModelPrices(ctx, map[string]store.ModelPrice{"paid": {Prompt: 2, Completion: 4}, "unused": {Prompt: 5, Completion: 6}, "free": {PromptConfigured: true, CompletionConfigured: true}}); err != nil {
		t.Fatal(err)
	}
	events := []usage.Event{}
	for i, name := range []string{"paid", "paid", "free", "missing", "paid"} {
		event := monitoringEvent("prox-"+string(rune('a'+i)), base+1000, name, "account", "source", i == 1, 1_000_000, 100_000, 0, 0, 1_100_000, &duration)
		event.Provider = "alpha"
		if i == 3 {
			event.Provider = "beta"
		}
		event.RawJSON = `{"instance_id":"real-instance","execution_id":"execution","usage_presence":"reported","usage_fields":{"input_tokens":true,"output_tokens":true,"cache_read_tokens":true,"cache_creation_tokens":true},"ttft_method":"first-byte","record_kind":"execution","authorization":"SECRET_SENTINEL","body":"BODY_SENTINEL"}`
		if i == 4 {
			event.RawJSON = `{"record_kind":"supplementary","parent_execution_id":"execution"}`
		}
		event.Path = "/v1/messages?api_key=PATH_SENTINEL"
		event.AccessTokenSHA256 = "HASH_SENTINEL"
		event.FailBody = "FAIL_BODY_SENTINEL"
		events = append(events, event)
	}
	if _, err := db.InsertEvents(ctx, events); err != nil {
		t.Fatal(err)
	}
	q := ProxAnalyticsQuery{FromMS: base, ToMS: base + 3600000, Granularity: "hour", TimeZone: "UTC", Outcome: "all", Limit: 1, Catalog: []RegisteredAnalyticsModel{{InstanceID: "configured", InstanceLabel: "Instance", Provider: "alpha", ID: "unused"}, {InstanceID: "configured", Provider: "alpha", ID: "never-priced"}}}
	service := New(db)
	got, err := service.ProxAnalytics(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if got.Summary.Calls != 4 || got.Requests.TotalCount != 4 || len(got.Requests.Items) != 1 || !got.Requests.HasMore {
		t.Fatalf("counts summary=%d page=%#v", got.Summary.Calls, got.Requests)
	}
	if len(got.Providers) != 2 || got.Providers[0].Calls != 3 || got.Providers[1].Calls != 1 {
		t.Fatalf("providers=%#v", got.Providers)
	}
	if got.Providers[0].AverageLatencyMS == nil || *got.Providers[0].AverageLatencyMS != 1000 || got.Providers[0].P95LatencyMS == nil {
		t.Fatalf("provider timing missing: %#v", got.Providers[0])
	}
	if got.Summary.Cost.USD == nil || math.Abs(*got.Summary.Cost.USD-4.8) > 1e-9 || got.Summary.Cost.Status != "partial" {
		t.Fatalf("cost=%#v", got.Summary.Cost)
	}
	if len(got.Timeline) != 1 || got.Timeline[0].P95LatencyMS == nil || got.Timeline[0].Cost.USD == nil || math.Abs(*got.Timeline[0].Cost.USD-4.8) > 1e-9 {
		t.Fatalf("timeline=%#v", got.Timeline)
	}
	if got.Rates[0].ID != "unused" || got.Rates[0].Status != "priced" || !got.Rates[0].Registered || got.Rates[1].Status != "unpriced" {
		t.Fatalf("rates=%#v", got.Rates)
	}
	bytes, _ := json.Marshal(got)
	for _, secret := range []string{"SECRET_SENTINEL", "BODY_SENTINEL", "HASH_SENTINEL", "FAIL_BODY_SENTINEL", "PATH_SENTINEL", "raw_json", "api_key_hash", "access_token_sha256"} {
		if strings.Contains(string(bytes), secret) {
			t.Fatalf("unsafe field %s", secret)
		}
	}
	seen := map[string]bool{got.Requests.Items[0].ID: true}
	for got.Requests.HasMore {
		q.BeforeMS = got.Requests.NextBeforeMS
		q.BeforeID = got.Requests.NextBeforeID
		got, err = service.ProxAnalytics(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range got.Requests.Items {
			if seen[item.ID] {
				t.Fatal("duplicate cursor row")
			}
			seen[item.ID] = true
		}
	}
	if len(seen) != 4 {
		t.Fatalf("same timestamp paging lost rows: %d", len(seen))
	}
	q.BeforeMS = nil
	q.BeforeID = nil
	q.Provider = "alpha"
	q.Outcome = "failed"
	q.Search = "paid"
	filtered, err := service.ProxAnalytics(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if filtered.Summary.Calls != 1 || len(filtered.Providers) != 1 || filtered.Providers[0].Calls != 1 || filtered.Summary.Cost.USD == nil || math.Abs(*filtered.Summary.Cost.USD-2.4) > 1e-9 {
		t.Fatalf("filtered=%#v", filtered)
	}
}

func TestProxQueryBoundsAndCancelledAdmission(t *testing.T) {
	valid := ProxAnalyticsQuery{FromMS: 1, ToMS: 1000, TimeZone: "UTC", Granularity: "hour", Outcome: "all"}
	cases := []func(*ProxAnalyticsQuery){func(q *ProxAnalyticsQuery) { q.ToMS = 32 * 24 * 3600000 }, func(q *ProxAnalyticsQuery) { q.Limit = 201 }, func(q *ProxAnalyticsQuery) { q.TimeZone = "Mars/Moon" }, func(q *ProxAnalyticsQuery) { q.Outcome = "unknown" }, func(q *ProxAnalyticsQuery) { q.Search = strings.Repeat("x", 257) }, func(q *ProxAnalyticsQuery) { v := int64(10); q.BeforeMS = &v }, func(q *ProxAnalyticsQuery) { q.Catalog = make([]RegisteredAnalyticsModel, 1001) }}
	for i, mutate := range cases {
		q := valid
		mutate(&q)
		if ValidateProxAnalyticsQuery(q) == nil {
			t.Fatalf("invalid query %d accepted", i)
		}
	}
	service := New(newMonitoringTestStore(t))
	service.proxQueries <- struct{}{}
	service.proxQueries <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := service.ProxAnalytics(ctx, valid); err == nil {
		t.Fatal("cancelled admission succeeded")
	}
}

func TestProxNullableRequestTokens(t *testing.T) {
	unknown := requestTokens(EventRow{InputTokens: 123})
	if unknown.InputTokens == nil || *unknown.InputTokens != 123 || unknown.OutputTokens != nil || unknown.CacheReadTokens != nil {
		t.Fatalf("unknown = %#v", unknown)
	}
	measured := requestTokens(EventRow{AnalyticsMetadata: usage.AnalyticsMetadata{UsagePresence: "reported", UsageFields: &usage.AnalyticsUsageFields{InputTokens: true, OutputTokens: true}}})
	if measured.InputTokens == nil || *measured.InputTokens != 0 || measured.OutputTokens == nil || *measured.OutputTokens != 0 || measured.CacheReadTokens != nil || measured.UncachedInputTokens != nil {
		t.Fatalf("measured = %#v", measured)
	}
}

func TestProxAliasDetailMatchesAggregatePrice(t *testing.T) {
	for _, rateModel := range []string{"alias", "alias(high)"} {
		t.Run(rateModel, func(t *testing.T) {
			db := newMonitoringTestStore(t)
			ctx := context.Background()
			base := int64(1778000000000)
			if err := db.SaveModelPrices(ctx, map[string]store.ModelPrice{rateModel: {Prompt: 2, Completion: 4}}); err != nil {
				t.Fatal(err)
			}
			event := monitoringEvent("alias-detail", base+1, "alias(high)", "account", "source", false, 1_000_000, 100_000, 0, 0, 1_100_000, nil)
			event.ResolvedModel = "unpriced-upstream"
			event.RequestedModel = "alias(high)"
			event.RawJSON = `{"service_tier":"priority","ttft_method":"unavailable"}`
			zero := int64(0)
			event.TTFTMS = &zero
			if _, err := db.InsertEvents(ctx, []usage.Event{event}); err != nil {
				t.Fatal(err)
			}
			result, err := New(db).ProxAnalytics(ctx, ProxAnalyticsQuery{FromMS: base, ToMS: base + 3600000, TimeZone: "UTC", Granularity: "hour", Outcome: "all"})
			if err != nil {
				t.Fatal(err)
			}
			detail := result.Requests.Items[0]
			if detail.Cost.USD == nil || result.Summary.Cost.USD == nil || *detail.Cost.USD != *result.Summary.Cost.USD {
				t.Fatalf("detail %v aggregate %v", detail.Cost, result.Summary.Cost)
			}
			if detail.RequestedServiceTier != "priority" || detail.TTFTMS != nil {
				t.Fatalf("metadata projection=%#v", detail)
			}
		})
	}
}

func TestProxRateSchedulesInheritBaseAndPreserveZero(t *testing.T) {
	price := store.ModelPrice{Prompt: 2, Completion: 4, PromptConfigured: true, CompletionConfigured: true,
		ContextTiers: []store.ModelPriceContextTier{{ThresholdTokens: 1000, Prompt: 3, PromptConfigured: true}, {ThresholdTokens: 2000, CompletionConfigured: true}},
		ServiceTiers: []store.ModelPriceServiceTier{{ServiceTier: "priority", Prompt: 5, PromptConfigured: true}, {Mode: "free-output", CompletionConfigured: true}},
	}
	rate := analyticsRate(RegisteredAnalyticsModel{ID: "alias"}, true, map[string]store.ModelPrice{"alias": price})
	if len(rate.ContextTiers) != 2 || len(rate.ServiceTiers) != 2 {
		t.Fatalf("rates=%#v", rate)
	}
	if rate.ContextTiers[0].OutputPerMillion == nil || *rate.ContextTiers[0].OutputPerMillion != 4 || rate.ContextTiers[1].InputPerMillion == nil || *rate.ContextTiers[1].InputPerMillion != 2 || rate.ContextTiers[1].OutputPerMillion == nil || *rate.ContextTiers[1].OutputPerMillion != 0 {
		t.Fatalf("context=%#v", rate.ContextTiers)
	}
	if rate.ServiceTiers[0].OutputPerMillion == nil || *rate.ServiceTiers[0].OutputPerMillion != 4 || rate.ServiceTiers[1].OutputPerMillion == nil || *rate.ServiceTiers[1].OutputPerMillion != 0 {
		t.Fatalf("service=%#v", rate.ServiceTiers)
	}
}
