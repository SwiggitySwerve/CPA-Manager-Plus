package pricing

import (
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"math"
	"testing"
)

func TestAnalyticsEstimateMissingFreeAndMeasuredZero(t *testing.T) {
	yes := true
	prices := map[string]model.ModelPrice{"free": {PromptConfigured: true, CompletionConfigured: true}, "empty": {}, "paid": {Prompt: 2, Completion: 4, CacheRead: 0.5, CacheReadConfigured: true}}
	for _, tc := range []struct {
		name, status string
		usd          *float64
	}{{"missing", "unpriced", nil}, {"empty", "unpriced", nil}, {"free", "explicit-zero", new(float64)}} {
		got := EstimateCost([]string{tc.name}, "", ModelTokens{InputTokens: 10, OutputTokens: 20}, prices, &yes)
		if got.Status != tc.status || (got.USD == nil) != (tc.usd == nil) {
			t.Fatalf("%s: %#v", tc.name, got)
		}
	}
	unknown := EstimateCost([]string{"paid"}, "", ModelTokens{}, prices, nil)
	if unknown.Status != "unavailable" || unknown.USD != nil {
		t.Fatalf("unknown zero = %#v", unknown)
	}
	measured := EstimateCost([]string{"paid"}, "", ModelTokens{}, prices, &yes)
	if measured.Status != "estimated" || measured.USD == nil || *measured.USD != 0 {
		t.Fatalf("measured zero = %#v", measured)
	}
	// Writes are omitted, not reclassified as uncached input. Failures use the
	// same arithmetic; outcomes never zero billable work.
	partial := EstimateCost([]string{"paid"}, "", ModelTokens{InputTokens: 1_000_000, CacheCreationTokens: 200_000, CacheReadTokens: 300_000, OutputTokens: 100_000}, prices, &yes)
	if partial.Status != "partial" || partial.USD == nil || math.Abs(*partial.USD-1.55) > 1e-10 {
		t.Fatalf("partial = %#v", partial)
	}
}

func TestAnalyticsPriceRevisionAndResolution(t *testing.T) {
	prices := map[string]model.ModelPrice{"upstream": {Prompt: 2, Completion: 4}, "alias": {Prompt: 7, Completion: 8}}
	resolved, ok := ResolveModelPrice([]string{"upstream", "alias"}, "", prices)
	if !ok || resolved.MatchedModel != "upstream" {
		t.Fatalf("resolved = %#v", resolved)
	}
	before := PriceRevision(prices)
	updated := prices["upstream"]
	updated.UpdatedAtMS = 100
	updated.RawJSON = `{"untrusted":"ignored"}`
	prices["upstream"] = updated
	if PriceRevision(prices) != before {
		t.Fatal("sync metadata changed monetary revision")
	}
	updated.Prompt = 3
	prices["upstream"] = updated
	if PriceRevision(prices) == before {
		t.Fatal("rate edit did not change revision")
	}
}
