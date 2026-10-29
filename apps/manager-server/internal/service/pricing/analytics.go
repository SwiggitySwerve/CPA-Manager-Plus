package pricing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
)

// Estimate is a known monetary subtotal, never an invoice or historical tariff.
type Estimate struct {
	USD           *float64 `json:"usd"`
	Status        string   `json:"status"`
	Reasons       []string `json:"reasons"`
	PricedCalls   int64    `json:"priced_calls,omitempty"`
	UnpricedCalls int64    `json:"unpriced_calls,omitempty"`
}

// EstimateCost uses the same resolver and arithmetic as legacy consumers, but
// excludes unsupported dimensions instead of disguising missing data as free.
// nil usagePresent means that measurement presence was not retained.
func EstimateCost(names []string, tier string, tokens ModelTokens, prices map[string]model.ModelPrice, usagePresent *bool) Estimate {
	result := Estimate{Status: "unpriced", Reasons: []string{}}
	if usagePresent != nil && !*usagePresent {
		result.Status = "unavailable"
		result.Reasons = append(result.Reasons, "usage-not-reported")
		return result
	}
	if usagePresent == nil && tokens.InputTokens == 0 && tokens.OutputTokens == 0 && tokens.CachedTokens == 0 && tokens.CacheReadTokens == 0 && tokens.CacheCreationTokens == 0 {
		result.Status = "unavailable"
		result.Reasons = append(result.Reasons, "usage-presence-unknown")
		return result
	}
	resolved, ok := ResolveModelPrice(names, tokens.PricingModel, prices)
	if !ok {
		result.Reasons = append(result.Reasons, "model-rate-unavailable")
		return result
	}
	price := resolved.Price
	if isGPT56Model(resolved.BehaviorModel) {
		price = enrichGPT56BasePrice(resolved.BehaviorModel, price)
	}
	effective := price
	if p, found := activeContextPrice(tokens, price); found {
		effective = p
	} else if p, found := model.ModelPriceForServiceTier(price, tier); found {
		effective = p
	}
	if !configuredPriceValue(effective.Prompt, effective.PromptConfigured) && !configuredPriceValue(effective.Completion, effective.CompletionConfigured) && !configuredPriceValue(effective.CacheRead, effective.CacheReadConfigured) && effective.Cache == 0 {
		result.Reasons = append(result.Reasons, "model-rates-not-configured")
		return result
	}
	if usagePresent == nil {
		result.Reasons = append(result.Reasons, "usage-presence-unknown")
	}
	// There is no TTL tariff dimension in the current price book. Exclude writes.
	if tokens.CacheCreationTokens > 0 {
		result.Reasons = append(result.Reasons, "cache-write-lifetime-unsupported")
		tokens.InputTokens = maxInt64(tokens.InputTokens-tokens.CacheCreationTokens, 0)
		tokens.LongInputTokens = maxInt64(tokens.LongInputTokens-tokens.LongCacheCreationTokens, 0)
		tokens.CacheCreationTokens = 0
		tokens.LongCacheCreationTokens = 0
	}
	if !configuredPriceValue(effective.Prompt, effective.PromptConfigured) && tokens.InputTokens > tokens.CachedTokens+tokens.CacheReadTokens {
		result.Reasons = append(result.Reasons, "input-rate-unavailable")
	}
	if !configuredPriceValue(effective.Completion, effective.CompletionConfigured) && tokens.OutputTokens > 0 {
		result.Reasons = append(result.Reasons, "output-rate-unavailable")
	}
	if tokens.CacheReadTokens > 0 && !configuredPriceValue(effective.CacheRead, effective.CacheReadConfigured) {
		result.Reasons = append(result.Reasons, "cache-read-rate-unavailable")
		tokens.InputTokens = maxInt64(tokens.InputTokens-tokens.CacheReadTokens, 0)
		tokens.LongInputTokens = maxInt64(tokens.LongInputTokens-tokens.LongCacheReadTokens, 0)
		tokens.CacheReadTokens = 0
		tokens.LongCacheReadTokens = 0
	}
	if tokens.CachedTokens > 0 && effective.Cache == 0 {
		result.Reasons = append(result.Reasons, "legacy-cache-rate-presence-unknown")
	}
	normalizedTier := strings.ToLower(strings.TrimSpace(tier))
	if normalizedTier != "" && normalizedTier != "default" && normalizedTier != "standard" {
		if _, found := model.ModelPriceForServiceTier(price, tier); !found {
			result.Reasons = append(result.Reasons, "service-tier-rate-not-explicit")
		}
	}
	value := costForPriceWithServiceTier(resolved.BehaviorModel, tier, tokens, price)
	result.USD = &value
	result.Status = "estimated"
	if len(result.Reasons) > 0 {
		result.Status = "partial"
	} else if value == 0 {
		if configuredPriceValue(effective.Prompt, effective.PromptConfigured) && configuredPriceValue(effective.Completion, effective.CompletionConfigured) && effective.Prompt == 0 && effective.Completion == 0 {
			result.Status = "explicit-zero"
		}
	}
	return result
}

// PriceRevision ignores sync clocks and raw source payloads. Rate-only changes
// change this digest; identical monetary books remain stable across refreshes.
func PriceRevision(prices map[string]model.ModelPrice) string {
	book := make(map[string]model.ModelPrice, len(prices))
	for name, p := range prices {
		p.RawJSON = ""
		p.UpdatedAtMS = 0
		p.SyncedAtMS = nil
		p.Source = ""
		p.SourceModelID = ""
		book[name] = p
	}
	data, _ := json.Marshal(book)
	digest := sha256.Sum256(append([]byte("analytics-rates-v1:"), data...))
	return hex.EncodeToString(digest[:])
}

// ConfiguredRate preserves missing versus explicitly configured zero rates.
func ConfiguredRate(value float64, configured bool) *float64 {
	if !configuredPriceValue(value, configured) {
		return nil
	}
	return &value
}

// CombineEstimates combines known subtotals and preserves unknown components.
func CombineEstimates(left, right Estimate) Estimate {
	if left.Reasons == nil {
		left.Reasons = []string{}
	}
	for _, reason := range right.Reasons {
		found := false
		for _, existing := range left.Reasons {
			if existing == reason {
				found = true
				break
			}
		}
		if !found {
			left.Reasons = append(left.Reasons, reason)
		}
	}
	if right.USD != nil {
		value := *right.USD
		if left.USD != nil {
			value += *left.USD
		}
		left.USD = &value
	}
	left.PricedCalls += right.PricedCalls
	left.UnpricedCalls += right.UnpricedCalls
	if left.USD == nil {
		left.Status = "unpriced"
	} else if len(left.Reasons) > 0 || left.UnpricedCalls > 0 {
		left.Status = "partial"
	} else {
		left.Status = "estimated"
	}
	return left
}

// ResolveModelRate exposes the base rates actually used by the pricing owner.
func ResolveModelRate(names []string, prices map[string]model.ModelPrice) (ResolvedPrice, bool) {
	resolved, ok := ResolveModelPrice(names, "", prices)
	if ok && isGPT56Model(resolved.BehaviorModel) {
		resolved.Price = enrichGPT56BasePrice(resolved.BehaviorModel, resolved.Price)
	}
	return resolved, ok
}
