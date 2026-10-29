package monitoring

import (
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/pricing"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func analyticsRate(entry RegisteredAnalyticsModel, registered bool, prices map[string]store.ModelPrice) AnalyticsModelRate {
	result := AnalyticsModelRate{RegisteredAnalyticsModel: entry, Registered: registered, Status: "unpriced", Source: "unavailable", ContextTiers: []AnalyticsContextRate{}, ServiceTiers: []AnalyticsServiceRate{}}
	// Registration does not authorize a guessed upstream identity for an alias.
	resolved, ok := pricing.ResolveModelRate([]string{entry.ID}, prices)
	if !ok {
		return result
	}
	result.MatchedModel = &resolved.MatchedModel
	price := resolved.Price
	result.InputPerMillion = pricing.ConfiguredRate(price.Prompt, price.PromptConfigured)
	result.OutputPerMillion = pricing.ConfiguredRate(price.Completion, price.CompletionConfigured)
	result.CacheReadPerMillion = pricing.ConfiguredRate(price.CacheRead, price.CacheReadConfigured)
	result.CacheWritePerMillion = pricing.ConfiguredRate(price.CacheCreation, price.CacheCreationConfigured)
	if result.InputPerMillion != nil && result.OutputPerMillion != nil {
		result.Status = "priced"
	}
	result.Source = usage.SanitizeCredentialText(price.Source)
	if result.Source == "" {
		result.Source = "configured-or-owner-fallback"
	}
	if price.UpdatedAtMS > 0 {
		result.UpdatedAtMS = &price.UpdatedAtMS
	}
	for _, tier := range price.ContextTiers {
		effective, ok := model.ModelPriceForContextThreshold(price, tier.ThresholdTokens)
		if !ok {
			continue
		}
		result.ContextTiers = append(result.ContextTiers, AnalyticsContextRate{AboveInputTokens: tier.ThresholdTokens, InputPerMillion: pricing.ConfiguredRate(effective.Prompt, effective.PromptConfigured), OutputPerMillion: pricing.ConfiguredRate(effective.Completion, effective.CompletionConfigured)})
	}
	for _, tier := range price.ServiceTiers {
		name := tier.ServiceTier
		if name == "" {
			name = tier.Mode
		}
		effective, ok := model.ModelPriceForServiceTier(price, name)
		if !ok {
			continue
		}
		result.ServiceTiers = append(result.ServiceTiers, AnalyticsServiceRate{Name: name, InputPerMillion: pricing.ConfiguredRate(effective.Prompt, effective.PromptConfigured), OutputPerMillion: pricing.ConfiguredRate(effective.Completion, effective.CompletionConfigured)})
	}
	return result
}
