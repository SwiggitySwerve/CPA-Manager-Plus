package monitoring

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageevent"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/pricing"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

// ProxAnalytics shares the existing readers and holds one rate-book snapshot
// across all views. Admission bounds concurrent expensive queries per service.
func (s *Service) ProxAnalytics(ctx context.Context, q ProxAnalyticsQuery) (ProxAnalyticsResponse, error) {
	if err := ValidateProxAnalyticsQuery(q); err != nil {
		return ProxAnalyticsResponse{}, err
	}
	select {
	case s.proxQueries <- struct{}{}:
		defer func() { <-s.proxQueries }()
	case <-ctx.Done():
		return ProxAnalyticsResponse{}, ctx.Err()
	}
	var result ProxAnalyticsResponse
	err := s.store.WithModelPriceSnapshot(func() error {
		var err error
		result, err = s.proxAnalytics(ctx, q, false)
		if errors.Is(err, errSupplementaryChanged) {
			result, err = s.proxAnalytics(ctx, q, true)
		}
		return err
	})
	return result, err
}

var errSupplementaryChanged = errors.New("supplementary records arrived during analytics")

func (s *Service) proxAnalytics(ctx context.Context, q ProxAnalyticsQuery, forceRaw bool) (ProxAnalyticsResponse, error) {
	hasSupplementary, err := s.store.UsageEvents.HasSupplementaryEvents(ctx, q.FromMS, q.ToMS)
	if err != nil {
		return ProxAnalyticsResponse{}, err
	}
	rawCoverage, err := s.store.UsageArchives.RawCoverage(ctx, q.FromMS, q.ToMS)
	if err != nil {
		return ProxAnalyticsResponse{}, err
	}
	// Archived rollups do not retain record kind. Use only raw primary events
	// when deletion occurred; never resurrect overlapping costs from aggregates.
	excludeSupplementary := forceRaw || hasSupplementary || rawCoverage.RawDeletedEventCount > 0
	includeFailed := q.Outcome != "success"
	limit := q.Limit
	if limit == 0 {
		limit = 100
	}
	req := Request{BoundedLatency: true, FromMS: q.FromMS, ToMS: q.ToMS, TimeZone: q.TimeZone, SearchQuery: q.Search, Filters: Filters{ExcludeSupplementary: excludeSupplementary, IncludeFailed: &includeFailed, FailedOnly: q.Outcome == "failed"}, Include: Include{Summary: true, SummaryProfile: "compact", Timeline: true, ModelStats: true, FilterSelectors: true, Granularity: q.Granularity, EventsPage: &EventsPage{Limit: limit, BeforeMS: q.BeforeMS, BeforeID: q.BeforeID}}}
	if q.Model != "" {
		req.Filters.Models = []string{q.Model}
	}
	if q.Provider != "" {
		req.Filters.Providers = []string{q.Provider}
	}
	req.SkipTimelinePercentiles = true
	location, err := resolveAnalyticsLocation(q.TimeZone)
	if err != nil {
		return ProxAnalyticsResponse{}, err
	}
	// Overall, timeline and per-group latency ranks come from one raw scan that runs
	// alongside the main read, which therefore skips its own percentile ranking.
	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	filter := buildFilter(req)
	var (
		wait       sync.WaitGroup
		latency    usageevent.LatencyBreakdown
		latencyErr error
	)
	wait.Add(1)
	go func() {
		defer wait.Done()
		latency, latencyErr = s.store.UsageEvents.LatencyBreakdownWithFilter(queryCtx, filter, q.Granularity, location)
	}()
	source, err := s.analytics(ctx, req)
	if err != nil {
		cancel()
	}
	wait.Wait()
	if err != nil {
		return ProxAnalyticsResponse{}, err
	}
	if latencyErr != nil {
		return ProxAnalyticsResponse{}, latencyErr
	}
	if source.Summary == nil || source.Events == nil {
		return ProxAnalyticsResponse{}, errors.New("missing analytics section")
	}
	prices, err := s.store.LoadModelPrices(ctx)
	if err != nil {
		return ProxAnalyticsResponse{}, err
	}
	result := ProxAnalyticsResponse{SchemaVersion: 1, GeneratedAtMS: source.GeneratedAtMS, FromMS: q.FromMS, ToMS: q.ToMS, Granularity: q.Granularity, Timeline: []ProxTimelinePoint{}, Models: []AnalyticsBreakdown{}, Providers: []AnalyticsBreakdown{}, Rates: []AnalyticsModelRate{}, FilterOptions: ProxFilterOptions{Models: []string{}, Providers: []string{}}, Requests: ProxAnalyticsRequests{Items: []AnalyticsRequest{}, TotalCount: source.Events.TotalCount, HasMore: source.Events.HasMore}, Coverage: ProxAnalyticsCoverage{Scope: "retained-usage-events", RawComplete: true, SourceAttribution: "unavailable", PriceBasis: "current-configured-rates", PriceRevision: pricing.PriceRevision(prices), TimingMethod: "mixed-or-unknown", Limitations: []string{
		"Retained usage events are not a complete ingress or client-outcome ledger.",
		"Costs are estimated API-equivalent subtotals at current configured rates; unknown measurement presence and unsupported components remain partial.",
		"Legacy numeric zero is not proof of reported zero. Aggregate usage presence is unknown; missing measurements remain included in retained-event counts.",
		"Known supplementary records are excluded from counts, cost and detail because overlap is unresolved. Legacy record kinds remain unknown; counts are retained primary events, not verified inbound requests.",
		"Historical canonical output and effective tiers are not rewritten; older Gemini output may omit thoughts.",
		"Output speed uses output tokens divided by full execution duration. TTFT methods may differ; no generation-time subtraction is performed.",
		"Views use successive reads; concurrent ingestion can change counts between views or pages.",
	}}}
	if source.Coverage != nil {
		result.Coverage.RawComplete = source.Coverage.RawComplete
		result.Coverage.Limitations = append(result.Coverage.Limitations, source.Coverage.FidelityLimitations...)
	}
	if source.FilterOptions != nil {
		result.FilterOptions.Models = append(result.FilterOptions.Models, source.FilterOptions.Models...)
		result.FilterOptions.Providers = append(result.FilterOptions.Providers, source.FilterOptions.Providers...)
	}
	summary := source.Summary
	result.Summary = metricsFromStats(source.pricingStats, prices)
	result.Summary.Calls = summary.TotalCalls
	result.Summary.SuccessCalls = summary.SuccessCalls
	result.Summary.FailureCalls = summary.FailureCalls
	result.Summary.AverageLatencyMS = summary.AverageLatencyMS
	result.Summary.P95LatencyMS = nullableFloat(latency.Overall.P95LatencyMS.Valid, latency.Overall.P95LatencyMS.Float64)
	result.Summary.P95TTFTMS = nullableFloat(latency.Overall.P95TTFTMS.Valid, latency.Overall.P95TTFTMS.Float64)
	modelGroups := map[string][]store.ModelStat{}
	for _, stat := range source.pricingStats {
		modelGroups[stat.Model] = append(modelGroups[stat.Model], stat)
	}
	for _, key := range sortedAnalyticsKeys(modelGroups) {
		result.Models = append(result.Models, AnalyticsBreakdown{Key: key, Label: key, AnalyticsMetrics: metricsFromStats(modelGroups[key], prices)})
	}
	timelineCosts := map[int64]pricing.Estimate{}
	for _, point := range source.pricingTimeline {
		estimate := pricing.EstimateCost([]string{point.BillingModel, point.Model}, point.ServiceTier, pricingTokensForTimelinePoint(point), prices, nil)
		estimate.Reasons = append(estimate.Reasons, "legacy-record-kind-unknown")
		if estimate.USD != nil {
			estimate.Status = "partial"
		}
		countEstimate(&estimate, point.Calls)
		timelineCosts[point.BucketMS] = pricing.CombineEstimates(timelineCosts[point.BucketMS], estimate)
	}
	bucketLatency := make(map[int64]usageevent.GroupedLatency, len(latency.Timeline))
	for _, bucket := range latency.Timeline {
		bucketLatency[bucket.BucketMS] = bucket
	}
	for _, point := range source.Timeline {
		ranked := bucketLatency[point.BucketMS]
		result.Timeline = append(result.Timeline, ProxTimelinePoint{BucketMS: point.BucketMS, AnalyticsMetrics: AnalyticsMetrics{AnalyticsTokens: analyticsTokens(point.InputTokens, point.OutputTokens, point.CachedTokens, point.CacheReadTokens, point.CacheCreationTokens, point.ReasoningTokens, point.TotalTokens), Calls: point.Calls, SuccessCalls: point.Success, FailureCalls: point.Failure, Cost: timelineCosts[point.BucketMS], AverageLatencyMS: point.AvgLatencyMS, P95LatencyMS: nullableFloat(ranked.P95LatencyMS.Valid, ranked.P95LatencyMS.Float64), P95TTFTMS: nullableFloat(ranked.P95TTFTMS.Valid, ranked.P95TTFTMS.Float64)}})
	}
	// When no raw event in range was archived and every model in it was served by exactly
	// one provider, the provider breakdown is the model stats regrouped. Otherwise group
	// the raw rows by provider; this owner query covers the whole filtered dataset.
	providerOf := map[string]string{}
	regroup := rawCoverage.RawDeletedEventCount == 0
	for _, stat := range source.pricingStats {
		if providers := latency.ModelProviders[stat.Model]; len(providers) == 1 {
			providerOf[stat.Model] = providers[0]
		} else {
			regroup = false
		}
	}
	var providerStats []usageevent.ProviderModelStat
	if regroup {
		for _, stat := range source.pricingStats {
			providerStats = append(providerStats, usageevent.ProviderModelStat{ModelStat: stat, Provider: providerOf[stat.Model]})
		}
	} else if providerStats, err = s.store.UsageEvents.ProviderModelStatsWithFilter(ctx, filter); err != nil {
		return ProxAnalyticsResponse{}, err
	}
	providerGroups := map[string][]store.ModelStat{}
	observed := map[string]RegisteredAnalyticsModel{}
	for _, stat := range providerStats {
		providerGroups[stat.Provider] = append(providerGroups[stat.Provider], stat.ModelStat)
		observed[stat.Provider+"\x00"+stat.Model] = RegisteredAnalyticsModel{Provider: stat.Provider, ID: stat.Model}
	}
	for _, key := range sortedAnalyticsKeys(providerGroups) {
		label := key
		if label == "" {
			label = "Unknown"
		}
		result.Providers = append(result.Providers, AnalyticsBreakdown{Key: key, Label: label, AnalyticsMetrics: metricsFromStats(providerGroups[key], prices)})
	}
	for dimension, groups := range map[string][]usageevent.GroupedLatency{"model": latency.Models, "provider": latency.Providers} {
		target := result.Models
		if dimension == "provider" {
			target = result.Providers
		}
		indices := make(map[string]int, len(target))
		for i, row := range target {
			indices[row.Key] = i
		}
		for _, group := range groups {
			if i, ok := indices[group.Key]; ok {
				target[i].AverageLatencyMS = nullableFloat(group.AverageLatencyMS.Valid, group.AverageLatencyMS.Float64)
				target[i].P95LatencyMS = nullableFloat(group.P95LatencyMS.Valid, group.P95LatencyMS.Float64)
				target[i].P95TTFTMS = nullableFloat(group.P95TTFTMS.Valid, group.P95TTFTMS.Float64)
			}
		}
	}
	if !result.Coverage.RawComplete {
		result.Coverage.Limitations = append(result.Coverage.Limitations, "Provider breakdown and request detail use retained raw rows; archived aggregate history may exceed these views.")
	}
	for _, row := range source.Events.Items {
		projected := proxRequest(row, prices)
		if projected.InstanceID != "" {
			result.Coverage.SourceAttribution = "partial"
		}
		result.Requests.Items = append(result.Requests.Items, projected)
	}
	if source.Events.HasMore {
		result.Requests.NextBeforeMS = &source.Events.NextBeforeMS
		result.Requests.NextBeforeID = &source.Events.NextBeforeID
	}
	seen := map[string]bool{}
	for _, entry := range q.Catalog {
		key := entry.InstanceID + "\x00" + entry.Provider + "\x00" + entry.ID
		if seen[key] {
			continue
		}
		seen[key] = true
		result.Rates = append(result.Rates, analyticsRate(entry, true, prices))
		delete(observed, entry.Provider+"\x00"+entry.ID)
	}
	keys := make([]string, 0, len(observed))
	for key := range observed {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result.Rates = append(result.Rates, analyticsRate(observed[key], false, prices))
	}
	if !excludeSupplementary {
		changed, err := s.store.UsageEvents.HasSupplementaryEvents(ctx, q.FromMS, q.ToMS)
		if err != nil {
			return ProxAnalyticsResponse{}, err
		}
		if changed {
			return ProxAnalyticsResponse{}, errSupplementaryChanged
		}
	}
	return result, nil
}

func sortedAnalyticsKeys(groups map[string][]store.ModelStat) []string {
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func analyticsTokens(input, output, cached, read, write, reasoning, total int64) AnalyticsTokens {
	uncached := max(input-cached-read-write, 0)
	return AnalyticsTokens{InputTokens: input, OutputTokens: output, CacheReadTokens: cached + read, CacheCreationTokens: write, ReasoningTokens: reasoning, TotalTokens: total, UncachedInputTokens: &uncached}
}
func countEstimate(estimate *pricing.Estimate, calls int64) {
	if estimate.USD == nil {
		estimate.UnpricedCalls = calls
	} else {
		estimate.PricedCalls = calls
	}
}
func metricsFromStats(stats []store.ModelStat, prices map[string]store.ModelPrice) AnalyticsMetrics {
	result := AnalyticsMetrics{Cost: pricing.Estimate{Status: "unavailable", Reasons: []string{"no-retained-events"}}}
	var input, output, cached, read, write, reasoning, total int64
	if len(stats) > 0 {
		result.Cost = pricing.Estimate{}
	}
	for _, stat := range stats {
		result.Calls += stat.Calls
		result.SuccessCalls += stat.SuccessCalls
		result.FailureCalls += stat.Calls - stat.SuccessCalls
		input += stat.InputTokens
		output += stat.OutputTokens
		cached += stat.CachedTokens
		read += stat.CacheReadTokens
		write += stat.CacheCreationTokens
		reasoning += stat.ReasoningTokens
		total += stat.TotalTokens
		estimate := pricing.EstimateCost([]string{stat.BillingModel, stat.Model}, stat.ServiceTier, pricingTokensForStat(stat), prices, nil)
		estimate.Reasons = append(estimate.Reasons, "legacy-record-kind-unknown")
		if estimate.USD != nil {
			estimate.Status = "partial"
		}
		countEstimate(&estimate, stat.Calls)
		result.Cost = pricing.CombineEstimates(result.Cost, estimate)
	}
	result.AnalyticsTokens = analyticsTokens(input, output, cached, read, write, reasoning, total)
	return result
}

func proxRequest(row EventRow, prices map[string]store.ModelPrice) AnalyticsRequest {
	metadata := row.AnalyticsMetadata
	tokens := pricing.ModelTokens{InputTokens: row.InputTokens, OutputTokens: row.OutputTokens, CachedTokens: row.CachedTokens, CacheReadTokens: row.CacheReadTokens, CacheCreationTokens: row.CacheCreationTokens}
	analyticsModel := row.AnalyticsModel
	if analyticsModel == "" {
		analyticsModel = usageidentity.AnalyticsModelForRequest(row.Model, row.RequestedModel)
	}
	// Match aggregate pricing: resolved, canonical, then full requested alias.
	names := []string{row.ResolvedModel, analyticsModel, usageidentity.EffectiveRequestedModel(row.Model, row.RequestedModel)}
	if resolved, ok := pricing.ResolveModelPrice(names, "", prices); ok {
		_, tokens.ContextThresholdTokens = model.ModelPriceForContext(resolved.Price, row.InputTokens)
	}
	var long usage.LongContextTokens
	long.AddIfLongContext(tokens.InputTokens, tokens.OutputTokens, tokens.CachedTokens, tokens.CacheReadTokens, tokens.CacheCreationTokens)
	tokens.LongInputTokens = long.LongInputTokens
	tokens.LongOutputTokens = long.LongOutputTokens
	tokens.LongCachedTokens = long.LongCachedTokens
	tokens.LongCacheReadTokens = long.LongCacheReadTokens
	tokens.LongCacheCreationTokens = long.LongCacheCreationTokens
	cost := pricing.EstimateCost(names, row.ServiceTier, tokens, prices, metadata.UsagePresent)
	if (metadata.UsageFields == nil || !metadata.UsageFields.InputTokens || !metadata.UsageFields.OutputTokens || !metadata.UsageFields.CacheReadTokens || !metadata.UsageFields.CacheCreationTokens) && cost.USD != nil {
		cost.Status = "partial"
		cost.Reasons = append(cost.Reasons, "usage-components-unreported")
	}
	if metadata.RecordKind == "supplementary" && cost.USD != nil {
		cost.Status = "partial"
		cost.Reasons = append(cost.Reasons, "supplementary-overlap-unknown")
	}
	result := AnalyticsRequest{AnalyticsRequestTokens: requestTokens(row), ID: row.EventHash, RequestID: row.RequestID, ExecutionID: metadata.ExecutionID, InstanceID: metadata.InstanceID, TimestampMS: row.TimestampMS, Provider: row.AuthProviderSnapshot, Model: row.AnalyticsModel, RequestedModel: row.RequestedModel, ResolvedModel: row.ResolvedModel, ResponseModel: row.ResponseModel, Failed: row.Failed, StatusCode: row.FailStatusCode, LatencyMS: row.LatencyMS, TTFTMS: row.TTFTMS, TimingMethod: metadata.TimingMethod, Cost: cost, Method: row.Method, Path: row.Path, Stream: row.Stream, ServiceTier: row.ServiceTier, RequestedServiceTier: metadata.RequestedServiceTier, EffectiveServiceTier: metadata.EffectiveServiceTier, ReasoningEffort: row.ReasoningEffort, ExecutorType: row.ExecutorType, SessionID: row.SessionID, ParentSessionID: row.ParentSessionID, TraceID: row.HeaderTraceID, ClientIP: row.ClientIP, UserAgent: row.UserAgent, AccountLabel: row.AuthLabelSnapshot, ErrorSummary: "", UsagePresence: metadata.UsagePresence, UsageFields: metadata.UsageFields, RecordKind: metadata.RecordKind, ParentExecutionID: metadata.ParentExecutionID, Timing: metadata.Timing}
	if result.Model == "" {
		result.Model = row.Model
	}
	result.TTFTMS = usage.ReportedAnalyticsTTFT(row.TTFTMS, metadata.TimingMethod)
	if row.LatencyMS != nil && *row.LatencyMS > 0 && result.OutputTokens != nil {
		speed := float64(row.OutputTokens) * 1000 / float64(*row.LatencyMS)
		result.OutputTokensPerSecond = &speed
	}
	if result.TimingMethod == "" {
		result.TimingMethod = "unknown"
	}
	if result.UsagePresence == "" {
		result.UsagePresence = "unknown"
	}
	if result.RecordKind == "" {
		result.RecordKind = "unknown"
	}
	// Re-sanitize public diagnostics; query strings can contain client credentials.
	result.Path = strings.SplitN(result.Path, "?", 2)[0]
	if row.Failed {
		result.ErrorSummary = "Execution failed"
		if row.FailStatusCode != nil && *row.FailStatusCode >= 400 && *row.FailStatusCode <= 599 {
			result.ErrorSummary = fmt.Sprintf("Upstream HTTP %d", *row.FailStatusCode)
		}
	}
	result.UserAgent = usage.SanitizeCredentialText(result.UserAgent)
	return result
}

func requestTokens(row EventRow) AnalyticsRequestTokens {
	fields := usage.AnalyticsUsageFields{}
	if row.AnalyticsMetadata.UsagePresence == "reported" && row.AnalyticsMetadata.UsageFields != nil {
		fields = *row.AnalyticsMetadata.UsageFields
	}
	values := analyticsTokens(row.InputTokens, row.OutputTokens, row.CachedTokens, row.CacheReadTokens, row.CacheCreationTokens, row.ReasoningTokens, row.TotalTokens)
	return AnalyticsRequestTokens{
		InputTokens:         usage.ReportedAnalyticsToken(values.InputTokens, fields.InputTokens),
		OutputTokens:        usage.ReportedAnalyticsToken(values.OutputTokens, fields.OutputTokens),
		CacheReadTokens:     usage.ReportedAnalyticsToken(values.CacheReadTokens, fields.CacheReadTokens),
		CacheCreationTokens: usage.ReportedAnalyticsToken(values.CacheCreationTokens, fields.CacheCreationTokens),
		ReasoningTokens:     usage.ReportedAnalyticsToken(values.ReasoningTokens, fields.ReasoningTokens),
		TotalTokens:         usage.ReportedAnalyticsToken(values.TotalTokens, fields.TotalTokens),
		UncachedInputTokens: usage.ReportedAnalyticsToken(*values.UncachedInputTokens, fields.InputTokens && fields.CacheReadTokens && fields.CacheCreationTokens),
	}
}
