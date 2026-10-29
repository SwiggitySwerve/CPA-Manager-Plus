package monitoring

import (
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/pricing"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

// ProxAnalyticsQuery is the fixed, panel-authorized server-to-server contract.
// Catalog is supplied by the trusted Prox adapter, never forwarded from browsers.
type ProxAnalyticsQuery struct {
	FromMS      int64                      `json:"from_ms"`
	ToMS        int64                      `json:"to_ms"`
	TimeZone    string                     `json:"time_zone"`
	Granularity string                     `json:"granularity"`
	Provider    string                     `json:"provider,omitempty"`
	Model       string                     `json:"model,omitempty"`
	Outcome     string                     `json:"outcome"`
	Search      string                     `json:"search,omitempty"`
	Limit       int                        `json:"limit,omitempty"`
	BeforeMS    *int64                     `json:"before_ms,omitempty"`
	BeforeID    *int64                     `json:"before_id,omitempty"`
	Catalog     []RegisteredAnalyticsModel `json:"catalog,omitempty"`
}

type RegisteredAnalyticsModel struct {
	InstanceID    string `json:"instance_id"`
	InstanceLabel string `json:"instance_label"`
	Provider      string `json:"provider"`
	ID            string `json:"id"`
}

type AnalyticsTokens struct {
	InputTokens         int64  `json:"input_tokens"`
	OutputTokens        int64  `json:"output_tokens"`
	CacheReadTokens     int64  `json:"cache_read_tokens"`
	CacheCreationTokens int64  `json:"cache_creation_tokens"`
	ReasoningTokens     int64  `json:"reasoning_tokens"`
	TotalTokens         int64  `json:"total_tokens"`
	UncachedInputTokens *int64 `json:"uncached_input_tokens"`
}

type AnalyticsMetrics struct {
	AnalyticsTokens
	Calls            int64            `json:"calls"`
	SuccessCalls     int64            `json:"success_calls"`
	FailureCalls     int64            `json:"failure_calls"`
	Cost             pricing.Estimate `json:"cost"`
	AverageLatencyMS *float64         `json:"average_latency_ms"`
	P95LatencyMS     *float64         `json:"p95_latency_ms"`
	P95TTFTMS        *float64         `json:"p95_ttft_ms"`
}
type ProxTimelinePoint struct {
	AnalyticsMetrics
	BucketMS int64 `json:"bucket_ms"`
}
type AnalyticsBreakdown struct {
	AnalyticsMetrics
	Key   string `json:"key"`
	Label string `json:"label"`
}

// AnalyticsRequestTokens preserves missing measurement versus explicitly reported zero.
type AnalyticsRequestTokens struct {
	InputTokens         *int64 `json:"input_tokens"`
	OutputTokens        *int64 `json:"output_tokens"`
	CacheReadTokens     *int64 `json:"cache_read_tokens"`
	CacheCreationTokens *int64 `json:"cache_creation_tokens"`
	ReasoningTokens     *int64 `json:"reasoning_tokens"`
	TotalTokens         *int64 `json:"total_tokens"`
	UncachedInputTokens *int64 `json:"uncached_input_tokens"`
}

type AnalyticsRequest struct {
	UsagePresence     string                      `json:"usage_presence"`
	UsageFields       *usage.AnalyticsUsageFields `json:"usage_fields"`
	RecordKind        string                      `json:"record_kind"`
	ParentExecutionID string                      `json:"parent_execution_id,omitempty"`
	Timing            *usage.AnalyticsTiming      `json:"timing"`
	AnalyticsRequestTokens
	ID                    string           `json:"id"`
	RequestID             string           `json:"request_id,omitempty"`
	ExecutionID           string           `json:"execution_id,omitempty"`
	InstanceID            string           `json:"instance_id,omitempty"`
	TimestampMS           int64            `json:"timestamp_ms"`
	Provider              string           `json:"provider"`
	Model                 string           `json:"model"`
	RequestedModel        string           `json:"requested_model,omitempty"`
	ResolvedModel         string           `json:"resolved_model,omitempty"`
	ResponseModel         string           `json:"response_model,omitempty"`
	Failed                bool             `json:"failed"`
	StatusCode            *int64           `json:"status_code,omitempty"`
	LatencyMS             *int64           `json:"latency_ms"`
	TTFTMS                *int64           `json:"ttft_ms"`
	OutputTokensPerSecond *float64         `json:"output_tokens_per_second"`
	TimingMethod          string           `json:"timing_method"`
	Cost                  pricing.Estimate `json:"cost"`
	Method                string           `json:"method,omitempty"`
	Path                  string           `json:"path,omitempty"`
	Stream                *bool            `json:"stream,omitempty"`
	ServiceTier           string           `json:"service_tier,omitempty"`
	RequestedServiceTier  string           `json:"requested_service_tier,omitempty"`
	EffectiveServiceTier  string           `json:"effective_service_tier,omitempty"`
	ReasoningEffort       string           `json:"reasoning_effort,omitempty"`
	ExecutorType          string           `json:"executor_type,omitempty"`
	SessionID             string           `json:"session_id,omitempty"`
	ParentSessionID       string           `json:"parent_session_id,omitempty"`
	TraceID               string           `json:"trace_id,omitempty"`
	ClientIP              string           `json:"client_ip,omitempty"`
	UserAgent             string           `json:"user_agent,omitempty"`
	AccountLabel          string           `json:"account_label,omitempty"`
	ErrorSummary          string           `json:"error_summary,omitempty"`
}

type AnalyticsModelRate struct {
	RegisteredAnalyticsModel
	Registered           bool                   `json:"registered"`
	MatchedModel         *string                `json:"matched_model"`
	Status               string                 `json:"status"`
	InputPerMillion      *float64               `json:"input_per_million"`
	OutputPerMillion     *float64               `json:"output_per_million"`
	CacheReadPerMillion  *float64               `json:"cache_read_per_million"`
	CacheWritePerMillion *float64               `json:"cache_write_per_million"`
	Source               string                 `json:"source"`
	UpdatedAtMS          *int64                 `json:"updated_at_ms"`
	ContextTiers         []AnalyticsContextRate `json:"context_tiers"`
	ServiceTiers         []AnalyticsServiceRate `json:"service_tiers"`
}
type AnalyticsContextRate struct {
	AboveInputTokens int64    `json:"above_input_tokens"`
	InputPerMillion  *float64 `json:"input_per_million"`
	OutputPerMillion *float64 `json:"output_per_million"`
}
type AnalyticsServiceRate struct {
	Name             string   `json:"name"`
	InputPerMillion  *float64 `json:"input_per_million"`
	OutputPerMillion *float64 `json:"output_per_million"`
}
type ProxAnalyticsResponse struct {
	SchemaVersion int                   `json:"schema_version"`
	GeneratedAtMS int64                 `json:"generated_at_ms"`
	FromMS        int64                 `json:"from_ms"`
	ToMS          int64                 `json:"to_ms"`
	Granularity   string                `json:"granularity"`
	Coverage      ProxAnalyticsCoverage `json:"coverage"`
	Summary       AnalyticsMetrics      `json:"summary"`
	Timeline      []ProxTimelinePoint   `json:"timeline"`
	Models        []AnalyticsBreakdown  `json:"models"`
	Providers     []AnalyticsBreakdown  `json:"providers"`
	FilterOptions ProxFilterOptions     `json:"filter_options"`
	Requests      ProxAnalyticsRequests `json:"requests"`
	Rates         []AnalyticsModelRate  `json:"rates"`
}
type ProxAnalyticsCoverage struct {
	Scope             string   `json:"scope"`
	RawComplete       bool     `json:"raw_complete"`
	Limitations       []string `json:"limitations"`
	SourceAttribution string   `json:"source_attribution"`
	PriceBasis        string   `json:"price_basis"`
	PriceRevision     string   `json:"price_revision"`
	TimingMethod      string   `json:"timing_method"`
}
type ProxFilterOptions struct {
	Models    []string `json:"models"`
	Providers []string `json:"providers"`
}
type ProxAnalyticsRequests struct {
	Items        []AnalyticsRequest `json:"items"`
	TotalCount   int64              `json:"total_count"`
	HasMore      bool               `json:"has_more"`
	NextBeforeMS *int64             `json:"next_before_ms"`
	NextBeforeID *int64             `json:"next_before_id"`
}
