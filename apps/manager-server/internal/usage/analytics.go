package usage

import (
	"encoding/json"
	"strings"
)

// AnalyticsMetadata is an allowlist parsed from retained, sanitized envelopes.
// It does not infer a source from account credentials or display model names.
type AnalyticsMetadata struct {
	InstanceID           string
	ExecutionID          string
	TimingMethod         string
	UsagePresent         *bool
	UsagePresence        string
	UsageFields          *AnalyticsUsageFields
	RecordKind           string
	ParentExecutionID    string
	Timing               *AnalyticsTiming
	RequestedServiceTier string
	EffectiveServiceTier string
}
type AnalyticsUsageFields struct {
	InputTokens         bool `json:"input_tokens"`
	OutputTokens        bool `json:"output_tokens"`
	ReasoningTokens     bool `json:"reasoning_tokens"`
	CacheReadTokens     bool `json:"cache_read_tokens"`
	CacheCreationTokens bool `json:"cache_creation_tokens"`
	TotalTokens         bool `json:"total_tokens"`
}
type AnalyticsTiming struct {
	Origin          string `json:"origin"`
	UpstreamStartMS *int64 `json:"upstream_start_ms"`
	FirstResponseMS *int64 `json:"first_response_ms"`
	FirstContentMS  *int64 `json:"first_content_ms"`
	RecordedMS      *int64 `json:"recorded_ms"`
}

func AnalyticsMetadataFromJSON(raw string) AnalyticsMetadata {
	result := AnalyticsMetadata{TimingMethod: "unknown", UsagePresence: "unknown", RecordKind: "unknown"}
	var record map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &record) != nil {
		return result
	}
	safe := func(key string) string {
		var value string
		_ = json.Unmarshal(record[key], &value)
		return truncateUTF8Bytes(SanitizeCredentialText(strings.TrimSpace(value)), 256)
	}
	result.InstanceID = safe("instance_id")
	result.ExecutionID = safe("execution_id")
	result.ParentExecutionID = safe("parent_execution_id")
	result.RequestedServiceTier = safe("request_service_tier")
	if result.RequestedServiceTier == "" {
		result.RequestedServiceTier = safe("service_tier")
	}
	result.EffectiveServiceTier = safe("effective_service_tier")
	if safe("usage_presence") == "reported" {
		result.UsagePresence = "reported"
		yes := true
		result.UsagePresent = &yes
	}
	if fields := record["usage_fields"]; len(fields) > 0 && string(fields) != "null" {
		var parsed AnalyticsUsageFields
		if json.Unmarshal(fields, &parsed) == nil {
			result.UsageFields = &parsed
		}
	}
	switch value := safe("ttft_method"); value {
	case "substantive-content", "first-byte", "fallback-packet", "unavailable":
		result.TimingMethod = value
	}
	var recordKind string
	_ = json.Unmarshal(record["record_kind"], &recordKind)
	switch value := recordKind; value {
	case "execution", "supplementary":
		result.RecordKind = value
	}
	if timing := record["timing"]; len(timing) > 0 && string(timing) != "null" {
		var parsed AnalyticsTiming
		if json.Unmarshal(timing, &parsed) == nil && parsed.Origin == "execution-start" {
			valid := true
			for _, offset := range []*int64{parsed.UpstreamStartMS, parsed.FirstResponseMS, parsed.FirstContentMS, parsed.RecordedMS} {
				if offset != nil && (*offset < 0 || *offset > 31*24*60*60*1000 || (parsed.RecordedMS != nil && *offset > *parsed.RecordedMS)) {
					valid = false
				}
			}
			if parsed.UpstreamStartMS != nil && parsed.FirstResponseMS != nil && *parsed.FirstResponseMS < *parsed.UpstreamStartMS {
				valid = false
			}
			if parsed.FirstResponseMS != nil && parsed.FirstContentMS != nil && *parsed.FirstContentMS < *parsed.FirstResponseMS {
				valid = false
			}
			if valid {
				result.Timing = &parsed
			}
		}
	}
	return result
}

// ApplyCanonicalAnalyticsFields changes only newly persisted derived values.
// Raw payloads, identity hashes, old rows and archive restoration stay intact.
func ApplyCanonicalAnalyticsFields(event *Event) {
	if event == nil || event.PreserveArchiveDerivedFields {
		return
	}
	if b, ok := canonicalTokenBreakdownFromJSON(event.RawJSON); ok && b.TotalTokens == event.TotalTokens && b.Output.ReasoningTokens == event.ReasoningTokens && (event.OutputTokens == b.Output.NonReasoningTokens || event.OutputTokens == b.Output.TotalTokens) {
		event.OutputTokens = b.Output.TotalTokens
	}
	metadata := AnalyticsMetadataFromJSON(event.RawJSON)
	if metadata.EffectiveServiceTier != "" {
		// The outgoing tier replaces the incoming request for billing, while the
		// existing owner still decides whether the provider's response wins.
		event.ServiceTier = EffectiveServiceTier(CacheInputContext{
			ExecutorType: event.ExecutorType, Provider: event.Provider,
			ProviderSnapshot: event.AuthProviderSnapshot, AuthType: event.AuthType,
		}, metadata.EffectiveServiceTier, "", event.ResponseServiceTier)
	}
}

// ReportedAnalyticsTTFT preserves measured zero but never interprets missing
// instrumentation or an ambiguous legacy zero as an instantaneous first token.
func ReportedAnalyticsTTFT(value *int64, method string) *int64 {
	if value == nil || *value < 0 || method == "unavailable" {
		return nil
	}
	if *value == 0 && method != "substantive-content" && method != "first-byte" && method != "fallback-packet" {
		return nil
	}
	return value
}

// SQLPrimaryAnalyticsRecord excludes only an explicit supplementary marker.
// The expression is owned here alongside the envelope parser; callers supply
// a trusted SQL column expression, never user input. Invalid/legacy JSON stays.
func SQLPrimaryAnalyticsRecord(rawColumn string) string {
	return "(case when json_valid(" + rawColumn + ") then coalesce(json_extract(" + rawColumn + ", '$.record_kind'), '') else '' end) <> 'supplementary'"
}

// ReportedAnalyticsToken keeps observed nonzero counters from legacy records,
// while absent zero remains unknown. Negative counters are invalid measurements.
func ReportedAnalyticsToken(value int64, reported bool) *int64 {
	if value < 0 || (value == 0 && !reported) {
		return nil
	}
	return &value
}

const AnalyticsSupplementaryIndex = "idx_usage_events_supplementary_v1"

func SQLSupplementaryAnalyticsRecord(rawColumn string) string {
	return "not (" + SQLPrimaryAnalyticsRecord(rawColumn) + ")"
}
