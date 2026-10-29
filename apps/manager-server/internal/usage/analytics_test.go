package usage

import "testing"

func TestAnalyticsEnvelopeProjection(t *testing.T) {
	metadata := AnalyticsMetadataFromJSON(`{"instance_id":"instance-a","execution_id":"exec-a","record_kind":"supplementary","parent_execution_id":"parent-a","usage_presence":"reported","usage_fields":{"input_tokens":true,"output_tokens":true},"ttft_method":"substantive-content","timing":{"origin":"execution-start","upstream_start_ms":0,"first_response_ms":2,"first_content_ms":4,"recorded_ms":9},"raw_body":"PRIVATE"}`)
	if metadata.InstanceID != "instance-a" || metadata.ParentExecutionID != "parent-a" || metadata.RecordKind != "supplementary" || metadata.UsagePresent == nil || !*metadata.UsagePresent || metadata.UsageFields == nil || !metadata.UsageFields.OutputTokens || metadata.UsageFields.CacheReadTokens || metadata.TimingMethod != "substantive-content" || metadata.Timing == nil || *metadata.Timing.UpstreamStartMS != 0 {
		t.Fatalf("metadata = %#v", metadata)
	}
	legacy := AnalyticsMetadataFromJSON(`{"tokens":{"total_tokens":0},"source":"not-an-instance","ttft_ms":20}`)
	if legacy.InstanceID != "" || legacy.UsagePresent != nil || legacy.TimingMethod != "unknown" {
		t.Fatalf("legacy inferred = %#v", legacy)
	}
	invalid := AnalyticsMetadataFromJSON(`{"timing":{"origin":"execution-start","first_content_ms":20,"recorded_ms":10}}`)
	if invalid.Timing != nil {
		t.Fatal("inconsistent timing accepted")
	}
}

func TestCanonicalAnalyticsOutputAndTierPreserveIdentity(t *testing.T) {
	event := Event{EventHash: "fixed-identity", InputTokens: 100, OutputTokens: 20, ReasoningTokens: 10, TotalTokens: 130, ServiceTier: "default", RawJSON: `{"effective_service_tier":"priority","accounting_version":2,"token_breakdown":{"schema_version":2,"quality":"complete","total_tokens":130,"unclassified_tokens":0,"input":{"total_tokens":100,"uncached_tokens":100,"cache_read_tokens":0,"cache_write_tokens":0},"output":{"total_tokens":30,"non_reasoning_tokens":20,"reasoning_tokens":10}}}`}
	originalRaw := event.RawJSON
	ApplyCanonicalAnalyticsFields(&event)
	if event.OutputTokens != 30 || event.ServiceTier != "priority" || event.EventHash != "fixed-identity" || event.RawJSON != originalRaw {
		t.Fatalf("canonical = %#v", event)
	}
	ApplyCanonicalAnalyticsFields(&event)
	if event.OutputTokens != 30 {
		t.Fatal("reasoning double counted")
	}
	event.PreserveArchiveDerivedFields = true
	event.OutputTokens = 20
	event.ServiceTier = "default"
	ApplyCanonicalAnalyticsFields(&event)
	if event.OutputTokens != 20 || event.ServiceTier != "default" {
		t.Fatal("archive semantics changed")
	}
}

func TestAnalyticsOutboundTierUsesProviderPrecedence(t *testing.T) {
	for _, tc := range []struct{ provider, response, want string }{
		{"openai", "default", "default"}, {"claude", "default", "default"}, {"codex", "default", "priority"}, {"openai", "", "priority"},
	} {
		event := Event{Provider: tc.provider, RequestServiceTier: "priority", ResponseServiceTier: tc.response, RawJSON: `{"effective_service_tier":"priority"}`}
		ApplyCanonicalAnalyticsFields(&event)
		if event.ServiceTier != tc.want {
			t.Errorf("provider %s response %s: got %s want %s", tc.provider, tc.response, event.ServiceTier, tc.want)
		}
	}
}
func TestAnalyticsCurrentRequestedTierAndTTFT(t *testing.T) {
	for raw, want := range map[string]string{`{"service_tier":"priority"}`: "priority", `{"request_service_tier":"flex","service_tier":"priority"}`: "flex"} {
		if got := AnalyticsMetadataFromJSON(raw).RequestedServiceTier; got != want {
			t.Errorf("request tier=%s want %s", got, want)
		}
	}
	for _, tc := range []struct {
		value  int64
		method string
		known  bool
	}{
		{0, "unavailable", false}, {25, "unavailable", false}, {0, "unknown", false}, {0, "", false}, {0, "first-byte", true}, {0, "substantive-content", true}, {0, "fallback-packet", true}, {25, "unknown", true}, {-1, "first-byte", false},
	} {
		got := ReportedAnalyticsTTFT(&tc.value, tc.method)
		if (got != nil) != tc.known || (got != nil && *got != tc.value) {
			t.Errorf("%s/%d=%v", tc.method, tc.value, got)
		}
	}
}
