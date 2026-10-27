package monitoring

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func TestAccountHistoryClaudeSourceAlias(t *testing.T) {
	for _, scenario := range []string{
		"corroborated", "masked corroborated", "no anchor", "provider conflict", "account conflict", "file collision", "current target collision",
		"mixed missing hash", "mixed conflicting hashes", "hash of mask", "forged masked source", "mixed legacy aliases",
	} {
		t.Run(scenario, func(t *testing.T) {
			st := newMonitoringTestStore(t)
			ctx := context.Background()
			baseMS := int64(1_700_006_000_000)
			target := AccountHistoryTarget{
				RowKey: "fixture-row", AuthFileSnapshot: "current.json", AuthIndex: "fixture-index",
				AuthProviderSnapshot: "claude", AccountSnapshot: "fixture@example.invalid",
			}
			var events []usage.Event
			for i, tokens := range []int64{100_000, 200_000, 200_713} {
				event := monitoringEvent(fmt.Sprintf("claude-missing-%d", i), baseMS+int64(i+1)*1000,
					"claude-fixture", target.AuthIndex, "fixture-source", false, tokens, 0, 0, 0, tokens, nil)
				event.Provider = "claude"
				event.Source = "fix***@example.invalid"
				event.SourceHash = usage.SourceHash(target.AccountSnapshot)
				if scenario == "corroborated" {
					event.Source = target.AccountSnapshot
				}
				event.AccountSnapshot = ""
				events = append(events, event)
			}
			anchor := events[0]
			anchor.EventHash = testCanonicalHash("claude-file-anchor")
			anchor.TimestampMS = baseMS + 4000
			anchor.InputTokens, anchor.TotalTokens = 11, 11
			anchor.AuthFileSnapshot = target.AuthFileSnapshot
			anchor.AuthProviderSnapshot = target.AuthProviderSnapshot
			anchor.AccountSnapshot = target.AccountSnapshot
			if scenario != "no anchor" {
				events = append(events, anchor)
			}
			switch scenario {
			case "provider conflict":
				events[0].Provider = "codex"
			case "account conflict":
				events[0].AuthAccountIDSnapshot = "other-account"
			case "file collision":
				events[0].AuthFileSnapshot = "other.json"
			case "mixed missing hash":
				events[0].SourceHash = ""
			case "mixed conflicting hashes":
				events[0].SourceHash = usage.SourceHash("fixture-other@example.invalid")
			case "hash of mask":
				events[0].SourceHash = usage.SourceHash(events[0].Source)
			case "forged masked source":
				events[0].Source = "other***@example.invalid"
			case "mixed legacy aliases":
				events[0].Source = target.AccountSnapshot
			}
			other := anchor
			other.EventHash = testCanonicalHash("claude-unrelated-index")
			other.AuthIndex, other.AuthFileSnapshot = "other-index", "other.json"
			other.TotalTokens, other.InputTokens = 19, 19
			events = append(events, other)
			if _, err := st.InsertEvents(ctx, events); err != nil {
				t.Fatal(err)
			}
			before, err := st.UsageEvents.ExportJSONL(ctx)
			if err != nil {
				t.Fatal(err)
			}
			otherTarget := target
			otherTarget.RowKey, otherTarget.AuthIndex, otherTarget.AuthFileSnapshot = "other-row", other.AuthIndex, other.AuthFileSnapshot
			req := AccountHistoryRequest{Accounts: []AccountHistoryTarget{target, otherTarget}, CatchUp: true}
			if scenario == "current target collision" {
				collision := target
				collision.RowKey, collision.AuthFileSnapshot = "collision-row", "new-file.json"
				req.Accounts = append(req.Accounts, collision)
			}
			service := New(st)
			for read := 0; read < 2; read++ {
				response, err := service.AccountHistory(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				if len(response.Items) != len(req.Accounts) {
					t.Fatal("missing response rows")
				}
				wantCalls, wantTokens := int64(1), int64(11)
				if scenario == "corroborated" || scenario == "masked corroborated" {
					wantCalls, wantTokens = 4, 500_724
				}
				if scenario == "no anchor" {
					wantCalls, wantTokens = 0, 0
				}
				item := response.Items[0]
				if item.TotalRequests != wantCalls || item.SuccessCalls != wantCalls || item.TotalTokens != wantTokens {
					t.Fatalf("read %d: calls=%d success=%d tokens=%d, want %d/%d/%d", read, item.TotalRequests, item.SuccessCalls, item.TotalTokens, wantCalls, wantCalls, wantTokens)
				}
				if (scenario == "corroborated" || scenario == "masked corroborated") && (item.FirstSeenMS == nil || *item.FirstSeenMS != baseMS+1000 || item.LastSeenMS == nil || *item.LastSeenMS != baseMS+4000) {
					t.Fatal("alias did not preserve history bounds")
				}
				if response.Items[1].TotalRequests != 1 || response.Items[1].TotalTokens != 19 {
					t.Fatal("shared account merged another credential")
				}
				if scenario == "current target collision" && response.Items[2].TotalRequests != 0 {
					t.Fatal("new conflicting credential received legacy history")
				}
				if scenario == "masked corroborated" {
					t.Logf("read=%d restored_calls=%d restored_tokens=%d separate_credential_calls=%d", read, item.TotalRequests-1, item.TotalTokens-11, response.Items[1].TotalRequests)
				}
				req.CatchUp = false
			}
			after, err := st.UsageEvents.ExportJSONL(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("history changed immutable raw events")
			}
			t.Log("raw_export_unchanged=true")
		})
	}
}
