package usagemonitoring_test

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	monitoringrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usagemonitoring"
	monitoringservice "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/monitoring"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

func TestClaudeAccountWindowLegacyAliasAcrossReadPaths(t *testing.T) {
	day := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC).UnixMilli()
	for _, interval := range []struct {
		name       string
		from, to   int64
		timestamps [4]int64
	}{
		{"short", day + 1000, day + 5000, [4]int64{day + 1000, day + 2000, day + 3000, day + 4999}},
		{"full_day", day, day + testDayMS, [4]int64{day, day + 1000, day + 2000, day + testDayMS - 1}},
		{"full_days_and_edges", day - 1000, day + 2*testDayMS + 1000, [4]int64{day - 1000, day + 3600000, day + testDayMS + 3600000, day + 2*testDayMS + 999}},
	} {
		for _, scenario := range []string{"masked", "plaintext", "hash_conflict", "missing_hash", "alias_conflict", "current_file_conflict", "current_provider_conflict", "current_account_conflict", "stale_alias"} {
			t.Run(interval.name+"/"+scenario, func(t *testing.T) {
				sqlDB, st := newMonitoringRepositoryStore(t)
				ctx := context.Background()
				window := store.AccountWindowUsageQuery{
					FromMS: interval.from, ToMS: interval.to, AuthFileSnapshot: "fixture.json",
					AuthIndex: "fixture-index", AuthProviderSnapshot: "claude", AccountSnapshot: "fixture@example.invalid",
				}
				var events []usage.Event
				for index, tokens := range []int64{100000, 118935, 200000, 200713} {
					event := usage.Event{
						EventHash:   canonicalMonitoringEventHash(fmt.Sprintf("window-event-%d", index)),
						TimestampMS: interval.timestamps[index], Timestamp: time.UnixMilli(interval.timestamps[index]).UTC().Format(time.RFC3339Nano),
						CreatedAtMS: interval.timestamps[index], Model: "claude-fixture", Provider: "claude",
						AuthIndex: window.AuthIndex, Source: "fix***@example.invalid", SourceHash: usage.SourceHash(window.AccountSnapshot),
						InputTokens: tokens, TotalTokens: tokens,
					}
					if scenario == "plaintext" {
						event.Source = window.AccountSnapshot
					}
					if index == 1 {
						event.AuthFileSnapshot = window.AuthFileSnapshot
						event.AuthProviderSnapshot = window.AuthProviderSnapshot
						event.AccountSnapshot = window.AccountSnapshot
					}
					events = append(events, event)
				}
				switch scenario {
				case "hash_conflict", "stale_alias":
					events[2].SourceHash = usage.SourceHash("fixture-other@example.invalid")
				case "missing_hash":
					events[2].SourceHash = ""
				case "alias_conflict":
					events[2].Source = window.AccountSnapshot
				}
				for index, at := range []int64{interval.from - 1, interval.to} {
					edge := events[1]
					edge.EventHash = canonicalMonitoringEventHash(fmt.Sprintf("excluded-boundary-%d", index))
					edge.TimestampMS, edge.CreatedAtMS = at, at
					edge.Timestamp = time.UnixMilli(at).UTC().Format(time.RFC3339Nano)
					edge.InputTokens, edge.TotalTokens = 900000, 900000
					events = append(events, edge)
				}
				other := events[1]
				other.EventHash = canonicalMonitoringEventHash("other-credential")
				other.AuthIndex, other.AuthFileSnapshot = "other-index", "other.json"
				other.InputTokens, other.TotalTokens = 19, 19
				events = append(events, other)
				if _, err := st.InsertEvents(ctx, events); err != nil {
					t.Fatal(err)
				}
				before, err := st.UsageEvents.ExportJSONL(ctx)
				if err != nil {
					t.Fatal(err)
				}
				sibling := window
				sibling.RequestIndex, sibling.AuthFileSnapshot, sibling.AuthIndex = 1, other.AuthFileSnapshot, other.AuthIndex
				repeated := window
				repeated.RequestIndex = 2
				windows := []store.AccountWindowUsageQuery{window, sibling, repeated}
				peer := window
				peer.RequestIndex = 3
				switch scenario {
				case "current_file_conflict":
					peer.AuthFileSnapshot = "new-file.json"
					windows = append(windows, peer)
				case "current_provider_conflict":
					peer.AuthProviderSnapshot = "openai"
					windows = append(windows, peer)
				case "current_account_conflict":
					peer.AccountSnapshot = "other@example.invalid"
					windows = append(windows, peer)
				case "stale_alias":
					windows[0].LegacyAccountKey, _ = usageidentity.AccountKey(usageidentity.Fields{Source: events[0].Source, AuthIndex: window.AuthIndex})
					windows[0].LegacyAccountKeyChecked = true
				}
				wantCalls, wantTokens := int64(1), int64(118935)
				if scenario == "masked" || scenario == "plaintext" {
					wantCalls, wantTokens = 4, 619648
				}
				request := monitoringservice.AccountWindowUsageRequest{}
				for _, target := range windows {
					request.Windows = append(request.Windows, monitoringservice.AccountWindowUsageTarget{
						RowKey: fmt.Sprint(target.RequestIndex), WindowKey: "fixture-window", FromMS: target.FromMS, ToMS: target.ToMS,
						AuthFileSnapshot: target.AuthFileSnapshot, AuthIndex: target.AuthIndex,
						AuthProviderSnapshot: target.AuthProviderSnapshot, AccountSnapshot: target.AccountSnapshot,
					})
				}
				service := monitoringservice.New(st)
				check := func(phase string, wantProjection bool, coverage int64) {
					t.Helper()
					raw, err := st.AccountWindowModelStats(ctx, windows)
					if err != nil {
						t.Fatal(err)
					}
					var calls, tokens, otherCalls, otherTokens, duplicateCalls int64
					for _, row := range raw {
						switch row.RequestIndex {
						case 0:
							calls += row.Calls
							tokens += row.TotalTokens
						case 1:
							otherCalls += row.Calls
							otherTokens += row.TotalTokens
						case 2:
							duplicateCalls += row.Calls
						}
					}
					if calls != wantCalls || tokens != wantTokens || duplicateCalls != wantCalls || otherCalls != 1 || otherTokens != 19 {
						t.Fatalf("%s raw calls=%d tokens=%d duplicate_calls=%d other=%d/%d; want %d/%d", phase, calls, tokens, duplicateCalls, otherCalls, otherTokens, wantCalls, wantTokens)
					}
					projected, state, available, err := st.UsageMonitoringAccountWindowStats(ctx, windows)
					if err != nil {
						t.Fatal(err)
					}
					if available != wantProjection || (available && state.CoverageEventID != coverage) {
						t.Fatalf("%s unexpected projection availability/coverage", phase)
					}
					if available && !reflect.DeepEqual(raw, projected) {
						t.Fatalf("%s projection/raw mismatch", phase)
					}
					response, err := service.AccountWindowUsage(ctx, request)
					if err != nil {
						t.Fatal(err)
					}
					if len(response.Items) != len(windows) || response.Items[0].TotalRequests != wantCalls || response.Items[0].TotalTokens != wantTokens || response.Items[2].TotalRequests != wantCalls || response.Items[1].TotalTokens != 19 {
						t.Fatalf("%s service totals disagree with guarded raw window", phase)
					}
					t.Logf("phase=%s calls=%d tokens=%d restored_calls=%d restored_tokens=%d", phase, calls, tokens, calls-1, tokens-118935)
				}
				check("raw_tail", true, 0)
				for _, catchUp := range []func(context.Context, int, int64) (store.UsageMonitoringCatchUpResult, error){st.CatchUpUsageMonitoringProjection, st.CatchUpUsageMonitoringStats} {
					result, err := catchUp(ctx, 2, time.Now().UnixMilli())
					if err != nil {
						t.Fatal(err)
					}
					if result.Processed != 2 || !result.Pending {
						t.Fatal("partial coverage fixture did not retain a raw tail")
					}
				}
				check("projection_prefix_and_raw_tail", true, 2)
				catchUpMonitoringRepository(t, ctx, st)
				check("complete_projection_and_daily", true, int64(len(events)))
				if _, err := sqlDB.Exec(`update usage_monitoring_rollup_state set schema_version = 0 where rollup_name = ?`, monitoringrepo.ProjectionRollupName); err != nil {
					t.Fatal(err)
				}
				check("service_raw_fallback", false, 0)
				after, err := st.UsageEvents.ExportJSONL(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(before, after) {
					t.Fatal("window reads changed immutable events")
				}
				t.Log("raw_export_unchanged=true; half_open_boundaries_preserved=true")
			})
		}
	}
}
