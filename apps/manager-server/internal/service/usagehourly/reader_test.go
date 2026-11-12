package usagehourly

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	sqliterepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/sqlite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func TestReaderMatchesRawCoreAndTimelines(t *testing.T) {
	db := newReaderTestStore(t)
	ctx := context.Background()
	fromMS := int64(1_800_000_000_000) + 15*60*1000
	toMS := fromMS + 3*hourMS + 20*60*1000
	latency100 := int64(100)
	latency300 := int64(300)

	first := readerEvent("reader-first", fromMS+10*60*1000, "alias-a", false, 100, 20, &latency100)
	first.ResolvedModel = "resolved-a"
	first.ServiceTier = "priority"
	second := readerEvent("reader-second", fromMS+hourMS+5*60*1000, "alias-a", true, 300_000, 30, &latency300)
	second.ResolvedModel = "resolved-a"
	second.ServiceTier = "priority"
	third := readerEvent("reader-third", toMS-5*60*1000, "model-b", false, 300, 40, nil)
	if _, err := db.InsertEvents(ctx, []usage.Event{first, second, third}); err != nil {
		t.Fatalf("insert events: %v", err)
	}

	rawAggregate, err := db.AggregateBetween(ctx, fromMS, toMS)
	if err != nil {
		t.Fatalf("raw aggregate: %v", err)
	}
	rawModels, err := db.ModelStatsBetween(ctx, fromMS, toMS)
	if err != nil {
		t.Fatalf("raw models: %v", err)
	}
	rawDashboardTimeline, err := db.HourlyTimelineBetween(ctx, fromMS, toMS)
	if err != nil {
		t.Fatalf("raw dashboard timeline: %v", err)
	}
	filter := store.AnalyticsFilter{FromMS: fromMS, ToMS: toMS, IncludeFailed: true}
	rawAnalyticsTimeline, err := db.TimelineWithFilter(ctx, filter, "day", time.UTC)
	if err != nil {
		t.Fatalf("raw analytics timeline: %v", err)
	}

	catchUpReaderRollup(t, ctx, db)
	reader := New(db, true)
	snapshot, ok := reader.Load(ctx, fromMS, toMS)
	if !ok {
		t.Fatal("reader did not use rollup")
	}
	if !reflect.DeepEqual(snapshot.Aggregate, rawAggregate) {
		t.Fatalf("aggregate mismatch\nrollup=%#v\nraw=%#v", snapshot.Aggregate, rawAggregate)
	}
	if snapshot.Aggregate.LongInputTokens != second.InputTokens {
		t.Fatalf("long input tokens = %d, want %d", snapshot.Aggregate.LongInputTokens, second.InputTokens)
	}
	if !reflect.DeepEqual(snapshot.ModelStats, rawModels) {
		t.Fatalf("model stats mismatch\nrollup=%#v\nraw=%#v", snapshot.ModelStats, rawModels)
	}
	dashboardTimeline, ok := reader.DashboardTimeline(ctx, snapshot, fromMS, toMS)
	if !ok || !reflect.DeepEqual(dashboardTimeline, rawDashboardTimeline) {
		t.Fatalf("dashboard timeline mismatch\nrollup=%#v\nraw=%#v", dashboardTimeline, rawDashboardTimeline)
	}
	analyticsTimeline, ok := reader.AnalyticsTimeline(ctx, snapshot, "day", time.UTC)
	if !ok || !reflect.DeepEqual(analyticsTimeline, rawAnalyticsTimeline) {
		t.Fatalf("analytics timeline mismatch\nrollup=%#v\nraw=%#v", analyticsTimeline, rawAnalyticsTimeline)
	}

	projected, ok := reader.LoadAnalytics(ctx, filter, "day", time.UTC, true)
	if !ok {
		t.Fatal("analytics reader did not use daily projection")
	}
	if !reflect.DeepEqual(projected.Aggregate, rawAggregate) || !reflect.DeepEqual(projected.ModelStats, rawModels) {
		t.Fatalf("projected core mismatch\nrollup=%#v %#v\nraw=%#v %#v", projected.Aggregate, projected.ModelStats, rawAggregate, rawModels)
	}
	projectedTimeline, ok := reader.AnalyticsTimeline(ctx, projected, "day", time.UTC)
	if !ok || !reflect.DeepEqual(projectedTimeline, rawAnalyticsTimeline) {
		t.Fatalf("projected timeline mismatch\nrollup=%#v\nraw=%#v", projectedTimeline, rawAnalyticsTimeline)
	}
	if _, ok := reader.DashboardTimeline(ctx, projected, fromMS, toMS); ok {
		t.Fatal("daily analytics projection unexpectedly exposed dashboard timeline")
	}

	modelOnly, ok := reader.LoadAnalytics(ctx, filter, "day", time.UTC, false)
	if !ok || !reflect.DeepEqual(modelOnly.Aggregate, rawAggregate) || !reflect.DeepEqual(modelOnly.ModelStats, rawModels) {
		t.Fatalf("model-only projection mismatch\nrollup=%#v %#v\nraw=%#v %#v", modelOnly.Aggregate, modelOnly.ModelStats, rawAggregate, rawModels)
	}
	for _, row := range modelOnly.rows {
		if row.BucketMS != 0 {
			t.Fatalf("model-only projection retained hourly bucket: %#v", row)
		}
	}
	if _, ok := reader.AnalyticsTimeline(ctx, modelOnly, "day", time.UTC); ok {
		t.Fatal("model-only projection unexpectedly exposed analytics timeline")
	}
}

func TestReaderAnalyticsTimelineFallsBackForHalfHourBuckets(t *testing.T) {
	db := newReaderTestStore(t)
	ctx := context.Background()
	fromMS := int64(1_800_000_000_000)
	toMS := fromMS + 3*hourMS
	if _, err := db.InsertEvents(ctx, []usage.Event{
		readerEvent("half-hour-zone", fromMS+10*60*1000, "model-a", false, 1, 2, nil),
	}); err != nil {
		t.Fatalf("insert events: %v", err)
	}
	catchUpReaderRollup(t, ctx, db)
	snapshot, ok := New(db, true).Load(ctx, fromMS, toMS)
	if !ok {
		t.Fatal("reader did not load rollup")
	}
	location, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	reader := New(db, true)
	if reader.CanRepresentAnalyticsTimeline(fromMS, toMS, "hour", location) {
		t.Fatal("half-hour timeline unexpectedly reported as representable")
	}
	if _, ok := reader.AnalyticsTimeline(ctx, snapshot, "hour", location); ok {
		t.Fatal("half-hour timeline unexpectedly used UTC hourly rollup")
	}
}

func TestReaderAnalyticsTimelineRejectsPartialHourAcrossFractionalDayBoundary(t *testing.T) {
	db := newReaderTestStore(t)
	ctx := context.Background()
	location, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	fromMS := time.Date(2027, time.January, 2, 0, 0, 0, 0, location).UnixMilli()
	toMS := fromMS + 2*hourMS
	if _, err := db.InsertEvents(ctx, []usage.Event{
		readerEvent("fractional-day-edge", fromMS+time.Minute.Milliseconds(), "model-a", false, 1, 2, nil),
	}); err != nil {
		t.Fatalf("insert events: %v", err)
	}
	catchUpReaderRollup(t, ctx, db)

	reader := New(db, true)
	snapshot, ok := reader.Load(ctx, fromMS, toMS)
	if !ok {
		t.Fatal("reader did not load aggregate snapshot")
	}
	if reader.CanRepresentAnalyticsTimeline(fromMS, toMS, "day", location) {
		t.Fatal("fractional day boundary unexpectedly reported as representable")
	}
	if _, ok := reader.AnalyticsTimeline(ctx, snapshot, "day", location); ok {
		t.Fatal("partial UTC hour crossing local midnight used hourly aggregate")
	}
}

func TestReaderRejectsUnsupportedAnalyticsFilters(t *testing.T) {
	db := newReaderTestStore(t)
	filter := store.AnalyticsFilter{
		FromMS:        int64(1_800_000_000_000),
		ToMS:          int64(1_800_000_000_000) + 2*hourMS,
		Accounts:      []string{"codex"},
		IncludeFailed: true,
	}
	if SupportsAnalyticsFilter(filter) {
		t.Fatalf("unsupported filter reported as supported: %#v", filter)
	}
	if _, ok := New(db, true).LoadAnalytics(context.Background(), filter, "hour", time.UTC, false); ok {
		t.Fatal("reader silently ignored unsupported analytics filter")
	}
}

func TestReaderPreservesEmptyLiteralDashAndWhitespaceModels(t *testing.T) {
	db := newReaderTestStore(t)
	ctx := context.Background()
	fromMS := int64(1_800_000_000_000)
	toMS := fromMS + 2*hourMS
	empty := readerEvent("empty-model", fromMS+1_000, "", false, 1, 2, nil)
	dash := readerEvent("dash-model", fromMS+2_000, "-", false, 2, 3, nil)
	padded := readerEvent("padded-model", fromMS+3_000, " model ", false, 3, 4, nil)
	padded.ResolvedModel = " resolved "
	padded.ServiceTier = " priority "
	if _, err := db.InsertEvents(ctx, []usage.Event{empty, dash, padded}); err != nil {
		t.Fatalf("insert events: %v", err)
	}
	catchUpReaderRollup(t, ctx, db)

	rawAggregate, err := db.AggregateBetween(ctx, fromMS, toMS)
	if err != nil {
		t.Fatalf("raw aggregate: %v", err)
	}
	rawModels, err := db.ModelStatsBetween(ctx, fromMS, toMS)
	if err != nil {
		t.Fatalf("raw models: %v", err)
	}
	filter := store.AnalyticsFilter{FromMS: fromMS, ToMS: toMS, IncludeFailed: true}
	rawTimeline, err := db.TimelineWithFilter(ctx, filter, "day", time.UTC)
	if err != nil {
		t.Fatalf("raw timeline: %v", err)
	}
	snapshot, ok := New(db, true).LoadAnalytics(ctx, filter, "day", time.UTC, false)
	if !ok {
		t.Fatal("dimension-preserving rollup was unavailable")
	}
	if !reflect.DeepEqual(snapshot.Aggregate, rawAggregate) || !reflect.DeepEqual(snapshot.ModelStats, rawModels) {
		t.Fatalf("dimension-preserving rollup mismatch\nrollup=%#v %#v\nraw=%#v %#v", snapshot.Aggregate, snapshot.ModelStats, rawAggregate, rawModels)
	}
	timelineSnapshot, ok := New(db, true).LoadAnalytics(ctx, filter, "day", time.UTC, true)
	if !ok {
		t.Fatal("dimension-preserving timeline rollup was unavailable")
	}
	rolledTimeline, ok := New(db, true).AnalyticsTimeline(ctx, timelineSnapshot, "day", time.UTC)
	sortTimelinePoints(rawTimeline)
	sortTimelinePoints(rolledTimeline)
	if !ok || !reflect.DeepEqual(rolledTimeline, rawTimeline) {
		t.Fatalf("dimension-preserving timeline mismatch\nrollup=%#v\nraw=%#v", rolledTimeline, rawTimeline)
	}
}

func TestAnalyticsTimelineAccumulatesLatencyBeforeAveraging(t *testing.T) {
	rows := []store.UsagePricingHourlyRow{
		{
			PricingBand:    usage.PricingBand{PricingModel: "model-a", ContextThresholdTokens: model.ModelPriceBaseContextThreshold},
			BucketMS:       0,
			Model:          "model-a",
			BillingModel:   "model-a",
			Calls:          1,
			LatencySumMS:   1,
			LatencySamples: 1,
			// The timeline reads the positive-only pair.
			PositiveLatencySumMS:   1,
			PositiveLatencySamples: 1,
		},
		{
			PricingBand:            usage.PricingBand{PricingModel: "model-a", ContextThresholdTokens: model.ModelPriceBaseContextThreshold},
			BucketMS:               hourMS,
			Model:                  "model-a",
			BillingModel:           "model-a",
			Calls:                  7,
			LatencySumMS:           29,
			LatencySamples:         7,
			PositiveLatencySumMS:   29,
			PositiveLatencySamples: 7,
		},
	}

	points := analyticsTimelineFromPricingRows(rows, "day", time.UTC)
	if len(points) != 1 {
		t.Fatalf("timeline points = %#v, want one point", points)
	}
	if !points[0].AvgLatencyMS.Valid || points[0].AvgLatencyMS.Float64 != 3.75 {
		t.Fatalf("average latency = %#v, want 3.75", points[0].AvgLatencyMS)
	}
}

func sortTimelinePoints(points []store.TimelinePoint) {
	sort.Slice(points, func(i, j int) bool {
		if points[i].BucketMS != points[j].BucketMS {
			return points[i].BucketMS < points[j].BucketMS
		}
		if points[i].Model != points[j].Model {
			return points[i].Model < points[j].Model
		}
		if points[i].BillingModel != points[j].BillingModel {
			return points[i].BillingModel < points[j].BillingModel
		}
		if points[i].PricingModel != points[j].PricingModel {
			return points[i].PricingModel < points[j].PricingModel
		}
		if points[i].ServiceTier != points[j].ServiceTier {
			return points[i].ServiceTier < points[j].ServiceTier
		}
		return points[i].ContextThresholdTokens < points[j].ContextThresholdTokens
	})
}

func TestReaderUsesRawDeltaWhileAggregateIsPending(t *testing.T) {
	db := newReaderTestStore(t)
	ctx := context.Background()
	fromMS := int64(1_800_000_000_000)
	toMS := fromMS + 2*hourMS
	if _, err := db.InsertEvents(ctx, []usage.Event{
		readerEvent("pending", fromMS+1_000, "model-a", false, 1, 2, nil),
	}); err != nil {
		t.Fatalf("insert events: %v", err)
	}
	if _, ok := New(db, false).Load(ctx, fromMS, toMS); ok {
		t.Fatal("disabled reader used rollup")
	}
	snapshot, ok := New(db, true).Load(ctx, fromMS, toMS)
	if !ok || snapshot.Aggregate.TotalCalls != 1 {
		t.Fatalf("pending aggregate did not serve raw delta: ok=%v snapshot=%#v", ok, snapshot)
	}
}

func TestReaderFallbackLogIsRateLimited(t *testing.T) {
	reader := New(newReaderTestStore(t), true)
	reader.logFallback("first")
	first := reader.lastFallbackLogMS.Load()
	if first <= 0 {
		t.Fatalf("first log timestamp = %d", first)
	}
	reader.logFallback("second")
	if got := reader.lastFallbackLogMS.Load(); got != first {
		t.Fatalf("fallback log was not rate limited: first=%d got=%d", first, got)
	}
}

func catchUpReaderRollup(t *testing.T, ctx context.Context, db *store.Store) {
	t.Helper()
	for {
		result, err := db.CatchUpUsageHourlyAggregate(ctx, 100, time.Now().UnixMilli())
		if err != nil {
			t.Fatalf("catch up rollup: %v", err)
		}
		if !result.Pending {
			break
		}
	}
	for {
		result, err := db.CatchUpUsageMonitoringProjection(ctx, 100, time.Now().UnixMilli())
		if err != nil {
			t.Fatalf("catch up projection: %v", err)
		}
		if !result.Pending {
			break
		}
	}
	for {
		result, err := db.CatchUpUsagePricing(ctx, 100, time.Now().UnixMilli())
		if err != nil {
			t.Fatalf("catch up pricing rollup: %v", err)
		}
		if !result.Pending {
			return
		}
	}
}

func newReaderTestStore(t *testing.T) *store.Store {
	t.Helper()
	db, err := store.Open(t.TempDir() + "/usage.sqlite")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func readerEvent(hash string, timestampMS int64, model string, failed bool, inputTokens, outputTokens int64, latencyMS *int64) usage.Event {
	sum := sha256.Sum256([]byte(hash))
	return usage.Event{
		EventHash:    hex.EncodeToString(sum[:]),
		TimestampMS:  timestampMS,
		Timestamp:    time.UnixMilli(timestampMS).UTC().Format(time.RFC3339Nano),
		Model:        model,
		Endpoint:     "POST /v1/chat/completions",
		Method:       "POST",
		Path:         "/v1/chat/completions",
		InputTokens:  inputTokens,
		OutputTokens: outputTokens,
		TotalTokens:  inputTokens + outputTokens,
		LatencyMS:    latencyMS,
		Failed:       failed,
	}
}

func newReaderTestStoreWithDB(t *testing.T) (*store.Store, *sql.DB) {
	t.Helper()
	sqlDB, err := sqliterepo.Open(filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db := store.New(sqlDB)
	t.Cleanup(func() { _ = db.Close() })
	return db, sqlDB
}

func TestReaderClassifiesSnapshotErrors(t *testing.T) {
	ctx := context.Background()
	fromMS := int64(1_800_000_000_000)
	toMS := fromMS + 2*hourMS
	filter := store.AnalyticsFilter{FromMS: fromMS, ToMS: toMS, IncludeFailed: true}

	t.Run("ordinary snapshot error falls back without ReadError", func(t *testing.T) {
		db, sqlDB := newReaderTestStoreWithDB(t)
		reader := New(db, true)
		if _, err := db.InsertEvents(ctx, []usage.Event{readerEvent("event-1", fromMS+1000, "m1", false, 10, 10, nil)}); err != nil {
			t.Fatal(err)
		}
		catchUpReaderRollup(t, ctx, db)
		if _, err := sqlDB.ExecContext(ctx, `drop table usage_hourly_aggregate_v1`); err != nil {
			t.Fatal(err)
		}
		snapshot, ok := reader.LoadAnalytics(ctx, filter, "hour", time.UTC, false)
		if ok {
			t.Fatal("expected snapshot to be abandoned on dropped aggregate table")
		}
		if snapshot.ReadError != nil {
			t.Fatalf("ordinary snapshot error must fallback without ReadError, got: %v", snapshot.ReadError)
		}
	})

	t.Run("pricing coverage incomplete sets fatal ReadError", func(t *testing.T) {
		db, sqlDB := newReaderTestStoreWithDB(t)
		reader := New(db, true)
		if _, err := db.InsertEvents(ctx, []usage.Event{readerEvent("event-2", fromMS+1000, "m1", false, 10, 10, nil)}); err != nil {
			t.Fatal(err)
		}
		catchUpReaderRollup(t, ctx, db)
		if _, err := sqlDB.ExecContext(ctx, `delete from usage_monitoring_rollup_state where rollup_name = 'projection_v1'`); err != nil {
			t.Fatal(err)
		}
		snapshot, ok := reader.LoadAnalytics(ctx, filter, "hour", time.UTC, false, DeletedEdges{Left: true})
		if ok {
			t.Fatal("expected snapshot to fail on incomplete coverage")
		}
		if !errors.Is(snapshot.ReadError, store.ErrUsagePricingCoverageIncomplete) {
			t.Fatalf("expected ErrUsagePricingCoverageIncomplete, got: %v", snapshot.ReadError)
		}
	})

	t.Run("pricing recovery failure sets fatal ReadError", func(t *testing.T) {
		db, sqlDB := newReaderTestStoreWithDB(t)
		reader := New(db, true)
		if _, err := db.InsertEvents(ctx, []usage.Event{readerEvent("event-3", fromMS+1000, "m1", false, 10, 10, nil)}); err != nil {
			t.Fatal(err)
		}
		catchUpReaderRollup(t, ctx, db)
		if _, err := sqlDB.ExecContext(ctx, `delete from usage_pricing_hourly_rollups_v1;
			alter table usage_monitoring_event_projection_v1 rename column normalized_total_input_tokens to unavailable_tokens`); err != nil {
			t.Fatal(err)
		}
		snapshot, ok := reader.LoadAnalytics(ctx, filter, "hour", time.UTC, false)
		if ok {
			t.Fatal("expected snapshot to fail on recovery query error")
		}
		if !errors.Is(snapshot.ReadError, store.ErrUsagePricingRecoveryFailed) {
			t.Fatalf("expected ErrUsagePricingRecoveryFailed, got: %v", snapshot.ReadError)
		}
		if errors.Is(snapshot.ReadError, store.ErrUsagePricingCoverageIncomplete) {
			t.Fatalf("recovery failure must not be classified as ErrUsagePricingCoverageIncomplete: %v", snapshot.ReadError)
		}
	})
}

// A provider filter is the raw analytics predicate: either the provider or the auth
// provider snapshot matches, in lower case. The provider-keyed rollup must return exactly
// what a raw scan with that filter returns, through stored rows, the raw tail past the
// rollup's coverage, and both partial-hour edges.
func TestReaderProviderFilterMatchesRawEvents(t *testing.T) {
	db, sqlDB := newReaderTestStoreWithDB(t)
	ctx := context.Background()
	fromMS := int64(1_800_000_000_000) + 15*60*1000
	toMS := fromMS + 4*hourMS + 20*60*1000
	if err := db.SaveModelPrices(ctx, map[string]store.ModelPrice{"shared": {
		Prompt:       1,
		Completion:   2,
		ContextTiers: []store.ModelPriceContextTier{{ThresholdTokens: 1_000, Prompt: 3, PromptConfigured: true}},
	}}); err != nil {
		t.Fatalf("save prices: %v", err)
	}
	pairs := [][2]string{{"claude", "claude"}, {"codex", "codex"}, {"claude", ""}, {"", "CODEX"}, {"gemini", "claude"}, {"", ""}}
	latency := []int64{0, 120, 950}
	events := []usage.Event{}
	// One event every seven minutes: every pair lands in the left edge, the full hours and
	// the right edge, on both sides of the context tier.
	for index, at := 0, fromMS+60_000; at < toMS; index, at = index+1, at+7*60_000 {
		pair := pairs[index%len(pairs)]
		event := readerEvent("provider-"+strconv.Itoa(index), at, []string{"shared", "codex-only"}[index%2], index%5 == 0, int64(400+index*90), int64(10+index), &latency[index%len(latency)])
		event.Provider, event.AuthProviderSnapshot = pair[0], pair[1]
		events = append(events, event)
	}
	covered := len(events) - 3
	if _, err := db.InsertEvents(ctx, events[:covered]); err != nil {
		t.Fatalf("insert events: %v", err)
	}
	catchUpReaderRollup(t, ctx, db)
	// The last events stay past the rollup's coverage.
	if _, err := db.InsertEvents(ctx, events[covered:]); err != nil {
		t.Fatalf("insert tail events: %v", err)
	}
	for {
		result, err := db.CatchUpUsageHourlyAggregate(ctx, 100, time.Now().UnixMilli())
		if err != nil {
			t.Fatalf("catch up core aggregate: %v", err)
		}
		if !result.Pending {
			break
		}
	}

	reader := New(db, true)
	assertMatchesRaw := func(t *testing.T, filter store.AnalyticsFilter) Snapshot {
		t.Helper()
		rawAggregate, err := db.AggregateWithFilter(ctx, filter)
		if err != nil {
			t.Fatalf("raw aggregate: %v", err)
		}
		rawModels, err := db.ModelStatsWithFilter(ctx, filter, 0)
		if err != nil {
			t.Fatalf("raw models: %v", err)
		}
		rawTimeline, err := db.TimelineWithFilter(ctx, filter, "hour", time.UTC)
		if err != nil {
			t.Fatalf("raw timeline: %v", err)
		}
		snapshot, ok := reader.LoadAnalytics(ctx, filter, "hour", time.UTC, true)
		if !ok {
			t.Fatalf("reader did not serve the provider filter: %v", snapshot.ReadError)
		}
		// The raw filtered aggregate reports the average latency but not its sample count.
		aggregate := snapshot.Aggregate
		aggregate.LatencySamples = 0
		if !reflect.DeepEqual(aggregate, rawAggregate) {
			t.Fatalf("aggregate mismatch\nrollup=%#v\nraw=%#v", aggregate, rawAggregate)
		}
		if !reflect.DeepEqual(snapshot.ModelStats, rawModels) {
			t.Fatalf("model stats mismatch\nrollup=%#v\nraw=%#v", snapshot.ModelStats, rawModels)
		}
		timeline, ok := reader.AnalyticsTimeline(ctx, snapshot, "hour", time.UTC)
		sortTimelinePoints(timeline)
		sortTimelinePoints(rawTimeline)
		if !ok || !reflect.DeepEqual(timeline, rawTimeline) {
			t.Fatalf("timeline mismatch\nrollup=%#v\nraw=%#v", timeline, rawTimeline)
		}
		return snapshot
	}
	filters := map[string]store.AnalyticsFilter{}
	for name, providers := range map[string][]string{
		"claude":         {"claude"},
		"upper case":     {" CODEX "},
		"two providers":  {"claude", "codex"},
		"provider only":  {"gemini"},
		"no such":        {"nobody"},
		"blank is unset": {"  "},
	} {
		filters[name] = store.AnalyticsFilter{FromMS: fromMS, ToMS: toMS, IncludeFailed: true, Providers: providers}
	}
	withModel := filters["claude"]
	withModel.Models = []string{"shared"}
	filters["claude and model"] = withModel
	failedOnly := filters["two providers"]
	failedOnly.FailedOnly = true
	filters["failed only"] = failedOnly
	successOnly := filters["upper case"]
	successOnly.IncludeFailed = false
	filters["success only"] = successOnly
	calls := map[string]int64{}
	for name, filter := range filters {
		t.Run(name, func(t *testing.T) {
			calls[name] = assertMatchesRaw(t, filter).Aggregate.TotalCalls
		})
	}
	if calls["claude"] == 0 || calls["upper case"] == 0 || calls["provider only"] == 0 || calls["no such"] != 0 ||
		calls["blank is unset"] != int64(len(events)) || calls["claude"] == calls["two providers"] {
		t.Fatalf("filters did not select distinct event sets: %v", calls)
	}

	// The stored provider rows are what a provider view reads: a change made to one of them
	// and to the core row it refines shows up, and only for that provider.
	bucketMS := fromMS - fromMS%hourMS + 2*hourMS
	for _, statement := range []string{
		`update usage_hourly_aggregate_v1 set calls = calls + 1000 where rowid = (
			select rowid from usage_hourly_aggregate_v1 where bucket_ms = ? and model = 'codex-only' and failed = 0 limit 1)`,
		`update usage_pricing_hourly_provider_rollups_v1 set calls = calls + 1000 where rowid = (
			select rowid from usage_pricing_hourly_provider_rollups_v1
			where bucket_ms = ? and model = 'codex-only' and failed = 0 and auth_provider_snapshot = 'codex' limit 1)`,
	} {
		result, err := sqlDB.ExecContext(ctx, statement, bucketMS)
		if err != nil {
			t.Fatalf("mark stored rows: %v", err)
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			t.Fatalf("marked %d rows with %s", changed, statement)
		}
	}
	for provider, want := range map[string]int64{"codex": calls["upper case"] + 1000, "claude": calls["claude"]} {
		snapshot, ok := reader.LoadAnalytics(ctx, store.AnalyticsFilter{FromMS: fromMS, ToMS: toMS, IncludeFailed: true, Providers: []string{provider}}, "hour", time.UTC, true)
		if !ok || snapshot.Aggregate.TotalCalls != want {
			t.Fatalf("%s calls from stored rows = %d (ok=%v), want %d", provider, snapshot.Aggregate.TotalCalls, ok, want)
		}
	}

	// Provider rows that no longer add up to the core aggregate are not served, whichever
	// provider is asked for: the view is recomputed from events and still matches a raw scan.
	if _, err := sqlDB.ExecContext(ctx, `update usage_hourly_aggregate_v1 set calls = calls - 1000 where calls > 1000`); err != nil {
		t.Fatalf("restore core row: %v", err)
	}
	for _, damage := range []struct{ name, statement string }{
		{"another provider's row is wrong", `update usage_pricing_hourly_provider_rollups_v1 set calls = calls - 1000 where calls > 1000;
			update usage_pricing_hourly_provider_rollups_v1 set input_tokens = input_tokens + 1 where auth_provider_snapshot = 'codex'`},
		{"another provider's rows are missing", `update usage_pricing_hourly_provider_rollups_v1 set input_tokens = input_tokens - 1 where auth_provider_snapshot = 'codex';
			delete from usage_pricing_hourly_provider_rollups_v1 where provider = '' and auth_provider_snapshot = ''`},
		{"table is empty", `delete from usage_pricing_hourly_provider_rollups_v1`},
	} {
		if _, err := sqlDB.ExecContext(ctx, damage.statement); err != nil {
			t.Fatalf("damage provider rows: %v", err)
		}
		t.Run(damage.name, func(t *testing.T) {
			assertMatchesRaw(t, filters["claude"])
			assertMatchesRaw(t, filters["provider only"])
		})
	}
}
