package usageevent

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

type GroupedLatency struct {
	Key              string
	BucketMS         int64
	AverageLatencyMS sql.NullFloat64
	P95LatencyMS     sql.NullFloat64
	P95TTFTMS        sql.NullFloat64
}

// GroupedLatencyWithFilter ranks values in SQLite. Go retains only one result
// per group, never an unbounded slice of latency samples or raw envelopes.
func (r *repository) GroupedLatencyWithFilter(ctx context.Context, filter AnalyticsFilter, dimension, granularity string, location *time.Location) ([]GroupedLatency, error) {
	expression := ""
	switch dimension {
	case "model":
		expression = usageidentity.SQLRequestAnalyticsModelExpression("model", "requested_model")
	case "provider":
		expression = "coalesce(nullif(auth_provider_snapshot, ''), provider, '')"
	case "timeline":
		if location == nil {
			location = time.UTC
		}
		if filter.ToMS-filter.FromMS > 31*24*60*60*1000 {
			return nil, errors.New("latency range exceeds 31 days")
		}
		var b strings.Builder
		b.WriteString("case")
		cursor := time.UnixMilli(filter.FromMS).In(location)
		for cursor.UnixMilli() < filter.ToMS {
			bucket := usage.AnalyticsBucketMS(cursor.UnixMilli(), granularity, location)
			// Bound at both the next wall-clock hour/day and a zone transition.
			// Half-hour DST changes must not absorb the following hour's samples.
			elapsed := time.Duration(cursor.Minute())*time.Minute + time.Duration(cursor.Second())*time.Second + time.Duration(cursor.Nanosecond())
			next := cursor.Add(time.Hour - elapsed)
			if granularity == "day" {
				next = time.Date(cursor.Year(), cursor.Month(), cursor.Day()+1, 0, 0, 0, 0, location)
			}
			_, zoneEnd := cursor.ZoneBounds()
			if !zoneEnd.IsZero() && zoneEnd.After(cursor) && zoneEnd.Before(next) {
				next = zoneEnd
			}
			if !next.After(cursor) {
				return nil, errors.New("invalid analytics bucket boundary")
			}
			b.WriteString(" when timestamp_ms < " + strconv.FormatInt(next.UnixMilli(), 10) + " then '" + strconv.FormatInt(bucket, 10) + "'")
			cursor = next
		}
		b.WriteString(" else '' end")
		expression = b.String()
	default:
		return nil, errors.New("unsupported latency dimension")
	}
	where, args := analyticsWhere(filter)
	query := `with filtered as materialized (
 select ` + expression + ` as group_key, latency_ms, ttft_ms from usage_events ` + where + `
 ), samples as (
 select group_key, 'latency' as kind, latency_ms as value from filtered where latency_ms > 0
 union all select group_key, 'ttft' as kind, ttft_ms as value from filtered where ttft_ms > 0
 ), ranked as (
 select group_key,kind,value,
 row_number() over(partition by group_key,kind order by value) as sample_number,
 count(*) over(partition by group_key,kind) as sample_count,
 avg(value) over(partition by group_key,kind) as mean
 from samples
 ) select group_key,kind,value,mean from ranked
 where sample_number=((sample_count*95)+99)/100 order by group_key,kind`
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []GroupedLatency{}
	for rows.Next() {
		var key, kind string
		var value, mean float64
		if err := rows.Scan(&key, &kind, &value, &mean); err != nil {
			return nil, err
		}
		if len(result) == 0 || result[len(result)-1].Key != key {
			group := GroupedLatency{Key: key}
			if dimension == "timeline" {
				group.BucketMS, _ = strconv.ParseInt(key, 10, 64)
			}
			result = append(result, group)
		}
		current := &result[len(result)-1]
		if kind == "latency" {
			current.AverageLatencyMS = sql.NullFloat64{Float64: mean, Valid: true}
			current.P95LatencyMS = sql.NullFloat64{Float64: value, Valid: true}
		} else {
			current.P95TTFTMS = sql.NullFloat64{Float64: value, Valid: true}
		}
	}
	return result, rows.Err()
}

// LatencyBreakdown holds latency ranks for the whole filtered range and for each
// timeline bucket, model and provider, plus the providers seen for each model.
type LatencyBreakdown struct {
	Overall        GroupedLatency
	Timeline       []GroupedLatency
	Models         []GroupedLatency
	Providers      []GroupedLatency
	ModelProviders map[string][]string
}

// LatencyBreakdownWithFilter reads the filtered rows once and ranks latency and TTFT in Go
// for the whole range and for each timeline bucket, model and provider. Each rank is
// exactly what LatencySummaryWithFilter or GroupedLatencyWithFilter returns, at a fraction
// of the cost on wide ranges: SQLite window ranking sorts every sample again per query.
func (r *repository) LatencyBreakdownWithFilter(ctx context.Context, filter AnalyticsFilter, granularity string, location *time.Location) (LatencyBreakdown, error) {
	if location == nil {
		location = time.UTC
	}
	query, args := latencyBreakdownQuery(filter)
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return LatencyBreakdown{}, err
	}
	defer rows.Close()
	overallGroup := &latencySamples{}
	overall := map[string]*latencySamples{"": overallGroup}
	byBucket, byModel, byProvider := map[string]*latencySamples{}, map[string]*latencySamples{}, map[string]*latencySamples{}
	pairs := map[string]map[string]bool{}
	// A request has few distinct model/provider pairs and consecutive rows share a bucket,
	// so resolve each pair's groups once and reuse the last bucket instead of hashing
	// and formatting every row.
	type pairGroups struct{ model, provider *latencySamples }
	resolved := map[[2]string]pairGroups{}
	var bucketGroup *latencySamples
	var bucketMS int64
	for rows.Next() {
		var timestampMS int64
		var effectiveModel, provider string
		var latency, ttft sql.NullFloat64
		if err := rows.Scan(&timestampMS, &effectiveModel, &provider, &latency, &ttft); err != nil {
			return LatencyBreakdown{}, err
		}
		if next := usage.AnalyticsBucketMS(timestampMS, granularity, location); bucketGroup == nil || next != bucketMS {
			bucketMS = next
			bucketGroup = latencyGroup(byBucket, strconv.FormatInt(bucketMS, 10))
		}
		groups, ok := resolved[[2]string{effectiveModel, provider}]
		if !ok {
			// The query returns the effective requested model; normalizing it here, once per
			// distinct value, replaces a SQLite-to-Go function call on every row.
			model := usageidentity.AnalyticsModel(effectiveModel)
			groups = pairGroups{latencyGroup(byModel, model), latencyGroup(byProvider, provider)}
			resolved[[2]string{effectiveModel, provider}] = groups
			if pairs[model] == nil {
				pairs[model] = map[string]bool{}
			}
			pairs[model][provider] = true
		}
		for _, group := range [4]*latencySamples{overallGroup, bucketGroup, groups.model, groups.provider} {
			if latency.Valid && latency.Float64 > 0 {
				group.latency = append(group.latency, latency.Float64)
			}
			if ttft.Valid && ttft.Float64 > 0 {
				group.ttft = append(group.ttft, ttft.Float64)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return LatencyBreakdown{}, err
	}
	result := LatencyBreakdown{Timeline: rankLatencyGroups(byBucket), Models: rankLatencyGroups(byModel), Providers: rankLatencyGroups(byProvider), ModelProviders: map[string][]string{}}
	for i := range result.Timeline {
		result.Timeline[i].BucketMS, _ = strconv.ParseInt(result.Timeline[i].Key, 10, 64)
	}
	if ranked := rankLatencyGroups(overall); len(ranked) > 0 {
		result.Overall = ranked[0]
	}
	for model, providers := range pairs {
		for provider := range providers {
			result.ModelProviders[model] = append(result.ModelProviders[model], provider)
		}
		sort.Strings(result.ModelProviders[model])
	}
	return result, nil
}

// latencyBreakdownQuery reads only columns held by idx_usage_events_ts_latency_groups_cover.
func latencyBreakdownQuery(filter AnalyticsFilter) (string, []any) {
	where, args := analyticsWhere(filter)
	return `select timestamp_ms, ` + usageidentity.SQLEffectiveRequestedModelExpression("model", "requested_model") + `, coalesce(nullif(auth_provider_snapshot, ''), provider, ''), latency_ms, ttft_ms from usage_events ` + where, args
}

type latencySamples struct{ latency, ttft []float64 }

func latencyGroup(groups map[string]*latencySamples, key string) *latencySamples {
	group := groups[key]
	if group == nil {
		group = &latencySamples{}
		groups[key] = group
	}
	return group
}

// nearestRank returns the value a full sort would place at ((n*95)+99)/100 counted from
// one, by quickselect: ranking only needs that one value, and the sort of every sample
// per group was the largest remaining cost of a 30-day view. It reorders values.
func nearestRank(values []float64) float64 {
	k := (len(values)*95+99)/100 - 1
	lo, hi := 0, len(values)-1
	for lo < hi {
		pivot := medianOfThree(values[lo], values[lo+(hi-lo)/2], values[hi])
		// Three-way partition keeps duplicate-heavy latency samples linear.
		below, i, above := lo, lo, hi
		for i <= above {
			switch {
			case values[i] < pivot:
				values[below], values[i] = values[i], values[below]
				below++
				i++
			case values[i] > pivot:
				values[i], values[above] = values[above], values[i]
				above--
			default:
				i++
			}
		}
		switch {
		case k < below:
			hi = below - 1
		case k > above:
			lo = above + 1
		default:
			return pivot
		}
	}
	return values[k]
}

func medianOfThree(a, b, c float64) float64 {
	if a > b {
		a, b = b, a
	}
	if b > c {
		b = c
	}
	return max(a, b)
}

// rankLatencyGroups applies GroupedLatencyWithFilter's nearest rank,
// ((count*95)+99)/100 counted from one, and its arithmetic mean of latency.
func rankLatencyGroups(groups map[string]*latencySamples) []GroupedLatency {
	keys := make([]string, 0, len(groups))
	for key, group := range groups {
		if len(group.latency) > 0 || len(group.ttft) > 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	result := make([]GroupedLatency, 0, len(keys))
	for _, key := range keys {
		group := groups[key]
		row := GroupedLatency{Key: key}
		if len(group.latency) > 0 {
			sum := 0.0
			for _, value := range group.latency {
				sum += value
			}
			row.AverageLatencyMS = sql.NullFloat64{Float64: sum / float64(len(group.latency)), Valid: true}
			row.P95LatencyMS = sql.NullFloat64{Float64: nearestRank(group.latency), Valid: true}
		}
		if len(group.ttft) > 0 {
			row.P95TTFTMS = sql.NullFloat64{Float64: nearestRank(group.ttft), Valid: true}
		}
		result = append(result, row)
	}
	return result
}
