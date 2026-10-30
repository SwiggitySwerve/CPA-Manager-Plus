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
	where, args := analyticsWhere(filter)
	query := `select timestamp_ms, ` + usageidentity.SQLRequestAnalyticsModelExpression("model", "requested_model") + `, coalesce(nullif(auth_provider_snapshot, ''), provider, ''), latency_ms, ttft_ms from usage_events ` + where
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return LatencyBreakdown{}, err
	}
	defer rows.Close()
	overall := map[string]*latencySamples{"": {}}
	byBucket, byModel, byProvider := map[string]*latencySamples{}, map[string]*latencySamples{}, map[string]*latencySamples{}
	pairs := map[string]map[string]bool{}
	for rows.Next() {
		var timestampMS int64
		var model, provider string
		var latency, ttft sql.NullFloat64
		if err := rows.Scan(&timestampMS, &model, &provider, &latency, &ttft); err != nil {
			return LatencyBreakdown{}, err
		}
		bucket := strconv.FormatInt(usage.AnalyticsBucketMS(timestampMS, granularity, location), 10)
		if pairs[model] == nil {
			pairs[model] = map[string]bool{}
		}
		pairs[model][provider] = true
		for _, entry := range []struct {
			groups map[string]*latencySamples
			key    string
		}{{overall, ""}, {byBucket, bucket}, {byModel, model}, {byProvider, provider}} {
			group := entry.groups[entry.key]
			if group == nil {
				group = &latencySamples{}
				entry.groups[entry.key] = group
			}
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

type latencySamples struct{ latency, ttft []float64 }

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
	nearest := func(values []float64) float64 {
		sort.Float64s(values)
		return values[(len(values)*95+99)/100-1]
	}
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
			row.P95LatencyMS = sql.NullFloat64{Float64: nearest(group.latency), Valid: true}
		}
		if len(group.ttft) > 0 {
			row.P95TTFTMS = sql.NullFloat64{Float64: nearest(group.ttft), Valid: true}
		}
		result = append(result, row)
	}
	return result
}
