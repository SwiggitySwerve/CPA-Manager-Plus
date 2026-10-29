package usageevent

import (
	"context"
	"database/sql"
	"errors"
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
