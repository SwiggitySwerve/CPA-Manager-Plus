package usageevent

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageprojection"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

const defaultResponseMetadataBackfillBatch = 1000

type responseMetadataBackfillRow struct {
	ID                   int64
	TimestampMS          int64
	RawJSON              string
	ResponseMetadataJSON string
}

type responseMetadataBackfillUpdate struct {
	ID                   int64
	PreviousMetadataJSON string
	Derived              usage.ResponseHeaderDerived
}

const responseMetadataBackfillSelect = `with candidates as (
	select
		id,
		timestamp_ms,
		coalesce(raw_json, '') as raw_json,
		coalesce(response_metadata_json, '') as response_metadata_json,
		response_metadata_json is null as response_metadata_missing,
		case when json_valid(response_metadata_json) then response_metadata_json else '{}' end as metadata_json,
		replace(lower(trim(coalesce(provider, ''))), '_', '-') as provider_identity,
		replace(lower(trim(coalesce(auth_provider_snapshot, ''))), '_', '-') as auth_provider_identity,
		replace(replace(replace(lower(trim(coalesce(executor_type, ''))), '-', ''), '_', ''), ' ', '') as executor_identity
	from usage_events
	where id > ? and raw_json is not null
)
select id, timestamp_ms, raw_json, response_metadata_json
from candidates
where
	(response_metadata_missing and (raw_json like '%response_headers%' or raw_json like '%responseHeaders%'))
	or (
		raw_json like '%subscription:free-usage-exhausted%'
		and (
			provider_identity in ('xai', 'x-ai')
			or auth_provider_identity in ('xai', 'x-ai')
			or (
				executor_identity = 'xaiexecutor'
				and provider_identity in ('', 'grok')
				and auth_provider_identity in ('', 'grok')
			)
		)
		and (
			json_type(metadata_json, '$.provider_usage') is null
			or json_type(metadata_json, '$.provider_usage.provider') is null
			or json_type(metadata_json, '$.provider_usage.kind') is null
			or json_type(metadata_json, '$.provider_usage.state') is null
			or json_type(metadata_json, '$.provider_usage.code') is null
			or json_type(metadata_json, '$.provider_usage.unit') is null
			or json_type(metadata_json, '$.provider_usage.window_kind') is null
			or json_type(metadata_json, '$.provider_usage.observed_at_ms') is null
			or json_type(metadata_json, '$.provider_usage.recover_at_ms') is null
			or json_type(metadata_json, '$.provider_usage.source') is null
			or (
				raw_json like '%actual/limit%'
				and (
					json_type(metadata_json, '$.provider_usage.actual') is null
					or json_type(metadata_json, '$.provider_usage.limit') is null
					or json_type(metadata_json, '$.provider_usage.remaining') is null
					or json_type(metadata_json, '$.provider_usage.overage') is null
				)
			)
		)
	)
	or (raw_json like '%X-Ratelimit-Limit-Requests%' and json_type(metadata_json, '$.rate_limit.requests.limit') is null)
	or (raw_json like '%X-Ratelimit-Remaining-Requests%' and json_type(metadata_json, '$.rate_limit.requests.remaining') is null)
	or (raw_json like '%X-Ratelimit-Limit-Tokens%' and json_type(metadata_json, '$.rate_limit.tokens.limit') is null)
	or (raw_json like '%X-Ratelimit-Remaining-Tokens%' and json_type(metadata_json, '$.rate_limit.tokens.remaining') is null)
	or (raw_json like '%X-Data-Retention%' and json_type(metadata_json, '$.data_policy.retention_mode') is null)
	or (
		(raw_json like '%X-Zero-Retention%' or raw_json like '%X-Zero-Data-Retention%')
		and json_type(metadata_json, '$.data_policy.zero_retention') is null
	)
order by id
limit ?`

// responseMetadataBackfillCursorKey names the settings row that records how far the
// backfill has checked usage_events. Rows are only ever inserted with new, larger IDs,
// so once a scan finds nothing to update, every row up to the largest ID that existed
// when it started is done and later starts check only newer rows: rows from this build
// need nothing, and rows written by an older build after a downgrade are newer. The
// cursor is kept only for the selection that produced it; a change to
// responseMetadataBackfillSelect, which is how a new header parser reaches old rows,
// starts over from the first row.
const responseMetadataBackfillCursorKey = "usage_response_metadata_backfill_v1"

type responseMetadataBackfillCursor struct {
	SelectSHA256     string `json:"select_sha256"`
	CheckedThroughID int64  `json:"checked_through_id"`
}

func responseMetadataBackfillSelectSHA256() string {
	sum := sha256.Sum256([]byte(responseMetadataBackfillSelect))
	return hex.EncodeToString(sum[:])
}

// responseMetadataBackfillStartID returns the ID after which rows may still need the
// backfill.
func (r *repository) responseMetadataBackfillStartID(ctx context.Context) (int64, error) {
	var raw string
	err := r.db.QueryRowContext(ctx, `select value from settings where key = ?`, responseMetadataBackfillCursorKey).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var cursor responseMetadataBackfillCursor
	if err := json.Unmarshal([]byte(raw), &cursor); err != nil || cursor.SelectSHA256 != responseMetadataBackfillSelectSHA256() {
		return 0, nil
	}
	return cursor.CheckedThroughID, nil
}

func (r *repository) saveResponseMetadataBackfillCursor(ctx context.Context, checkedThroughID int64) error {
	value, err := json.Marshal(responseMetadataBackfillCursor{
		SelectSHA256:     responseMetadataBackfillSelectSHA256(),
		CheckedThroughID: checkedThroughID,
	})
	if err != nil {
		return err
	}
	_, err = r.db.ExecContext(ctx, `insert into settings(key, value, updated_at_ms) values (?, ?, ?)
		on conflict(key) do update set value = excluded.value, updated_at_ms = excluded.updated_at_ms`,
		responseMetadataBackfillCursorKey, string(value), time.Now().UnixMilli())
	return err
}

func (r *repository) collectResponseMetadataBackfillUpdates(ctx context.Context, afterID int64, maxUpdates int, pageLimit int) ([]responseMetadataBackfillUpdate, error) {
	if maxUpdates <= 0 {
		return nil, nil
	}
	if pageLimit <= 0 {
		pageLimit = defaultResponseMetadataBackfillBatch
	}

	updates := make([]responseMetadataBackfillUpdate, 0, maxUpdates)
	lastID := afterID
	for len(updates) < maxUpdates {
		items, err := r.responseMetadataBackfillPage(ctx, lastID, pageLimit)
		if err != nil {
			return nil, err
		}
		if len(items) == 0 {
			break
		}
		for _, item := range items {
			lastID = item.ID
			metadata := mergeResponseMetadata(
				item.ResponseMetadataJSON,
				usage.ParseResponseHeaderMetadataFromRawJSON(item.RawJSON, time.UnixMilli(item.TimestampMS)),
			)
			derived := usage.DeriveResponseHeaderMetadata(metadata)
			if derived.MetadataJSON == "" {
				if strings.TrimSpace(item.ResponseMetadataJSON) == "" {
					// Preserve the old backfill's processed-row behavior. A valid empty
					// object prevents unsupported-only header rows from being rescanned
					// on every startup while explicit future feature clauses can still
					// select the row when a new parser is added.
					updates = append(updates, responseMetadataBackfillUpdate{
						ID:                   item.ID,
						PreviousMetadataJSON: item.ResponseMetadataJSON,
						Derived:              usage.ResponseHeaderDerived{MetadataJSON: "{}"},
					})
					if len(updates) == maxUpdates {
						break
					}
				}
				continue
			}
			if responseMetadataJSONEqual(item.ResponseMetadataJSON, derived.MetadataJSON) {
				continue
			}
			updates = append(updates, responseMetadataBackfillUpdate{
				ID:                   item.ID,
				PreviousMetadataJSON: item.ResponseMetadataJSON,
				Derived:              derived,
			})
			if len(updates) == maxUpdates {
				break
			}
		}
		if len(items) < pageLimit {
			break
		}
	}
	return updates, nil
}

func (r *repository) ResponseMetadataBackfillPending(ctx context.Context) (bool, error) {
	startID, err := r.responseMetadataBackfillStartID(ctx)
	if err != nil {
		return false, err
	}
	updates, err := r.collectResponseMetadataBackfillUpdates(ctx, startID, 1, defaultResponseMetadataBackfillBatch)
	if err != nil {
		return false, err
	}
	return len(updates) > 0, nil
}

func (r *repository) BackfillResponseMetadata(ctx context.Context, batchLimit int) (int, error) {
	if batchLimit <= 0 {
		batchLimit = defaultResponseMetadataBackfillBatch
	}

	startID, err := r.responseMetadataBackfillStartID(ctx)
	if err != nil {
		return 0, err
	}
	// Read before the scan: every row up to it is checked by the scan below.
	var latestID int64
	if err := r.db.QueryRowContext(ctx, `select coalesce(max(id), 0) from usage_events`).Scan(&latestID); err != nil {
		return 0, err
	}
	updates, err := r.collectResponseMetadataBackfillUpdates(ctx, startID, batchLimit, batchLimit)
	if err != nil {
		return 0, err
	}
	if len(updates) == 0 {
		if latestID > startID {
			if err := r.saveResponseMetadataBackfillCursor(ctx, latestID); err != nil {
				return 0, err
			}
			if startID == 0 {
				log.Printf("usage response metadata backfill: rows checked through id=%d; later starts check only newer rows", latestID)
			}
		}
		return 0, nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	stmt, err := tx.PrepareContext(ctx, `update usage_events set
	response_metadata_json = ?,
	header_quota_recover_at_ms = ?,
	header_quota_used_percent = ?,
	header_quota_plan_type = ?,
	header_error_kind = ?,
	header_error_code = ?,
	header_trace_id = ?
	where id = ? and coalesce(response_metadata_json, '') = ?`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	updatedIDs := make([]int64, 0, len(updates))
	for _, update := range updates {
		derived := update.Derived
		res, err := stmt.ExecContext(
			ctx,
			derived.MetadataJSON,
			nullPositiveInt64(derived.QuotaRecoverAtMS),
			nullFloat(derived.QuotaUsedPercent),
			nullString(derived.QuotaPlanType),
			nullString(derived.ErrorKind),
			nullString(derived.ErrorCode),
			nullString(derived.TraceID),
			update.ID,
			update.PreviousMetadataJSON,
		)
		if err != nil {
			return 0, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		if affected == 1 {
			updatedIDs = append(updatedIDs, update.ID)
		}
	}
	if len(updatedIDs) == 0 {
		return 0, nil
	}
	nowMS := time.Now().UnixMilli()
	if err := usageprojection.UpsertEventIDs(ctx, tx, updatedIDs, nowMS); err != nil {
		return 0, err
	}
	if err := usageprojection.UpsertHeaderIDs(ctx, tx, updatedIDs, nowMS); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(updatedIDs), nil
}

func (r *repository) responseMetadataBackfillPage(ctx context.Context, afterID int64, limit int) ([]responseMetadataBackfillRow, error) {
	rows, err := r.db.QueryContext(ctx, responseMetadataBackfillSelect, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]responseMetadataBackfillRow, 0, limit)
	for rows.Next() {
		var item responseMetadataBackfillRow
		if err := rows.Scan(&item.ID, &item.TimestampMS, &item.RawJSON, &item.ResponseMetadataJSON); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func responseMetadataJSONEqual(existingJSON string, candidateJSON string) bool {
	existing := usage.DeriveResponseHeaderMetadata(usage.ResponseHeaderMetadataFromJSON(existingJSON)).MetadataJSON
	return existing != "" && existing == candidateJSON
}

func mergeResponseMetadata(existingJSON string, parsed *usage.ResponseHeaderMetadata) *usage.ResponseHeaderMetadata {
	existing := usage.ResponseHeaderMetadataFromJSON(existingJSON)
	return usage.MergeResponseHeaderMetadata(existing, parsed)
}
