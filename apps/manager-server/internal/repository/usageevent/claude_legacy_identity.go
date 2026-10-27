package usageevent

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

// ResolveLegacyAccountKey retains the Codex compatibility contract and adds a
// narrowly corroborated Claude source/index alias for account-history reads.
func (r *repository) ResolveLegacyAccountKey(ctx context.Context, fields usageidentity.Fields) (string, bool, error) {
	if normalizeIdentityProvider(fields.AuthProviderSnapshot) != "claude" {
		return r.ResolveCodexLegacyAccountKey(ctx, fields)
	}
	if strings.TrimSpace(fields.AuthFileSnapshot) == "" || strings.TrimSpace(fields.AuthIndex) == "" || strings.TrimSpace(fields.AccountSnapshot) == "" {
		return "", false, nil
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", false, err
	}
	defer func() { _ = tx.Rollback() }()
	var incomplete bool
	if err := tx.QueryRowContext(ctx, `select exists (
		select 1 from usage_event_identity_ledger where raw_event_id is null
	)`).Scan(&incomplete); err != nil {
		return "", false, err
	}
	if incomplete {
		return "", false, nil
	}
	indices, err := tx.QueryContext(ctx, `select distinct auth_index from usage_events where auth_index is not null`)
	if err != nil {
		return "", false, err
	}
	var variants []string
	for indices.Next() {
		var index string
		if err := indices.Scan(&index); err != nil {
			indices.Close()
			return "", false, err
		}
		if strings.TrimSpace(index) == strings.TrimSpace(fields.AuthIndex) {
			variants = append(variants, index)
		}
	}
	err = indices.Err()
	indices.Close()
	if err != nil || len(variants) == 0 {
		return "", false, err
	}
	encoded, err := json.Marshal(variants)
	if err != nil {
		return "", false, err
	}
	rows, err := tx.QueryContext(ctx, `select distinct
		coalesce(provider, ''), coalesce(auth_provider_snapshot, ''),
		coalesce(auth_file_snapshot, ''), coalesce(auth_index, ''),
		coalesce(account_snapshot, ''), coalesce(auth_account_id_snapshot, ''),
		coalesce(auth_project_id_snapshot, ''), coalesce(auth_label_snapshot, ''), coalesce(source, ''), coalesce(source_hash, '')
		from usage_events where auth_index in (select value from json_each(?))`, string(encoded))
	if err != nil {
		return "", false, err
	}
	defer rows.Close()
	var evidence []usageidentity.ClaudeLegacyEvidence
	for rows.Next() {
		var event usageidentity.ClaudeLegacyEvidence
		var sourceHash string
		if err := rows.Scan(&event.Provider, &event.AuthProviderSnapshot, &event.AuthFileSnapshot,
			&event.AuthIndex, &event.AccountSnapshot, &event.AuthAccountIDSnapshot,
			&event.AuthProjectIDSnapshot, &event.AuthLabelSnapshot, &event.Source, &sourceHash); err != nil {
			return "", false, err
		}
		event.SourceMatchesAccount = usage.SourceMatchesAccount(event.Source, sourceHash, fields.AccountSnapshot)
		evidence = append(evidence, event)
	}
	if err := rows.Err(); err != nil {
		return "", false, err
	}
	key, allowed := usageidentity.ClaudeLegacyAccountKey(fields, evidence)
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return key, allowed, nil
}
