package usageidentity

import "strings"

// ClaudeLegacyEvidence contains identity snapshots and the source verification
// supplied by the usage repository through usage.SourceMatchesAccount.
type ClaudeLegacyEvidence struct {
	Fields
	Provider             string
	SourceMatchesAccount bool
}

// ClaudeLegacyTargetConflicts also checks current request targets: a new
// credential reusing the index may not have produced a stored event yet.
func ClaudeLegacyTargetConflicts(target Fields, peers []Fields) bool {
	if normalizeProvider(target.AuthProviderSnapshot) != "claude" {
		return false
	}
	index := strings.TrimSpace(target.AuthIndex)
	for _, peer := range peers {
		if strings.TrimSpace(peer.AuthIndex) != index {
			continue
		}
		if normalizeProvider(peer.AuthProviderSnapshot) != "claude" {
			return true
		}
		for _, pair := range [][2]string{
			{peer.AuthFileSnapshot, target.AuthFileSnapshot},
			{peer.AccountSnapshot, target.AccountSnapshot},
			{peer.AuthAccountIDSnapshot, target.AuthAccountIDSnapshot},
			{peer.AuthProjectIDSnapshot, target.AuthProjectIDSnapshot},
		} {
			if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
				return true
			}
		}
	}
	return false
}

// ClaudeLegacyAccountKey authorizes a source/index read alias only when a
// stored file snapshot corroborates the target and all evidence for that index
// agrees. Source identity must be verified by the usage owner.
// Callers must supply complete history for the index, including conflicts.
func ClaudeLegacyAccountKey(target Fields, evidence []ClaudeLegacyEvidence) (string, bool) {
	file := strings.TrimSpace(target.AuthFileSnapshot)
	index := strings.TrimSpace(target.AuthIndex)
	account := strings.TrimSpace(target.AccountSnapshot)
	if normalizeProvider(target.AuthProviderSnapshot) != "claude" || file == "" || index == "" || account == "" {
		return "", false
	}
	primary, _ := AccountKey(target)
	alias := ""
	anchored := false
	for _, event := range evidence {
		if strings.TrimSpace(event.AuthIndex) != index {
			return "", false
		}
		hasProvider := false
		for _, provider := range []string{event.Provider, event.AuthProviderSnapshot} {
			if normalized := normalizeProvider(provider); normalized != "" {
				if normalized != "claude" {
					return "", false
				}
				hasProvider = true
			}
		}
		if !hasProvider {
			return "", false
		}
		for _, pair := range [][2]string{
			{event.AccountSnapshot, target.AccountSnapshot},
			{event.AuthAccountIDSnapshot, target.AuthAccountIDSnapshot},
			{event.AuthProjectIDSnapshot, target.AuthProjectIDSnapshot},
		} {
			if value := strings.TrimSpace(pair[0]); value != "" && value != strings.TrimSpace(pair[1]) {
				return "", false
			}
		}
		if eventFile := strings.TrimSpace(event.AuthFileSnapshot); eventFile != "" {
			if eventFile != file {
				return "", false
			}
			if normalizeProvider(event.AuthProviderSnapshot) == "claude" && strings.TrimSpace(event.AccountSnapshot) == account &&
				strings.TrimSpace(event.AuthAccountIDSnapshot) == strings.TrimSpace(target.AuthAccountIDSnapshot) &&
				strings.TrimSpace(event.AuthProjectIDSnapshot) == strings.TrimSpace(target.AuthProjectIDSnapshot) {
				anchored = true
			}
			continue
		}
		key, valid := AccountKey(event.Fields)
		expected, _ := AccountKey(Fields{Source: event.Source, AuthIndex: index, AuthProviderSnapshot: "claude"})
		if !event.SourceMatchesAccount || !valid || key != expected || key == primary || (alias != "" && key != alias) {
			return "", false
		}
		alias = key
	}
	if !anchored || alias == "" {
		return "", false
	}
	return alias, true
}
