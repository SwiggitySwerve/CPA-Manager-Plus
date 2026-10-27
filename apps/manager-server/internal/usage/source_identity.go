package usage

import "strings"

// SourceMatchesAccount verifies a stored source against the original account.
// Masked sources require the original account's exact SourceHash. Plaintext
// sources may omit the hash, but a supplied hash must agree.
func SourceMatchesAccount(source, sourceHash, account string) bool {
	account = strings.TrimSpace(account)
	source = strings.TrimSpace(source)
	if account == "" || source == "" {
		return false
	}
	expectedHash := SourceHash(account)
	if sourceHash != "" && sourceHash != expectedHash {
		return false
	}
	if source == account {
		return true
	}
	return sourceHash == expectedHash && source == maskSource(account)
}
