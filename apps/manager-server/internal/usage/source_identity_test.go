package usage

import (
	"encoding/json"
	"testing"
)

func TestSourceIdentityCompatibilityFixtures(t *testing.T) {
	var snapshots [][4]string
	for _, source := range []string{"", " \t\u2003", "abc", " fixture@example.invalid\t", "a@example.invalid", "sk-ant-api03-fixture-secret-value"} {
		detail := map[string]any{"source": source, "timestamp": "2026-01-02T03:04:05Z", "model": "fixture-model", "api_key": "fixture-key"}
		raw, err := json.Marshal(detail)
		if err != nil {
			t.Fatal(err)
		}
		normalized, err := NormalizeRaw(raw)
		if err != nil {
			t.Fatal(err)
		}
		legacy, err := eventFromLegacyDetail("-", "", "", "fixture-model", detail, 0, 0, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		if normalized.Source != legacy.Source || normalized.SourceHash != legacy.SourceHash {
			t.Fatal("normalization and legacy import disagree on source identity")
		}
		persisted := PrepareSensitiveFieldsForPersistence(Event{Source: source})
		for _, event := range []Event{normalized, legacy, persisted} {
			snapshots = append(snapshots, [4]string{event.Source, event.SourceHash, event.APIKeyHash, event.EventHash})
		}
	}
	encoded, err := json.Marshal(snapshots)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("source_identity_fixture_digest=%s", sha256Hex(string(encoded)))
	if SourceHash(" \tabc\u2003") != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" || SourceHash(" \t") != "" {
		t.Fatal("source hashing changed its trim, empty, or SHA-256 contract")
	}
}

func TestSourceMatchesAccount(t *testing.T) {
	account := "fixture@example.invalid"
	masked := maskSource(account)
	hash := SourceHash(account)
	otherHash := SourceHash("fixture-other@example.invalid")
	if masked != maskSource("fixture-other@example.invalid") || hash == otherHash {
		t.Fatal("fixture must demonstrate a masking collision without a hash collision")
	}
	for _, test := range []struct {
		name, source, hash, account string
		want                        bool
	}{
		{name: "masked corroborated", source: masked, hash: hash, account: account, want: true},
		{name: "trimmed original", source: masked, hash: hash, account: " \t" + account + "\u2003", want: true},
		{name: "masked missing hash", source: masked, account: account},
		{name: "mask hashed instead of original", source: masked, hash: SourceHash(masked), account: account},
		{name: "same mask different account", source: masked, hash: otherHash, account: account},
		{name: "forged source with correct hash", source: "other***@example.invalid", hash: hash, account: account},
		{name: "malformed hash", source: masked, hash: "invalid", account: account},
		{name: "plaintext without hash", source: account, account: account, want: true},
		{name: "plaintext with correct hash", source: account, hash: hash, account: account, want: true},
		{name: "plaintext conflicting hash", source: account, hash: otherHash, account: account},
		{name: "empty account", source: masked, hash: hash},
		{name: "empty source", hash: hash, account: account},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := SourceMatchesAccount(test.source, test.hash, test.account); got != test.want {
				t.Fatalf("source matches=%v, want %v", got, test.want)
			}
		})
	}
}
