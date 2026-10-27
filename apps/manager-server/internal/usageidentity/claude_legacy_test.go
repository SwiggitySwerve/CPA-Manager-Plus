package usageidentity

import "testing"

func TestClaudeLegacyTargetConflicts(t *testing.T) {
	target := Fields{AuthFileSnapshot: "fixture.json", AuthIndex: "fixture-index", AuthProviderSnapshot: "claude", AccountSnapshot: "fixture@example.invalid"}
	for _, test := range []struct {
		name     string
		mutate   func(*Fields)
		conflict bool
	}{
		{name: "duplicate target"},
		{name: "different file", mutate: func(f *Fields) { f.AuthFileSnapshot = "other.json" }, conflict: true},
		{name: "different provider", mutate: func(f *Fields) { f.AuthProviderSnapshot = "codex" }, conflict: true},
		{name: "different account", mutate: func(f *Fields) { f.AccountSnapshot = "other@example.invalid" }, conflict: true},
		{name: "different account ID", mutate: func(f *Fields) { f.AuthAccountIDSnapshot = "other" }, conflict: true},
		{name: "different project", mutate: func(f *Fields) { f.AuthProjectIDSnapshot = "other" }, conflict: true},
		{name: "different index", mutate: func(f *Fields) { f.AuthIndex = "other-index"; f.AuthFileSnapshot = "other.json" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			peer := target
			if test.mutate != nil {
				test.mutate(&peer)
			}
			if got := ClaudeLegacyTargetConflicts(target, []Fields{target, peer}); got != test.conflict {
				t.Fatalf("conflict=%v, want %v", got, test.conflict)
			}
		})
	}
	target.AuthProviderSnapshot = "codex"
	if ClaudeLegacyTargetConflicts(target, []Fields{{AuthIndex: target.AuthIndex}}) {
		t.Fatal("Claude request guard changed Codex behavior")
	}
}
