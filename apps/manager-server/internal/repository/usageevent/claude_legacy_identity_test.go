package usageevent

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	sqliterepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/sqlite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

func TestResolveLegacyAccountKeyClaudeEvidence(t *testing.T) {
	masked := func(f *usageidentity.Fields, e []usage.Event) []usage.Event {
		e[0].Source = "fix***@example.invalid"
		e[0].SourceHash = usage.SourceHash(f.AccountSnapshot)
		return e
	}
	tests := []struct {
		name     string
		mutate   func(*usageidentity.Fields, []usage.Event) []usage.Event
		archived bool
		allowed  bool
	}{
		{name: "corroborated source alias", allowed: true},
		{name: "masked corroborated source alias", allowed: true, mutate: masked},
		{name: "masked missing hash", mutate: func(f *usageidentity.Fields, e []usage.Event) []usage.Event {
			e = masked(f, e)
			e[0].SourceHash = ""
			return e
		}},
		{name: "masked malformed hash", mutate: func(f *usageidentity.Fields, e []usage.Event) []usage.Event {
			e = masked(f, e)
			e[0].SourceHash = "not-a-hash"
			return e
		}},
		{name: "masked hash of mask", mutate: func(f *usageidentity.Fields, e []usage.Event) []usage.Event {
			e = masked(f, e)
			e[0].SourceHash = usage.SourceHash(e[0].Source)
			return e
		}},
		{name: "same mask different account hash", mutate: func(f *usageidentity.Fields, e []usage.Event) []usage.Event {
			e = masked(f, e)
			e[0].SourceHash = usage.SourceHash("fixture-other@example.invalid")
			return e
		}},
		{name: "forged masked source correct hash", mutate: func(f *usageidentity.Fields, e []usage.Event) []usage.Event {
			e = masked(f, e)
			e[0].Source = "other***@example.invalid"
			return e
		}},
		{name: "mixed plaintext and masked aliases", mutate: func(f *usageidentity.Fields, e []usage.Event) []usage.Event {
			plain := e[0]
			plain.EventHash = canonicalTestHash("plain-candidate")
			return append(masked(f, e), plain)
		}},
		{name: "mixed masked source hashes", mutate: func(f *usageidentity.Fields, e []usage.Event) []usage.Event {
			e = masked(f, e)
			other := e[0]
			other.EventHash = canonicalTestHash("conflicting-hash")
			other.SourceHash = usage.SourceHash("fixture-other@example.invalid")
			return append(e, other)
		}},
		{name: "mixed masked source missing hash", mutate: func(f *usageidentity.Fields, e []usage.Event) []usage.Event {
			e = masked(f, e)
			other := e[0]
			other.EventHash = canonicalTestHash("missing-hash")
			other.SourceHash = ""
			return append(e, other)
		}},
		{name: "repeated masked alias", allowed: true, mutate: func(f *usageidentity.Fields, e []usage.Event) []usage.Event {
			e = masked(f, e)
			other := e[0]
			other.EventHash = canonicalTestHash("repeated-candidate")
			return append(e, other)
		}},
		{name: "masked without anchor", mutate: func(f *usageidentity.Fields, e []usage.Event) []usage.Event { return masked(f, e)[:1] }},
		{name: "plaintext correct hash", allowed: true, mutate: func(f *usageidentity.Fields, e []usage.Event) []usage.Event {
			e[0].SourceHash = usage.SourceHash(f.AccountSnapshot)
			return e
		}},
		{name: "plaintext conflicting hash", mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event {
			e[0].SourceHash = usage.SourceHash("other@example.invalid")
			return e
		}},
		{name: "no stored file anchor", mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event { return e[:1] }},
		{name: "no legacy candidate", mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event { return e[1:] }},
		{name: "unknown source", mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event { e[0].Source = "unrelated"; return e }},
		{name: "missing source", mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event { e[0].Source = ""; return e }},
		{name: "missing provider", mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event { e[0].Provider = ""; return e }},
		{name: "foreign provider", mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event { e[0].Provider = "codex"; return e }},
		{name: "conflicting provider snapshot", mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event {
			e[0].AuthProviderSnapshot = "openai"
			return e
		}},
		{name: "conflicting account snapshot", mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event {
			e[0].AccountSnapshot = "other@example.invalid"
			return e
		}},
		{name: "conflicting account ID", mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event {
			e[0].AuthAccountIDSnapshot = "different"
			return e
		}},
		{name: "conflicting project", mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event {
			e[0].AuthProjectIDSnapshot = "different"
			return e
		}},
		{name: "file index collision", mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event {
			other := e[1]
			other.EventHash = canonicalTestHash("collision")
			other.AuthFileSnapshot = "other.json"
			return append(e, other)
		}},
		{name: "explicit file in alias bucket", mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event {
			e[0].AuthFileSnapshot = e[0].Source
			return e
		}},
		{name: "unverified file snapshot", mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event { e[1].AuthProviderSnapshot = ""; return e }},
		{name: "unverified account snapshot", mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event { e[1].AccountSnapshot = ""; return e }},
		{name: "target account ID lacks anchor", mutate: func(f *usageidentity.Fields, e []usage.Event) []usage.Event {
			f.AuthAccountIDSnapshot = "unverified"
			return e
		}},
		{name: "target project lacks anchor", mutate: func(f *usageidentity.Fields, e []usage.Event) []usage.Event {
			f.AuthProjectIDSnapshot = "unverified"
			return e
		}},
		{name: "target provider missing", mutate: func(f *usageidentity.Fields, e []usage.Event) []usage.Event { f.AuthProviderSnapshot = ""; return e }},
		{name: "target filename missing", mutate: func(f *usageidentity.Fields, e []usage.Event) []usage.Event { f.AuthFileSnapshot = ""; return e }},
		{name: "target index missing", mutate: func(f *usageidentity.Fields, e []usage.Event) []usage.Event { f.AuthIndex = ""; return e }},
		{name: "target account missing", mutate: func(f *usageidentity.Fields, e []usage.Event) []usage.Event { f.AccountSnapshot = ""; return e }},
		{name: "shared email different index remains separate", allowed: true, mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event {
			other := e[1]
			other.EventHash = canonicalTestHash("unrelated")
			other.AuthIndex = "other-index"
			other.AuthFileSnapshot = "other.json"
			return append(e, other)
		}},
		{name: "archived evidence unavailable", archived: true},
		{name: "padded index collision", mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event {
			other := e[1]
			other.EventHash = canonicalTestHash("padded-collision")
			other.AuthIndex = " " + other.AuthIndex + " "
			other.AuthFileSnapshot = "other.json"
			return append(e, other)
		}},
		{name: "unicode padded index collision", mutate: func(_ *usageidentity.Fields, e []usage.Event) []usage.Event {
			other := e[1]
			other.EventHash = canonicalTestHash("unicode-collision")
			other.AuthIndex = "\u2003" + other.AuthIndex + "\t"
			other.AuthFileSnapshot = "other.json"
			return append(e, other)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := sqliterepo.Open(filepath.Join(t.TempDir(), "usage.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			repo := New(db)
			fields := usageidentity.Fields{AuthFileSnapshot: "current.json", AuthIndex: "fixture-index", AuthProviderSnapshot: "claude", AccountSnapshot: "fixture@example.invalid"}
			legacy := identityTestEvent("claude-legacy", 1, "", fields.AuthIndex, "claude", "")
			legacy.Provider = "claude"
			legacy.AuthProviderSnapshot = ""
			legacy.AccountSnapshot = ""
			legacy.AuthLabelSnapshot = ""
			legacy.Source = fields.AccountSnapshot
			anchor := legacy
			anchor.EventHash = canonicalTestHash("claude-anchor")
			anchor.AuthFileSnapshot = fields.AuthFileSnapshot
			anchor.AuthProviderSnapshot = "claude"
			anchor.AccountSnapshot = fields.AccountSnapshot
			events := []usage.Event{legacy, anchor}
			if test.mutate != nil {
				events = test.mutate(&fields, events)
			}
			ctx := context.Background()
			if _, err := repo.InsertBatch(ctx, events); err != nil {
				t.Fatal(err)
			}
			if test.archived {
				if _, err := db.Exec(`insert into usage_event_identity_ledger(event_hash, timestamp_ms, bucket_ms, first_seen_at_ms, updated_at_ms) values (?, 1, 1, 1, 1)`, canonicalTestHash("archived")); err != nil {
					t.Fatal(err)
				}
			}
			before, err := repo.ExportJSONL(ctx)
			if err != nil {
				t.Fatal(err)
			}
			key, allowed, err := repo.ResolveLegacyAccountKey(ctx, fields)
			if err != nil {
				t.Fatal(err)
			}
			if allowed != test.allowed {
				t.Fatalf("alias allowed=%v, want %v", allowed, test.allowed)
			}
			if allowed {
				want, _ := usageidentity.AccountKey(usageidentity.Fields{Source: events[0].Source, AuthIndex: fields.AuthIndex, AuthProviderSnapshot: "claude"})
				if key != want {
					t.Fatal("alias does not match immutable source bucket")
				}
			}
			after, err := repo.ExportJSONL(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("identity read changed raw events")
			}
		})
	}
}
