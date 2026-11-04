package inventory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/cpaauthfiles"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/managerconfig"
	proxysvc "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/proxy"
	quotasnapshotsvc "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/quotasnapshot"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

// fakeUpstream is one CLI-Proxy: it lists and deletes auth files, and can be told to answer badly.
type fakeUpstream struct {
	*httptest.Server
	mu         sync.Mutex
	files      []map[string]any
	mode       string // "", "fail" (HTTP 500), "nolist" (200 {}), "garbage" (200, truncated JSON)
	deleteFail bool
	listings   int // successful or failed GETs of the list
}

func newFakeUpstream(t *testing.T, files ...map[string]any) *fakeUpstream {
	t.Helper()
	f := &fakeUpstream{files: files}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.Header.Get("Authorization") != "Bearer management-key" {
			http.Error(w, "bad key", http.StatusUnauthorized)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v0/management/auth-files":
			f.listings++
			switch f.mode {
			case "fail":
				http.Error(w, "boom", http.StatusInternalServerError)
			case "nolist":
				_, _ = w.Write([]byte(`{}`))
			case "garbage":
				_, _ = w.Write([]byte(`{"files":[{"name":"x`))
			default:
				_ = json.NewEncoder(w).Encode(map[string]any{"files": f.files})
			}
		case r.Method == http.MethodDelete && r.URL.Path == "/v0/management/auth-files":
			if f.deleteFail {
				http.Error(w, "nope", http.StatusInternalServerError)
				return
			}
			name := r.URL.Query().Get("name")
			kept := f.files[:0:0]
			for _, file := range f.files {
				if file["name"] != name {
					kept = append(kept, file)
				}
			}
			f.files = kept
			_, _ = w.Write([]byte(`{"status":"ok","deleted":1,"files":["` + name + `"]}`))
		default:
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeUpstream) set(files ...map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files = files
}

func (f *fakeUpstream) listingCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listings
}

func (f *fakeUpstream) setMode(mode string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mode = mode
}

func (f *fakeUpstream) setDeleteFail(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteFail = v
}

func authFile(name, provider, authIndex string) map[string]any {
	return map[string]any{"name": name, "provider": provider, "auth_index": authIndex}
}

var (
	codexA  = authFile("codex-a.json", "codex", "idx-a")
	codexC  = authFile("codex-c.json", "codex", "idx-c")
	claudeB = authFile("claude-b.json", "claude", "idx-b")
	claudeD = authFile("claude-d.json", "claude", "idx-d")
)

type harness struct {
	t       *testing.T
	st      *store.Store
	primary *fakeUpstream
	extra   *fakeUpstream
	svc     *Service
	quota   *quotasnapshotsvc.Service
	config  *managerconfig.Service
	nowMS   int64
}

// newHarness starts a codex upstream (a, c) and a claude upstream (b, d) and an inventory service over them.
func newHarness(t *testing.T) *harness {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	primary := newFakeUpstream(t, codexA, codexC)
	extra := newFakeUpstream(t, claudeB, claudeD)
	cfg := config.Config{CPAUpstreamURL: primary.URL, ManagementKey: "management-key"}
	mc := managerconfig.New(cfg, nil, nil)
	svc := New(st, mc, []string{extra.URL})
	h := &harness{t: t, st: st, primary: primary, extra: extra, svc: svc, quota: quotasnapshotsvc.New(st), config: mc, nowMS: time.Now().UnixMilli()}
	svc.now = func() time.Time { return time.UnixMilli(h.nowMS) }
	return h
}

func (h *harness) refresh(reason string) Result {
	h.t.Helper()
	res, err := h.svc.Refresh(context.Background(), reason)
	if err != nil {
		h.t.Fatalf("Refresh(%s): %v", reason, err)
	}
	return res
}

func (h *harness) seedCooldown(name, authIndex, provider string) {
	h.t.Helper()
	if _, err := h.st.UpsertQuotaCooldown(context.Background(), store.QuotaCooldownUpsert{
		AuthFileName: name, AuthIndex: authIndex, Provider: provider, ReasonCode: "quota",
		RecoverAtMS: h.nowMS + 3_600_000, Owner: "cpamp", DisabledAtMS: h.nowMS,
	}); err != nil {
		h.t.Fatalf("seed cooldown: %v", err)
	}
}

func (h *harness) seedCandidate(name, authIndex, provider string) {
	h.t.Helper()
	if _, err := h.st.UpsertAccountActionCandidate(context.Background(), store.AccountActionCandidateUpsert{
		ActionType: model.AccountActionTypeDelete, Provider: provider, AuthFileName: name, AuthIndex: authIndex,
		ReasonCode: "unauthorized", Reason: "401", SeenAtMS: h.nowMS,
	}); err != nil {
		h.t.Fatalf("seed candidate: %v", err)
	}
}

// seedInspection stores one result row per file name in a single completed run and returns the run id.
func (h *harness) seedInspection(names ...string) int64 {
	h.t.Helper()
	ctx := context.Background()
	run, err := h.st.CodexInspections.CreateRun(ctx, model.CodexInspectionRun{
		TriggerType: "manual", Status: model.CodexInspectionStatusCompleted, StartedAtMS: h.nowMS,
		Settings: model.DefaultCodexInspectionConfig(),
	})
	if err != nil {
		h.t.Fatalf("seed inspection run: %v", err)
	}
	for _, name := range names {
		if _, err := h.st.CodexInspections.InsertResult(ctx, model.CodexInspectionResult{
			RunID: run.ID, AccountKey: name + "::-::acct", FileName: name, DisplayAccount: name, Provider: "codex", Action: "keep",
		}); err != nil {
			h.t.Fatalf("seed inspection result: %v", err)
		}
	}
	return run.ID
}

func (h *harness) seedOwnership(name string) {
	h.t.Helper()
	if err := h.st.UpsertCodexInspectionDisableOwnership(context.Background(), store.CodexInspectionDisableOwnership{
		FileName: name, Provider: "codex", DisabledAtMS: h.nowMS, UpdatedAtMS: h.nowMS,
	}); err != nil {
		h.t.Fatalf("seed ownership: %v", err)
	}
}

func quotaAccount(name, provider, authIndex string) quotasnapshotsvc.AccountTarget {
	return quotasnapshotsvc.AccountTarget{AuthFileSnapshot: name, AuthProviderSnapshot: provider, AuthIndex: authIndex}
}

func (h *harness) seedQuota(name, provider, authIndex string) {
	h.t.Helper()
	start, end, duration := h.nowMS-3_600_000, h.nowMS+3_600_000, int64(7200)
	used := 40.0
	if _, err := h.quota.Write(context.Background(), quotasnapshotsvc.WriteRequest{Entries: []quotasnapshotsvc.WriteEntry{{
		Provider: provider, Account: quotaAccount(name, provider, authIndex),
		Windows: []quotasnapshotsvc.WindowInput{{
			ProviderWindowID: "rate_limit:five_hour", WindowKind: "five_hour", WindowMode: "fixed", ModelScopeKind: "all",
			Source: "api_query", ObservedAtMS: h.nowMS - 1000, BoundaryAccuracy: "exact",
			CycleStartMS: &start, CycleEndMS: &end, DurationSeconds: &duration, UsedPercent: &used,
		}},
	}}}); err != nil {
		h.t.Fatalf("seed quota window: %v", err)
	}
}

// quotaWindows returns how many windows the account's quota query returns.
func (h *harness) quotaWindows(name, provider, authIndex string, includeInactive bool) int {
	h.t.Helper()
	res, err := h.quota.Query(context.Background(), quotasnapshotsvc.QueryRequest{
		NowMS: h.nowMS, IncludeInactive: includeInactive,
		Accounts: []quotasnapshotsvc.QueryAccount{{RowKey: "r", Provider: provider, Account: quotaAccount(name, provider, authIndex)}},
	})
	if err != nil {
		h.t.Fatalf("query quota: %v", err)
	}
	if len(res.Items) != 1 {
		h.t.Fatalf("quota items = %d", len(res.Items))
	}
	return len(res.Items[0].Windows)
}

func (h *harness) cooldownStatus(name string) string {
	h.t.Helper()
	active, err := h.st.QuotaCooldowns.ListActive(context.Background())
	if err != nil {
		h.t.Fatalf("list cooldowns: %v", err)
	}
	for _, c := range active {
		if c.AuthFileName == name {
			return model.QuotaCooldownStatusActive
		}
	}
	return "not-active"
}

// candidateStatus returns the status of the candidate for name, or "" if none.
func (h *harness) candidateStatus(name string) string {
	h.t.Helper()
	items, err := h.st.ListAccountActionCandidates(context.Background(), "", 500)
	if err != nil {
		h.t.Fatalf("list candidates: %v", err)
	}
	for _, c := range items {
		if c.AuthFileName == name {
			return c.Status
		}
	}
	return ""
}

func (h *harness) visibleResults(runID int64) map[string]bool {
	h.t.Helper()
	results, err := h.st.ListCodexInspectionResults(context.Background(), runID)
	if err != nil {
		h.t.Fatalf("list inspection results: %v", err)
	}
	seen := map[string]bool{}
	for _, r := range results {
		seen[r.FileName] = true
	}
	return seen
}

func (h *harness) ownershipNames() map[string]bool {
	h.t.Helper()
	items, err := h.st.ListCodexInspectionDisableOwnership(context.Background())
	if err != nil {
		h.t.Fatalf("list ownership: %v", err)
	}
	names := map[string]bool{}
	for _, item := range items {
		names[item.FileName] = true
	}
	return names
}

// seedMetadata gives codex-a (primary) and claude-b (extra) one row in every reconciled table and returns the inspection run id.
func (h *harness) seedMetadata() int64 {
	h.t.Helper()
	for _, f := range []struct{ name, idx, provider string }{
		{"codex-a.json", "idx-a", "codex"}, {"claude-b.json", "idx-b", "claude"},
	} {
		h.seedCooldown(f.name, f.idx, f.provider)
		h.seedCandidate(f.name, f.idx, f.provider)
		h.seedOwnership(f.name)
		h.seedQuota(f.name, f.provider, f.idx)
	}
	return h.seedInspection("codex-a.json", "claude-b.json", "codex-c.json")
}

type usageSnapshot struct {
	count  int64
	export string
}

func (h *harness) seedUsageHistory() usageSnapshot {
	h.t.Helper()
	ctx := context.Background()
	events := make([]usage.Event, 0, 2)
	for i, name := range []string{"codex-a.json", "claude-b.json"} {
		ts := time.UnixMilli(h.nowMS - int64(i+1)*60_000)
		sum := sha256.Sum256([]byte("inventory-test-event-" + name))
		events = append(events, usage.Event{
			EventHash: hex.EncodeToString(sum[:]), TimestampMS: ts.UnixMilli(), Timestamp: ts.Format(time.RFC3339Nano),
			Provider: "codex", Model: "gpt-5", AuthFileSnapshot: name, AuthIndex: "idx", AuthProviderSnapshot: "codex",
			CreatedAtMS: ts.UnixMilli(),
		})
	}
	if _, err := h.st.UsageEvents.InsertBatch(ctx, events); err != nil {
		h.t.Fatalf("seed usage events: %v", err)
	}
	return h.usageSnapshot()
}

func (h *harness) usageSnapshot() usageSnapshot {
	h.t.Helper()
	ctx := context.Background()
	count, err := h.st.UsageEvents.Count(ctx)
	if err != nil {
		h.t.Fatalf("count usage events: %v", err)
	}
	export, err := h.st.UsageEvents.ExportJSONL(ctx)
	if err != nil {
		h.t.Fatalf("export usage events: %v", err)
	}
	return usageSnapshot{count: count, export: string(export)}
}

func TestRemovalReconcilesAccountMetadata(t *testing.T) {
	h := newHarness(t)
	runID := h.seedMetadata()

	first := h.refresh(ReasonPoll)
	if !first.Changed || len(first.Removed) != 0 {
		t.Fatalf("first listing = %#v, want changed with nothing removed", first)
	}
	// Everything is still listed: the priming pass changes no row.
	if h.cooldownStatus("codex-a.json") != model.QuotaCooldownStatusActive ||
		h.candidateStatus("codex-a.json") != model.AccountActionStatusPending ||
		!h.visibleResults(runID)["codex-a.json"] || !h.ownershipNames()["codex-a.json"] ||
		h.quotaWindows("codex-a.json", "codex", "idx-a", false) != 1 {
		t.Fatalf("a listed credential's metadata was changed by the first listing")
	}

	// codex-a is deleted at the source, behind this manager's back.
	h.primary.set(codexC)
	res := h.refresh(ReasonPoll)

	if !res.Changed || res.Version == first.Version || strings.Join(res.Removed, ",") != "codex-a.json" {
		t.Fatalf("second listing = %#v, want codex-a.json removed under a new version", res)
	}
	if got := h.cooldownStatus("codex-a.json"); got != "not-active" {
		t.Errorf("removed credential's cooldown = %s, want it no longer active", got)
	}
	if got := h.candidateStatus("codex-a.json"); got != model.AccountActionStatusDeleted {
		t.Errorf("removed credential's candidate status = %q, want deleted", got)
	}
	if h.visibleResults(runID)["codex-a.json"] {
		t.Errorf("removed credential still appears in the inspection results")
	}
	if h.ownershipNames()["codex-a.json"] {
		t.Errorf("removed credential still has disable ownership")
	}
	if n := h.quotaWindows("codex-a.json", "codex", "idx-a", false); n != 0 {
		t.Errorf("removed credential has %d current quota windows, want 0", n)
	}
	if n := h.quotaWindows("codex-a.json", "codex", "idx-a", true); n != 1 {
		t.Errorf("removed credential's quota history has %d windows with inactive included, want the 1 kept", n)
	}

	// Credentials that are still listed keep everything, on both upstreams.
	if h.cooldownStatus("claude-b.json") != model.QuotaCooldownStatusActive ||
		h.candidateStatus("claude-b.json") != model.AccountActionStatusPending ||
		!h.visibleResults(runID)["claude-b.json"] || !h.visibleResults(runID)["codex-c.json"] ||
		!h.ownershipNames()["claude-b.json"] || h.quotaWindows("claude-b.json", "claude", "idx-b", false) != 1 {
		t.Errorf("a credential that is still listed lost metadata")
	}
}

func TestRemovalFromAnExtraUpstreamReconciles(t *testing.T) {
	h := newHarness(t)
	h.seedMetadata()
	h.refresh(ReasonPoll)

	h.extra.set(claudeD)
	res := h.refresh(ReasonPoll)

	if strings.Join(res.Removed, ",") != "claude-b.json" {
		t.Fatalf("removed = %v, want claude-b.json", res.Removed)
	}
	if h.candidateStatus("claude-b.json") != model.AccountActionStatusDeleted || h.quotaWindows("claude-b.json", "claude", "idx-b", false) != 0 {
		t.Errorf("claude credential removed from the extra upstream was not reconciled")
	}
	if h.candidateStatus("codex-a.json") != model.AccountActionStatusPending {
		t.Errorf("the codex credential must be untouched")
	}
}

func TestSameNameOnAnotherUpstreamIsNotRemoved(t *testing.T) {
	h := newHarness(t)
	h.seedCandidate("shared.json", "idx-s", "codex")
	h.primary.set(codexA, authFile("shared.json", "codex", "idx-s"))
	h.extra.set(claudeB, authFile("shared.json", "codex", "idx-s"))
	h.refresh(ReasonPoll)

	h.primary.set(codexA)
	res := h.refresh(ReasonPoll)

	if !res.Changed {
		t.Fatalf("the membership did change (an upstream lost a file)")
	}
	if got := h.candidateStatus("shared.json"); got != model.AccountActionStatusPending {
		t.Fatalf("candidate status = %q; the name is still listed by the other upstream", got)
	}
}

func TestAddedCredentialAppearsAndReturningOneIsRestored(t *testing.T) {
	h := newHarness(t)
	runID := h.seedMetadata()
	before := h.refresh(ReasonPoll)

	// codex-a goes away and its inspection rows are hidden.
	h.primary.set(codexC)
	h.refresh(ReasonPoll)
	if h.visibleResults(runID)["codex-a.json"] {
		t.Fatalf("precondition: codex-a rows should be hidden")
	}

	// codex-a comes back and a brand new credential is added.
	newcomer := authFile("codex-new.json", "codex", "idx-new")
	h.primary.set(codexA, codexC, newcomer)
	res := h.refresh(ReasonPoll)

	if strings.Join(res.Added, ",") != "codex-a.json,codex-new.json" {
		t.Fatalf("added = %v, want codex-a.json and codex-new.json", res.Added)
	}
	if !h.visibleResults(runID)["codex-a.json"] {
		t.Errorf("a returning credential's inspection rows were not restored")
	}
	snap := h.svc.Snapshot()
	if snap.Count != 5 || snap.Version != res.Version || snap.Version == before.Version || snap.SyncedAtMS != h.nowMS {
		t.Errorf("snapshot = %#v, want 5 credentials under the new version %s", snap, res.Version)
	}
}

func TestFailedOrPartialListingChangesNothing(t *testing.T) {
	cases := []struct {
		name string
		mode string
	}{
		{"http error", "fail"},
		{"200 without a file list", "nolist"},
		{"truncated body", "garbage"},
	}
	for _, tc := range cases {
		for _, which := range []string{"primary", "extra"} {
			t.Run(tc.name+" from "+which, func(t *testing.T) {
				h := newHarness(t)
				runID := h.seedMetadata()
				first := h.refresh(ReasonPoll)

				// The other upstream really did lose a credential; the failure of this one must still hold everything back.
				bad, other := h.extra, h.primary
				if which == "primary" {
					bad, other = h.primary, h.extra
				}
				bad.setMode(tc.mode)
				if which == "primary" {
					other.set(claudeD) // claude-b gone
				} else {
					other.set(codexC) // codex-a gone
				}

				res, err := h.svc.Refresh(context.Background(), ReasonPoll)
				if err == nil {
					t.Fatalf("Refresh = %#v, want an error", res)
				}
				if got := h.svc.Snapshot(); got.Version != first.Version {
					t.Errorf("version moved to %s on a failed listing", got.Version)
				}
				for _, name := range []string{"codex-a.json", "claude-b.json"} {
					if h.candidateStatus(name) != model.AccountActionStatusPending ||
						h.cooldownStatus(name) != model.QuotaCooldownStatusActive ||
						!h.visibleResults(runID)[name] || !h.ownershipNames()[name] {
						t.Errorf("%s: metadata changed although the listing failed", name)
					}
				}
				if h.quotaWindows("codex-a.json", "codex", "idx-a", false) != 1 || h.quotaWindows("claude-b.json", "claude", "idx-b", false) != 1 {
					t.Errorf("quota windows changed although the listing failed")
				}

				// Once the upstream answers again the pending removal is applied.
				bad.setMode("")
				res = h.refresh(ReasonPoll)
				if len(res.Removed) != 1 {
					t.Errorf("after recovery removed = %v, want exactly the one deleted credential", res.Removed)
				}
			})
		}
	}
}

func TestAnUpstreamTurningEmptyNeedsToRepeat(t *testing.T) {
	h := newHarness(t)
	runID := h.seedMetadata()
	h.refresh(ReasonPoll)

	// An unmounted auth directory looks like an empty list.
	h.primary.set()
	held := h.refresh(ReasonPoll)
	if !held.Held {
		t.Fatalf("first empty poll = %#v, want it held", held)
	}
	if h.candidateStatus("codex-a.json") != model.AccountActionStatusPending || !h.visibleResults(runID)["codex-a.json"] {
		t.Fatalf("metadata changed on the first empty poll")
	}

	// It is back before the second poll: nothing was ever removed.
	h.primary.set(codexA, codexC)
	if res := h.refresh(ReasonPoll); res.Held || len(res.Removed) != 0 {
		t.Fatalf("recovered listing = %#v, want a clean pass", res)
	}

	// Empty twice in a row is a real removal.
	h.primary.set()
	if res := h.refresh(ReasonPoll); !res.Held {
		t.Fatalf("empty poll after recovery = %#v, want held again", res)
	}
	res := h.refresh(ReasonPoll)
	if res.Held || strings.Join(res.Removed, ",") != "codex-a.json,codex-c.json" {
		t.Fatalf("confirmed empty poll = %#v, want both codex credentials removed", res)
	}
	if h.candidateStatus("codex-a.json") != model.AccountActionStatusDeleted {
		t.Errorf("confirmed removal was not applied")
	}
}

func TestAnalyticsHistoryIsUntouchedByReconcile(t *testing.T) {
	h := newHarness(t)
	h.seedMetadata()
	before := h.seedUsageHistory()
	if before.count != 2 {
		t.Fatalf("seeded %d usage events, want 2", before.count)
	}
	h.refresh(ReasonPoll)

	h.primary.set(codexC)
	h.extra.set(claudeD)
	res := h.refresh(ReasonPoll)
	if len(res.Removed) != 2 {
		t.Fatalf("removed = %v, want codex-a and claude-b", res.Removed)
	}

	after := h.usageSnapshot()
	if after != before {
		t.Fatalf("usage history changed by the reconcile:\nbefore %+v\nafter  %+v", before, after)
	}
	// The removed credentials' events are still attributed to them.
	if !strings.Contains(after.export, "codex-a.json") || !strings.Contains(after.export, "claude-b.json") {
		t.Fatalf("usage export no longer carries the removed credentials: %s", after.export)
	}
}

func TestProxiedDeleteReconcilesWithoutWaitingForAPoll(t *testing.T) {
	h := newHarness(t)
	runID := h.seedMetadata()
	h.refresh(ReasonPoll) // the watcher has seen the credentials; no poll runs after this

	proxy := proxysvc.New(h.config, h.st).WithExtraUpstreams([]string{h.extra.URL}).WithAuthFilesChangeHook(h.svc.Notify)
	serve := func(method, target string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		proxy.ProxyManagement(w, httptest.NewRequest(method, target, nil), func(w http.ResponseWriter, status int, err error) {
			http.Error(w, err.Error(), status)
		})
		return w
	}

	// A delete the upstream rejects changes nothing.
	h.primary.setDeleteFail(true)
	if w := serve(http.MethodDelete, "/v0/management/auth-files?name=codex-a.json"); w.Code == http.StatusOK {
		t.Fatalf("delete status = %d, want the upstream failure", w.Code)
	}
	if h.candidateStatus("codex-a.json") != model.AccountActionStatusPending || !h.visibleResults(runID)["codex-a.json"] {
		t.Fatalf("a failed proxied delete reconciled metadata")
	}

	// A delete that succeeds is reflected by the time the request returns.
	h.primary.setDeleteFail(false)
	if w := serve(http.MethodDelete, "/v0/management/auth-files?name=codex-a.json"); w.Code != http.StatusOK {
		t.Fatalf("delete status = %d: %s", w.Code, w.Body.String())
	}
	if got := h.candidateStatus("codex-a.json"); got != model.AccountActionStatusDeleted {
		t.Errorf("candidate status after proxied delete = %q, want deleted immediately", got)
	}
	if h.cooldownStatus("codex-a.json") != "not-active" || h.visibleResults(runID)["codex-a.json"] ||
		h.ownershipNames()["codex-a.json"] || h.quotaWindows("codex-a.json", "codex", "idx-a", false) != 0 {
		t.Errorf("metadata of the deleted credential survived the proxied delete")
	}
	if h.candidateStatus("claude-b.json") != model.AccountActionStatusPending {
		t.Errorf("an unrelated credential was touched")
	}
}

func TestProxiedDeleteOfTheLastCredentialDoesNotWaitForConfirmation(t *testing.T) {
	h := newHarness(t)
	h.primary.set(codexA)
	h.seedMetadata()
	h.refresh(ReasonPoll)

	proxy := proxysvc.New(h.config, h.st).WithExtraUpstreams([]string{h.extra.URL}).WithAuthFilesChangeHook(h.svc.Notify)
	w := httptest.NewRecorder()
	proxy.ProxyManagement(w, httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name=codex-a.json", nil),
		func(w http.ResponseWriter, status int, err error) { http.Error(w, err.Error(), status) })
	if w.Code != http.StatusOK {
		t.Fatalf("delete status = %d: %s", w.Code, w.Body.String())
	}
	if got := h.candidateStatus("codex-a.json"); got != model.AccountActionStatusDeleted {
		t.Fatalf("candidate status = %q; the user's own delete is the evidence, no second poll needed", got)
	}
}

func TestVersionIgnoresOrderAndTracksMembershipAndDisabled(t *testing.T) {
	file := func(name string, disabled bool) cpaauthfiles.File {
		return cpaauthfiles.File{Name: name, Disabled: disabled}
	}
	a := map[string]cpaauthfiles.File{fileKey("u1", "a"): file("a", false), fileKey("u2", "b"): file("b", false)}
	reordered := map[string]cpaauthfiles.File{fileKey("u2", "b"): file("b", false), fileKey("u1", "a"): file("a", false)}
	if versionOf(a) != versionOf(reordered) {
		t.Fatalf("version depends on map order")
	}
	disabled := map[string]cpaauthfiles.File{fileKey("u1", "a"): file("a", true), fileKey("u2", "b"): file("b", false)}
	moved := map[string]cpaauthfiles.File{fileKey("u2", "a"): file("a", false), fileKey("u2", "b"): file("b", false)}
	removed := map[string]cpaauthfiles.File{fileKey("u1", "a"): file("a", false)}
	seen := map[string]string{versionOf(a): "base"}
	for name, files := range map[string]map[string]cpaauthfiles.File{"disabled": disabled, "moved upstream": moved, "removed": removed} {
		v := versionOf(files)
		if prev, dup := seen[v]; dup {
			t.Errorf("%s has the same version as %s", name, prev)
		}
		seen[v] = name
	}
	if len(versionOf(a)) != 16 {
		t.Errorf("version %q is not 16 hex characters", versionOf(a))
	}
}

func TestFreshListsOnlyWhenTheLastAttemptIsOlderThanMaxAge(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	first := h.svc.Fresh(ctx, 2*time.Second)
	if first.Count != 4 || first.Version == "" || h.primary.listingCount() != 1 {
		t.Fatalf("first Fresh = %#v after %d listings, want 4 credentials from one listing", first, h.primary.listingCount())
	}

	h.primary.set(codexA)
	h.nowMS += 1_000
	if got := h.svc.Fresh(ctx, 2*time.Second); got != first || h.primary.listingCount() != 1 {
		t.Fatalf("Fresh within maxAge = %#v after %d listings, want the cached snapshot", got, h.primary.listingCount())
	}

	h.nowMS += 2_000
	got := h.svc.Fresh(ctx, 2*time.Second)
	if got.Count != 3 || got.Version == first.Version || h.primary.listingCount() != 2 {
		t.Fatalf("Fresh after maxAge = %#v after %d listings, want a new listing with 3 credentials", got, h.primary.listingCount())
	}

	// A failing upstream is not hammered: one attempt per maxAge, and the snapshot is kept.
	h.primary.setMode("fail")
	h.nowMS += 3_000
	failed := h.svc.Fresh(ctx, 2*time.Second)
	h.svc.Fresh(ctx, 2*time.Second)
	if failed != got || h.primary.listingCount() != 3 {
		t.Fatalf("Fresh against a failing upstream = %#v after %d listings, want the kept snapshot from a single attempt", failed, h.primary.listingCount())
	}
}
