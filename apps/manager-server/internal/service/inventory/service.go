// Package inventory keeps CPA Manager Plus's account metadata in step with the
// CLI-Proxy credential listings it can reach.
//
// The CLI-Proxy auth files are the only real credential store. This service
// lists them from every configured upstream every few seconds, derives a
// membership version, and when credentials were removed or added it reconciles
// the metadata keyed by them (cooldowns, account-action candidates, Codex
// inspection results and ownership, quota-snapshot current windows). Usage,
// request and dollar history is never touched.
//
// Absence is only evidence when every upstream answered with a real list. A
// failed, partial or malformed listing changes nothing.
package inventory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/cpa"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/cpaauthfiles"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

const (
	// PollInterval is how often the watcher lists the upstreams.
	PollInterval = 5 * time.Second
	// refreshTimeout bounds one listing-and-reconcile pass.
	refreshTimeout = 20 * time.Second
	// emptyConfirmations is how many consecutive polls must report an upstream
	// as empty before that counts. An unmounted or wiped auth directory looks
	// like an empty list; a real "last credential deleted" through this manager
	// skips the wait because the mutation itself is the evidence.
	emptyConfirmations = 2

	ReasonPoll     = "poll"
	ReasonMutation = "mutation"

	cooldownRemovedReason = "credential removed from CLI-Proxy"
)

// SetupResolver yields the primary upstream and the management key shared by all upstreams.
type SetupResolver interface {
	ResolveSetup(ctx context.Context) (store.Setup, bool, error)
}

// Lister lists one upstream's auth files and fails unless the answer held a real list.
type Lister interface {
	FetchInventory(ctx context.Context, baseURL string, managementKey string) ([]cpaauthfiles.File, error)
}

// Result describes one refresh.
type Result struct {
	Version string
	// Changed is true when the membership version moved (or this is the first listing).
	Changed bool
	// Held is true when a listing was complete but an upstream reported itself empty
	// for the first time, so nothing was applied yet.
	Held    bool
	Added   []string
	Removed []string
}

// Snapshot is the last applied inventory.
type Snapshot struct {
	Version    string `json:"version"`
	SyncedAtMS int64  `json:"syncedAtMs"`
	Count      int    `json:"count"`
}

type Service struct {
	setups SetupResolver
	lister Lister
	store  *store.Store
	extra  []string
	now    func() time.Time

	mu          sync.Mutex
	primed      bool
	version     string
	syncedAt    time.Time
	count       int
	files       map[string]cpaauthfiles.File // "upstream\x00name" -> file, from the last applied listing
	names       map[string]struct{}          // live names and runtime ids
	emptyStreak map[string]int               // upstream -> consecutive empty complete listings
	lastErr     string
}

// New builds the service. extraUpstreams are the CPA_EXTRA_UPSTREAM_URLS bases.
func New(st *store.Store, setups SetupResolver, extraUpstreams []string) *Service {
	return NewWithLister(st, setups, cpaauthfiles.New(nil), extraUpstreams)
}

// NewWithLister is New with an explicit lister.
func NewWithLister(st *store.Store, setups SetupResolver, lister Lister, extraUpstreams []string) *Service {
	s := &Service{
		setups:      setups,
		lister:      lister,
		store:       st,
		now:         time.Now,
		emptyStreak: map[string]int{},
	}
	for _, raw := range extraUpstreams {
		if base := cpa.NormalizeBaseURL(raw); base != "" {
			s.extra = append(s.extra, base)
		}
	}
	return s
}

// Start polls until ctx ends.
func (s *Service) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(PollInterval)
		defer ticker.Stop()
		for {
			s.poll(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *Service) poll(ctx context.Context) {
	runCtx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()
	if _, err := s.Refresh(runCtx, ReasonPoll); err != nil && ctx.Err() == nil {
		s.logOnce(err)
	}
}

// logOnce logs an error only when it differs from the previous one, so a down
// upstream does not log every five seconds.
func (s *Service) logOnce(err error) {
	s.mu.Lock()
	msg := err.Error()
	repeat := s.lastErr == msg
	s.lastErr = msg
	s.mu.Unlock()
	if !repeat {
		log.Printf("inventory: refresh failed; metadata left unchanged: %v", err)
	}
}

// Snapshot returns the last applied inventory version.
func (s *Service) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := Snapshot{Version: s.version, Count: s.count}
	if !s.syncedAt.IsZero() {
		snap.SyncedAtMS = s.syncedAt.UnixMilli()
	}
	return snap
}

// Notify is for callers that just mutated auth files through this manager. It
// reconciles immediately and does not wait for the next poll.
func (s *Service) Notify(ctx context.Context) {
	runCtx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()
	if _, err := s.Refresh(runCtx, ReasonMutation); err != nil && ctx.Err() == nil {
		s.logOnce(err)
	}
}

// Refresh lists every upstream and, when the membership changed, reconciles.
// Any listing failure returns an error before anything is changed.
func (s *Service) Refresh(ctx context.Context, reason string) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	setup, ok, err := s.setups.ResolveSetup(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("resolve setup: %w", err)
	}
	if !ok {
		return Result{}, nil
	}
	bases := s.bases(setup.CPAUpstreamURL)

	next := make(map[string]cpaauthfiles.File)
	counts := make(map[string]int, len(bases))
	for _, base := range bases {
		files, err := s.lister.FetchInventory(ctx, base, setup.ManagementKey)
		if err != nil {
			return Result{Version: s.version}, fmt.Errorf("list %s: %w", base, err)
		}
		counts[base] = len(files)
		for _, file := range files {
			next[fileKey(base, file.Name)] = file
		}
	}

	if reason != ReasonMutation && s.holdEmpty(bases, counts) {
		return Result{Version: s.version, Held: true}, nil
	}
	for _, base := range bases {
		if counts[base] > 0 {
			delete(s.emptyStreak, base)
		}
	}

	version := versionOf(next)
	liveNames := namesOf(next)
	result := Result{Version: version, Changed: !s.primed || version != s.version}
	result.Added, result.Removed = diffNames(s.names, liveNames)
	namesChanged := !s.primed || len(result.Added) > 0 || len(result.Removed) > 0

	if namesChanged {
		if err := s.reconcile(ctx, s.files, next, liveNames); err != nil {
			// Keep the previous applied state so the next pass retries.
			return result, fmt.Errorf("reconcile: %w", err)
		}
	}
	s.primed = true
	s.version = version
	s.files = next
	s.names = liveNames
	s.count = len(next)
	s.syncedAt = s.now()
	return result, nil
}

// holdEmpty reports whether an upstream listed as empty has not yet been
// confirmed. It counts the streak as a side effect.
func (s *Service) holdEmpty(bases []string, counts map[string]int) bool {
	hold := false
	for _, base := range bases {
		if counts[base] > 0 {
			continue
		}
		if s.primed && s.countFor(base) == 0 {
			continue // it was already empty; nothing is being removed
		}
		s.emptyStreak[base]++
		if s.emptyStreak[base] < emptyConfirmations {
			hold = true
		}
	}
	return hold
}

func (s *Service) countFor(base string) int {
	n := 0
	prefix := base + "\x00"
	for key := range s.files {
		if strings.HasPrefix(key, prefix) {
			n++
		}
	}
	return n
}

func (s *Service) bases(primary string) []string {
	first := cpa.NormalizeBaseURL(primary)
	bases := []string{first}
	seen := map[string]bool{first: true}
	for _, base := range s.extra {
		if !seen[base] {
			seen[base] = true
			bases = append(bases, base)
		}
	}
	return bases
}

func fileKey(base, name string) string { return base + "\x00" + name }

// versionOf hashes the sorted (upstream, name, disabled) of every listed file.
// It carries no account data other than file names.
func versionOf(files map[string]cpaauthfiles.File) string {
	lines := make([]string, 0, len(files))
	for key, file := range files {
		base, _, _ := strings.Cut(key, "\x00")
		lines = append(lines, fmt.Sprintf("%s\t%s\t%t", base, file.Name, file.Disabled))
	}
	sort.Strings(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return hex.EncodeToString(sum[:])[:16]
}

// namesOf is the set of names a stored row may refer to: the listing name and the runtime id.
func namesOf(files map[string]cpaauthfiles.File) map[string]struct{} {
	names := make(map[string]struct{}, len(files)*2)
	for _, file := range files {
		if name := strings.TrimSpace(file.Name); name != "" {
			names[name] = struct{}{}
		}
		if id := strings.TrimSpace(file.ID); id != "" {
			names[id] = struct{}{}
		}
	}
	return names
}

func diffNames(prev, next map[string]struct{}) (added, removed []string) {
	for name := range next {
		if _, ok := prev[name]; !ok {
			added = append(added, name)
		}
	}
	for name := range prev {
		if _, ok := next[name]; !ok {
			removed = append(removed, name)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	return added, removed
}

// reconcile applies a complete, successful listing to the metadata tables.
func (s *Service) reconcile(
	ctx context.Context,
	prev map[string]cpaauthfiles.File,
	next map[string]cpaauthfiles.File,
	live map[string]struct{},
) error {
	isLive := func(name string) bool {
		_, ok := live[strings.TrimSpace(name)]
		return ok
	}
	nowMS := s.now().UnixMilli()
	var errs []error

	// Cooldowns: the credential is gone, so CPA Manager Plus can never re-enable it.
	cooldowns, err := s.store.QuotaCooldowns.ListActive(ctx)
	if err != nil {
		errs = append(errs, fmt.Errorf("list cooldowns: %w", err))
	}
	for _, cooldown := range cooldowns {
		if isLive(cooldown.AuthFileName) {
			continue
		}
		if err := s.store.QuotaCooldowns.MarkSkipped(ctx, cooldown.ID, cooldownRemovedReason); err != nil {
			errs = append(errs, fmt.Errorf("skip cooldown %d: %w", cooldown.ID, err))
		}
	}

	if _, err := s.store.AccountActions.MarkRemovedCredentials(ctx, isLive, nowMS); err != nil {
		errs = append(errs, fmt.Errorf("account action candidates: %w", err))
	}

	if _, err := s.store.CodexInspections.SyncResultCredentialVisibility(ctx, isLive, nowMS); err != nil {
		errs = append(errs, fmt.Errorf("codex inspection results: %w", err))
	}

	ownerships, err := s.store.CodexInspections.ListDisableOwnership(ctx)
	if err != nil {
		errs = append(errs, fmt.Errorf("list disable ownership: %w", err))
	}
	revoked := map[string]bool{}
	for _, item := range ownerships {
		if isLive(item.FileName) || revoked[item.FileName] {
			continue
		}
		revoked[item.FileName] = true
		if err := s.store.CodexInspections.DeleteDisableOwnership(ctx, model.CodexInspectionDisableOwnershipTarget{FileName: item.FileName}); err != nil {
			errs = append(errs, fmt.Errorf("disable ownership %q: %w", item.FileName, err))
		}
	}

	if keys := removedAccountKeys(prev, next, live); len(keys) > 0 {
		if _, err := s.store.QuotaSnapshots.DeactivateAccounts(ctx, keys, nowMS); err != nil {
			errs = append(errs, fmt.Errorf("quota snapshot windows: %w", err))
		}
	}
	return errors.Join(errs...)
}

// removedAccountKeys returns the quota-window account keys of credentials that
// were in prev and are gone from next. Quota windows are keyed by an opaque
// identity derived from the credential's fields, not by file name, so the keys
// come from the previous listing. A key that a still-listed credential also
// produces is never returned.
func removedAccountKeys(prev, next map[string]cpaauthfiles.File, live map[string]struct{}) []string {
	liveKeys := map[string]struct{}{}
	for _, file := range next {
		for _, key := range accountKeys(file) {
			liveKeys[key] = struct{}{}
		}
	}
	seen := map[string]struct{}{}
	var keys []string
	for _, file := range prev {
		if _, ok := live[strings.TrimSpace(file.Name)]; ok {
			continue
		}
		for _, key := range accountKeys(file) {
			if _, isLive := liveKeys[key]; isLive {
				continue
			}
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

// accountKeys are the identities quota windows of this credential may be stored under.
func accountKeys(file cpaauthfiles.File) []string {
	label, _ := file.Raw["label"].(string)
	project := file.AccountID
	if strings.EqualFold(file.Provider, "codex") {
		project = usageidentity.CodexAccountIDSnapshot(file.AccountID)
	}
	fields := usageidentity.Fields{
		AuthFileSnapshot:      strings.TrimSpace(file.Name),
		AuthIndex:             strings.TrimSpace(file.AuthIndex),
		AuthProviderSnapshot:  file.Provider,
		AuthAccountIDSnapshot: file.AccountID,
		AuthProjectIDSnapshot: project,
		AccountSnapshot:       file.AccountSnapshot,
		AuthLabelSnapshot:     strings.TrimSpace(label),
	}
	var keys []string
	if key, ok := usageidentity.AccountKey(fields); ok {
		keys = append(keys, key)
	}
	if key, ok := usageidentity.LegacyAccountKey(fields); ok && (len(keys) == 0 || keys[0] != key) {
		keys = append(keys, key)
	}
	return keys
}
