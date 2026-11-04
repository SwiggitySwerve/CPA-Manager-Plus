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
// Absence is only evidence when every upstream answered with a real list, and a
// name only counts as removed once it is absent from two consecutive complete
// listings. The one exception is a name a proxied delete just removed through
// this manager. A failed, partial or malformed listing changes nothing.
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
	// listTimeout bounds the listing half of one pass.
	listTimeout = 20 * time.Second
	// reconcileTimeout bounds the database half of one pass. It is separate so a
	// slow write is not cut short by (or charged to) the listing budget.
	reconcileTimeout = 2 * time.Minute
	// absentConfirmations is how many consecutive complete listings must lack a
	// name before it counts as removed. A partial answer looks like missing names;
	// one listing is not proof.
	absentConfirmations = 2
	// emptyConfirmations is the same for names of an upstream that now lists
	// nothing at all, which is what an unmounted or wiped auth directory looks like.
	emptyConfirmations = 3

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
	// Held is true while some names are absent but not yet confirmed.
	Held bool
	// Added and Removed are names that newly counted as listed or as removed.
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

	// mu serialises passes and guards the working state below.
	mu      sync.Mutex
	primed  bool
	swept   bool                         // the one-time sweep of rows from before this process ran has been done
	files   map[string]cpaauthfiles.File // "upstream\x00name" -> file, from the last applied listing (plus names still pending removal)
	names   map[string]struct{}          // applied live names and runtime ids, including names pending removal
	missing map[string]int               // name -> consecutive complete listings it was absent from
	// emptyStreak counts consecutive complete listings in which an upstream listed nothing.
	emptyStreak map[string]int
	// known are the auth file names the last process applied; only quota keys naming one of
	// them are swept, because a file-kind key can embed a usage source string instead of a file.
	known      map[string]struct{}
	knownSaved string
	epoch      int // bumped by every pass that reconciled, so the version reflects applied state

	// snapMu guards what readers see, so a slow pass never blocks Snapshot.
	snapMu      sync.Mutex
	version     string
	syncedAt    time.Time
	lastAttempt time.Time
	count       int
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
		missing:     map[string]int{},
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
			s.run(ctx, ReasonPoll, cpaauthfiles.MutationNotice{})
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// run takes the pass lock, then does one pass and logs a new failure.
func (s *Service) run(ctx context.Context, reason string, notice cpaauthfiles.MutationNotice) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runLocked(ctx, reason, notice)
}

func (s *Service) runLocked(ctx context.Context, reason string, notice cpaauthfiles.MutationNotice) {
	if _, err := s.refreshLocked(ctx, reason, notice); err != nil && ctx.Err() == nil {
		s.logOnce(err)
	}
}

// logOnce logs an error only when it differs from the previous one, so a down
// upstream does not log every five seconds.
func (s *Service) logOnce(err error) {
	s.snapMu.Lock()
	msg := err.Error()
	repeat := s.lastErr == msg
	s.lastErr = msg
	s.snapMu.Unlock()
	if !repeat {
		log.Printf("inventory: refresh failed; metadata left unchanged: %v", err)
	}
}

// Snapshot returns the last applied inventory version.
func (s *Service) Snapshot() Snapshot {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	snap := Snapshot{Version: s.version, Count: s.count}
	if !s.syncedAt.IsZero() {
		snap.SyncedAtMS = s.syncedAt.UnixMilli()
	}
	return snap
}

// Fresh returns the last applied inventory, listing the upstreams first when no
// pass has been attempted within maxAge. The age is checked after taking the
// pass lock, so concurrent callers share one listing, and a failing upstream is
// retried at most once per maxAge however many callers ask.
func (s *Service) Fresh(ctx context.Context, maxAge time.Duration) Snapshot {
	s.mu.Lock()
	s.snapMu.Lock()
	stale := s.lastAttempt.IsZero() || s.now().Sub(s.lastAttempt) > maxAge
	s.snapMu.Unlock()
	if stale {
		s.runLocked(ctx, ReasonPoll, cpaauthfiles.MutationNotice{})
	}
	s.mu.Unlock()
	return s.Snapshot()
}

// Notify is for callers that just mutated auth files through this manager. It
// lists immediately instead of waiting for the next poll, and treats the names
// the notice says were deleted as removed without a second listing.
func (s *Service) Notify(ctx context.Context, notice cpaauthfiles.MutationNotice) {
	s.run(ctx, ReasonMutation, notice)
}

// Refresh runs one pass. Any listing failure returns an error before anything is changed.
func (s *Service) Refresh(ctx context.Context, reason string) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refreshLocked(ctx, reason, cpaauthfiles.MutationNotice{})
}

func (s *Service) refreshLocked(ctx context.Context, reason string, notice cpaauthfiles.MutationNotice) (Result, error) {
	s.snapMu.Lock()
	s.lastAttempt = s.now()
	prevVersion := s.version
	s.snapMu.Unlock()

	listCtx, cancel := context.WithTimeout(ctx, listTimeout)
	defer cancel()
	setup, ok, err := s.setups.ResolveSetup(listCtx)
	if err != nil {
		return Result{}, fmt.Errorf("resolve setup: %w", err)
	}
	if !ok {
		return Result{}, nil
	}
	bases := s.bases(setup.CPAUpstreamURL)
	next := make(map[string]cpaauthfiles.File)
	perBase := make(map[string]int, len(bases))
	for _, base := range bases {
		files, err := s.lister.FetchInventory(listCtx, base, setup.ManagementKey)
		if err != nil {
			return Result{Version: prevVersion}, fmt.Errorf("list %s: %w", base, err)
		}
		perBase[base] = len(files)
		for _, file := range files {
			next[fileKey(base, file.Name)] = file
		}
	}
	listed := namesOf(next)
	listedCount := len(next)
	for _, base := range bases {
		if perBase[base] == 0 {
			s.emptyStreak[base]++
		} else {
			delete(s.emptyStreak, base)
		}
	}

	if !s.primed {
		// First complete listing of this process: only observe. Rows left from
		// before the process started are swept on a later pass, once a second
		// listing agrees.
		s.primed = true
		s.loadKnown(ctx)
		s.files, s.names = next, listed
		version := versionOf(next, s.epoch)
		s.publish(version, listedCount)
		return Result{Version: version, Changed: true}, nil
	}

	// Names applied as live that this listing lacks, and for how many listings in a row.
	result := Result{}
	nextMissing := map[string]int{}
	confirmed := map[string]struct{}{}
	for name := range s.names {
		if _, ok := listed[name]; ok {
			continue
		}
		if s.deletedByMutation(name, notice) || s.missing[name]+1 >= s.requiredAbsences(name) {
			confirmed[name] = struct{}{}
			continue
		}
		nextMissing[name] = s.missing[name] + 1
	}
	effective := make(map[string]struct{}, len(listed)+len(nextMissing))
	for name := range listed {
		effective[name] = struct{}{}
		if _, ok := s.names[name]; !ok {
			result.Added = append(result.Added, name)
		}
	}
	for name := range nextMissing {
		effective[name] = struct{}{}
	}
	for name := range confirmed {
		result.Removed = append(result.Removed, name)
	}
	sort.Strings(result.Added)
	sort.Strings(result.Removed)
	result.Held = len(nextMissing) > 0

	// The sweep of rows from before this process cannot tell "removed" from "not mounted
	// yet", so it waits while an upstream lists nothing, until that has persisted.
	sweepReady := true
	for _, base := range bases {
		if n := s.emptyStreak[base]; n > 0 && n < emptyConfirmations {
			sweepReady = false
		}
	}
	reconciled := false
	if (s.swept || sweepReady) && (!s.swept || len(confirmed) > 0 || len(result.Added) > 0) {
		quotaKeys, err := s.quotaKeysToDeactivate(ctx, next, confirmed, effective)
		if err != nil {
			return result, fmt.Errorf("reconcile: %w", err)
		}
		// The writes get their own budget: they must not be cut short by, or
		// charged to, the listing deadline, and they outlive a cancelled request.
		writeCtx, cancelWrite := context.WithTimeout(context.WithoutCancel(ctx), reconcileTimeout)
		defer cancelWrite()
		if err := s.reconcile(writeCtx, effective, quotaKeys); err != nil {
			// State is not advanced, so the next pass retries.
			return result, fmt.Errorf("reconcile: %w", err)
		}
		s.swept, reconciled = true, true
	}

	// Keep the files of names still pending removal so their quota keys survive until confirmed.
	for key, file := range s.files {
		if _, pending := nextMissing[file.Name]; pending {
			next[key] = file
		}
	}
	if reconciled {
		s.epoch++
	}
	s.files, s.names, s.missing = next, effective, nextMissing
	s.saveKnown(ctx, effective)
	// The version reflects what has been applied, not the raw listing: it moves on the
	// pass that reconciled, so a panel that refetches on it sees the cleaned rows.
	result.Version = versionOf(next, s.epoch)
	result.Changed = result.Version != prevVersion
	s.publish(result.Version, listedCount)
	return result, nil
}

// requiredAbsences is how many consecutive listings must lack name: more when the
// upstream that held it now lists nothing.
func (s *Service) requiredAbsences(name string) int {
	for key, file := range s.files {
		if file.Name != name && file.ID != name {
			continue
		}
		base, _, _ := strings.Cut(key, "\x00")
		if s.emptyStreak[base] > 0 {
			return emptyConfirmations
		}
	}
	return absentConfirmations
}

func (s *Service) loadKnown(ctx context.Context) {
	names, err := s.store.Settings.LoadKnownAuthFiles(ctx)
	if err != nil {
		log.Printf("inventory: load known auth files: %v", err)
	}
	s.known = make(map[string]struct{}, len(names))
	for _, name := range names {
		s.known[name] = struct{}{}
	}
}

// saveKnown records the applied names (best effort) so the next process knows which names were auth files.
func (s *Service) saveKnown(ctx context.Context, names map[string]struct{}) {
	list := make([]string, 0, len(names))
	for name := range names {
		list = append(list, name)
	}
	sort.Strings(list)
	joined := strings.Join(list, "\x00")
	if joined == s.knownSaved {
		return
	}
	if err := s.store.Settings.SaveKnownAuthFiles(context.WithoutCancel(ctx), list); err != nil {
		log.Printf("inventory: save known auth files: %v", err)
		return
	}
	s.knownSaved = joined
}

func (s *Service) publish(version string, count int) {
	s.snapMu.Lock()
	s.version, s.count, s.syncedAt = version, count, s.now()
	s.snapMu.Unlock()
}

// deletedByMutation reports whether a proxied delete just removed this name.
func (s *Service) deletedByMutation(name string, notice cpaauthfiles.MutationNotice) bool {
	for _, deleted := range notice.Deleted {
		if strings.TrimSpace(deleted) == name {
			return true
		}
	}
	if !notice.DeletedAll || notice.Upstream == "" {
		return false
	}
	prefix := cpa.NormalizeBaseURL(notice.Upstream) + "\x00"
	for key, file := range s.files {
		if strings.HasPrefix(key, prefix) && (file.Name == name || file.ID == name) {
			return true
		}
	}
	return false
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

// versionOf hashes the sorted (upstream, name, disabled) of every applied file and
// the count of reconciling passes. It carries no account data other than file names.
func versionOf(files map[string]cpaauthfiles.File, epoch int) string {
	lines := make([]string, 0, len(files))
	for key, file := range files {
		base, _, _ := strings.Cut(key, "\x00")
		lines = append(lines, fmt.Sprintf("%s\t%s\t%t", base, file.Name, file.Disabled))
	}
	sort.Strings(lines)
	lines = append(lines, fmt.Sprintf("epoch\t%d", epoch))
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

// reconcile applies confirmed absence to the metadata tables. live holds every
// name that must keep its rows.
func (s *Service) reconcile(ctx context.Context, live map[string]struct{}, quotaKeys []string) error {
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

	liveNames := make([]string, 0, len(live))
	for name := range live {
		liveNames = append(liveNames, name)
	}
	if _, err := s.store.CodexInspections.SyncResultCredentialVisibility(ctx, liveNames, nowMS); err != nil {
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

	if len(quotaKeys) > 0 {
		if _, err := s.store.QuotaSnapshots.DeactivateAccounts(ctx, quotaKeys, nowMS); err != nil {
			errs = append(errs, fmt.Errorf("quota snapshot windows: %w", err))
		}
	}
	return errors.Join(errs...)
}

// quotaKeysToDeactivate returns the quota-window account keys of credentials
// that were removed. Quota windows are keyed by an opaque identity derived from
// the credential's fields, not by file name, so:
//   - for names confirmed removed in this process, the keys come from their
//     last listed file;
//   - on the first sweep, active window keys are read from the database, and a
//     key counts only when it embeds the name of an auth file a previous process
//     applied (s.known) that is no longer live (file-based keys only; Codex member
//     and workspace keys name no file).
//
// A key that a still-listed credential also produces is never returned.
func (s *Service) quotaKeysToDeactivate(
	ctx context.Context,
	next map[string]cpaauthfiles.File,
	confirmed map[string]struct{},
	live map[string]struct{},
) ([]string, error) {
	liveKeys := map[string]struct{}{}
	for _, file := range next {
		for _, key := range accountKeys(file) {
			liveKeys[key] = struct{}{}
		}
	}
	seen := map[string]struct{}{}
	var keys []string
	add := func(key string) {
		if _, isLive := liveKeys[key]; isLive {
			return
		}
		if _, dup := seen[key]; dup {
			return
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	for _, file := range s.files {
		if _, gone := confirmed[strings.TrimSpace(file.Name)]; !gone {
			continue
		}
		for _, key := range accountKeys(file) {
			add(key)
		}
	}
	if !s.swept {
		active, err := s.store.QuotaSnapshots.ActiveAccountKeys(ctx)
		if err != nil {
			return nil, fmt.Errorf("list active quota windows: %w", err)
		}
		for _, key := range active {
			name, ok := usageidentity.AuthFileOfKey(key)
			if !ok {
				continue
			}
			// A file-kind key embeds the usage "source" when no auth file snapshot was
			// recorded, so only a name a previous listing really had is an auth file.
			if _, wasAuthFile := s.known[name]; !wasAuthFile {
				continue
			}
			if _, stillLive := live[name]; !stillLive {
				add(key)
			}
		}
	}
	sort.Strings(keys)
	return keys, nil
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
