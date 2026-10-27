package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/cpa"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
)

// Deployments that run one CPA instance per provider (CPA_EXTRA_UPSTREAM_URLS) have each credential on exactly
// one instance. The panel only talks to the primary, so this file makes the management proxy present all
// instances as one: GET auth-files is the union, and every request that names a credential (api-call by
// auth_index, auth-file reads and mutations by name) is sent to the instance that owns it. All instances share
// the primary connection's management key. Requests that name no credential go to the primary, as before.

const (
	ownerCacheTTL           = 5 * time.Second
	authFilesListLimitBytes = 64 * 1024 * 1024
	routingBodyLimitBytes   = 1024 * 1024
	authFilesPath           = "/v0/management/auth-files"
	upstreamErrorsHeader    = "X-CPAMP-Upstream-Errors"
	upstreamConflictsHeader = "X-CPAMP-Upstream-Conflicts"
)

var upstreamHTTPClient = &http.Client{Timeout: 30 * time.Second}

// upstreamOwners remembers which upstream holds each auth_index and file name.
type upstreamOwners struct {
	mu        sync.Mutex
	byKey     map[string]string
	fetchedAt time.Time
}

// WithExtraUpstreams adds the instances whose credentials are merged into the primary's.
func (s *Service) WithExtraUpstreams(urls []string) *Service {
	s.extraUpstreams = nil
	for _, raw := range urls {
		if base := cpa.NormalizeBaseURL(raw); base != "" {
			s.extraUpstreams = append(s.extraUpstreams, base)
		}
	}
	return s
}

// upstreamBases returns the primary first, then each distinct extra.
func (s *Service) upstreamBases(primary string) []string {
	first := cpa.NormalizeBaseURL(primary)
	bases := []string{first}
	seen := map[string]bool{first: true}
	for _, base := range s.extraUpstreams {
		if !seen[base] {
			seen[base] = true
			bases = append(bases, base)
		}
	}
	return bases
}

type authFilesListing struct {
	base  string
	top   map[string]json.RawMessage
	files []json.RawMessage
	err   error
}

func fetchAuthFiles(ctx context.Context, base, managementKey, rawQuery string) authFilesListing {
	listing := authFilesListing{base: base}
	target := base + authFilesPath
	if rawQuery != "" {
		target += "?" + rawQuery
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		listing.err = err
		return listing
	}
	req.Header.Set("Authorization", "Bearer "+managementKey)
	res, err := upstreamHTTPClient.Do(req)
	if err != nil {
		listing.err = err
		return listing
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, authFilesListLimitBytes))
	if err != nil {
		listing.err = err
		return listing
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		listing.err = fmt.Errorf("HTTP %d %s", res.StatusCode, strings.TrimSpace(string(body[:min(len(body), 200)])))
		return listing
	}
	if err := json.Unmarshal(body, &listing.top); err != nil {
		listing.err = fmt.Errorf("decode auth-files: %w", err)
		return listing
	}
	if raw, ok := listing.top["files"]; ok {
		if err := json.Unmarshal(raw, &listing.files); err != nil {
			listing.err = fmt.Errorf("decode auth-files.files: %w", err)
		}
	}
	return listing
}

func fetchAllAuthFiles(ctx context.Context, bases []string, managementKey, rawQuery string) []authFilesListing {
	listings := make([]authFilesListing, len(bases))
	var wg sync.WaitGroup
	for i, base := range bases {
		wg.Add(1)
		go func(i int, base string) {
			defer wg.Done()
			listings[i] = fetchAuthFiles(ctx, base, managementKey, rawQuery)
		}(i, base)
	}
	wg.Wait()
	return listings
}

// authFileIdentity returns the key a file is deduplicated by (auth_index, else name) and its name.
func authFileIdentity(raw json.RawMessage) (identity, authIndex, name string) {
	var file struct {
		Name      string          `json:"name"`
		AuthIndex json.RawMessage `json:"auth_index"`
	}
	_ = json.Unmarshal(raw, &file)
	authIndex = normalizeJSONScalarString(file.AuthIndex)
	name = strings.TrimSpace(file.Name)
	if authIndex != "" {
		return authIndex, authIndex, name
	}
	return "name:" + name, "", name
}

// mergeAuthFiles unions the listings in order. A credential reported by two upstreams is kept once, from the
// earlier upstream, and named in the returned conflicts.
func mergeAuthFiles(listings []authFilesListing) (merged []json.RawMessage, conflicts []string) {
	merged = make([]json.RawMessage, 0)
	seen := map[string]bool{}
	for _, listing := range listings {
		if listing.err != nil {
			continue
		}
		for _, file := range listing.files {
			identity, _, name := authFileIdentity(file)
			if identity == "name:" && name == "" {
				merged = append(merged, file) // nothing to deduplicate on; do not drop it
				continue
			}
			if seen[identity] {
				conflicts = append(conflicts, strings.TrimPrefix(identity, "name:"))
				continue
			}
			seen[identity] = true
			merged = append(merged, file)
		}
	}
	return merged, conflicts
}

// remember replaces the ownership map with what the successful listings say. Entries of an upstream that
// failed to answer are kept, so a brief outage does not reroute its credentials to the wrong instance.
func (o *upstreamOwners) remember(listings []authFilesListing) {
	o.mu.Lock()
	defer o.mu.Unlock()
	next := map[string]string{}
	failed := map[string]bool{}
	for _, listing := range listings {
		if listing.err != nil {
			failed[listing.base] = true
			continue
		}
		for _, file := range listing.files {
			_, authIndex, name := authFileIdentity(file)
			for _, key := range []string{authIndex, name} {
				if _, taken := next[key]; key != "" && !taken {
					next[key] = listing.base
				}
			}
		}
	}
	for key, base := range o.byKey {
		if _, replaced := next[key]; failed[base] && !replaced {
			next[key] = base
		}
	}
	o.byKey = next
	o.fetchedAt = time.Now()
}

func isAuthFilesListRequest(r *http.Request) bool {
	return r.Method == http.MethodGet && strings.TrimRight(r.URL.Path, "/") == authFilesPath
}

func (s *Service) serveMergedAuthFiles(w http.ResponseWriter, r *http.Request, bases []string, managementKey string,
	writeError func(http.ResponseWriter, int, error)) {
	listings := fetchAllAuthFiles(r.Context(), bases, managementKey, r.URL.RawQuery)
	if listings[0].err != nil {
		// Without the primary the list would silently lose its accounts, so fail like a plain proxy would.
		writeError(w, http.StatusBadGateway, fmt.Errorf("GET %s on %s: %w", authFilesPath, listings[0].base, listings[0].err))
		return
	}
	var failures []string
	for _, listing := range listings[1:] {
		if listing.err != nil {
			log.Printf("proxy: auth-files from extra upstream %s failed: %v", listing.base, listing.err)
			failures = append(failures, listing.base)
		}
	}
	s.owners.remember(listings)
	merged, conflicts := mergeAuthFiles(listings)
	if len(conflicts) > 0 {
		log.Printf("proxy: credentials present on more than one upstream, using the first: %s", strings.Join(conflicts, ", "))
		w.Header().Set(upstreamConflictsHeader, strings.Join(conflicts, ","))
	}
	if len(failures) > 0 {
		w.Header().Set(upstreamErrorsHeader, strings.Join(failures, ","))
	}
	top := listings[0].top
	filesJSON, err := json.Marshal(merged)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	top["files"] = filesJSON
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(top)
}

// routeSetup points the request at the upstream that owns the credential it names, or leaves it on the primary.
func (s *Service) routeSetup(r *http.Request, setup store.Setup) store.Setup {
	if len(s.providerUpstreams) > 0 {
		if base := s.oauthRoute(r); base != "" {
			setup.CPAUpstreamURL = base
			return setup
		}
	}
	bases := s.upstreamBases(setup.CPAUpstreamURL)
	if len(bases) < 2 {
		return setup
	}
	keys := requestRoutingKeys(r)
	if len(keys) == 0 {
		return setup
	}
	if base := s.ownerOf(r.Context(), bases, setup.ManagementKey, keys); base != "" {
		setup.CPAUpstreamURL = base
	}
	return setup
}

func (s *Service) ownerOf(ctx context.Context, bases []string, managementKey string, keys []string) string {
	lookup := func() string {
		for _, key := range keys {
			if base, ok := s.owners.byKey[key]; ok {
				return base
			}
		}
		return ""
	}
	s.owners.mu.Lock()
	fresh := time.Since(s.owners.fetchedAt) < ownerCacheTTL
	base := lookup()
	s.owners.mu.Unlock()
	// A fresh cache is trusted for what it knows; a key it lacks may name a credential created since.
	if fresh && base != "" {
		return base
	}
	s.owners.remember(fetchAllAuthFiles(ctx, bases, managementKey, ""))
	s.owners.mu.Lock()
	defer s.owners.mu.Unlock()
	return lookup()
}

// requestRoutingKeys lists the auth_index values and file names a management request refers to.
func requestRoutingKeys(r *http.Request) []string {
	path := strings.TrimRight(r.URL.Path, "/")
	if path != "/v0/management/api-call" && path != authFilesPath && !strings.HasPrefix(path, authFilesPath+"/") {
		return nil
	}
	var keys []string
	add := func(values ...string) {
		for _, value := range values {
			if value = strings.TrimSpace(value); value != "" {
				keys = append(keys, value)
			}
		}
	}
	query := r.URL.Query()
	add(query.Get("auth_index"), query.Get("authIndex"), query.Get("name"), r.Header.Get(authFilePhysicalNameHeader))
	if r.Method != http.MethodGet && isJSONContentType(r.Header.Get("Content-Type")) {
		if body, ok := peekRequestBody(r, routingBodyLimitBytes); ok {
			var payload struct {
				Name         string          `json:"name"`
				Names        []string        `json:"names"`
				AuthIndex    json.RawMessage `json:"auth_index"`
				AuthIndexCC  json.RawMessage `json:"authIndex"`
				PhysicalName string          `json:"cpamp_physical_name"`
			}
			if json.Unmarshal(body, &payload) == nil {
				add(normalizeJSONScalarString(payload.AuthIndex), normalizeJSONScalarString(payload.AuthIndexCC),
					payload.Name, payload.PhysicalName)
				add(payload.Names...)
			}
		}
	}
	return keys
}

// peekRequestBody returns up to limit bytes of the body and leaves r.Body readable from the start. ok is false
// when there is no body, it cannot be read, or it is longer than limit.
func peekRequestBody(r *http.Request, limit int64) (body []byte, ok bool) {
	if r.Body == nil {
		return nil, false
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	rest := r.Body
	r.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(body), rest), rest}
	return body, err == nil && int64(len(body)) <= limit
}

type upstreamModels struct {
	base   string
	status int
	header http.Header
	body   []byte
	err    error
}

func fetchModels(ctx context.Context, base string, auth http.Header) upstreamModels {
	out := upstreamModels{base: base}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/models", nil)
	if err != nil {
		out.err = err
		return out
	}
	for _, name := range []string{"Authorization", "X-Api-Key"} {
		if value := auth.Get(name); value != "" {
			req.Header.Set(name, value)
		}
	}
	res, err := upstreamHTTPClient.Do(req)
	if err != nil {
		out.err = err
		return out
	}
	defer res.Body.Close()
	out.status, out.header = res.StatusCode, res.Header
	out.body, out.err = io.ReadAll(io.LimitReader(res.Body, authFilesListLimitBytes))
	return out
}

// serveMergedModels answers GET /v1/models with the models of every upstream, using the caller's own API key
// on each. A model on several upstreams is listed once, from the earlier one. The primary's error is returned
// unchanged; an extra that fails is left out and named in X-CPAMP-Upstream-Errors.
func (s *Service) serveMergedModels(w http.ResponseWriter, r *http.Request, bases []string,
	writeError func(http.ResponseWriter, int, error)) {
	results := make([]upstreamModels, len(bases))
	var wg sync.WaitGroup
	for i, base := range bases {
		wg.Add(1)
		go func(i int, base string) {
			defer wg.Done()
			results[i] = fetchModels(r.Context(), base, r.Header)
		}(i, base)
	}
	wg.Wait()

	primary := results[0]
	if primary.err != nil {
		writeError(w, http.StatusBadGateway, fmt.Errorf("GET /v1/models on %s: %w", primary.base, primary.err))
		return
	}
	var top map[string]json.RawMessage
	if primary.status < 200 || primary.status >= 300 || json.Unmarshal(primary.body, &top) != nil {
		if ct := primary.header.Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.WriteHeader(primary.status)
		_, _ = w.Write(primary.body)
		return
	}
	merged := make([]json.RawMessage, 0)
	seen := map[string]bool{}
	var failures []string
	for _, result := range results {
		var list struct {
			Data []json.RawMessage `json:"data"`
		}
		if result.err == nil && (result.status < 200 || result.status >= 300) {
			result.err = fmt.Errorf("HTTP %d", result.status)
		}
		if result.err == nil {
			result.err = json.Unmarshal(result.body, &list)
		}
		if result.err != nil {
			log.Printf("proxy: /v1/models from extra upstream %s failed: %v", result.base, result.err)
			failures = append(failures, result.base)
			continue
		}
		for _, model := range list.Data {
			var entry struct {
				ID string `json:"id"`
			}
			_ = json.Unmarshal(model, &entry)
			if entry.ID != "" && seen[entry.ID] {
				continue
			}
			seen[entry.ID] = true
			merged = append(merged, model)
		}
	}
	if len(failures) > 0 {
		w.Header().Set(upstreamErrorsHeader, strings.Join(failures, ","))
	}
	top["data"], _ = json.Marshal(merged)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(top)
}
