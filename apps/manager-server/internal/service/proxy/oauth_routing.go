package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/cpa"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
)

// When each provider has its own CPA instance (CPA_PROVIDER_UPSTREAMS, e.g. claude=http://host:8319), a new
// credential has to be created on the instance that serves its provider: OAuth login starts there, and an
// uploaded auth file goes there. Every instance accepts every provider, so the choice cannot be left to them.
// Providers without an entry stay on the primary.

const (
	oauthSessionTTL = time.Hour
	managementPath  = "/v0/management/"
)

// oauthSessions remembers which upstream issued each OAuth state, so status, cancel and callback return to it.
type oauthSessions struct {
	mu      sync.Mutex
	byState map[string]oauthSession
}

type oauthSession struct {
	base string
	at   time.Time
}

func (o *oauthSessions) remember(state, base string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.byState == nil {
		o.byState = map[string]oauthSession{}
	}
	for old, session := range o.byState {
		if time.Since(session.at) > oauthSessionTTL {
			delete(o.byState, old)
		}
	}
	o.byState[state] = oauthSession{base: base, at: time.Now()}
}

func (o *oauthSessions) baseOf(state string) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	session, ok := o.byState[state]
	if !ok || time.Since(session.at) > oauthSessionTTL {
		return ""
	}
	return session.base
}

// WithProviderUpstreams sets which instance takes new credentials of a provider, from "provider=url" entries.
func (s *Service) WithProviderUpstreams(entries []string) *Service {
	s.providerUpstreams = nil
	for _, entry := range entries {
		provider, raw, found := strings.Cut(entry, "=")
		base := cpa.NormalizeBaseURL(raw)
		if !found || base == "" || canonicalProvider(provider) == "" {
			log.Printf("proxy: ignoring provider upstream %q, want provider=url", entry)
			continue
		}
		if s.providerUpstreams == nil {
			s.providerUpstreams = map[string]string{}
		}
		s.providerUpstreams[canonicalProvider(provider)] = base
	}
	return s
}

// canonicalProvider lowercases a provider name; the OAuth name "anthropic" is the credential type "claude".
func canonicalProvider(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "anthropic" {
		return "claude"
	}
	return name
}

// oauthStartProvider is the provider of a GET /v0/management/<provider>-auth-url request, else "".
func oauthStartProvider(r *http.Request) string {
	path := strings.TrimRight(r.URL.Path, "/")
	if r.Method != http.MethodGet || !strings.HasPrefix(path, managementPath) || !strings.HasSuffix(path, "-auth-url") {
		return ""
	}
	return canonicalProvider(strings.TrimSuffix(strings.TrimPrefix(path, managementPath), "-auth-url"))
}

// serveOAuthStart opens the login session on the instance for the provider and remembers its state.
func (s *Service) serveOAuthStart(w http.ResponseWriter, r *http.Request, setup store.Setup, provider string,
	writeError func(http.ResponseWriter, int, error)) {
	base := cpa.NormalizeBaseURL(setup.CPAUpstreamURL)
	if mapped := s.providerUpstreams[provider]; mapped != "" {
		base = mapped
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, base+r.URL.Path, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	req.URL.RawQuery = r.URL.RawQuery
	req.Header.Set("Authorization", "Bearer "+setup.ManagementKey)
	res, err := upstreamHTTPClient.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Errorf("GET %s on %s: %w", r.URL.Path, base, err))
		return
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, maxAuthFileMutationResponseBytes))
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Errorf("GET %s on %s: %w", r.URL.Path, base, err))
		return
	}
	var started struct {
		State string `json:"state"`
	}
	if res.StatusCode/100 == 2 && json.Unmarshal(body, &started) == nil && started.State != "" {
		s.oauth.remember(started.State, base)
	}
	if ct := res.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(res.StatusCode)
	_, _ = w.Write(body)
}

// oauthRoute returns the upstream a login-follow-up or an upload belongs to, or "" to leave it on the primary.
func (s *Service) oauthRoute(r *http.Request) string {
	path := strings.TrimRight(r.URL.Path, "/")
	switch {
	case r.Method == http.MethodGet && path == managementPath+"get-auth-status",
		r.Method == http.MethodDelete && path == managementPath+"oauth-session":
		return s.oauth.baseOf(r.URL.Query().Get("state"))
	case r.Method == http.MethodPost && path == managementPath+"oauth-callback":
		return s.callbackRoute(r)
	case r.Method == http.MethodPost && path == authFilesPath:
		return s.providerUpstreams[uploadedProvider(r)]
	}
	return ""
}

// callbackRoute prefers the instance that issued the callback's state, then the one for its provider.
func (s *Service) callbackRoute(r *http.Request) string {
	body, ok := peekRequestBody(r, routingBodyLimitBytes)
	if !ok {
		return ""
	}
	var payload struct {
		Provider    string `json:"provider"`
		State       string `json:"state"`
		RedirectURL string `json:"redirect_url"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return ""
	}
	state := payload.State
	if redirect, err := url.Parse(payload.RedirectURL); err == nil && redirect.Query().Get("state") != "" {
		state = redirect.Query().Get("state")
	}
	if base := s.oauth.baseOf(state); base != "" {
		return base
	}
	return s.providerUpstreams[canonicalProvider(payload.Provider)]
}

// uploadedProvider reads the "type" of the credential file in an auth-files upload (multipart file or raw JSON).
func uploadedProvider(r *http.Request) string {
	body, ok := peekRequestBody(r, maxAuthFileMutationRequestBytes)
	if !ok {
		return ""
	}
	content := body
	if mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err == nil && params["boundary"] != "" &&
		strings.HasPrefix(mediaType, "multipart/") {
		reader := multipart.NewReader(bytes.NewReader(body), params["boundary"])
		content = nil
		for {
			part, err := reader.NextPart()
			if err != nil {
				break
			}
			if part.FileName() != "" {
				content, _ = io.ReadAll(part)
				break
			}
		}
	}
	var credential struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(content, &credential) != nil {
		return ""
	}
	return canonicalProvider(credential.Type)
}
