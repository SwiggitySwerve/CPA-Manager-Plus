package proxy

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newSplitService is a Codex primary plus a Claude extra, the layout of the cli-proxy deployment.
func newSplitService(t *testing.T) (svc *Service, codex, claude *fakeCPA) {
	t.Helper()
	codex, claude = newFakeCPA(t, 200, codexFile), newFakeCPA(t, 200, claudeFile)
	return newMultiService(t, codex, claude).WithProviderUpstreams([]string{"claude=" + claude.URL}), codex, claude
}

func uploadRequest(t *testing.T, name, content string) *http.Request {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	part, err := mw.CreateFormFile("file", name)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write([]byte(content))
	_ = mw.Close()
	r := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	r.Header.Set("Content-Type", mw.FormDataContentType())
	return r
}

func TestOAuthStartGoesToTheUpstreamThatTakesTheProvider(t *testing.T) {
	svc, codex, claude := newSplitService(t)

	// "anthropic" is the OAuth name for provider "claude".
	w := serve(svc, httptest.NewRequest(http.MethodGet, "/v0/management/anthropic-auth-url?is_webui=true", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "state-of-"+claude.URL) {
		t.Fatalf("anthropic start: %d %s, want the Claude instance's session", w.Code, w.Body.String())
	}
	if !strings.Contains(claude.lastRequest(), "is_webui=true") {
		t.Fatalf("query was not forwarded: %q", claude.lastRequest())
	}
	if codex.hits.Load() != 0 {
		t.Fatalf("anthropic start reached the primary")
	}

	w = serve(svc, httptest.NewRequest(http.MethodGet, "/v0/management/codex-auth-url", nil))
	if !strings.Contains(w.Body.String(), "state-of-"+codex.URL) {
		t.Fatalf("codex start: %s, want the primary's session", w.Body.String())
	}
}

func TestOAuthStatusAndCancelFollowTheInstanceThatIssuedTheState(t *testing.T) {
	svc, codex, claude := newSplitService(t)
	serve(svc, httptest.NewRequest(http.MethodGet, "/v0/management/anthropic-auth-url", nil))
	serve(svc, httptest.NewRequest(http.MethodGet, "/v0/management/codex-auth-url", nil))
	codexState, claudeState := "state-of-"+codex.URL, "state-of-"+claude.URL

	for _, tc := range []struct {
		method, path string
		want         *fakeCPA
	}{
		{http.MethodGet, "/v0/management/get-auth-status?state=" + claudeState, claude},
		{http.MethodGet, "/v0/management/get-auth-status?state=" + codexState, codex},
		{http.MethodDelete, "/v0/management/oauth-session?state=" + claudeState, claude},
	} {
		tc.want.last.Store("")
		w := serve(svc, httptest.NewRequest(tc.method, tc.path, nil))
		if w.Code != 200 || !strings.HasPrefix(tc.want.lastRequest(), tc.method+" "+tc.path) {
			t.Fatalf("%s %s: status %d, last request on expected instance = %q", tc.method, tc.path, w.Code, tc.want.lastRequest())
		}
	}
}

func TestOAuthStatusForAnUnknownStateGoesToThePrimary(t *testing.T) {
	svc, codex, claude := newSplitService(t)

	serve(svc, httptest.NewRequest(http.MethodGet, "/v0/management/get-auth-status?state=nobody-issued-this", nil))

	if codex.hits.Load() != 1 || claude.hits.Load() != 0 {
		t.Fatalf("hits primary=%d claude=%d, want the primary only", codex.hits.Load(), claude.hits.Load())
	}
}

func TestOAuthCallbackRoutesByStateThenByProvider(t *testing.T) {
	svc, codex, claude := newSplitService(t)
	serve(svc, httptest.NewRequest(http.MethodGet, "/v0/management/anthropic-auth-url", nil))
	state := "state-of-" + claude.URL

	post := func(body string) {
		r := httptest.NewRequest(http.MethodPost, "/v0/management/oauth-callback", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		if w := serve(svc, r); w.Code != 200 {
			t.Fatalf("callback %s: %d %s", body, w.Code, w.Body.String())
		}
	}

	post(`{"provider":"anthropic","redirect_url":"http://localhost:54545/callback?code=c&state=` + state + `"}`)
	if got := claude.lastRequest(); !strings.Contains(got, "/oauth-callback") || !strings.Contains(got, "code=c") {
		t.Fatalf("callback with a known state: Claude instance saw %q (body must arrive intact)", got)
	}

	codex.hits.Store(0)
	post(`{"provider":"claude","redirect_url":"http://localhost:54545/callback?code=d&state=forgotten"}`)
	if codex.hits.Load() != 0 || !strings.Contains(claude.lastRequest(), "code=d") {
		t.Fatalf("callback with a forgotten state must still go by provider; claude saw %q", claude.lastRequest())
	}
}

func TestUploadedCredentialGoesToTheInstanceForItsType(t *testing.T) {
	svc, codex, claude := newSplitService(t)

	if w := serve(svc, uploadRequest(t, "c.json", `{"type":"claude","email":"x@y"}`)); w.Code != 200 {
		t.Fatalf("claude upload: %d %s", w.Code, w.Body.String())
	}
	if !strings.Contains(claude.lastRequest(), `"type":"claude"`) || codex.hits.Load() != 0 {
		t.Fatalf("claude upload: claude saw %q, primary hits %d", claude.lastRequest(), codex.hits.Load())
	}

	for _, content := range []string{`{"type":"codex"}`, `{"type":"mystery"}`, `not json`} {
		claude.last.Store("")
		if w := serve(svc, uploadRequest(t, "o.json", content)); w.Code != 200 {
			t.Fatalf("upload %s: %d %s", content, w.Code, w.Body.String())
		}
		if !strings.Contains(codex.lastRequest(), content) || claude.lastRequest() != "" {
			t.Fatalf("upload %s: primary saw %q, claude saw %q", content, codex.lastRequest(), claude.lastRequest())
		}
	}
}

func TestOAuthAndUploadStayOnThePrimaryWithoutProviderUpstreams(t *testing.T) {
	codex, claude := newFakeCPA(t, 200, codexFile), newFakeCPA(t, 200, claudeFile)
	svc := newMultiService(t, codex, claude)

	serve(svc, httptest.NewRequest(http.MethodGet, "/v0/management/anthropic-auth-url", nil))
	serve(svc, uploadRequest(t, "c.json", `{"type":"claude"}`))

	if claude.hits.Load() != 0 {
		t.Fatalf("extra upstream hit %d times with no provider mapping", claude.hits.Load())
	}
}
