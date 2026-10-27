package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/managerconfig"
)

// fakeCPA is one CLIProxyAPI instance: it lists the given auth files and records every other request.
type fakeCPA struct {
	*httptest.Server
	hits atomic.Int32
	last atomic.Value // string: "METHOD path?query body"
}

func newFakeCPA(t *testing.T, status int, files ...map[string]any) *fakeCPA {
	t.Helper()
	f := &fakeCPA{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.hits.Add(1)
		if r.Header.Get("Authorization") != "Bearer management-key" {
			http.Error(w, "bad key", http.StatusUnauthorized)
			return
		}
		body, _ := io.ReadAll(r.Body)
		f.last.Store(r.Method + " " + r.URL.RequestURI() + " " + string(body))
		if status != http.StatusOK {
			http.Error(w, "boom", status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && r.URL.Path == "/v0/management/auth-files" {
			_ = json.NewEncoder(w).Encode(map[string]any{"files": files, "observed_at": f.URL})
			return
		}
		if strings.HasSuffix(r.URL.Path, "-auth-url") {
			_, _ = w.Write([]byte(`{"status":"ok","state":"state-of-` + f.URL + `","url":"https://login"}`))
			return
		}
		_, _ = w.Write([]byte(`{"from":"` + f.URL + `"}`))
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeCPA) lastRequest() string {
	v, _ := f.last.Load().(string)
	return v
}

func newMultiService(t *testing.T, primary *fakeCPA, extras ...*fakeCPA) *Service {
	t.Helper()
	cfg := config.Config{CPAUpstreamURL: primary.URL, ManagementKey: "management-key"}
	svc := New(managerconfig.New(cfg, nil, nil))
	urls := make([]string, 0, len(extras))
	for _, e := range extras {
		urls = append(urls, e.URL)
	}
	return svc.WithExtraUpstreams(urls)
}

func serve(svc *Service, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	svc.ProxyManagement(w, r, func(w http.ResponseWriter, status int, err error) {
		http.Error(w, err.Error(), status)
	})
	return w
}

func listedFiles(t *testing.T, w *httptest.ResponseRecorder) []map[string]any {
	t.Helper()
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body.String())
	}
	var out struct {
		Files []map[string]any `json:"files"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v: %s", err, w.Body.String())
	}
	return out.Files
}

var (
	codexFile  = map[string]any{"name": "codex-a.json", "auth_index": "codex-idx", "provider": "codex"}
	claudeFile = map[string]any{"name": "claude-b.json", "auth_index": "claude-idx", "provider": "claude"}
)

func TestMergedAuthFilesListsEveryUpstream(t *testing.T) {
	primary := newFakeCPA(t, 200, codexFile)
	extra := newFakeCPA(t, 200, claudeFile)
	svc := newMultiService(t, primary, extra)

	w := serve(svc, httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil))

	files := listedFiles(t, w)
	if len(files) != 2 || files[0]["auth_index"] != "codex-idx" || files[1]["auth_index"] != "claude-idx" {
		t.Fatalf("merged files = %v, want codex then claude", files)
	}
	var body map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if body["observed_at"] != primary.URL {
		t.Fatalf("observed_at = %v, want the primary's", body["observed_at"])
	}
	for _, h := range []string{"X-CPAMP-Upstream-Errors", "X-CPAMP-Upstream-Conflicts"} {
		if v := w.Header().Get(h); v != "" {
			t.Fatalf("%s = %q on a clean merge", h, v)
		}
	}
}

func TestMergedAuthFilesKeepsPrimaryEntryOnCollisionAndSaysSo(t *testing.T) {
	primary := newFakeCPA(t, 200, map[string]any{"name": "same.json", "auth_index": "dup", "provider": "codex"})
	extra := newFakeCPA(t, 200,
		map[string]any{"name": "same.json", "auth_index": "dup", "provider": "claude"}, claudeFile)
	svc := newMultiService(t, primary, extra)

	w := serve(svc, httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil))

	files := listedFiles(t, w)
	if len(files) != 2 || files[0]["provider"] != "codex" {
		t.Fatalf("files = %v, want the primary's duplicate kept once plus the extra's unique file", files)
	}
	if got := w.Header().Get("X-CPAMP-Upstream-Conflicts"); !strings.Contains(got, "dup") {
		t.Fatalf("conflict header = %q, want it to name the duplicated auth_index", got)
	}
}

func TestMergedAuthFilesSurvivesAnExtraUpstreamOutage(t *testing.T) {
	primary := newFakeCPA(t, 200, codexFile)
	extra := newFakeCPA(t, 500)
	svc := newMultiService(t, primary, extra)

	w := serve(svc, httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil))

	if files := listedFiles(t, w); len(files) != 1 {
		t.Fatalf("files = %v, want only the primary's", files)
	}
	if got := w.Header().Get("X-CPAMP-Upstream-Errors"); !strings.Contains(got, extra.URL) {
		t.Fatalf("error header = %q, want it to name the failed upstream", got)
	}
}

func TestMergedAuthFilesFailsWhenThePrimaryIsDown(t *testing.T) {
	primary := newFakeCPA(t, 500)
	extra := newFakeCPA(t, 200, claudeFile)
	svc := newMultiService(t, primary, extra)

	w := serve(svc, httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil))

	if w.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 (a partial list without the primary would hide its accounts)", w.Code)
	}
}

func TestAuthFilesPassThroughUnchangedWithoutExtraUpstreams(t *testing.T) {
	primary := newFakeCPA(t, 200, codexFile)
	svc := newMultiService(t, primary)

	w := serve(svc, httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil))

	if files := listedFiles(t, w); len(files) != 1 {
		t.Fatalf("files = %v", files)
	}
}

func TestAPICallRoutesToTheUpstreamThatOwnsTheAuthIndex(t *testing.T) {
	primary := newFakeCPA(t, 200, codexFile)
	extra := newFakeCPA(t, 200, claudeFile)
	svc := newMultiService(t, primary, extra)

	for _, tc := range []struct {
		name, body string
		want, not  *fakeCPA
	}{
		{"claude camelCase", `{"authIndex":"claude-idx","method":"GET","url":"https://x"}`, extra, primary},
		{"claude snake_case", `{"auth_index":"claude-idx","method":"GET","url":"https://x"}`, extra, primary},
		{"codex", `{"authIndex":"codex-idx","method":"GET","url":"https://x"}`, primary, extra},
		{"unknown falls back to primary", `{"authIndex":"nobody","method":"GET","url":"https://x"}`, primary, extra},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.want.hits.Store(0)
			tc.not.hits.Store(0)
			tc.want.last.Store("")
			tc.not.last.Store("")
			r := httptest.NewRequest(http.MethodPost, "/v0/management/api-call", strings.NewReader(tc.body))
			r.Header.Set("Content-Type", "application/json")
			w := serve(svc, r)
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d: %s", w.Code, w.Body.String())
			}
			if !strings.Contains(tc.want.lastRequest(), "POST /v0/management/api-call") ||
				!strings.Contains(tc.want.lastRequest(), tc.body) {
				t.Fatalf("expected upstream got %q", tc.want.lastRequest())
			}
			if n := tc.not.hits.Load(); n > 1 { // the auth-files lookup is allowed, the call itself is not
				t.Fatalf("other upstream was hit %d times", n)
			}
			if strings.Contains(tc.not.lastRequest(), "api-call") {
				t.Fatalf("api-call also reached the wrong upstream: %q", tc.not.lastRequest())
			}
		})
	}
}

func TestAuthFileRequestsRouteByNameToTheOwningUpstream(t *testing.T) {
	primary := newFakeCPA(t, 200, codexFile)
	extra := newFakeCPA(t, 200, claudeFile)
	svc := newMultiService(t, primary, extra)

	w := serve(svc, httptest.NewRequest(http.MethodGet, "/v0/management/auth-files/download?name=claude-b.json", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(extra.lastRequest(), "download?name=claude-b.json") {
		t.Fatalf("extra got %q", extra.lastRequest())
	}
	if strings.Contains(primary.lastRequest(), "download") {
		t.Fatalf("download reached the primary: %q", primary.lastRequest())
	}
}

func TestOwnershipIsRelearnedWhenAnAccountMovesBetweenUpstreams(t *testing.T) {
	var onExtra atomic.Bool
	primary := newFakeCPA(t, 200, codexFile)
	extra := newFakeCPA(t, 200, claudeFile)
	moved := map[string]any{"name": "moved.json", "auth_index": "moved-idx", "provider": "claude"}
	// "moved" first lives on the primary, then is deleted there and appears on the extra.
	primary.Server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		files := []map[string]any{codexFile}
		if !onExtra.Load() {
			files = append(files, moved)
		}
		primary.last.Store(r.Method + " " + r.URL.RequestURI())
		_ = json.NewEncoder(w).Encode(map[string]any{"files": files})
	})
	extra.Server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		files := []map[string]any{claudeFile}
		if onExtra.Load() {
			files = append(files, moved)
		}
		extra.last.Store(r.Method + " " + r.URL.RequestURI())
		_ = json.NewEncoder(w).Encode(map[string]any{"files": files})
	})
	svc := newMultiService(t, primary, extra)
	call := func() {
		r := httptest.NewRequest(http.MethodPost, "/v0/management/api-call",
			strings.NewReader(`{"authIndex":"moved-idx"}`))
		r.Header.Set("Content-Type", "application/json")
		serve(svc, r)
	}

	call()
	if !strings.Contains(primary.lastRequest(), "api-call") {
		t.Fatalf("first call should reach the primary, got %q", primary.lastRequest())
	}
	onExtra.Store(true)
	svc.owners.fetchedAt = time.Time{} // the cached answer has expired
	call()
	if !strings.Contains(extra.lastRequest(), "api-call") {
		t.Fatalf("after the move the call should reach the extra, got %q", extra.lastRequest())
	}
}

func TestWithExtraUpstreamsIgnoresBlankAndPrimaryDuplicates(t *testing.T) {
	primary := newFakeCPA(t, 200, codexFile)
	svc := newMultiService(t, primary)
	svc = svc.WithExtraUpstreams([]string{"", " ", primary.URL + "/", primary.URL})
	bases := svc.upstreamBases(primary.URL)
	if len(bases) != 1 {
		t.Fatalf("bases = %v, want just the primary", bases)
	}
}

// modelServer is an instance's OpenAI-style /v1/models, which the panel calls with a client API key.
func modelServer(t *testing.T, status int, ids ...string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer client-key" {
			http.Error(w, `{"error":"Invalid API key"}`, http.StatusUnauthorized)
			return
		}
		if status != http.StatusOK {
			http.Error(w, "boom", status)
			return
		}
		data := make([]map[string]any, 0, len(ids))
		for _, id := range ids {
			data = append(data, map[string]any{"id": id, "object": "model"})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func serveModels(t *testing.T, primary *httptest.Server, extras ...*httptest.Server) *httptest.ResponseRecorder {
	t.Helper()
	svc := New(managerconfig.New(config.Config{CPAUpstreamURL: primary.URL, ManagementKey: "management-key"}, nil, nil))
	urls := make([]string, 0, len(extras))
	for _, e := range extras {
		urls = append(urls, e.URL)
	}
	svc.WithExtraUpstreams(urls)
	r := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	r.Header.Set("Authorization", "Bearer client-key")
	w := httptest.NewRecorder()
	svc.ProxyModelList(w, r,
		func(w http.ResponseWriter, status int, err error) { http.Error(w, err.Error(), status) },
		func(w http.ResponseWriter) { http.Error(w, "method", http.StatusMethodNotAllowed) })
	return w
}

func modelIDs(t *testing.T, w *httptest.ResponseRecorder) []string {
	t.Helper()
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body.String())
	}
	var out struct {
		Object string           `json:"object"`
		Data   []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v: %s", err, w.Body.String())
	}
	ids := make([]string, 0, len(out.Data))
	for _, m := range out.Data {
		ids = append(ids, m["id"].(string))
	}
	return ids
}

func TestMergedModelListUnionsEveryUpstream(t *testing.T) {
	w := serveModels(t, modelServer(t, 200, "gpt-a", "gpt-b"), modelServer(t, 200, "claude-c"))
	if got := strings.Join(modelIDs(t, w), ","); got != "gpt-a,gpt-b,claude-c" {
		t.Fatalf("models = %s", got)
	}
}

func TestMergedModelListListsAModelOnceAndSurvivesAnExtraOutage(t *testing.T) {
	w := serveModels(t, modelServer(t, 200, "shared", "gpt-a"), modelServer(t, 200, "shared", "claude-c"))
	if got := strings.Join(modelIDs(t, w), ","); got != "shared,gpt-a,claude-c" {
		t.Fatalf("models = %s", got)
	}
	down := serveModels(t, modelServer(t, 200, "gpt-a"), modelServer(t, 500))
	if got := strings.Join(modelIDs(t, down), ","); got != "gpt-a" {
		t.Fatalf("models with extra down = %s", got)
	}
	if down.Header().Get("X-CPAMP-Upstream-Errors") == "" {
		t.Fatalf("an unreachable extra upstream must be reported")
	}
}

func TestMergedModelListReturnsThePrimarysErrorUnchanged(t *testing.T) {
	primary := modelServer(t, 200, "gpt-a")
	svc := New(managerconfig.New(config.Config{CPAUpstreamURL: primary.URL, ManagementKey: "management-key"}, nil, nil)).
		WithExtraUpstreams([]string{modelServer(t, 200, "claude-c").URL})
	r := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	r.Header.Set("Authorization", "Bearer wrong-key")
	w := httptest.NewRecorder()
	svc.ProxyModelList(w, r, func(w http.ResponseWriter, status int, err error) { http.Error(w, err.Error(), status) },
		func(w http.ResponseWriter) {})
	if w.Code != http.StatusUnauthorized || !strings.Contains(w.Body.String(), "Invalid API key") {
		t.Fatalf("got %d %s, want the primary's 401 body", w.Code, w.Body.String())
	}
}

func TestModelListPassesThroughWithoutExtraUpstreams(t *testing.T) {
	if got := strings.Join(modelIDs(t, serveModels(t, modelServer(t, 200, "gpt-a"))), ","); got != "gpt-a" {
		t.Fatalf("models = %s", got)
	}
}
