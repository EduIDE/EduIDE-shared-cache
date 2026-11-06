package server

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/kevingruber/gradle-cache/internal/config"
	"github.com/kevingruber/gradle-cache/internal/storage"
	"github.com/rs/zerolog"
)

// fakeStorage is an in-memory Storage used for tests. A single backing map is
// shared across all namespaced views so we can assert the exact keys written.
type fakeStorage struct {
	mu        *sync.Mutex
	data      map[string][]byte
	namespace string
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{mu: &sync.Mutex{}, data: map[string][]byte{}}
}

func (f *fakeStorage) key(k string) string {
	if f.namespace == "" {
		return k
	}
	return f.namespace + ":" + k
}

func (f *fakeStorage) Get(_ context.Context, key string) (io.ReadCloser, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[f.key(key)]
	if !ok {
		return nil, 0, storage.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(v)), int64(len(v)), nil
}

func (f *fakeStorage) Put(_ context.Context, key string, reader io.Reader, _ int64) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[f.key(key)] = data
	return nil
}

func (f *fakeStorage) Exists(_ context.Context, key string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.data[f.key(key)]
	return ok, nil
}

func (f *fakeStorage) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, f.key(key))
	return nil
}

func (f *fakeStorage) Ping(_ context.Context) error { return nil }

func (f *fakeStorage) WithNamespace(namespace string) storage.Storage {
	ns := namespace
	if f.namespace != "" {
		ns = f.namespace + ":" + namespace
	}
	return &fakeStorage{mu: f.mu, data: f.data, namespace: ns}
}

func testConfig() *config.Config {
	return &config.Config{
		Cache: config.CacheConfig{MaxEntrySizeMB: 100},
		Auth: config.AuthConfig{
			Enabled: true,
			Reader:  config.UserAuth{Username: "reader", Password: "r-pass"},
			Writer:  config.UserAuth{Username: "writer", Password: "w-pass"},
		},
		Metrics: config.MetricsConfig{Enabled: false},
		Logging: config.LoggingConfig{Level: "error", Format: "json"},
	}
}

func newTestServer(t *testing.T, cfg *config.Config, store storage.Storage) *Server {
	t.Helper()
	return New(cfg, store, zerolog.Nop())
}

func doPut(s *Server, key, user, pass, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPut, "/gradle/"+key, bytes.NewBufferString(body))
	if user != "" || pass != "" {
		req.SetBasicAuth(user, pass)
	}
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)
	return w
}

func doGet(s *Server, key, user, pass string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/gradle/"+key, nil)
	if user != "" || pass != "" {
		req.SetBasicAuth(user, pass)
	}
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)
	return w
}

// A configured namespace prefixes every Gradle key with "<ns>:gradle:".
func TestGradleKeyIsNamespaced(t *testing.T) {
	store := newFakeStorage()
	cfg := testConfig()
	cfg.Cache.Namespace = "ex1"
	s := newTestServer(t, cfg, store)

	if w := doPut(s, "abc", "writer", "w-pass", "payload"); w.Code != http.StatusCreated {
		t.Fatalf("PUT: got %d, want 201", w.Code)
	}

	if got, ok := store.data["ex1:gradle:abc"]; !ok || string(got) != "payload" {
		t.Fatalf("expected key %q with %q, have keys %v", "ex1:gradle:abc", "payload", keys(store))
	}

	if w := doGet(s, "abc", "reader", "r-pass"); w.Code != http.StatusOK || w.Body.String() != "payload" {
		t.Fatalf("GET: got %d %q, want 200 %q", w.Code, w.Body.String(), "payload")
	}
}

// A writer in one namespace cannot reach another namespace's entries.
func TestNamespaceIsolation(t *testing.T) {
	store := newFakeStorage()

	cfgA := testConfig()
	cfgA.Cache.Namespace = "ex1"
	srvA := newTestServer(t, cfgA, store)

	cfgB := testConfig()
	cfgB.Cache.Namespace = "ex2"
	srvB := newTestServer(t, cfgB, store)

	if w := doPut(srvA, "shared", "writer", "w-pass", "from-a"); w.Code != http.StatusCreated {
		t.Fatalf("PUT A: got %d, want 201", w.Code)
	}

	if w := doGet(srvB, "shared", "reader", "r-pass"); w.Code != http.StatusNotFound {
		t.Fatalf("GET B: got %d, want 404 (cross-namespace must miss)", w.Code)
	}
}

// Even a key crafted to look like another namespace stays inside the caller's prefix.
func TestKeyCannotEscapeNamespace(t *testing.T) {
	store := newFakeStorage()
	cfg := testConfig()
	cfg.Cache.Namespace = "ex1"
	s := newTestServer(t, cfg, store)

	if w := doPut(s, "ex2:gradle:evil", "writer", "w-pass", "x"); w.Code != http.StatusCreated {
		t.Fatalf("PUT: got %d, want 201", w.Code)
	}
	if _, ok := store.data["ex2:gradle:evil"]; ok {
		t.Fatalf("key escaped its namespace")
	}
	if _, ok := store.data["ex1:gradle:ex2:gradle:evil"]; !ok {
		t.Fatalf("key not stored under caller namespace, have %v", keys(store))
	}
}

func TestWriteRequiresWriterRole(t *testing.T) {
	store := newFakeStorage()
	s := newTestServer(t, testConfig(), store)

	if w := doPut(s, "k", "", "", "data"); w.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous PUT: got %d, want 401", w.Code)
	}
	if w := doPut(s, "k", "reader", "r-pass", "data"); w.Code != http.StatusForbidden {
		t.Fatalf("reader PUT: got %d, want 403", w.Code)
	}
	if w := doPut(s, "k", "writer", "wrong", "data"); w.Code != http.StatusUnauthorized {
		t.Fatalf("bad-password PUT: got %d, want 401", w.Code)
	}
	if w := doPut(s, "k", "writer", "w-pass", "data"); w.Code != http.StatusCreated {
		t.Fatalf("writer PUT: got %d, want 201", w.Code)
	}
}

// With auth disabled, reads are anonymous but writes must still be authenticated.
func TestAuthDisabledStillProtectsWrites(t *testing.T) {
	store := newFakeStorage()
	cfg := testConfig()
	cfg.Auth.Enabled = false
	s := newTestServer(t, cfg, store)

	if w := doGet(s, "k", "", ""); w.Code != http.StatusNotFound {
		t.Fatalf("anonymous GET: got %d, want 404 (anonymous reads allowed)", w.Code)
	}
	if w := doPut(s, "k", "", "", "data"); w.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous PUT: got %d, want 401 (writes always require auth)", w.Code)
	}
	// Writer credentials still work for writes even when auth.enabled is false.
	if w := doPut(s, "k", "writer", "w-pass", "data"); w.Code != http.StatusCreated {
		t.Fatalf("writer PUT: got %d, want 201", w.Code)
	}
}

// A wrong username is rejected (constant-time comparison must still deny access).
func TestWrongUsernameRejected(t *testing.T) {
	store := newFakeStorage()
	s := newTestServer(t, testConfig(), store)

	if w := doGet(s, "k", "nobody", "r-pass"); w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong username GET: got %d, want 401", w.Code)
	}
}

func keys(f *fakeStorage) []string {
	out := make([]string, 0, len(f.data))
	for k := range f.data {
		out = append(out, k)
	}
	return out
}
