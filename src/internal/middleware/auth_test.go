package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/kevingruber/gradle-cache/internal/config"
)

func init() { gin.SetMode(gin.TestMode) }

func run(auth config.AuthConfig, requireWriter bool, user, pass string) int {
	r := gin.New()
	r.GET("/", CacheAuth(auth, requireWriter), func(c *gin.Context) { c.Status(http.StatusOK) })
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if user != "" || pass != "" {
		req.SetBasicAuth(user, pass)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w.Code
}

// A role with empty configured credentials must never match, even if the client
// also sends empty credentials.
func TestEmptyConfiguredCredentialsNeverMatch(t *testing.T) {
	auth := config.AuthConfig{
		Reader: config.UserAuth{Username: "", Password: ""},
		Writer: config.UserAuth{Username: "", Password: ""},
	}
	if code := run(auth, false, "", ""); code != http.StatusUnauthorized {
		t.Fatalf("empty creds against empty config: got %d, want 401", code)
	}
}

func TestValidCredentials(t *testing.T) {
	auth := config.AuthConfig{
		Reader: config.UserAuth{Username: "reader", Password: "r"},
		Writer: config.UserAuth{Username: "writer", Password: "w"},
	}
	if code := run(auth, false, "reader", "r"); code != http.StatusOK {
		t.Fatalf("reader read: got %d, want 200", code)
	}
	if code := run(auth, true, "reader", "r"); code != http.StatusForbidden {
		t.Fatalf("reader write: got %d, want 403", code)
	}
	if code := run(auth, true, "writer", "w"); code != http.StatusOK {
		t.Fatalf("writer write: got %d, want 200", code)
	}
	if code := run(auth, false, "writer", "bad"); code != http.StatusUnauthorized {
		t.Fatalf("bad password: got %d, want 401", code)
	}
}
