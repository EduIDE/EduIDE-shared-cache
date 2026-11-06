package middleware

import (
	"crypto/subtle"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/kevingruber/gradle-cache/internal/config"
)

// CacheAuth creates a middleware that validates HTTP Basic Authentication
func CacheAuth(auth config.AuthConfig, requireWriter bool) gin.HandlerFunc {

	return func(c *gin.Context) {
		username, password, ok := c.Request.BasicAuth()
		if !ok {
			c.Header("WWW-Authenticate", `Basic realm="Gradle Build Cache"`)
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		// Check credentials. Both username and password are compared in
		// constant time, and a role with an unset username or password can
		// never match (prevents an empty-credential bypass).
		isReader := credentialsMatch(username, password, auth.Reader)
		isWriter := credentialsMatch(username, password, auth.Writer)

		if !isReader && !isWriter {
			c.Header("WWW-Authenticate", `Basic realm="Gradle Build Cache"`)
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		if requireWriter && !isWriter {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}

		c.Next()
	}
}

// credentialsMatch reports whether the supplied username and password match the
// configured user, using constant-time comparison for both fields. A user with
// an empty configured username or password is treated as disabled and never matches.
func credentialsMatch(username, password string, u config.UserAuth) bool {
	if u.Username == "" || u.Password == "" {
		return false
	}
	userOK := subtle.ConstantTimeCompare([]byte(username), []byte(u.Username)) == 1
	passOK := subtle.ConstantTimeCompare([]byte(password), []byte(u.Password)) == 1
	return userOK && passOK
}
