package middlewares

import (
	"encoding/json"
	"errors"
	"github.com/golang-jwt/jwt/v5"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/goburrow/cache"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

type (
	Auth0Config struct {
		// Skipper defines a function to skip middleware.
		Skipper middleware.Skipper

		// BeforeFunc defines a function which is executed just before the middleware.
		BeforeFunc middleware.BeforeFunc

		Cache         cache.LoadingCache
		Wellknown     string
		Audience      string
		Issuer        string
		AudienceAuth0 string
		IssuerAuth0   string
	}
)

type CustomClaims struct {
	Scope string `json:"scope"`
	Gty   string `json:"gty"`
	Adm   bool   `json:"adm"`
	jwt.RegisteredClaims
}

func (claims CustomClaims) scopes() []string {
	return strings.Split(claims.Scope, ",")
}

type Response struct {
	Message string `json:"message"`
}

type Jwks struct {
	Keys []JSONWebKeys `json:"keys"`
}

type JSONWebKeys struct {
	Kty string   `json:"kty"`
	Kid string   `json:"kid"`
	Use string   `json:"use"`
	N   string   `json:"n"`
	E   string   `json:"e"`
	X5c []string `json:"x5c"`
}

// Errors
var (
	ErrJWTMissing = echo.NewHTTPError(http.StatusBadRequest, "missing or malformed jwt")
	parser        = jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}), jwt.WithIssuedAt())
)

func newCache(wellknown string) cache.LoadingCache {
	load := func(k cache.Key) (cache.Value, error) {
		resp, err := http.Get(wellknown)
		if err != nil {
			return nil, err
		}

		defer resp.Body.Close()

		var jwks = Jwks{}
		err = json.NewDecoder(resp.Body).Decode(&jwks)

		return jwks, err
	}

	lc := cache.NewLoadingCache(load,
		cache.WithMaximumSize(10),
		cache.WithExpireAfterAccess(10*time.Second),
		cache.WithRefreshAfterWrite(60*time.Second),
	)
	return lc
}

func JWTHandler(config *Auth0Config) echo.MiddlewareFunc {
	if config.Cache == nil {
		config.Cache = newCache(config.Wellknown)
	}

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if config.Skipper(c) {
				return next(c)
			}

			if config.BeforeFunc != nil {
				config.BeforeFunc(c)
			}

			auth, err := extractToken(c)
			if err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized, err.Error())
			}

			token := new(jwt.Token)
			token, err = parser.ParseWithClaims(auth, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
				cert, err := getPemCert(token, config)
				if err != nil {
					return nil, err
				}
				result, err := jwt.ParseRSAPublicKeyFromPEM([]byte(cert))
				if err != nil {
					return nil, err
				}
				return result, nil
			})

			if err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized, err.Error())
			}

			// we need to handle compatibility between our and auth0 tokens
			var (
				audience string
				issuer   string
			)
			claims := token.Claims.(*CustomClaims)
			if strings.Index(claims.Issuer, "auth0.com") != -1 { // auth0
				audience = config.AudienceAuth0
				issuer = config.IssuerAuth0
			} else { // ours?
				audience = config.Audience
				issuer = config.Issuer
			}

			if err = verifyAudienceAndIssuer(claims, audience, issuer); err == nil {
				c.Set("user", token)
				return next(c)
			}

			return echo.NewHTTPError(http.StatusUnauthorized, err.Error())
		}
	}
}

// verifyAudienceAndIssuer accepts a token without aud or iss claims, and otherwise requires them to match.
func verifyAudienceAndIssuer(claims *CustomClaims, audience string, issuer string) error {
	if claims.Issuer != "" && claims.Issuer != issuer {
		return errors.New("invalid issuer")
	}
	if len(claims.Audience) > 0 && !slices.Contains(claims.Audience, audience) {
		return errors.New("invalid audience")
	}
	return nil
}

func (config *Auth0Config) SigningKey() func(token *jwt.Token) string {
	return func(token *jwt.Token) string {
		cert, err := getPemCert(token, config)
		if err != nil {
			return ""
		}
		result, _ := jwt.ParseRSAPublicKeyFromPEM([]byte(cert))
		return result.N.String()
	}
}

func getPemCert(token *jwt.Token, config *Auth0Config) (string, error) {
	cert := ""
	result, err := config.Cache.Get("well-known")

	if err != nil {
		return cert, err
	}

	switch jwks := result.(type) {
	case Jwks:
		for k := range jwks.Keys {
			if token.Header["kid"] == jwks.Keys[k].Kid {
				cert = "-----BEGIN CERTIFICATE-----\n" + jwks.Keys[k].X5c[0] + "\n-----END CERTIFICATE-----"
			}
		}

		if cert == "" {
			err := errors.New("Unable to find appropriate key.")
			return cert, err
		}

		return cert, nil
	default:
		err := errors.New("No Jwks returned")
		return cert, err
	}

}

func extractToken(c echo.Context) (string, error) {
	auth := c.Request().Header.Get("Authorization")
	l := len("Bearer")
	if len(auth) > l+1 && auth[:l] == "Bearer" {
		return auth[l+1:], nil
	}
	return "", ErrJWTMissing
}
