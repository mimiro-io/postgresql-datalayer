package middlewares

import (
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

func TestVerifyAudienceAndIssuer(t *testing.T) {
	tests := []struct {
		name     string
		aud      jwt.ClaimStrings
		iss      string
		expected string
	}{
		{"no claims", nil, "", ""},
		{"matching claims", jwt.ClaimStrings{"other", "aud"}, "iss", ""},
		{"wrong audience", jwt.ClaimStrings{"other"}, "iss", "invalid audience"},
		{"wrong issuer", jwt.ClaimStrings{"aud"}, "other", "invalid issuer"},
		{"both wrong reports issuer", jwt.ClaimStrings{"other"}, "other", "invalid issuer"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claims := &CustomClaims{RegisteredClaims: jwt.RegisteredClaims{Audience: tt.aud, Issuer: tt.iss}}
			err := verifyAudienceAndIssuer(claims, "aud", "iss")
			got := ""
			if err != nil {
				got = err.Error()
			}
			if got != tt.expected {
				t.Errorf("got %q, want %q", got, tt.expected)
			}
		})
	}
}
