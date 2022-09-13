package conf

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gojektech/heimdall/v6/httpclient"
	"go.uber.org/zap"
)

// JWTConfig contains the auth configuration
type JWTConfig struct {
	ClientId     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	Audience     string `json:"audience"`
	GrantType    string `json:"grant_type"`
	endpoint     string
	logger       *zap.SugaredLogger
	cache        *cache
	mu           sync.Mutex
}

type cache struct {
	until time.Time
	token string
}

type JWTResponse struct {
	AccessToken string `json:"access_token"`
	Scope       string `json:"scope"`
	ExpiresIn   int64  `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

// NewJWTConfig creates a new JWT Auth Config struct, populated with the values from Viper.
func NewJWTConfig(env *Env) *JWTConfig {
	clientId := env.Jwt.ClientId
	if clientId == "" { // we assume it is not configured, so we leave it alone
		return nil
	}

	config := &JWTConfig{
		ClientId:     clientId,
		ClientSecret: env.Jwt.ClientSecret,
		Audience:     env.Jwt.Audience,
		GrantType:    env.Jwt.GrantType,
		endpoint:     env.Jwt.Endpoint,
		logger:       env.Logger.Named("JWT"),
	}
	return config
}

// Token returns a valid token, or an error caused when getting one
// Successive calls to this will return cached values, where the cache
// validity equals the token validity. Experience-wise, this can lead to
// race conditions when the caller asks for a valid token that is about to
// run out, and then it runs out before it can be used.
func (jc *JWTConfig) Token() (string, error) {
	jc.mu.Lock()
	defer jc.mu.Unlock()
	token, err := jc.generateOrGetToken()
	if err != nil {
		jc.logger.Warnf("Error getting token: %w", err)
		return "", err
	}
	return fmt.Sprintf("Bearer %s", token), nil
}

func (jc *JWTConfig) generateOrGetToken() (string, error) {
	now := time.Now()
	if jc.cache == nil || now.After(jc.cache.until) {
		// first run
		res, err := jc.callRemote()
		if err != nil {
			return "", err
		}
		jc.cache = &cache{
			until: time.Now().Add(time.Duration(res.ExpiresIn) * time.Second),
			token: res.AccessToken,
		}
	}

	return jc.cache.token, nil
}

func (jc *JWTConfig) callRemote() (*JWTResponse, error) {
	timeout := 1000 * time.Millisecond
	client := httpclient.NewClient(httpclient.WithHTTPTimeout(timeout))

	jc.logger.Infof("Got: clientid: %s, audience: %s, grantType: %s", jc.ClientId, jc.Audience, jc.GrantType)

	requestBody, err := json.Marshal(map[string]string{
		"client_id":     jc.ClientId,
		"client_secret": jc.ClientSecret,
		"audience":      jc.Audience,
		"grant_type":    jc.GrantType,
	})
	if err != nil {
		jc.logger.Warn(err)
		return nil, err
	}

	req, err := http.NewRequest("POST", jc.endpoint, bytes.NewBuffer(requestBody))
	if err != nil {
		jc.logger.Warn(err)
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")

	jc.logger.Debugf("Calling token issuer: %s", jc.endpoint)
	res, err := client.Do(req)
	if err != nil {
		jc.logger.Warn(err)
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, errors.New("not authorized, missing secret perhaps")
	}

	jwtResponse := &JWTResponse{}
	err = json.NewDecoder(res.Body).Decode(jwtResponse)
	if err != nil {
		return nil, err
	}
	return jwtResponse, nil
}
