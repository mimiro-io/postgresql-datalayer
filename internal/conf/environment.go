package conf

import (
	"fmt"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"strings"
)

type Env struct {
	Logger          *zap.SugaredLogger
	Env             string
	LogLevel        string `koanf:"server.log.level"`
	Port            string `koanf:"server.port"`
	ServiceName     string `koanf:"server.service.name"`
	AgentHost       string `koanf:"dd.agent.host"`
	ConfigLocation  string
	RefreshInterval string
	Jwt             JwtConfig `koanf:"jwt"`
	User            User
	Auth            AuthConfig
}

type User struct {
	UserName string
	Password string
}

type AuthConfig struct {
	WellKnown     string
	Audience      string
	AudienceAuth0 string
	Issuer        string
	IssuerAuth0   string
	Middleware    string
}

type JwtConfig struct {
	ClientId     string `koanf:"client_id"`
	ClientSecret string `koanf:"client_secret"`
	Audience     string `koanf:"audience"`
	GrantType    string `koanf:"grant_type"`
	Endpoint     string `koanf:"endpoint"`
}

func NewEnv() (*Env, error) {
	profile, found := os.LookupEnv("PROFILE")
	if !found {
		profile = "local"
	}

	service, _ := os.LookupEnv("SERVICE_NAME")
	logger := GetLogger(profile, zapcore.InfoLevel, service) // add a default logger while loading the env
	logger.Infof("Loading env: %s", profile)

	k := koanf.New(".")
	_ = k.Load(confmap.Provider(map[string]interface{}{
		"server.port":             "8080",
		"log.level":               "INFO",
		"service.name":            "postgresql-datalayer",
		"config.refresh.interval": "@every 60s",
	}, "."), nil) // load defaults

	// Use the POSIX compliant pflag lib instead of Go's flag lib.
	f := flag.NewFlagSet("config", flag.ContinueOnError)
	f.Usage = func() {
		fmt.Println(f.FlagUsages())
		os.Exit(0)
	}

	// Path to one or more config files to load into koanf along with some config params.
	f.String("conf", "local/settings.yaml", "path to the .yaml config file")
	_ = f.Parse(os.Args[1:])

	cFile, err := f.GetString("conf")
	if err != nil {
		return nil, err
	}

	if strings.HasSuffix(cFile, ".yaml") || strings.HasSuffix(cFile, ".yml") {
		if err := k.Load(file.Provider(cFile), yaml.Parser()); err != nil { // allow starting without the file
			logger.Info(fmt.Sprintf("File %s not loaded", cFile))
			logger.Warn(err)
		}
	}

	_ = k.Load(env.ProviderWithValue("", ".", func(s string, v string) (string, interface{}) {
		// Lowercase and get the key while also replacing
		// the _ character with . in the key (koanf delimeter).
		key := strings.Replace(strings.ToLower(s), "_", ".", -1)

		if strings.Contains(v, ",") {
			return key, strings.Split(v, ",")
		}

		if strings.Contains(v, " ") {
			return key, strings.Split(v, " ")
		}

		// Otherwise, return the plain string.
		return key, v
	}), nil)

	c := &Env{
		Logger:          logger,
		Env:             profile,
		LogLevel:        k.String("log.level"),
		Port:            k.String("server.port"),
		ServiceName:     k.String("service.name"),
		AgentHost:       k.String("dd.agent.host"),
		ConfigLocation:  k.String("config.location"),
		RefreshInterval: k.String("config.refresh.interval"),
		User: User{
			UserName: k.String("postgres.db.user"),
			Password: k.String("postgres.db.password"),
		},
		Auth: AuthConfig{
			WellKnown:     k.String("token.well.known"),
			Audience:      k.String("token.audience"),
			AudienceAuth0: k.String("token.audience.auth0"),
			Issuer:        k.String("token.issuer"),
			IssuerAuth0:   k.String("token.issuer.auth0"),
			Middleware:    k.String("authorization.middleware"),
		},
		Jwt: JwtConfig{
			ClientId:     k.String("auth0.client.id"),
			ClientSecret: k.String("auth0.client.secret"),
			Audience:     k.String("auth0.audience"),
			GrantType:    k.String("auth0.grant.type"),
			Endpoint:     k.String("auth0.endpoint"),
		},
	}
	return c, nil
}
