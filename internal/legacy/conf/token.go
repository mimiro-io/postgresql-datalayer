package conf

type TokenProvider interface {
	Token() (string, error)
}

type TokenProviders struct {
	Providers map[string]interface{}
}

// NewTokenProviders provides a map of token providers, keyed on the
// name of the token provider struct as lower_case.
func NewTokenProviders(env *Env) *TokenProviders {
	var providers = make(map[string]interface{}, 0)
	jc := NewJWTConfig(env)
	if jc != nil {
		providers["jwttokenprovider"] = jc
	}

	return &TokenProviders{
		Providers: providers,
	}

}

func NoOpTokenProviders() *TokenProviders {
	var providers = make(map[string]interface{}, 0)
	return &TokenProviders{Providers: providers}
}
