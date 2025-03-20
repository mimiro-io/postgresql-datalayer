package conf

import (
	"context"
	"github.com/DataDog/datadog-go/v5/statsd"
	"go.uber.org/fx"
)

func NewStatsd(lc fx.Lifecycle, env *Env) (statsd.ClientInterface, error) {
	var client statsd.ClientInterface
	agentEndpoint := env.AgentHost
	service := env.ServiceName
	if agentEndpoint != "" {
		opt := statsd.WithNamespace(service)
		env.Logger.Info("Statsd is configured on: ", agentEndpoint)
		c, err := statsd.New(agentEndpoint, opt)
		if err != nil {
			return nil, err
		}
		client = c
	} else {
		env.Logger.Debug("Using NoOp statsd client")
		client = &statsd.NoOpClient{}
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			env.Logger.Infof("Flushing statsd")
			return client.Flush()
		},
	})

	return client, nil
}
