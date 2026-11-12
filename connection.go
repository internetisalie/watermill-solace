package solace

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	sdk "solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
)

// ConnectionConfig describes how to reach a Solace message VPN.
type ConnectionConfig struct {
	// Host is the broker address, e.g. "tcp://broker:55555" or "tcps://broker:55443".
	Host string
	// VPN is the message VPN name.
	VPN string

	// Username and Password select basic authentication when Username is set.
	// For any other scheme (client certificate, OAuth, Kerberos) leave these
	// empty and supply the scheme through Properties.
	Username string
	Password string

	// ClientName is the broker-visible client name. Empty lets the SDK generate one.
	ClientName string

	// ReconnectInterval is the wait between reconnection attempts after the
	// connection drops. The connection retries forever. Zero means 3s.
	ReconnectInterval time.Duration

	// Properties are applied last and override anything derived from the
	// fields above. Use them for TLS, other authentication schemes and tuning.
	Properties config.ServicePropertyMap
}

func (c ConnectionConfig) validate() error {
	if c.Host == "" {
		return errors.New("solace: ConnectionConfig.Host is required")
	}
	if c.VPN == "" {
		return errors.New("solace: ConnectionConfig.VPN is required")
	}
	return nil
}

func (c ConnectionConfig) properties() config.ServicePropertyMap {
	props := config.ServicePropertyMap{
		config.TransportLayerPropertyHost: c.Host,
		config.ServicePropertyVPNName:     c.VPN,
		// Lets consumers compute message age from the publisher's clock.
		config.ServicePropertyGenerateSendTimestamps: true,
		// Direct subscriptions are re-added after a reconnect instead of
		// silently going quiet.
		config.ServicePropertyReceiverDirectSubscriptionReapply: true,
	}
	if c.Username != "" {
		props[config.AuthenticationPropertyScheme] = config.AuthenticationSchemeBasic
		props[config.AuthenticationPropertySchemeBasicUserName] = c.Username
		props[config.AuthenticationPropertySchemeBasicPassword] = c.Password
	}
	if c.ClientName != "" {
		props[config.ClientPropertyName] = c.ClientName
	}
	for k, v := range c.Properties {
		props[k] = v
	}
	return props
}

// Connection is one connected Solace MessagingService, shared by publishers
// and subscribers. It is safe for concurrent use.
type Connection struct {
	svc    solace.MessagingService
	logger watermill.LoggerAdapter

	closeOnce sync.Once
	closeErr  error
}

// Connect builds a MessagingService from cfg and connects it, returning when
// the connection is up, the attempt fails, or ctx is done.
//
// If ctx ends first the in-flight attempt is abandoned and disconnected in the
// background.
func Connect(ctx context.Context, cfg ConnectionConfig, logger watermill.LoggerAdapter) (*Connection, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = watermill.NopLogger{}
	}
	interval := cfg.ReconnectInterval
	if interval <= 0 {
		interval = 3 * time.Second
	}

	svc, err := sdk.NewMessagingServiceBuilder().
		FromConfigurationProvider(cfg.properties()).
		WithReconnectionRetryStrategy(config.RetryStrategyForeverRetryWithInterval(interval)).
		Build()
	if err != nil {
		return nil, fmt.Errorf("solace: building messaging service: %w", err)
	}

	select {
	case err := <-svc.ConnectAsync():
		if err != nil {
			return nil, fmt.Errorf("solace: connecting to %s (vpn %q): %w", cfg.Host, cfg.VPN, err)
		}
	case <-ctx.Done():
		go func() { _ = svc.Disconnect() }()
		return nil, fmt.Errorf("solace: connecting to %s (vpn %q): %w", cfg.Host, cfg.VPN, ctx.Err())
	}

	fields := watermill.LogFields{"host": cfg.Host, "vpn": cfg.VPN}
	svc.AddReconnectionAttemptListener(func(e solace.ServiceEvent) {
		logger.Info("Solace connection lost, reconnecting", fields.Add(watermill.LogFields{"cause": e.GetCause()}))
	})
	svc.AddReconnectionListener(func(solace.ServiceEvent) {
		logger.Info("Solace connection re-established", fields)
	})
	logger.Debug("Connected to Solace", fields)

	return &Connection{svc: svc, logger: logger}, nil
}

// Service returns the underlying MessagingService, for anything this package
// does not wrap (request/reply, endpoint provisioning, metrics).
func (c *Connection) Service() solace.MessagingService { return c.svc }

// IsConnected reports whether the service is currently connected. It is false
// while a reconnect is in progress, which makes it a usable readiness signal.
func (c *Connection) IsConnected() bool { return c.svc.IsConnected() }

// Close disconnects. Close publishers and subscribers built on this connection
// first; it is safe to call more than once.
func (c *Connection) Close() error {
	c.closeOnce.Do(func() {
		if err := c.svc.Disconnect(); err != nil {
			c.closeErr = fmt.Errorf("solace: disconnecting: %w", err)
		}
	})
	return c.closeErr
}
