//go:build integration

package solace_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"code.internetisalie.net/containers"
	"github.com/ThreeDotsLabs/watermill"

	solace "code.internetisalie.net/watermill-solace"
)

// brokerImageTag pins the broker the tests were last run against.
const brokerImageTag = "10.26.0"

// brokerHost is the SMF address of the broker started by TestMain, and
// brokerName its container name (for the reconnect test's restart command).
var brokerHost, brokerName string

func TestMain(m *testing.M) {
	os.Exit(runWithBroker(m))
}

func runWithBroker(m *testing.M) int {
	// Bounds pull, create and start as well as readiness.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	broker, err := containers.StartContainerContext(ctx, containers.ContainerConfig{
		Name:       "watermill-solace-test",
		Repository: "solace/solace-pubsub-standard",
		Tag:        brokerImageTag,
		Port:       "55555", // SMF
		Ephemeral:  true,
		AutoRemove: true,
		Env: []string{
			"username_admin_globalaccesslevel=admin",
			"username_admin_password=admin",
			"system_scaling_maxconnectioncount=100",
		},
		// The broker's power-on self test refuses to start with less shared
		// memory or a lower nofile hard limit than these.
		Limits: containers.Limits{
			MemoryBytes: 4 << 30,
			CPUs:        2,
			ShmBytes:    1 << 30,
			Ulimits:     []containers.Ulimit{{Name: "nofile", Soft: 2448, Hard: 1048576}},
		},
		Timeout: 3 * time.Minute,
		// The SMF port accepts TCP well before the message VPN is ready, so
		// readiness is a real client connection, not a dial.
		Ready: func(address string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := solace.Connect(ctx, connConfig("tcp://"+address), nil)
			if err != nil {
				return err
			}
			return conn.Close()
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "starting Solace broker: %v\n", err)
		return 1
	}
	defer func() {
		if err := broker.Purge(); err != nil {
			fmt.Fprintf(os.Stderr, "purging Solace broker: %v\n", err)
		}
	}()

	brokerHost = "tcp://" + broker.HostPort()
	brokerName = broker.Name()
	return m.Run()
}

func connConfig(host string) solace.ConnectionConfig {
	return solace.ConnectionConfig{
		Host:     host,
		VPN:      "default",
		Username: "default",
		Password: "default",
	}
}

// connect opens a connection to the test broker, closed when t ends.
func connect(t *testing.T) *solace.Connection {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := solace.Connect(ctx, connConfig(brokerHost), logger())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close connection: %v", err)
		}
	})
	return conn
}

func logger() watermill.LoggerAdapter {
	if os.Getenv("SOLACE_TEST_DEBUG") != "" {
		return watermill.NewStdLogger(true, true)
	}
	return watermill.NopLogger{}
}
