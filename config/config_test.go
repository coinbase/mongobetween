package config

import (
	"flag"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
	"os"
	"testing"
	"time"
)

type statsdWriterWrapper struct{}

func (statsdWriterWrapper) SetWriteTimeout(time.Duration) error {
	return nil
}

func (statsdWriterWrapper) Close() error {
	return nil
}

func (statsdWriterWrapper) Write(p []byte) (n int, err error) {
	return 0, nil
}

func resetFlags() {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
}

func TestParseFlags(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"mongobetween",
		"-pretty",
		"-statsd", "statsd:1234",
		"-loglevel", "debug",
		"-network", "unix",
		"-username", "username",
		"-password", "password",
		"-ping",
		"-unlink",
		"/tmp/mongo1.sock=mongodb://localhost:27127/database?maxpoolsize=5&label=cluster1",
		"/tmp/mongo2.sock=mongodb://localhost:27128/database?maxpoolsize=10&label=cluster2",
	}

	//mock out newStatsdClient
	originalFunc := newStatsdClientInit
	newStatsdClientInit = func(stats string) (*statsd.Client, error) {
		return statsd.NewWithWriter(statsdWriterWrapper{})
	}
	defer func() {
		newStatsdClientInit = originalFunc
	}()

	resetFlags()
	c, err := parseFlags()
	assert.Nil(t, err)

	assert.True(t, c.pretty)
	assert.Equal(t, "statsd:1234", c.statsdaddr)
	assert.Equal(t, zapcore.DebugLevel, c.LogLevel())
	assert.Equal(t, "unix", c.network)
	assert.True(t, c.ping)
	assert.True(t, c.unlink)

	assert.Equal(t, 2, len(c.clients))
	client1 := c.clients[0]
	client2 := c.clients[1]
	if client1.label == "cluster2" {
		temp := client1
		client1 = client2
		client2 = temp
	}

	assert.Equal(t, "cluster1", client1.label)
	assert.Equal(t, []string{"localhost:27127"}, client1.opts.Hosts)
	assert.Equal(t, "username", client1.opts.Auth.Username)
	assert.Equal(t, "password", client1.opts.Auth.Password)
	assert.Equal(t, uint64(5), *client1.opts.MaxPoolSize)

	assert.Equal(t, "cluster2", client2.label)
	assert.Equal(t, []string{"localhost:27128"}, client2.opts.Hosts)
	assert.Equal(t, "username", client2.opts.Auth.Username)
	assert.Equal(t, "password", client2.opts.Auth.Password)
	assert.Equal(t, uint64(10), *client2.opts.MaxPoolSize)
}

func TestEnvExpansion(t *testing.T) {
	env := "TEST_ENV"
	oldEnv := os.Getenv(env)
	defer func() { _ = os.Setenv(env, oldEnv) }()
	_ = os.Setenv(env, "env_value")

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"mongobetween",
		"-password", "before_${TEST_ENV}_after",
		"/tmp/mongo1.sock=mongodb://localhost:27127/database?maxpoolsize=5&label=cluster1",
	}

	resetFlags()
	c, err := parseFlags()
	assert.Nil(t, err)

	assert.Equal(t, "before_env_value_after", c.clients[0].opts.Auth.Password)
}

func TestInvalidLogLevel(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"mongobetween",
		"-loglevel", "wrong",
		"/tmp/mongo1.sock=mongodb://localhost:27127/database?maxpoolsize=5&label=cluster1",
	}

	resetFlags()
	_, err := parseFlags()
	assert.EqualError(t, err, "invalid loglevel: wrong")
}

func TestInvalidNetwork(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"mongobetween",
		"-network", "wrong",
		"/tmp/mongo1.sock=mongodb://localhost:27127/database?maxpoolsize=5&label=cluster1",
	}

	resetFlags()
	_, err := parseFlags()
	assert.EqualError(t, err, "invalid network: wrong")
}

func TestAddressCollision(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"mongobetween",
		"/tmp/mongo1.sock=mongodb://localhost:27127/database?maxpoolsize=5&label=cluster1",
		"/tmp/mongo1.sock=mongodb://localhost:27128/database?maxpoolsize=10&label=cluster2",
	}

	resetFlags()
	_, err := parseFlags()
	assert.EqualError(t, err, "uri already defined for address: /tmp/mongo1.sock")
}

func TestMissingAddresses(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"mongobetween",
	}

	resetFlags()
	_, err := parseFlags()
	assert.EqualError(t, err, "missing address=uri(s)")
}
