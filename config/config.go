package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/DataDog/datadog-go/statsd"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/coinbase/mongobetween/mongo"
	"github.com/coinbase/mongobetween/proxy"
)

const usernamePlaceholder = "_"
const defaultStatsdAddress = "localhost:8125"

var validNetworks = []string{"tcp", "tcp4", "tcp6", "unix", "unixpacket"}

type Config struct {
	network string
	unlink  bool
	ping    bool
	pretty  bool
	clients []client
	statsd  string
	level   zapcore.Level
	dynamic string
}

type client struct {
	address string
	label   string
	opts    *options.ClientOptions
}

func ParseFlags() *Config {
	config, err := parseFlags()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		flag.Usage()
		os.Exit(2)
	}
	return config
}

func (c *Config) LogLevel() zapcore.Level {
	return c.level
}

func (c *Config) Pretty() bool {
	return c.pretty
}

func (c *Config) Proxies(log *zap.Logger) (proxies []*proxy.Proxy, err error) {
	sd, err := statsd.New(c.statsd, statsd.WithNamespace("mongobetween"))
	if err != nil {
		return nil, err
	}

	d, err := NewDynamic(c.dynamic, log)
	if err != nil {
		return nil, err
	}

	mongos := make(map[string]*mongo.Mongo)
	for _, client := range c.clients {
		m, err := mongo.Connect(log, sd, client.opts, c.ping)
		if err != nil {
			return nil, err
		}
		mongos[client.address] = m
	}
	mongoLookup := func(address string) *mongo.Mongo {
		return mongos[address]
	}

	for _, client := range c.clients {
		p, err := proxy.NewProxy(log, sd, client.label, c.network, client.address, c.unlink, mongoLookup, d)
		if err != nil {
			return nil, err
		}
		proxies = append(proxies, p)
	}
	return
}

func validNetwork(network string) bool {
	for _, n := range validNetworks {
		if n == network {
			return true
		}
	}
	return false
}

func parseFlags() (*Config, error) {
	flag.Usage = func() {
		fmt.Printf("Usage: %s [OPTIONS] address1=uri1 [address2=uri2] ...\n", os.Args[0])
		flag.PrintDefaults()
	}

	var unlink, ping, pretty bool
	var network, username, password, stats, loglevel, dynamic string
	flag.StringVar(&network, "network", "tcp4", "One of: tcp, tcp4, tcp6, unix or unixpacket")
	flag.StringVar(&username, "username", "", "MongoDB username")
	flag.StringVar(&password, "password", "", "MongoDB password")
	flag.StringVar(&stats, "statsd", defaultStatsdAddress, "Statsd address")
	flag.BoolVar(&unlink, "unlink", false, "Unlink existing unix sockets before listening")
	flag.BoolVar(&ping, "ping", false, "Ping downstream MongoDB before listening")
	flag.BoolVar(&pretty, "pretty", false, "Pretty print logging")
	flag.StringVar(&loglevel, "loglevel", "info", "One of: debug, info, warn, error, dpanic, panic, fatal")
	flag.StringVar(&dynamic, "dynamic", "", "URL to query for dynamic configuration")

	flag.Parse()

	network = expandEnv(network)
	username = expandEnv(username)
	password = expandEnv(password)
	stats = expandEnv(stats)
	loglevel = expandEnv(loglevel)
	dynamic = expandEnv(dynamic)

	level := zap.InfoLevel
	if loglevel != "" {
		err := level.Set(loglevel)
		if err != nil {
			return nil, fmt.Errorf("invalid loglevel: %s", loglevel)
		}
	}

	if !validNetwork(network) {
		return nil, fmt.Errorf("invalid network: %s", network)
	}

	addressMap := make(map[string]string)
	for _, arg := range flag.Args() {
		arg = expandEnv(arg)
		all := strings.FieldsFunc(arg, func(r rune) bool {
			return r == '|' || r == '\n'
		})
		for _, v := range all {
			split := strings.SplitN(v, "=", 2)
			if len(split) != 2 {
				return nil, errors.New("malformed address=uri option")
			}
			if _, ok := addressMap[split[0]]; ok {
				return nil, fmt.Errorf("uri already defined for address: %s", split[0])
			}
			addressMap[split[0]] = split[1]
		}
	}

	if len(addressMap) == 0 {
		return nil, errors.New("missing address=uri(s)")
	}

	var clients []client
	for address, uri := range addressMap {
		label, opts, err := clientOptions(uri, username, password)
		if err != nil {
			return nil, err
		}
		clients = append(clients, client{
			address: address,
			label:   label,
			opts:    opts,
		})
	}

	return &Config{
		network: network,
		unlink:  unlink,
		ping:    ping,
		pretty:  pretty,
		clients: clients,
		statsd:  stats,
		level:   level,
		dynamic: dynamic,
	}, nil
}

func expandEnv(config string) string {
	// more restrictive version of os.ExpandEnv that only replaces exact matches of ${ENV}
	return regexp.MustCompile(`\${(\w+)}`).ReplaceAllStringFunc(config, func(s string) string {
		return os.ExpandEnv(s)
	})
}

func clientOptions(uri, username, password string) (string, *options.ClientOptions, error) {
	uri = uriWorkaround(uri, username)

	cs, err := connstring.Parse(uri)
	if err != nil {
		return "", nil, err
	}

	label := ""
	if len(cs.UnknownOptions["label"]) > 0 {
		label = cs.UnknownOptions["label"][0]
	}

	opts := options.Client()
	opts.ApplyURI(uri)

	if username != "" {
		if opts.Auth == nil {
			opts.SetAuth(options.Credential{Username: username})
		} else if opts.Auth.Username == "" || opts.Auth.Username == usernamePlaceholder {
			opts.Auth.Username = username
		}
	}

	if password != "" {
		if opts.Auth == nil {
			opts.SetAuth(options.Credential{Password: password})
		} else if opts.Auth.Password == "" {
			opts.Auth.Password = password
		}
	}

	if err := opts.Validate(); err != nil {
		return "", nil, err
	}

	return label, opts, nil
}

func uriWorkaround(uri, username string) string {
	// Workaround for a feature in the Mongo driver URI parsing where you can't set a URI
	// without setting the username ("error parsing uri: authsource without username is
	// invalid"). This method force-adds a username in the URI, which can be overridden
	// using SetAuth(). This workaround can be removed once the 1.4 driver is released
	// (see https://jira.mongodb.org/browse/GODRIVER-1473).
	if !strings.Contains(uri, "@") && username != "" {
		split := strings.SplitN(uri, "//", 2)
		if len(split) == 2 {
			uri = fmt.Sprintf("%s//%s@%s", split[0], usernamePlaceholder, split[1])
		} else {
			uri = fmt.Sprintf("%s@%s", usernamePlaceholder, split[0])
		}
	}
	return uri
}
