package mongo

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/DataDog/datadog-go/statsd"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
	"go.uber.org/zap"

	"github.com/coinbase/mongobetween/util"
)

const pingTimeout = 60 * time.Second
const disconnectTimeout = 10 * time.Second

type Mongo struct {
	log    *zap.Logger
	statsd *statsd.Client
	opts   *options.ClientOptions

	mu       sync.RWMutex
	client   *mongo.Client
	topology *topology.Topology
	cursors  *cursorCache

	roundTripCtx    context.Context
	roundTripCancel func()
}

func extractTopology(c *mongo.Client) *topology.Topology {
	e := reflect.ValueOf(c).Elem()
	d := e.FieldByName("deployment")
	d = reflect.NewAt(d.Type(), unsafe.Pointer(d.UnsafeAddr())).Elem() // #nosec G103
	return d.Interface().(*topology.Topology)
}

func Connect(log *zap.Logger, sd *statsd.Client, opts *options.ClientOptions, ping bool) (*Mongo, error) {
	// timeout shouldn't be hit if ping == false, as Connect doesn't block the current goroutine
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()

	opts = opts.SetPoolMonitor(poolMonitor(sd))

	var err error
	log.Info("Connect")
	c, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}

	if ping {
		log.Info("Ping")
		err = c.Ping(ctx, readpref.Primary())
		if err != nil {
			return nil, err
		}
	}

	t := extractTopology(c)
	go topologyMonitor(log, t)

	rtCtx, rtCancel := context.WithCancel(context.Background())
	m := Mongo{
		log:             log,
		statsd:          sd,
		opts:            opts,
		client:          c,
		topology:        t,
		cursors:         newCursorCache(),
		roundTripCtx:    rtCtx,
		roundTripCancel: rtCancel,
	}
	go m.cursorMonitor()

	return &m, nil
}

func poolMonitor(sd *statsd.Client) *event.PoolMonitor {
	checkedOut, checkedIn := util.StatsdBackgroundGauge(sd, "pool.checked_out_connections", []string{})
	opened, closed := util.StatsdBackgroundGauge(sd, "pool.open_connections", []string{})

	return &event.PoolMonitor{
		Event: func(e *event.PoolEvent) {
			snake := strings.ToLower(regexp.MustCompile("([a-z0-9])([A-Z])").ReplaceAllString(e.Type, "${1}_${2}"))
			name := fmt.Sprintf("pool_event.%s", snake)
			tags := []string{
				fmt.Sprintf("address:%s", e.Address),
				fmt.Sprintf("reason:%s", e.Reason),
			}
			switch e.Type {
			case event.ConnectionCreated:
				opened(name, tags)
			case event.ConnectionClosed:
				closed(name, tags)
			case event.GetSucceeded:
				checkedOut(name, tags)
			case event.ConnectionReturned:
				checkedIn(name, tags)
			default:
				_ = sd.Incr(name, tags, 1)
			}
		},
	}
}

func (m *Mongo) Description() description.Topology {
	return m.topology.Description()
}

func (m *Mongo) cursorMonitor() {
	for {
		_ = m.statsd.Gauge("cursors", float64(m.cursors.count()), []string{}, 1)
		time.Sleep(1 * time.Second)
	}
}

func (m *Mongo) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client == nil {
		// already closed
		return
	}

	m.roundTripCancel()

	m.log.Info("Disconnect")
	ctx, cancel := context.WithTimeout(context.Background(), disconnectTimeout)
	defer cancel()
	err := m.client.Disconnect(ctx)
	m.client = nil
	if err != nil {
		m.log.Info("Error disconnecting", zap.Error(err))
	}
}

func (m *Mongo) RoundTrip(msg *Message) (*Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.client == nil {
		return nil, errors.New("connection closed")
	}

	// FIXME this assumes that cursorIDs are unique on the cluster, but two servers can have the same cursorID reference different cursors
	requestCursorID, _ := msg.Op.CursorID()
	server, err := m.selectServer(requestCursorID)
	if err != nil {
		return nil, err
	}

	// FIXME transactions should be pinned to servers, similar to cursors above

	conn, err := m.checkoutConnection(server)
	if err != nil {
		return nil, err
	}

	defer func() {
		err := conn.Close()
		if err != nil {
			m.log.Error("Error closing Mongo connection", zap.Error(err))
		}
	}()

	// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.4/x/mongo/driver/operation.go#L369-L371
	ep, ok := server.(driver.ErrorProcessor)
	if !ok {
		return nil, errors.New("server ErrorProcessor type assertion failed")
	}

	var wm []byte
	if msg.Op.Unacknowledged() {
		err = m.unacknowledgedRoundTrip(conn, msg.Wm)
	} else {
		wm, err = m.roundTrip(conn, msg.Wm)
	}
	if err != nil {
		ep.ProcessError(err)
		return nil, err
	}
	if msg.Op.Unacknowledged() {
		return &Message{}, nil
	}

	op, err := Decode(wm)
	if err != nil {
		return nil, err
	}

	// check if an error is returned in the server response
	opErr := op.Error()
	if opErr != nil {
		// process the error, but don't return it as we still want to forward the response to the client
		ep.ProcessError(opErr)
	}

	if responseCursorID, ok := op.CursorID(); ok {
		if responseCursorID != 0 {
			m.cursors.add(responseCursorID, server)
		} else if requestCursorID != 0 {
			m.cursors.remove(requestCursorID)
		}
	}

	return &Message{
		Wm: wm,
		Op: op,
	}, nil
}

func (m *Mongo) selectServer(requestCursorID int64) (server driver.Server, err error) {
	defer func(start time.Time) {
		_ = m.statsd.Timing("server_selection", time.Since(start), []string{fmt.Sprintf("success:%v", err == nil)}, 1)
	}(time.Now())

	if requestCursorID != 0 {
		server, ok := m.cursors.peek(requestCursorID)
		if ok {
			return server, nil
		}
	}

	selector := description.CompositeSelector([]description.ServerSelector{
		description.ReadPrefSelector(readpref.Primary()),   // ignored by sharded clusters
		description.LatencySelector(15 * time.Millisecond), // default localThreshold for the client
	})
	return m.topology.SelectServer(m.roundTripCtx, selector)
}

func (m *Mongo) checkoutConnection(server driver.Server) (conn driver.Connection, err error) {
	defer func(start time.Time) {
		address := ""
		if conn != nil {
			address = conn.Address().String()
		}
		_ = m.statsd.Timing("checkout_connection", time.Since(start), []string{
			fmt.Sprintf("address:%s", address),
			fmt.Sprintf("success:%v", err == nil),
		}, 1)
	}(time.Now())

	conn, err = server.Connection(m.roundTripCtx)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (m *Mongo) unacknowledgedRoundTrip(conn driver.Connection, req []byte) (err error) {
	if err = conn.WriteWireMessage(m.roundTripCtx, req); err != nil {
		return wrapNetworkError(err)
	}

	return nil
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.4/x/mongo/driver/operation.go#L532-L561
func (m *Mongo) roundTrip(conn driver.Connection, req []byte) (res []byte, err error) {
	defer func(start time.Time) {
		addressTag := fmt.Sprintf("address:%s", conn.Address().String())
		_ = m.statsd.Distribution("request_size", float64(len(req)), []string{addressTag}, 1)
		if err == nil {
			_ = m.statsd.Distribution("response_size", float64(len(res)), []string{addressTag}, 1)
		}
		_ = m.statsd.Timing("round_trip", time.Since(start), []string{addressTag, fmt.Sprintf("success:%v", err == nil)}, 1)
	}(time.Now())

	if err = conn.WriteWireMessage(m.roundTripCtx, req); err != nil {
		return nil, wrapNetworkError(err)
	}

	if res, err = conn.ReadWireMessage(m.roundTripCtx, req[:0]); err != nil {
		return nil, wrapNetworkError(err)
	}

	return res, nil
}

func wrapNetworkError(err error) error {
	labels := []string{driver.NetworkError}
	return driver.Error{Message: err.Error(), Labels: labels, Wrapped: err}
}
