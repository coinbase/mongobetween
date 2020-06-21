package mongo

import (
	"context"
	"errors"
	"fmt"
	"net"
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

	ctx    context.Context
	cancel func()

	serverCtxMap serverContextMap
}

func extractTopology(c *mongo.Client) *topology.Topology {
	e := reflect.ValueOf(c).Elem()
	d := e.FieldByName("deployment")
	d = reflect.NewAt(d.Type(), unsafe.Pointer(d.UnsafeAddr())).Elem() // #nosec G103
	return d.Interface().(*topology.Topology)
}

func extractConnection(c driver.Connection) net.Conn {
	e := reflect.ValueOf(c).Elem()
	d := e.FieldByName("nc")
	if !d.IsValid() {
		return nil
	}
	d = reflect.NewAt(d.Type(), unsafe.Pointer(d.UnsafeAddr())).Elem() // #nosec G103
	conn, _ := d.Interface().(net.Conn)
	return conn
}

func Connect(log *zap.Logger, sd *statsd.Client, opts *options.ClientOptions, ping bool) (*Mongo, error) {
	// timeout shouldn't be hit if ping == false, as Connect doesn't block the current goroutine
	connectCtx, connectCancel := context.WithTimeout(context.Background(), pingTimeout)
	defer connectCancel()

	opts = opts.SetPoolMonitor(poolMonitor(sd))

	var err error
	log.Info("Connect")
	c, err := mongo.Connect(connectCtx, opts)
	if err != nil {
		return nil, err
	}

	if ping {
		log.Info("Ping")
		err = c.Ping(connectCtx, readpref.Primary())
		if err != nil {
			return nil, err
		}
	}

	t := extractTopology(c)
	go topologyMonitor(log, t)

	ctx, cancel := context.WithCancel(context.Background())
	m := Mongo{
		log:          log,
		statsd:       sd,
		opts:         opts,
		client:       c,
		topology:     t,
		cursors:      newCursorCache(),
		ctx:          ctx,
		cancel:       cancel,
		serverCtxMap: newServerContextMap(),
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

	m.cancel()

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
	server, err := m.selectServer(m.ctx, requestCursorID)
	if err != nil {
		return nil, err
	}

	// FIXME transactions should be pinned to servers, similar to cursors above

	wm, err := m.serverRoundTrip(server, msg.Wm, msg.Op.Unacknowledged())
	if err != nil {
		// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.4/x/mongo/driver/operation.go#L369-L371
		server.ProcessError(err)
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
		server.ProcessError(opErr)
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

func (m *Mongo) selectServer(ctx context.Context, requestCursorID int64) (server *topology.SelectedServer, err error) {
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

	s, err := m.topology.SelectServer(ctx, selector)
	if err != nil {
		return nil, err
	}

	ts, ok := s.(*topology.SelectedServer)
	if !ok {
		return nil, errors.New("server type assertion failed")
	}
	return ts, nil
}

func (m *Mongo) serverRoundTrip(server *topology.SelectedServer, req []byte, unacknowledged bool) (res []byte, err error) {
	ctx, err := m.serverCtxMap.get(m.ctx, server)
	if err != nil {
		return nil, err
	}

	conn, err := m.checkoutConnection(ctx, server)
	if err != nil {
		return nil, err
	}

	defer func() {
		err := conn.Close()
		if err != nil {
			m.log.Error("Error closing Mongo connection", zap.Error(err))
		}
	}()

	complete := make(chan struct{})
	defer close(complete)

	go func() {
		select {
		case <-ctx.Done():
			select {
			case <-complete:
				// pretend we didn't notice the cancelation
			default:
				if ctx.Err() == context.Canceled && server.Server.Description().Kind == description.Unknown {
					m.timeoutConnection(conn)
				}
			}
		case <-complete:
		}
	}()

	return m.roundTrip(ctx, conn, req, unacknowledged)
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.4/x/mongo/driver/operation.go#L532-L561
func (m *Mongo) roundTrip(ctx context.Context, conn driver.Connection, req []byte, unacknowledged bool) (res []byte, err error) {
	defer func(start time.Time) {
		tags := []string{
			fmt.Sprintf("address:%s", conn.Address().String()),
			fmt.Sprintf("unacknowledged:%v", unacknowledged),
		}

		_ = m.statsd.Distribution("request_size", float64(len(req)), tags, 1)
		if err == nil && !unacknowledged {
			// There is no response size for unacknowledged writes.
			_ = m.statsd.Distribution("response_size", float64(len(res)), tags, 1)
		}

		roundTripTags := append(tags, fmt.Sprintf("success:%v", err == nil))
		_ = m.statsd.Timing("round_trip", time.Since(start), roundTripTags, 1)
	}(time.Now())

	if err = conn.WriteWireMessage(ctx, req); err != nil {
		return nil, wrapNetworkError(err)
	}

	if unacknowledged {
		return nil, nil
	}

	if res, err = conn.ReadWireMessage(ctx, req[:0]); err != nil {
		return nil, wrapNetworkError(err)
	}

	return res, nil
}

func (m *Mongo) checkoutConnection(ctx context.Context, server *topology.SelectedServer) (conn driver.Connection, err error) {
	defer func(start time.Time) {
		addr := ""
		if conn != nil {
			addr = conn.Address().String()
		}
		_ = m.statsd.Timing("checkout_connection", time.Since(start), []string{
			fmt.Sprintf("address:%s", addr),
			fmt.Sprintf("success:%v", err == nil),
		}, 1)
	}(time.Now())

	return server.Connection(ctx)
}

func (m *Mongo) timeoutConnection(conn driver.Connection) {
	nc := extractConnection(conn)
	if nc != nil {
		// immediately timeout the connection
		err := nc.SetDeadline(time.Now())
		if err != nil {
			m.log.Warn("Error timing out connection", zap.Error(err))
		} else {
			m.log.Info("Force time out")
		}
	} else {
		m.log.Warn("Error extracting net.Conn from connection")
	}
}

func wrapNetworkError(err error) error {
	labels := []string{driver.NetworkError}
	return driver.Error{Message: err.Error(), Labels: labels, Wrapped: err}
}
